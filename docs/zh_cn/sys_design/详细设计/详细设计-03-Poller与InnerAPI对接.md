# 详细设计-03：Poller 与 InnerAPI 对接

## 1. 通用轮询框架

```go
package poller

// Source 由各具体 poller 实现：Fetch 返回 (changed, newVersion, payload)。
type Source[T any] interface {
    Fetch(ctx context.Context, version string) (changed bool, ver string, data T, err error)
}

// Handle 是变更回调；实现方保证处理失败不中断轮询。
type Handle[T any] func(ctx context.Context, data T) error

type Poller[T any] struct { /* ticker、backoff、lastVersion、lastSync、failures 指标 */ }

func New[T any](src Source[T], h Handle[T], interval, timeout time.Duration) *Poller[T]
func (p *Poller[T]) Start(ctx context.Context) error // 阻塞运行；首拉成功前 server 不就绪
```

**状态机**：`INIT → SYNCING ⇄ BACKOFF(err 指数退避) → SYNCED（常态 tick）`

**fail-static 原则**：`BACKOFF` 期间一切消费行为不变——数据不清理、角色不回退，只退避重试。

## 2. innerapi client

```go
package innerapi

type Client struct { baseURL, token string; http *http.Client /* 连接池 */ }

// Get 解析统一信封 {ErrNum, ErrMsg, Data{Version, Config}, WorkMode}；
// Data 为 null 时返回 changed=false。
func (c *Client) Get(ctx context.Context, path string, version string, out any) (changed bool, newVersion string, err error)
```

三 poller 共享一个 client 实例；鉴权头 `Authorization: Token <token>`（与 BFE 拉取 InnerAPI 同款）。

## 3. ClusterDiscovery（数据源：cluster_table）

**Source**：`GET /configs/gslb_data/cluster_table?version=` → `map[cluster]map[subCluster][]BackendConf`

**Handle（per-cluster diff 算法）**：

```
fetch 全量 Config[cluster][subCluster][]BackendConf
for cluster, backends in 全量:
    目标集合 = { e.ID : EndpointMetadata{...} | e.Weight != 0 }   // Weight=0 视为摘除（需求 §3.1.1）
    cell = cells.Get(cluster)；无 cell（未分配）→ skip + count
    既有集合 = cell.ds 快照键集
    Upsert: 目标 - 既有；Delete: 既有 - 目标；变更（EndpointMetadata.Equal 为 false）: Upsert
    顺序保证：同 key 的 Delete 先于 Upsert（先删旧再插新），全部经单一 goroutine 串行下发
              （llm-d DiscoveryNotifier 的顺序契约，discovery.go:86-94）
```

职责划分注记：插件接口适配在 `eppplugin/clustertable`（Cell 内），本 poller 只负责取数与向 Manager/Cell 分发；diff 的落点（经 discovery 插件的 notifier 还是直接调 datastore）在 spike 中定，原则：**插件接口零侵入复用**。

## 4. ConfigPoller（数据源：picker_config）

**Source**：`GET /configs/epp_data/picker_config?version=` → `map[cluster]EndpointPickerConfig`（契约见《EPP配置定义说明-picker_config.md》，`docs/zh_cn/configuration/`）

**Handle**：

```
取 本实例分配 ∩ Config
for cluster, cfg:
    hash = sha256(canonical(cfg))
    if hash == cell.currentHash: continue
    cell.SwapEngine 路径（见详细设计-02 §4 编译切换算法）
```

一致性规则：cluster 出现在 picker_config 但不在 assignment → 跳过 + 指标告警（Cell 就绪需双源齐备）。

## 5. AssignmentWatcher（数据源：assignment）

**Source**：`GET /configs/epp_data/assignment?instance=<id>` → 本实例角色视图 `{cluster: role}` + 上报通道

**Handle**：

```
fetch → 期望集合 {cluster: role}
for cluster, role in 期望:
    cell = cells.Get(cluster)
    无 → cells.Ensure(cluster, standby)   // 一律先备，等 promote（配合"先同步后翻转"）
    有且 role 变化 → Promote/Demote（角色矩阵，详细设计-02 §2）
for cell in cells 不在期望中 → cells.Drop(cluster)
```

**就绪上报**（配合 ai-gateway-api 再平衡顺序约束）：每轮上报

```
{ instance, cells: [{key, role, state, ready_since, engine_version}] }
```

`ready` 的定义：discovery 首同步完成 **且** engine 首编译成功——即 Cell 生命周期图中 CREATING→STANDBY 的跃迁点。

## 6. 失败矩阵

| 故障 | 行为 |
|---|---|
| API 5xx/网络错 | 指数退避（1s→2s→…→max 30s），指标 poller_failures_total / poller_backoff_state |
| 首拉未成功 | server 不就绪（gRPC health 非 SERVING） |
| 持续不可用 | fail-static：全部沿用最后已知状态（NFR-4：30 分钟无劣化） |
| 数据形状错误（契约破坏） | 单轮丢弃 + error 指标，不等 version 前进（防脏数据固化） |
