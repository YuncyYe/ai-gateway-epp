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

**fail-static 原则**：`BACKOFF` 期间一切消费行为不变——数据不清理、已持有 cell 不释放，只退避重试。

## 2. innerapi client

```go
package innerapi

type Client struct { baseURL, token string; http *http.Client /* 连接池 */ }

// Get 解析统一信封 {ErrNum, ErrMsg, Data{Version, Config}, WorkMode}；
// Data 为 null 时返回 changed=false。
func (c *Client) Get(ctx context.Context, path string, version string, out any) (changed bool, newVersion string, err error)
```

两个 poller 共享一个 client 实例；鉴权头 `Authorization: Token <token>`（与 BFE 拉取 InnerAPI 同款）。

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

## 4. EppDataWatcher（数据源：epp_data/config）

**Source**：`GET /configs/epp_data/config?version=` → 单 topic 单 version 快照，Config 含两段（契约见《EPP配置定义说明-epp_config.md》，`docs/zh_cn/configuration/`）：

```go
type EppDataConfig struct {
    EppConfig  map[string]json.RawMessage `json:"epp_config"`  // cluster → api 编译后的完整 EndpointPickerConfig
    Assignment map[string]AssignmentEntry `json:"assignment"`  // cluster → {group} 分配的实例组，所有实例相同
}

type AssignmentEntry struct {
    Group string `json:"group"`  // 分配的 EPP 实例组名（/epp-pool 中的组名）；空视为异常（跳过）
}
```

原 `GET /configs/epp_data/picker_config`（ConfigPoller）与 `GET /configs/epp_data/assignment?instance=`（AssignmentWatcher）两个 Source 合并为本 Watcher——一次拉取、一次 version、两段天然同快照，消除"配置已更新但角色未变"（或反之）的跨端点偏移窗口。数据量极小，全量重发代价可忽略。

**自匹配（纯函数，单测覆盖全分支）**：

```go
// resolveAssignmentEntry 报告该 cluster 是否归本实例：分配的组 == 本实例组（w.eppgroup）。
// 无角色概念：命中即服务，未命中即无。
func resolveAssignmentEntry(self string, e AssignmentEntry) (mine bool) {
    return e.Group != "" && e.Group == self
}
```

**Handle（两段同快照应用）**：

```
epp_config 段（per-cell 编译切换，算法见详细设计-02 §4）：
取 本实例命中的 cluster ∩ EppConfig
for cluster, cfg:
    hash = sha256(canonical(cfg))
    if hash == cell.currentHash: continue
    cell.SwapEngine 路径（见详细设计-02 §4 编译切换算法）

assignment 段（cluster→group 视图 → 本实例命中集合 → diff 驱动 cell）：
fetch → 期望集合 = { cluster | resolveAssignmentEntry(本实例组名, entry) 命中 }
for cluster in 期望:
    cell = cells.Get(cluster)
    无 → cells.Ensure(cluster)        // 幂等
for cell in cells 不在期望中 → cells.Drop(cluster)
```

- **同快照一致性**：epp_config 段与 assignment 段在同一 version 快照内一起应用，命中与配置不会错配。
- **一致性规则**：cluster 出现在 epp_config 但 assignment 未命中本实例组（或条目 group 为空）→ 跳过 + 指标告警（Cell 就绪需双源齐备）。
- **未命中任何分配**（`-epp-group` 与 `/epp-pool` 组名不一致）：全部 cluster 跳过，EPP 无 cell、不服务；启动日志与指标告警 `ai_epp_assignment_no_match`，便于部署排查。
- **无 peer 概念**：单实例组恒无对端，原 `peerID`/"assignment peer" 日志已删除。
- **无上报义务**：EPP 不注册、不心跳、不就绪上报——failover 由 BFE 侧 EPPAddr 连接滞回驱动，api 侧不"等就绪再翻转"；原 `POST /configs/epp_data/assignment/report`（ReadyReport）已随 api 侧端点取消同步删除。

## 5. 失败矩阵

| 故障 | 行为 |
|---|---|
| API 5xx/网络错 | 指数退避（1s→2s→…→max 30s），指标 poller_failures_total / poller_backoff_state |
| 首拉未成功 | server 不就绪（gRPC health 非 SERVING） |
| 持续不可用 | fail-static：全部沿用最后已知状态（NFR-4：30 分钟无劣化） |
| 数据形状错误（契约破坏） | 单轮丢弃 + error 指标，不等 version 前进（防脏数据固化） |
| 本实例组名未命中 assignment 任何条目 | 全部 cluster 跳过、无 cell 不服务；`ai_epp_assignment_no_match` 告警（部署核对 `-epp-group` 与 `/epp-pool` 组名） |
