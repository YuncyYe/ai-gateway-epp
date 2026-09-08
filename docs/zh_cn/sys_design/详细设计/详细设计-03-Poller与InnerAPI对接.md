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
    Assignment map[string]AssignmentEntry `json:"assignment"`  // cluster → {primary, standby} 全量视图，所有实例相同
}

type AssignmentEntry struct {
    Primary *string `json:"primary"`  // 实例 id；null 视为异常（跳过）
    Standby *string `json:"standby"`  // 实例 id；单实例组为 null
}
```

原 `GET /configs/epp_data/picker_config`（ConfigPoller）与 `GET /configs/epp_data/assignment?instance=`（AssignmentWatcher）两个 Source 合并为本 Watcher——一次拉取、一次 version、两段天然同快照，消除"配置已更新但角色未变"（或反之）的跨端点偏移窗口。数据量极小，全量重发代价可忽略。

**自匹配（纯函数，单测覆盖全分支）**：

```go
// resolveRole 返回本实例在 cluster 的角色。
func resolveRole(self string, e AssignmentEntry) (role cell.Role, mine bool) {
    if e.Primary != nil && *e.Primary == self { return cell.RolePrimary, true }
    if e.Standby != nil && *e.Standby == self { return cell.RoleStandby, true }
    return cell.RoleNone, false
}
```

**Handle（两段同快照应用）**：

```
epp_config 段（per-cell 编译切换，算法见详细设计-02 §4）：
取 本实例有角色的 cluster ∩ EppConfig
for cluster, cfg:
    hash = sha256(canonical(cfg))
    if hash == cell.currentHash: continue
    cell.SwapEngine 路径（见详细设计-02 §4 编译切换算法）

assignment 段（全量视图 → 本地角色视图 → diff 驱动 cell）：
fetch → 期望集合 = { cluster: role | resolveRole(本实例 id, entry) 命中 }
for cluster, role in 期望:
    cell = cells.Get(cluster)
    无 → cells.Ensure(cluster, role)
    有且 role 变化 → Promote/Demote（角色矩阵，详细设计-02 §2）
for cell in cells 不在期望中 → cells.Drop(cluster)
```

- **同快照一致性**：epp_config 段与 assignment 段在同一 version 快照内一起应用（先配置后角色，或反之均可），角色与配置不会错配。
- **一致性规则**：cluster 出现在 epp_config 但 assignment 无本实例角色（或条目缺 primary）→ 跳过 + 指标告警（Cell 就绪需双源齐备）。
- **未命中任何角色**（`-instance-id` 与 `/epp-pool` 不一致）：全部 cluster 跳过，EPP 无 cell、不服务；启动日志与指标告警 `epp_assignment_no_match`，便于部署排查。
- **peer 观测**：AssignmentEntry 含同组另一端实例 id（primary 视角的 standby、反之亦然），本期仅输出日志/指标（`epp_assignment_peer`），供未来备 Cell 建联/双活跃自检使用，不建任何连接。
- **无上报义务**：EPP 不注册、不心跳、不就绪上报——failover 由 BFE 侧 EPPAddr 连接滞回驱动，api 侧不"等就绪再翻转"；原 `POST /configs/epp_data/assignment/report`（ReadyReport）已随 api 侧端点取消同步删除。

## 5. 失败矩阵

| 故障 | 行为 |
|---|---|
| API 5xx/网络错 | 指数退避（1s→2s→…→max 30s），指标 poller_failures_total / poller_backoff_state |
| 首拉未成功 | server 不就绪（gRPC health 非 SERVING） |
| 持续不可用 | fail-static：全部沿用最后已知状态（NFR-4：30 分钟无劣化） |
| 数据形状错误（契约破坏） | 单轮丢弃 + error 指标，不等 version 前进（防脏数据固化） |
| 本实例 id 未命中 assignment 任何角色 | 全部 cluster 跳过、无 cell 不服务；`epp_assignment_no_match` 告警（部署核对 `-instance-id` 与 `/epp-pool`） |
