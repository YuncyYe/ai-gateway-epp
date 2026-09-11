# EPP 调度对接（ai-gateway-epp 侧改造）设计变更

对应控制面方案：`ai-gateway-api/design-docs/modifications/2026-09-08-epp-scheduling-integration/`。本文只描述 ai-gateway-epp 仓库内的改造。

## 1. 现状与目标态差异

| 维度 | 现状 | 目标态 |
|------|------|--------|
| epp_data 拉取 | 2 个端点：`GET /configs/epp_data/picker_config`（config poller）、`GET /configs/epp_data/assignment?instance=<id>`（assignment watcher） | 单端点 `GET /configs/epp_data/config`，Config 含 `epp_config` + `assignment` 两段，单 version 快照 |
| assignment 形态 | 服务端按 `?instance=` 过滤的 per-instance 视图 `{cluster → role}` | 全量视图 `{cluster → {primary, standby}}`，所有实例相同，EPP 本地自匹配 |
| 上报 | 每轮 assignment 变更后 `POST /configs/epp_data/assignment/report`（ReadyReport：instance + cells 状态/ready_since/engine_version） | 删除（api 侧端点已取消，failover 由 BFE 滞回驱动） |
| 配置内容 | picker_config 段为 cluster → 原始 JSON（EPP 以 llm-d loader 加载构造 engine） | epp_config 段为 cluster → api 编译后的完整 `EndpointPickerConfig`（加载路径不变） |
| cluster_table | `GET /configs/gslb_data/cluster_table` | **不变** |
| 轮询框架 | `Source[T]` + version 增量 + backoff + fail-static（`pkg/poller/poller.go`） | **复用** |

关键代码锚点（现状）：

- 轮询框架：`pkg/poller/poller.go:55-57`（`Source[T]`）、`:103-144`（Start 循环）
- HTTP 客户端/envelope：`pkg/innerapi/client.go:33-46`（`{ErrNum, ErrMsg, Data, WorkMode}`，`Data==null` = 无变更，`?version=` 增量），轮询间隔/超时 `-poll-interval`(5s)/`-poll-timeout`(3s)（`cmd/epp/config.go:60-61`）
- picker_config Source：`pkg/poller/config.go:26`（`PickerConfigPath`）、`:41-48`（`map[string]json.RawMessage`）、`:53-66`（Handle → `Manager.ApplyConfig`）
- assignment Source：`pkg/poller/assignment.go:29,51`（带 `?instance=`）、`:64-97`（Handle → `manager.Ensure/Drop`）
- ReadyReport 上报：`pkg/poller/assignment.go:31-32,90-119`；结构 `pkg/assignment/types.go:32-46`；client `pkg/innerapi/client.go:128-150`
- cell 生命周期：`pkg/cell/manager.go`（Ensure `:155-171`、Promote/Demote `:189-213`、Drop/drain `:216-294`）；角色 `pkg/cell/role.go:24-56`
- engine 热交换：`pkg/cell/manager.go:233-294`（hash 比对 → 新 engine 编译 → `Swap` 原子换指针 → 旧 engine drain（默认 60s）后销毁；编译失败 per-cluster 隔离沿用旧引擎）
- standby 不服务：`pkg/demux/server.go:79-81`（非 Primary 返回 `ErrCellDraining`）
- 插件/feature gate：`cmd/epp/plugins.go:74`（`flowcontrol.FeatureGate` 已注册、默认关）、`:77-114`（prefix/session scorer、flowcontrol 插件族、approx-prefix-cache 默认 producer 等均已注册）
- 实例 id：`cmd/epp/config.go:59,76-83`（`-instance-id` / `AI_GATEWAY_EPP_INSTANCE_ID`，缺省 hostname）。部署约定：StatefulSet 部署、不传参，实例 id = pod hostname，`/epp-pool` 登记同名

## 2. 改造点明细

### 2.1 新增 epp_data/config 消费端点（pkg/innerapi + pkg/poller）

- `pkg/innerapi/client.go`：新增/改造 `Config` 结构为两段形态——

  ```go
  type EppDataConfig struct {
      EppConfig  map[string]json.RawMessage                    `json:"epp_config"`  // cluster → 编译后 EndpointPickerConfig
      Assignment map[string]AssignmentEntry                    `json:"assignment"`  // cluster → {primary, standby}
  }
  type AssignmentEntry struct {
      Primary *string `json:"primary"`  // 实例 id；null 视为异常（跳过）
      Standby *string `json:"standby"`  // 实例 id；单实例组为 null
  }
  ```

  路径常量 `EppDataConfigPath = "/configs/epp_data/config"`。envelope、`?version=` 增量、`Data==null`、Token 鉴权全部复用现有 client。
- `pkg/poller/`：新增 `EppDataWatcher`（实现 `Source[EppDataConfig]`），取代现有 `ConfigPoller`（`config.go`）与 `AssignmentWatcher`（`assignment.go`）两个 Source；`cmd/epp/main.go:76-85` 组装处三 poller 改为二（cluster_table + epp_data/config）。
- 旧路径常量（`PickerConfigPath`、`assignment?instance=` 路径）与对应 Handle 逻辑删除。

### 2.2 assignment 全量视图自匹配（pkg/poller + pkg/assignment）

- 新增本实例角色解析（纯函数，单测覆盖）：

  ```go
  // resolveRole 返回本实例在 cluster 的角色。
  func resolveRole(self string, e AssignmentEntry) (role cell.Role, mine bool) {
      if e.Primary != nil && *e.Primary == self { return cell.RolePrimary, true }
      if e.Standby != nil && *e.Standby == self { return cell.RoleStandby, true }
      return cell.RoleNone, false
  }
  ```

- `EppDataWatcher.Handle`：对全量 assignment 逐 cluster 解析出**本地角色视图**（与现状 per-instance 视图同形：`map[cluster]Role`），与现有 cell 集合 diff 后驱动 `Manager.Ensure / Promote / Demote / Drop`：
  - 新 cluster 且本实例有角色 → `Ensure(cluster, role)`；
  - 角色变更（primary↔standby）→ `Promote` / `Demote`（保留数据面热切换语义）；
  - 本实例角色消失（cluster 删除 / 改派他实例 / cluster 退出 EPP）→ `Drop`。
- **epp_config 段与 assignment 段在同一 version 快照内处理**：先应用配置（`ApplyConfig`）再应用角色（或反之均可，因为同一快照天然一致），消除旧双端点下两段错配的窗口。
- peer 信息：`AssignmentEntry` 含同组另一端实例 id（primary 视角的 standby、standby 视角的 primary），本期仅输出日志/指标（`epp_assignment_peer`），供未来备 Cell 建联/双活跃自检使用，不建任何连接。
- 未命中任何角色的实例（如 `-instance-id` 与 `/epp-pool` 不一致）：全部 cluster 跳过，EPP 无 cell、不服务；启动日志与指标中告警（`epp_assignment_no_match`），便于部署排查。

### 2.3 删除 ReadyReport 上报

- 删除 `pkg/poller/assignment.go:90-119` 的上报循环、`pkg/assignment/types.go:32-46` 的 `ReadyReport` 结构、`pkg/innerapi/client.go:128-150` 的 report 方法及路径常量。
- 理由：api 侧 allocator 不再"等就绪再翻转"（failover 由 BFE 侧 EPPAddr 连接滞回驱动），上报无人消费；保留会造成"通道健康"假象。
- 测试调整：SC03（主备角色切换）/ SC07 中断言 report 请求 mock 的用例，改为断言**未发起 report 请求**且角色切换行为不变。

### 2.4 epp_config 段消费（pkg/cell 编译路径）

- epp_config 段与现状 picker_config 段同为 `map[cluster]json.RawMessage`（内容即完整 `EndpointPickerConfig` JSON），`Manager.ApplyConfig` 的加载/热交换路径（`pkg/cell/manager.go:233-294`）**不变**——api 已保证编译产物合法，EPP 继续以 llm-d loader 加载；per-cluster 编译失败隔离（失败 cluster 沿用旧引擎）保留，作为防御兜底。
- `pkg/poller/config.go` 中"原始 JSON"相关注释/命名随端点合并清理。

### 2.5 feature gate 启用路径（无改码，验证项）

- `flowControl` gate 现状已注册（`cmd/epp/plugins.go:74`）但默认关闭；ai-gateway-api 在 epp_config 含 `flow_control` 时编译 `featureGates: ["flowControl"]`。
- EPP 的 llm-d loader 从配置 `featureGates` 字段启用 gate——**无改码**，需补一个用例验证：picker_config 带 `flowControl` gate + flowControl 段时流控生效（排队/TTL），不带时保持默认关闭。

### 2.6 行为语义固化（无改码，文档化）

以下行为本次**不改动**，在 `docs/zh_cn/sys_design/ai-gateway-epp系统设计.md` 相应章节补充语义说明：

- **standby 热数据冷准入**：standby cell 建 engine、收配置（`Manager.ApplyConfig` 对 standby 正常生效），demux 拒绝服务（`ErrCellDraining`）；failover 后"开闸即服务"。
- **软亲和**：prefix/session scorer 为加权求和中的普通一项（输出 clamp [0,1] × 固定权重 1.0），filter（utilization-filter）先于打分；匹配/session 命中不保证一定选中（被过滤或总分反超即打破）。SC11/SC12 已验证。
- **状态全本地内存**：prefix 亲和 LRU（每后端容量 = GPU block 数 autoTune 或默认 31250 blocks，约 5MB/后端）、session binding、流控队列与在飞计数均为进程内状态；failover/重启后冷启动重新收敛（亲和重学、流控清零由 BFE 重试兜底）。EPP 无全局内存上限配置，内存边界靠容器限额。

## 3. 涉及文件清单

| 文件 | 改造点 |
|------|--------|
| `pkg/innerapi/client.go` | 新增 `EppDataConfigPath` 与两段 Config 结构；删除 report 方法 |
| `pkg/poller/epp_data.go`（新增） | `EppDataWatcher`：`Source[EppDataConfig]` 实现 + `resolveRole` + assignment diff 驱动 cell |
| `pkg/poller/config.go`、`pkg/poller/assignment.go` | 删除（被 epp_data.go 取代） |
| `pkg/assignment/types.go` | `View` 改为全量视图类型；删除 `ReadyReport` |
| `pkg/cell/manager.go`、`pkg/cell/compile.go` | 消费路径注释/命名清理；加载逻辑不变 |
| `cmd/epp/main.go` | poller 组装三改二 |
| `test/.../scenario-SC03-*`、`scenario-SC07-*` | 去除 report 断言，改断言"无上报" |
| `docs/zh_cn/sys_design/ai-gateway-epp系统设计.md` | §4 补充软亲和/状态本地内存/standby 语义说明 |

## 4. 测试计划

### 4.1 单元测试

- `resolveRole`：primary 命中 / standby 命中 / 未命中 / `standby=null` 单实例组 / 空段，全分支覆盖。
- `EppDataWatcher.Handle`：assignment diff → Ensure/Promote/Demote/Drop 调用序列；version 不变返回 `Data: null` 时不触发任何变更；epp_config 段与 assignment 段同快照应用顺序。
- 无上报：assignment 多次变更后 innerapi client mock 断言零 report 调用。

### 4.2 场景测试（改造既有 SC）

- SC11/SC12：picker_config 来源切换为 epp_data/config 段（编译后含 prefix/session scorer），亲和行为断言不变（收敛、弃权、摘除迁移、重学）。
- SC03：assignment 全量视图驱动 Promote/Demote（替代 per-instance 视图 + report 触发），failover 行为断言不变；新增"实例 id 不在池中 → 无 cell + 告警"用例。
- flowControl gate：带 `featureGates:["flowControl"]` 的 epp_config 使流控生效（新场景或并入既有流控用例）。

## 5. 风险与注意事项

| 风险 | 说明 | 缓解 |
|------|------|------|
| 上报删除后 SC03 语义依赖变化 | 旧测试以 report 作为 failover 排序信号 | 新设计 failover 完全由 BFE 滞回驱动，EPP 只保证 standby 热数据；用例改断言行为而非通道 |
| 全量视图带来短暂双活跃窗口 | api 翻转 assignment 与 BFE 切换 EPPAddr 之间存在时序差，新旧 primary 可能短暂同时服务 | 与旧设计一致（per-instance 视图同样存在该窗口），session binding 本地内存本就不共享；SC03 双活跃观测属 P2 |
| 未匹配实例静默无服务 | instance-id 配错时 EPP 无任何 cell | 启动即告警指标（`epp_assignment_no_match`），部署 checklist 核对 `-instance-id` 与 `/epp-pool` |
