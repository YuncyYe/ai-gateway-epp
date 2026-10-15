# EPP 单实例化（assignment 改 group 映射）设计变更说明

变更记录：`change-summary.md`。涉及模块：`pkg/innerapi/`、`cmd/epp/`、`pkg/poller/`、`test/common/` 与场景测试。

## 1. 契约结构（`pkg/innerapi/client.go`）

**原：**

```go
// AssignmentEntry is one cluster entry of the exported assignment section.
type AssignmentEntry struct {
	Primary string  `json:"primary"`
	Standby *string `json:"standby"`
}
```

**新（最终形态，无兼容字段）：**

```go
// AssignmentEntry is one cluster entry of the exported assignment section:
// the cluster to assigned-group mapping. The control plane switched the
// assignment granularity from instances to groups (single-instance stage:
// exactly one instance per group).
type AssignmentEntry struct {
	Group string `json:"group"`
}
```

> 解码路径（`Client.Get` 与本地文件模式 `LocalFileSource.Fetch`）均为普通 `encoding/json.Unmarshal`，无 `DisallowUnknownFields`：旧版 api 下发的 `{primary, standby}` 会解出 `Group == ""`，导致全部 cluster 未命中、`ai_epp_assignment_no_match` 告警——**不兼容旧版 api 是显式设计**，跨版本混部会立即暴露（告警/日志），不会静默工作。

## 2. 进程配置（`cmd/epp/`）

- `config.go`：新增 `-epp-group` flag（环境变量 `AI_GATEWAY_EPP_GROUP`），存入 `Config.EppGroup`；**必填、无缺省值**（`Config.Validate` 在未设置时返回错误，进程拒绝启动——避免静默取 hostname 或静默不服务掩盖误配），部署侧须保证与 `/epp-pool` 中组名一致。**删除 `-instance-id` / `AI_GATEWAY_EPP_INSTANCE_ID`**（`Config.InstanceID` 字段移除）：实例不再有独立于组名的身份，避免两套标识并存造成误配。
- `epp_data.go`：构造 watcher 时把组名注入（`NewEppDataWatcher(client, eppgroup, manager, logger)`），保存为 `w.eppgroup`——即 `EppDataWatcher` 原 `instance` 字段直接改名为 `eppgroup`，不再保留实例维度身份。

## 3. 角色解析（`pkg/poller/epp_data.go`）

### 3.1 `Handle` 主循环

- 逐 cluster 调 `resolveAssignmentEntry`；命中进 `mine`（`map[string]bool`，**watcher 侧不再出现 `cell.Role`**）；**删除 peer 日志调用**（原 `"assignment peer"` 日志随 `peerID` 一并移除）。
- 全部未命中 → `ai_epp_assignment_no_match` gauge = 1 + 告警日志（不变）。

### 3.2 `resolveAssignmentEntry`：group 匹配，返回 bool

分配匹配不再产生"角色"——组名相等即"该 cluster 归我"，返回 `mine bool`：

```go
// resolveAssignmentEntry reports whether the cluster's assigned group is
// this instance's own group (w.eppgroup): the cluster is then served by
// this instance. There is no role distinction in the assignment path.
func resolveAssignmentEntry(self string, e innerapi.AssignmentEntry) (mine bool) {
	return e.Group != "" && e.Group == self
}
```

`Handle` 中以 mine 集合驱动 cell 生命周期（命中 → `Ensure`，未命中 → `Drop`），不再传递 primary/standby 角色。

### 3.3 `peerID` 整体删除

- 删除 `peerID` 函数（primary↔standby 对端实例 id 互查，约 15 行）与 `Handle` 中的调用（约 3 行）。
- 单实例组恒无对端；`peerID` 在单实例组下本就返回 `""`。

### 3.4 `cell.Role` 类型一并删除（级联）

assignment 链路不再驱动 standby 后，`cell.Role`/`RoleStandby`/`StateStandby` 失去唯一生产者，本次**一并删除**（替代原"保留为防御性路径"的决策）：

- `pkg/cell/role.go` 整体删除（`Role`、`RolePrimary`、`RoleStandby`、`StateStandby` 及 `String()`）。
- `manager.Ensure(ctx, key, role)` 简化为 `Ensure(ctx, key)`：assignment 命中即建/保 cell，内部不再有 role 状态机与 `Demote`；`manager.Demote`、`cell.StateStandby` 路径删除。
- demux 拒绝逻辑由 `Role() != RolePrimary` 改为"cell 引擎缺失即拒绝"（`eng == nil` → `ErrCellDraining`），语义不变。
- 级联调整 `pkg/cell`（manager/cell/metrics）、`pkg/demux` 及其全部测试（`manager_test.go`、`cell_test.go`、`engine_test.go`、`server_test.go`、`role_test.go` 删除）。

## 4. 测试调整

| 测试文件 | 调整 |
|----------|------|
| `pkg/poller/epp_data_test.go` | `TestResolveRole` 改为 **`TestResolveAssignmentEntry`**：standby 用例删除，改为 group 命中（`mine == true`）/未命中（含 `Group == ""`）用例；`entry()` helper 去 standby 参数、改 group 参数；`TestEppDataHandleLifecycle` 的 demote/standby 场景断言改为 Ensure/Drop 语义；`TestEppDataFetchChanged` 删 `Standby` 断言 |
| `pkg/poller/local_source_test.go` | `AssignmentEntry` 字面量去 `Standby` 字段引用，改 `{Group: ...}` |
| `cmd/epp/config_test.go` | 新增 `-epp-group` / `AI_GATEWAY_EPP_GROUP` 解析用例（flag 优先 / env / 未设置保持为空） |
| `pkg/cell/`（`manager_test.go`、`cell_test.go`、`engine_test.go`、`role_test.go` 删除） | 随 `Role`/`StateStandby`/`Demote` 删除级联改写：Ensure 两参断言改单参；standby 状态断言删除 |
| `pkg/demux/server_test.go` | 拒绝语义断言由 role 判断改为"引擎缺失即拒绝" |
| `test/common/mock_ai_gateway_api.go` | `SetAssignment` 的 standby role 展开逻辑删除（`fullView` 的 `e.Standby = sp(...)`）；mock 的 assignment 下发形态改为 `{group}` |
| `test/common/local_env.go` | `WithLocalRoles` 的 `"standby"` 约定删除；`EppDataConfigFor` 的 standby 分支删除；改为 group 维度构造 |
| `test/implementation/scenario-SC03-assignment-role-switch/` | 用例语义从"demote-to-standby/failover 翻转"改为"摘除/重分配"（group 粒度） |
| `test/implementation/scenario-SC09-dual-instance-group/` | **场景语义基础失效**：改为"多 cluster 共享单实例组"或整体删除（本次一并落地） |
| `test/implementation/scenario-SC15-local-config-file/` | `TestTC04_StandbyRole` 删除或改为 primary 用例；`{primary, standby: null}` 示例改 `{group}` |

## 5. 文档同步

- `docs/zh_cn/configuration/EPP配置定义说明-epp_config.md`：assignment 契约章节（结构、示例、自匹配消费方式）重写为 `{group}` + `-epp-group`。
- `docs/zh_cn/sys_design/ai-gateway-epp需求分析.md`、`ai-gateway-epp系统设计.md`、`详细设计/详细设计-01~05`：primary/standby 分配、双实例互备组、resolveRole/Role 伪代码等描述清理。
- `docs/zh_cn/modifications/2026-09-08-epp-scheduling-integration/*.md`：历史文档保留为快照，不回改；现行约束以本文档与 `EPP配置定义说明-epp_config.md` 为准。
- `test/测试设计文档/`：SC01/SC03/SC09/SC15 场景说明与 TC 文档同步（随场景改造一并更新）。

## 6. 发版与回滚

**发版**：EPP 与 ai-gateway-api **同版本同步发布、同步升级**（无兼容窗口，不允许跨版本混部）：

1. 升级前：api 侧将存量 EPP 实例池收敛为单实例组；备份 `epp_assignments` 表。
2. 同一发布批次：升级 api（含 schema 删列）+ 全量 EPP 实例（配置 `-epp-group`）。
3. 升级后：观察 `ai_epp_assignment_no_match` 指标——应归零（恒 1 表示有实例 group 配置错误或残留旧版混部）。

**回滚**：需 **EPP 与 api 同批回滚**（旧版 api 下发 `{primary, standby}`，新 EPP 解出 `Group == ""` 会全部 no-match 告警；新版 api 下发 `{group}`，旧 EPP 的 `Primary` 匹配恒不命中同样告警——单向回滚任一侧都会触发显式告警，不会静默错误）。

## 7. 非目标

见 `change-summary.md` §5。核心：不兼容旧版 api（无双字段/回退逻辑）；不改 epp_config 消费路径；不动 bfe。
