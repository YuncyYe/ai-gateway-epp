# EPP 单实例化（assignment 改 group 映射）变更摘要

## 1. 背景

ai-gateway-api 控制面落地 EPP 单实例化（方案见 `ai-gateway-api/design-docs/modifications/2026-10-14-epp-single-instance/`，设计定稿见 `yxy-note/rainway-ai-gateway/issues/rm_epp_standy/design-doc.md`），核心变化：

1. **分配粒度由实例收敛为实例组**：`epp_assignments` 只存 cluster → group 映射（删除 `primary_instance_id` 列），实例由组派生；primary/standby 概念全系统删除。
2. **epp_data 的 assignment 段字段重构（breaking）**：`assignment[cluster]` 条目由 `{primary: "epp-a", standby: "epp-b"}` 改为 **`{group: "g1"}`**（cluster → 分配的实例组名）。
3. **OpenAPI `/epp-assignments` 视图同步收缩**：只保留 `{cluster, group}`，`primary`/`standby`/`instances`/`degraded` 字段全部删除——本仓库不消费 OpenAPI，无直接影响。
4. **每组恰好 1 实例**：`/epp-pool` 校验收紧，主备双实例不再合法；存量双实例组需先收敛（api 侧升级脚本附说明）。

EPP 侧需要对应改造 assignment 消费链路。**本次变更为干净的 breaking 切换，不提供对旧版 api 的兼容**——两侧同步发版、同步升级。

## 2. 目标

- `AssignmentEntry` 直接改为 `{Group string}`，与 api 侧存储/导出粒度一致；**不保留** `primary`/`standby` 字段与任何回退解码逻辑。
- EPP 以**自身所属组名**参与分配匹配（`EppDataWatcher` 的 instance 字段改为 **eppgroup**，进程级配置 `-epp-group`）；`resolveRole` 改为 **`resolveAssignmentEntry(self, e) (mine bool)`**——分配匹配只回答"该 cluster 是否归我"，**`cell.Role` 类型从 assignment 链路删除**。
- 删除 standby 视角的 **peer 概念**（`peerID` / "assignment peer" 日志）：单实例组恒无对端。
- **删除** `cell.Role`/`RoleStandby`/`StateStandby`/`Demote`（assignment 链路已无角色概念，级联调整 cell/demux/manager 及测试；demux 拒绝逻辑改为"引擎缺失即拒绝"，语义不变）。

## 3. 范围

| 范围 | 说明 |
|------|------|
| 涉及仓库 | `ai-gateway-epp`（本仓库）；上游 `ai-gateway-api`（breaking 来源，另仓已落地）；`bfe` 零改动 |
| 主要模块 | `pkg/innerapi/`（契约结构）、`cmd/epp/`（新增 group 配置项）、`pkg/poller/`（角色解析、peer 删除）、`test/common/`（mock/本地环境）、SC03/SC09/SC15 场景测试 |
| 接口消费 | `GET /configs/epp_data/config` 的 assignment 段结构变更；epp_config 段、version 增量、`Data: null` 语义均不变 |
| 发版依赖 | api 侧 assignment 重构 breaking——**EPP 与 api 同版本同步发布、同步升级**（无兼容窗口，不允许跨版本混部） |

## 4. 关键决策

| 决策 | 说明 |
|------|------|
| 一次性 breaking 切换，不兼容旧版 | 不引入双字段兼容形态：同一版本内 api 只下发 `{group}`、EPP 只按 `{group}` 解码。发版时存量 EPP 实例与 api **必须同批升级**；回滚亦需两侧同批回滚（见 `design-changes.md` §6）。省去兼容字段与回退逻辑的长期维护成本。 |
| group 作为自身身份标识 | assignment 不再携带实例 id，`-instance-id` 无法参与匹配。`EppDataWatcher` 的 `instance` 字段改名为 **`eppgroup`**；进程配置新增 `-epp-group` flag / `AI_GATEWAY_EPP_GROUP` 环境变量，存入 `Config.EppGroup`（**必填、无缺省值**，未设置则 `Config.Validate` 拒绝启动，避免静默不服务；部署侧保证与 `/epp-pool` 组名一致）；`-instance-id` 一并删除，实例不再有独立于组名的身份。 |
| resolveAssignmentEntry 返回 mine bool | 原型 `func resolveAssignmentEntry(self string, e innerapi.AssignmentEntry) (mine bool)`：`e.Group != "" && e.Group == self`。watcher 侧不再出现 `cell.Role`，Handle 以 mine 集合驱动 Ensure/Drop。JSON 解码为宽松模式（无 `DisallowUnknownFields`），但旧版 api 下发的 `{primary, standby}` 会解出 `Group == ""` → 全部未命中 → `ai_epp_assignment_no_match` 告警，**不会**静默工作——这正是所期望的"不同版本不混部"失败显式化。 |
| 删除 peer 概念 | 单实例组恒无对端：`peerID()`（primary↔standby 互查）与 "assignment peer" 日志整体删除；`ai_epp_assignment_no_match` 指标与告警保留（全部未命中时置 1）。docs 中提及的 `epp_assignment_peer` 指标本就未实现，无需处理。 |
| 删除 cell.Role 类型 | assignment 不再驱动 standby 后，`Role`/`RoleStandby`/`StateStandby` 失去唯一生产者；随本次一并删除（`Ensure` 简化为两参，demux 改为 `eng == nil` 拒绝）。级联波及 cell/demux/manager 全部测试，一并改写。 |
| SC09 场景随本次重设计 | SC09（双实例互备组）的语义基础（双实例组）已不存在：用例改为"多 cluster 共享单实例组"或整体删除，随本次一并落地。 |

## 5. 非目标

- 不提供对旧版 api（`{primary, standby}` 下发）的兼容解码；不允许 EPP 与 api 跨版本混部。
- 不再保留 `RoleStandby`/`StateStandby`/`Demote`——随 `cell.Role` 一并删除（见 §4）。
- 不修改 epp_config 编译/消费路径（`EndpointPickerConfig` 结构不变）。
- 不实现任何注册/心跳/上报通道（维持现状）。
- 不修改 bfe 侧（EPPAddr 通用列表天然兼容单元素）。

## 6. 关联文档

- 控制面方案：`ai-gateway-api/design-docs/modifications/2026-10-14-epp-single-instance/`（`change-summary.md` / `design-changes.md` / `api-changes.md`）
- 设计定稿（评估/决策演进）：`yxy-note/rainway-ai-gateway/issues/rm_epp_standy/design-doc.md`（V0.6）
- 配置契约权威定义：`ai-gateway-epp/docs/zh_cn/configuration/EPP配置定义说明-epp_config.md`
- 既有行为验证：`test/测试设计文档/scenario-SC03-assignment-role-switch/`、`scenario-SC09-dual-instance-group/`、`scenario-SC15-local-config-file/`
