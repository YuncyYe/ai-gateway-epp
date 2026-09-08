# EPP 调度对接（ai-gateway-epp 侧改造）变更摘要

## 1. 背景

ai-gateway-api 控制面落地 EPP 调度对接（方案见 `ai-gateway-api/design-docs/modifications/2026-09-08-epp-scheduling-integration/`），核心变化：

1. **epp_data 下发合并为单端点**：`GET /inner-api/v1/configs/epp_data/config`（单 topic `ConfigTopicEppData`、version 增量），Config 含两段——`epp_config`（map[cluster名]编译后的 llm-d `EndpointPickerConfig`，由 api 从简化用户形态确定性编译）与 `assignment`（map[cluster名]{primary, standby} **全量视图**，所有 EPP 实例返回相同内容）。
2. **assignment 全量视图 + 实例自匹配**：api 不再按 `?instance=` 过滤；EPP 以 `-instance-id` 自匹配角色。
3. **无上报义务**：api 侧取消了 `assignment/report` 端点（failover 由 BFE 侧 EPPAddr 连接滞回驱动，不依赖 api 侧就绪排序）；EPP 现有的 ReadyReport 上报需同步删除。
4. **无注册/心跳**：实例池由 OpenAPI `/epp-pool` 静态配置，EPP 无需 register/heartbeat（本仓库本就未实现，保持不变）。
5. **亲和与流控能力**：`prefix-cache-scorer`、`session-affinity-scorer` 已注册（`cmd/epp/plugins.go`），随编译后配置下发启用；`flowControl` feature gate 由配置 `featureGates` 字段按需启用（api 在 `flow_control` 存在时追加）。

EPP 侧需要对应改造配置消费链路，本文档与 `design-changes.md` 描述改造内容。

## 2. 目标

- 三个轮询端点（`picker_config`、`assignment?instance=`、`epp_data` 体系）合并为**单个 epp_data/config 轮询**（cluster_table 轮询保留不变），一次拉取获得 epp_config + assignment 同一 version 快照。
- assignment 消费从"服务端 per-instance 过滤"改为"**全量视图 + 本实例自匹配**"：新增自匹配逻辑，cell 的 Ensure/Promote/Demote/Drop 由本地 diff 驱动。
- **删除 ReadyReport 上报**（`POST /configs/epp_data/assignment/report`）及其数据结构、SC03/SC07 相关断言。
- 直接消费 api **编译后**的 `EndpointPickerConfig`（epp_config 段），移除 EPP 侧对"原始用户形态配置"的假设（本仓库本就只加载不编译，改造主要是来源与结构切换）。
- 固化既有行为语义（无需改码，文档化）：standby cell 热数据冷准入（demux 拒绝服务）、软亲和（加权求和取最高分，非硬路由）、亲和/流控状态全部本地内存、failover 冷启动重新收敛。
- 验证 `flowControl` feature gate 经 `featureGates` 配置启用的端到端路径。

## 3. 范围

| 范围 | 说明 |
|------|------|
| 涉及仓库 | `ai-gateway-epp` |
| 主要模块 | `pkg/poller/`（改造为主）、`pkg/innerapi/`、`pkg/assignment/`、`pkg/cell/`（compile/消费路径）、`cmd/epp/main.go`（组装）、SC03/SC07/SC11/SC12 测试 |
| 接口消费 | 新增消费 `GET /configs/epp_data/config`（契约见 ai-gateway-api 侧 `api-changes.md` §2.1）；停用 `GET /configs/epp_data/picker_config`、`GET /configs/epp_data/assignment`；停用 `POST /configs/epp_data/assignment/report`；`GET /configs/gslb_data/cluster_table` 不变 |
| 插件 | 不新增/不修改插件；`prefix-cache-scorer`、`session-affinity-scorer`、flowcontrol 插件族均已在 `cmd/epp/plugins.go` 注册 |
| ai-gateway-api 依赖 | api 侧需先提供 epp_data/config 端点；EPP 与 api 同版本发布 |

## 4. 关键决策

| 决策 | 说明 |
|------|------|
| 单 poller 合并消费 | 复用既有 `Source[T]` version 增量框架（`pkg/poller/poller.go`），三个 Source 合并为一个 `EppDataWatcher`：一次拉取、一次 version、两段配置天然同快照，消除"配置已更新但角色未变"（或反之）的跨端点偏移窗口。数据量极小，全量重发代价可忽略。 |
| 全量视图本地自匹配 | api 返回所有 cluster 的 {primary, standby}；EPP 用 `-instance-id` 逐 cluster 匹配：`primary == 本实例 id` → RolePrimary；`standby == 本实例 id` → RoleStandby；均未命中 → 跳过。优点：实例身份校验从服务端下放到本地（服务端无 4xx 边缘）；EPP 顺带获知同组 peer id（备 Cell 建联、双活跃自检预留，本期仅记日志/指标）。 |
| 删除 ReadyReport | 新设计中 failover 由 BFE 侧连接滞回触发，api 侧 allocator 不再"等就绪再翻转"；上报的消费者已不存在，保留反而引入"上报成功但无人消费"的假象。SC03/SC07 中断言上报的用例改为断言"无上报且行为正确"。 |
| 编译后配置直消费 | api 导出的 epp_config 段已是完整 `EndpointPickerConfig`（插件引用/DAG 由 api 模板构造保证合法）；EPP 用 llm-d loader 直接加载，per-cluster 编译失败隔离逻辑保留（失败 cluster 沿用旧引擎）。 |
| 行为语义不变 | standby 不对外服务（demux `ErrCellDraining`，BFE 重试语义）、软亲和、本地内存状态、failover 冷启动收敛——均为既有实现，本次仅文档固化，不改码。 |
| 实例 id 约定（同参数启动） | EPP 以 StatefulSet 部署，实例 id = **Pod hostname**（`-instance-id` 缺省值即 hostname，同组副本共享完全相同的启动参数，零 per-instance 配置）；`/epp-pool` 登记 id 取 pod 名，部署流程 reconcile 时从 StatefulSet pod 名生成 PATCH。前提：StatefulSet 保证 hostname 稳定唯一（禁用随机名 Deployment）；非 K8s 部署显式传 `-instance-id`。 |

## 5. 非目标

- 不实现 register/heartbeat（实例池由 api `/epp-pool` 静态配置）。
- 不新增任何上报/心跳/健康回调通道（P2 若需要，届时统一设计）。
- 不修改插件实现（prefix/session scorer、flowcontrol 插件族、cluster-table-discovery 均沿用）。
- 不实现跨实例亲和状态同步（crossReplicaSyncer 预留，本期 standby 冷准入可接受）。

## 6. 关联文档

- 控制面方案：`ai-gateway-api/design-docs/modifications/2026-09-08-epp-scheduling-integration/`（`change-summary.md` / `api-changes.md` / `design-changes.md`）
- BFE 侧方案：`bfe/docs/zh_cn/modifications/2026-09-06-epp-ai-gateway-integration/design-changes.md`
- 配置契约权威定义：`ai-gateway-epp/docs/zh_cn/configuration/EPP配置定义说明-epp_config.md`（原 `EPP配置定义说明-picker_config.md` 已随端点合并更名重写）
- 既有行为验证：`test/测试设计文档/scenario-SC03-主备角色切换/`、`scenario-SC11-PrefixCacheAffinity调度/`、`scenario-SC12-SessionAffinity调度/`
