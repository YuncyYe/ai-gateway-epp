# 详细设计 05 — 错误约定、可观测、测试策略与 Spike 验证

> 全局约定见 [README](README.md)：Go 签名为示意；引擎只读；每 Cell 独立流控；数据面常驻、政策面原子指针热切换。
> 并发模型（各执行体数量与所有权、锁纪律）见 [详细设计-02-Cell管理与热加载](详细设计-02-Cell管理与热加载.md)。

## 1. 错误约定

| 错误 | 类型 | 对 BFE 语义 |
|---|---|---|
| ErrNoPoolMetadata / ErrUnknownPool | typed sentinel | ext-proc 返回 INTERNAL 级错误，BFE 记日志 + 回退本地均衡 |
| 配置编译失败 | 日志 + 指标 | 无请求面影响（旧引擎继续服务） |
| Poller 持续失败 | 指标 + 退避 | fail-static（保持最后已知配置） |
| Cell DRAINING 中请求 | typed sentinel | 可重试错误（BFE 应重试到备实例） |
| drain 超时强杀 | 日志 + 指标 | 在飞请求计量丢失（TTL 自愈，FR-H3 既定语义） |

## 2. 指标清单（新增部分）

```
ai_epp_engine_reloads_total{cluster, result}      counter   FR-O1
ai_epp_engine_current_version{cluster}            gauge     FR-O1
ai_epp_poller_last_sync_timestamp{poller}         gauge     FR-O2
ai_epp_poller_failures_total{poller}              counter   FR-O2
ai_epp_poller_backoff_state{poller}               gauge     FR-O2
ai_epp_cell_state{cluster, role, state}           gauge     FR-O4（双活跃告警数据源）
ai_epp_demux_errors_total{reason}                 counter   排障
ai_epp_drain_duration_seconds{cluster}            histogram 排障
```

说明：

- `engine_current_version` 与 poller 指标支撑配置热生效的可观测性（改了配置后版本号是否前移、何时前移）。
- `cell_state{role,state}` 是从 EPP 侧输出的主/备双活跃（脑裂）告警数据源之一，与 ai-gateway-api 侧 assignment 对账口径一致。
- 进程自身运行指标（goroutine 数、内存、GRPC 延迟）沿用 Go/gRPC 运行时标准导出，不在此重复列举。

## 3. 测试策略

| 层 | 内容 |
|---|---|
| 单元 | diff 算法（增删改 / Weight=0 摘除 / Delete 先于 Upsert 顺序契约）、角色矩阵六转换、编译失败路径、demux metadata 解析 |
| llm-d 引擎 | 复用其既有测试，不重复；集成面只测装配正确性 |
| 集成（fake innerapi） | httptest 提供三接口 + version 增量；验证：新 cluster 自动建 Cell → Ready → 可调度；配置变更热生效（新旧引擎行为可区分）；kill 主备切换路径 |
| 契约 | picker_config / assignment 的 JSON 与 ai-gateway-api 侧样例对拍（golden file） |
| 性能 | demux 开销基准（每请求 Load vs 基线）；Cell 数规模化（100 cluster）内存/调度延迟 |

## 4. 配置与部署形态（补充）

- 每实例一个进程级 Config（见 01 号文档 §2），cluster 级配置全部来自 picker_config。
- 实例组拓扑不进进程配置：实例只知道自己是谁（InstanceID），角色全听 assignment——**实例可互换**，组内任意实例可被分配器互换角色（与 NFR-5 零接触一致）。

## 5. Spike 验证清单（M0 前置）

1. `datastore.NewDatastore + WithEndpointPool` 可在外部装配，采集 goroutine 正常起停。
2. file-discovery 插件可在外部注册并驱动 datastore（引用 `EndpointDiscovery` 接口即可）。
3. `scheduling.NewSchedulerWithConfig` + 至少一个 profile 编译成功。
4. ext-proc server 可用 `runserver.ExtProcServerRunner` 或等价导出启动，BFE `BalanceEpp` 完成一次真实选端点。
5. 逐项记录"需要从 cmd 复制/重构到 pkg"的胶水函数清单（预期：`initAdmissionControl`、插件注册列表、health server 装配）。

任何一项依赖未导出符号 → 触发升级路径：优先向上游 llm-d-router 提 PR 导出所需符号；PR 不可行时以本地最小补丁兜底，并在代码注释中标注待上游化。
