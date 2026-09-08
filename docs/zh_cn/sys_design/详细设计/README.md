# ai-gateway-epp 详细设计（索引）

《ai-gateway-epp系统设计.md》的实现层展开，按子系统拆分为五份：

| 文档 | 内容 | 对应系统设计 |
|---|---|---|
| [详细设计-01-包结构与进程配置](详细设计-01-包结构与进程配置.md) | 仓库包结构、依赖方向、进程级启动配置、部署形态 | §3.1、§8 |
| [详细设计-02-Cell管理与热加载](详细设计-02-Cell管理与热加载.md) | Cell/Manager/Engine 数据结构、角色转换、生命周期状态机、编译切换、drain、并发模型 | §3.2、§3.6、§4.2 |
| [详细设计-03-Poller与InnerAPI对接](详细设计-03-Poller与InnerAPI对接.md) | 通用轮询框架、ClusterDiscovery/EppDataWatcher 算法（epp_data/config 两段消费 + 自匹配）、fail-static | §3.4、§5 |
| [详细设计-04-Demux与请求路径](详细设计-04-Demux与请求路径.md) | pool metadata 路由、多 Cell ext-proc server、请求面错误语义 | §3.3、§4.1 |
| [详细设计-05-错误处理-可观测-测试与Spike](详细设计-05-错误处理-可观测-测试与Spike.md) | 错误约定总表、指标清单、测试策略、部署形态补充、M0 spike 验证清单 | §6、§7、§11 |

**全局约定**（各文档共同遵守）：

- Go 签名仅为设计示意，以实现为准；引擎侧 API 以 `llm-d-router` 当前导出面为准
- 引擎包只读引用（禁 import `llm-d-router/cmd/...`）；每 Cell 独立流控组件，绝不跨 Cell 共享
- 数据面（datastore/采集）常驻于 Cell，政策面（Engine）经 `atomic.Pointer` 热切换

**依赖方向**：`cmd → {cell, demux, poller, eppplugin}`；`poller → innerapi, cell, assignment`；`demux → cell`；`cell → llm-d 引擎包`。禁止反向。
