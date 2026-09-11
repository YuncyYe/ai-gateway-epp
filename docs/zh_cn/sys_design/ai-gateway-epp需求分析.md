# ai-gateway-epp 需求分析

## 0. 文档目的

对齐 ai-gateway-epp 项目（新建代码库）要实现的目标。本文档是需求层定义：**只说什么必须成立，不说怎么实现**；实现设计见本仓库 `docs/zh_cn/sys_design/` 下的系统设计文档与详细设计，引擎行为背景见上游 `github.com/llm-d/llm-d-router` 源码。

## 1. 背景与问题陈述

llm-d EPP（Endpoint Picker）是成熟的推理调度引擎（调度、流控、插件框架经过生产验证），但存在与目标部署形态不匹配的四个问题：

1. **K8s 强耦合**：端点发现依赖 Pod watch + InferencePool CRD，控制面需要 controller manager/RBAC/leader election 全套设施
2. **配置静态**：EndpointPickerConfig 启动时固化，改调度策略/插件/流控参数必须重启进程
3. **单 pool 单进程**：一个 EPP 只服务一个 InferencePool，pool 数量线性放大部署单元
4. **无外部配置权威**：配置散落在 CRD 与启动参数中，没有统一的配置管理中心

目标部署形态：BFE 作数据面网关 + ai-gateway-api 作配置权威中心，EPP 作为调度服务运行在其间。

## 2. 目标对齐

**一句话目标**：构建一个以 ai-gateway-api 为唯一配置权威、可热加载、池化主备部署的多 cluster 推理调度服务，复用 llm-d EPP 引擎，通过 ext-proc 协议为 BFE 提供选端点能力。

| # | 目标 | 成功的可验证标志 |
|---|---|---|
| G1 | 脱离 K8s 运行 | EPP 在无任何 K8s API 访问的环境下完整启动并服务 |
| G2 | ai-gateway-api 为唯一配置权威 | 全部运行时配置（实例列表、调度策略、流控参数、主备分配）只从 ai-gateway-api InnerAPI 获取；修改后生效不依赖任何 K8s 资源 |
| G3 | 配置热加载 | 修改 cluster 级配置后到生效全程不重启进程、不中断该 cluster 服务 |
| G4 | 单实例多 cluster | 一个 EPP 进程同时调度多个 cluster，新增/移除 cluster 自动生效 |
| G5 | 池化主备 | 实例池按"每 cluster 一主一备"分配，主故障自动切换，资源池化利用 |
| G6 | 引擎复用 | 调度/流控/协议核心直接引用 llm-d 上游模块，不复制不魔改 |

## 3. 功能需求

### 3.1 端点发现（FR-D）

| 编号 | 需求 | 优先级 |
|---|---|---|
| FR-D1 | 从 ai-gateway-api InnerAPI `cluster_table` 接口拉取全部 cluster 的后端实例列表（Name/Addr/Port/Weight），`version` 参数增量同步，配置未变化时（`Data: null`）零成本空转 | P0 |
| FR-D2 | 将实例列表按 cluster 映射为内部端点模型（直连 Addr:Port，Weight=0 视为摘除），驱动对应 cluster 的端点增删改 | P0 |
| FR-D3 | API 故障时 fail-static：保留最后已知实例列表继续服务，指数退避重试，恢复后自动追上 | P0 |
| FR-D4 | 新 cluster 出现时自动纳入服务；cluster 消失时摘除其全部端点 | P0 |

#### Weight 语义说明（已对齐）

cluster_table 的 `Weight` 字段在 EPP 链路上**只生效为注册/摘除开关（0=摘除，非0=注册），不作为流量比例权重**。依据：

- EPP 端点模型 `EndpointMetadata`（`llm-d-router/pkg/epp/framework/interface/datalayer/endpoint_metadata.go:32-46`）无 Weight 字段——同一 pool 内端点天然平等，K8s 路径下亦然
- EPP 流量分配由调度引擎基于**实时指标**动态决定（filter → scorer 打分 → picker 选择，机制见上游 llm-d-router 调度框架）；调度 profile 里的"权重"是 scorer 组合权重，与端点权重无关
- BFE 侧仅在自身 balance 模式（WRR/WLC）下消费 Weight；`BalanceMode=EPP` 时 host 选择完全委托 EPP，Weight 被绕过

**比例分流（灰度/金丝雀）的正解**：拆 cluster（比例在 BFE 路由层/ai-gateway-api 注册侧控制），或模型名改写拆分；确需端点级静态加权时可经 `Labels` + 自定义 scorer 插件实现（扩展点，非内置行为）。此语义应写入 ai-gateway-api 的 cluster_table 接口文档，避免使用者误解。

### 3.2 配置中心与热加载（FR-C）

| 编号 | 需求 | 优先级 |
|---|---|---|
| FR-C1 | 从 ai-gateway-api InnerAPI `epp_data/config` 接口的 `epp_config` 段（`map[cluster]EndpointPickerConfig`，api 已从简化用户形态确定性编译）拉取各 cluster 的 EPP 配置，version 增量同步 | P0 |
| FR-C2 | 每个 cluster 的配置独立编译为独立调度引擎（Engine）：插件链、调度 profile、流控参数完全按 cluster 隔离，互不相同 | P0 |
| FR-C3 | cluster 配置变更后：编译 → 校验 → 原子切换，新请求立即使用新引擎；旧引擎排空（队列出空 + 在飞请求完结，超时强制）后销毁 | P0 |
| FR-C4 | 配置编译失败：该 cluster 沿用旧引擎继续服务，其他 cluster 不受影响；失败以指标和日志暴露 | P0 |
| FR-C5 | 回滚 = 在 ai-gateway-api 改回配置后自动重载生效，无需任何进程操作 | P0 |
| FR-C6 | 结构不变仅调值的参数（权重/阈值/band 大小）支持不重建引擎的原子热改 | P1 |

### 3.3 多 cluster 与请求路由（FR-M）

| 编号 | 需求 | 优先级 |
|---|---|---|
| FR-M1 | 单进程内按 cluster 隔离运行单元（Cell）：每 cluster 独立的数据面（端点集合+指标采集）与政策面（引擎），共享 gRPC server | P0 |
| FR-M2 | 请求 demux：从 ext-proc 消息 metadata 读取 pool 名（BFE 注入，约定 namespace/key），路由到对应 Cell；未知 pool 返回可重试错误 | P0 |
| FR-M3 | cluster 生命周期数据驱动：随 cluster_table 与 `epp_data/config`（assignment 段）数据自动创建、就绪、drain、销毁 Cell | P0 |
| FR-M4 | 请求路径指标携带 cluster/pool 维度标签 | P1 |

### 3.4 主备池化（FR-H）

| 编号 | 需求 | 优先级 |
|---|---|---|
| FR-H1 | ai-gateway-api 为每 cluster 分配所属实例组与组内主实例，经 `epp_data/config` 的 assignment 段（`map[cluster]{primary, standby}` 全量视图，所有实例返回相同内容）下发；EPP 以本实例 id 在视图中自匹配角色，只为指定 cluster 建 Cell（主=服务态，备=热数据冷准入）。**第一期采用固定 2 实例互备组**（见 §3.4.1） | P0 |
| FR-H2 | BFE 对指定 cluster 优先请求主 EPP，失败后故障转移到同组另一实例（若配置了备）；要求 BFE failover 带滞回（冷却期 + 连续健康检查通过才回切），防抖动 | P0 |
| FR-H3 | 主备切换不丢"已派发"请求语义：failover 重试只针对未派发请求（派发标记机制），容忍在飞请求计量丢失（TTL 自愈） | P1 |
| FR-H4 | 脑裂（主备同时活跃）可接受不防御：按共享背压有界退化处理，但必须有"同一 cluster 双活跃"告警指标 | P1 |
| FR-H5 | 备 Cell 热数据冷准入：failover 后无需冷启动即可服务 | P1 |

#### 主备模型与实例组（已对齐）

**第一期：固定 2 实例互备组**。基本单元是"实例组"= 两个 EPP 实例（如 A、B）互为备份：cluster 分配到组后，组内再分为"A 主"和"B 主"两个子集（A 是这些 cluster 的主、B 是备；反之亦然）。分配数据结构为 `cluster → {group, primary}`，备由组内另一实例隐式得出；下发形态为 assignment 全量视图 `map[cluster]{primary, standby}`（同组副本收到的内容完全相同），EPP 侧以本实例 id 逐 cluster 自匹配角色：

```
组 g1 = {epp-A, epp-B}
  cluster-1 → {g1, primary: epp-A}   （备 = epp-B）
  cluster-2 → {g1, primary: epp-B}   （备 = epp-A）
```

**扩容 = 增加实例组**：新组 {epp-C, epp-D} 加入后，ai-gateway-api 在组间再平衡 cluster（组内 A/B 子集也可再平衡）；已有实例零接触（NFR-5）。组的粒度使反亲和、容量规划、故障域都有自然的边界。

**单实例（测试）场景**：允许单实例组（组内只有 A），此时所有 cluster 仅有主、无备；BFE 侧 EPPAddr 只配单个地址，主故障时按既有 `BalanceEpp` 回退语义降级为本地负载均衡——测试场景可接受，生产组必须是完整 2 实例。

**组内约束**：同一 cluster 的主备必须是组内两个不同实例（2 实例组天然满足）；跨机架/可用区反亲和为 P2 增强。

**实例身份约定（自匹配的前提）**：EPP 以 StatefulSet 部署，实例 id = **Pod hostname**（启动参数 `-instance-id` 的缺省值，同组副本共享完全相同的启动参数，零 per-instance 配置）；实例池由 ai-gateway-api OpenAPI `/epp-pool` 静态登记（id 取 pod 名）。非 K8s 部署显式传 `-instance-id`；id 与 `/epp-pool` 不一致的实例自匹配落空、无 cell 不服务，须以告警暴露。

**角色转换与扩缩容（NFR-5 的配套语义）**：分配变化经 InnerAPI 拉取生效，已有实例不重启。Cell 按角色转换矩阵处理：

| 旧角色 \ 新角色 | 主 | 备 | 无分配 |
|---|---|---|---|
| 主 | 不变 | 停止准入（drain），保留数据 | drain 后销毁 |
| 备 | 开闸即服务 | 不变 | 销毁 |

**再平衡顺序**：EPP **无上报义务**（无 register/心跳/就绪上报）——主备 failover 由 BFE 侧 EPPAddr 连接滞回驱动（FR-H2），ai-gateway-api 不"等就绪再翻转"。分配器只需保证新实例先以"备"角色进入并完成数据同步（standby 热数据冷准入，FR-H5），再翻转主角色即可。

### 3.5 与 BFE 的协议配合（FR-B，BFE 侧需求，本项目协调落地）

| 编号 | 需求 | 优先级 |
|---|---|---|
| FR-B1 | BFE ext-proc 客户端在 ProcessingRequest metadata 注入 pool 名（`llm-d.ai/inference-pool: <cluster名>`），值即 BFE cluster 名 | P0 |
| FR-B2 | BFE `EPPAddr` 支持按序主备消费（当前只用 addrs[0]，需改造）+ failover 滞回 | P0 |
| FR-B3 | 修复 ext-proc 客户端响应体 channel 满静默丢块问题（计量正确性前提） | P0 |
| FR-B4 | EPP 客户端生产化：TLS 证书校验（替换 InsecureSkipVerify）、多地址、连接复用 | P1 |

### 3.6 可观测性（FR-O）

| 编号 | 需求 | 优先级 |
|---|---|---|
| FR-O1 | 每 cluster 独立暴露：引擎版本/重载结果（`engine_reloads_total{cluster,result}`、`engine_current_version{cluster}`） | P0 |
| FR-O2 | 拉取组件健康：最近同步时间、失败计数、退避状态（discovery 与 config 各一份） | P0 |
| FR-O3 | 复用 llm-d 既有指标体系（调度/流控/端点指标），不改引擎内部埋点；指标统一由 metrics 端口（`--metrics-port`，默认 9090）的 `/metrics` 暴露 | P0 |
| FR-O4 | 双活跃告警数据源：Cell 按角色暴露活跃态指标（`cell_state{cluster, role, state}`） | P1 |
| FR-O5 | 健康检查走 gRPC health 协议（`grpc.health.v1`），作为 BFE failover 判活与就绪门控的唯一健康信号源（与 FR-B2 滞回判断对接）：独立健康端口（`--health-port`）与 ext-proc 主端口（`--grpc-port`）同时挂载 health 服务，BFE 探测数据地址即可拿到判活结果；gRPC 服务端 TLS 可配（`--grpc-tls-cert`/`--grpc-tls-key`，两者同配生效，缺省明文） | P0 |
| FR-O6 | pprof（`/debug/pprof/*`，引擎默认开启）提供显式开关；生产部署默认关闭，排障时开启 | P1 |

## 4. 非功能需求

| 编号 | 需求 | 指标/口径 |
|---|---|---|
| NFR-1 | 性能不低于原生 EPP 单实例水平 | 同配置同负载下，调度延迟、最大可持续 QPS 与端点规模上限无回退（demux 每请求一次原子 Load 的开销可忽略） |
| NFR-2 | 配置生效时效 | 配置提交到 ai-gateway-api 后，≤ 2×pollInterval 内该 cluster 新请求按新策略执行（默认 pollInterval 5s 时 ≤ 10s） |
| NFR-3 | 故障转移时效 | kill 主 EPP 后，BFE 在滞回冷却期（30~60s）内完成切换，该 cluster 请求成功率无持续跌落 |
| NFR-4 | 配置面故障韧性 | ai-gateway-api 完全不可用 30 分钟内，全部 cluster 服务无劣化（fail-static） |
| NFR-5 | 水平扩展 | **扩容 = 增加实例组**（每组 2 实例互备）；组加入后 ai-gateway-api 在组间再平衡 cluster，可承载 cluster 数随组数线性增长；单实例 cluster 数上限由资源规格决定并纳入分配器约束。**已有实例零接触**：分配经 InnerAPI 拉取生效，旧实例按角色转换（主→备=drain 保留数据）响应，不重启不滚动；切换窗口的双活跃按 FR-H4 接受语义处理 |
| NFR-6 | 上游兼容 | llm-d 模块以 go.mod 依赖引入，跟进上游 minor 版本升级的适配成本 ≤ 人天级 |
| NFR-7 | 引擎只读纪律 | 新代码库不 import `llm-d-router/cmd/...`；引擎包修改必须上游 PR 或被 CI 检查显式豁免 |
| NFR-8 | 管理端口暴露面收敛 | metrics 端口（/metrics + pprof）仅监听内网/管理网段，禁止公网可达；与 gRPC 业务端口、health 端口分离规划 |

## 5. 边界（Non-goals）

- **不做**：多 cluster 间的全局公平流控/全局配额（容量域按 cluster 隔离是有意设计）
- **不做**：脱离 BFE 的通用网关适配（ext-proc demux 依赖 BFE 注入 pool 名；Envoy/GIE 形态不是目标场景）
- **不做**：LLM 协议之外的推理协议适配（复用 llm-d parser 体系）
- **不做**：K8s 模式的继续演进（不维护 CRD 控制器路径；引擎内的 K8s 代码仅作为依赖存在，不进新仓库构建）
- **不做**：端点级静态加权路由（Weight 仅作注册/摘除开关，语义见 §3.1.1；比例分流由拆 cluster / 模型改写承担）
- **不承诺**：参数级热通道（FR-C6）覆盖所有配置项，仅权重/阈值/band 类

## 6. 约束与依赖

| 项 | 内容 |
|---|---|
| 引擎依赖 | `github.com/llm-d/llm-d-router`（Apache 2.0），pin 版本，禁 import 其 cmd/ 包；插件注册列表在新仓库组合根重建 |
| 数据源契约 | ai-gateway-api InnerAPI：`cluster_table`（已有）+ `epp_data/config`（新增，单端点含 `epp_config` 与 `assignment` 两段、同一 version 快照；契约见《EPP配置定义说明-epp_config.md》）。格式遵循 InnerAPI 统一信封约定：`ErrNum/ErrMsg/Data{Version, Config}`，`Data: null` 表示无变化，`Authorization: Token` 鉴权 |
| 网关契约 | BFE fork（本工作区 `bfe/`）的 ext-proc 客户端与 BalanceEpp；协议 metadata 约定 `llm-d.ai/inference-pool` |
| 启动前提 | spike 验证通过：新仓库仅用导出 API 可完成最小装配（datastore + discovery + scheduler + ext-proc server） |
| 团队约定 | 设计文档（本仓库 `docs/zh_cn/sys_design/`）为设计基线；改动目标需同步修订对应设计文档 |

## 7. 里程碑与验收

| 里程碑 | 内容 | 验收（对齐 G1-G6） |
|---|---|---|
| M0：spike | 新仓库最小装配跑通一个请求 | G6 成立；可行性确认（1-2 天） |
| M1：单 cluster 闭环 | discovery + epp_data/config 轮询 + 单 Cell 引擎 + BFE metadata demux 全链路 | G1、G2、G4（单 cluster 子集）、G6 |
| M2：热加载 | per-cell 引擎编译切换 + drain + 失败隔离 | G3、G5（主备子集：切换不丢已派发语义） |
| M3：多 cluster + 实例组主备 | Cell 生命周期 + 组分配与组内主实例下发 + BFE 按序 failover（第一期 2 实例互备组） | G4、G5 完整 |
| M4：生产加固 | P1 项（参数热通道、双活跃告警、备 Cell 热备、BFE 客户端生产化）+ NFR 全量压测 | NFR-1 ~ NFR-7 |

## 8. 关键决策摘要（详见设计文档）

| 决策 | 结论 | 出处 |
|---|---|---|
| 改造 vs 重写 | 保留引擎（~85% 库引用），重写组合根（~15%） | 《ai-gateway-epp系统设计.md》 |
| 代码库形态 | 新仓库 + go module 引用，非拷贝 | 《ai-gateway-epp系统设计.md》 |
| 发现源 | InnerAPI cluster_table 轮询，fail-static | 《详细设计-03-Poller与InnerAPI对接.md》 |
| 配置通道 | InnerAPI `epp_data/config`（epp_config + assignment 两段）+ per-cell 编译切换 | 《详细设计-02-Cell管理与热加载.md》 |
| demux | BFE ext-proc metadata 注入 pool 名 | 《详细设计-04-Demux与请求路径.md》 |
| 部署形态 | 实例池 + 每 cluster 主备；脑裂接受、滞回必须 | 《ai-gateway-epp系统设计.md》 |
| 扩展模型 | Cell 架构（单 EPP 多 cluster） | 《ai-gateway-epp系统设计.md》 |
