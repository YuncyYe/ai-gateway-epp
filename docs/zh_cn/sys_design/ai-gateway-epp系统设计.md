# ai-gateway-epp 系统设计文档

## 1. 概述

ai-gateway-epp 是面向 BFE + ai-gateway-api 部署形态的推理调度服务：以 go module 方式复用 llm-d EPP 引擎（调度/流控/协议），自研组合层实现**多 cluster Cell 化、配置中心拉取与热加载、实例组主备**三大能力。需求基线见《ai-gateway-epp需求分析.md》（G1-G6），本文档是其系统设计。

**与上游的边界**：`github.com/llm-d/llm-d-router` 的 pkg/ 引擎只读引用，不 import 其 cmd/；插件注册在新仓库组合根重建（`cmd/epp/plugins.go`）。

## 2. 总体架构

```
                    ai-gateway-api（配置权威）
                    ┌────────────────────────────────────────┐
                    │ InnerAPI 导出:                           │
                    │  /configs/gslb_data/cluster_table   已有  │ ──┐
                    │  /configs/tls_conf/server_data_conf 已有  │   │ 同轮/version 增量/Token
                    │  /configs/epp_data/picker_config    新增  │   │ 拉取，fail-static
                    │  /configs/epp_data/assignment       新增  │ ──┘
                    └──────────────────┬─────────────────────┘
        ┌──────────────────┬───────────┼───────────┬──────────────────┐
        │                  │           │           │                  │
   ┌────▼────┐        ┌────▼────┐ ┌────▼────┐ ┌────▼────┐        ┌────▼────┐
   │  BFE    │ ext-proc│  epp-A  │ │  epp-B  │ │  epp-C  │        │  epp-D  │
   │cluster-x│◀───────▶│(组g1)   │ │(组g1)   │ │(组g2)   │        │(组g2)   │
   │         │ 注入pool│ 主:x,y  │ │ 备:x,y  │ │ 主:u,v  │        │ 备:u,v  │
   │         │ 名metadata        └─────────┘ └─────────┘        └─────────┘
   └─────────┘
```

**EPP 进程内部结构**（单实例）：

```
┌─────────────────────────── EPP 进程 ───────────────────────────┐
│ gRPC ext-proc server（共享，Engine 指针经 atomic.Pointer 派发） │
│                                                                │
│ PoolDemux：metadata["llm-d.ai/inference-pool"] → CellKey       │
│                                                                │
│ CellManager：map[CellKey]*Cell  生命周期=创建/就绪/drain/销毁    │
│   Cell = { 数据面: datastore + 采集 goroutine（常驻）            │
│            政策面: Engine 指针（可热更）                        │
│            角色: primary(服务态) / standby(热数据冷准入) }        │
│                                                                │
│ Poller ×3（共享 innerapi client）：                              │
│   ClusterDiscovery  → cluster_table  → 全量 diff → 各 Cell 端点  │
│   ConfigPoller      → picker_config  → per-cell 引擎编译切换     │
│   AssignmentWatcher → assignment     → 本实例角色/分配变更       │
│                                                                │
│ 常驻：跨副本同步（可选）、/metrics + pprof、gRPC health          │
└────────────────────────────────────────────────────────────────┘
```

关键边界：**数据面（datastore/采集）常驻于 Cell，政策面（Engine）经原子指针热切换**——这是热加载不丢端点状态的根本原因。

## 3. 模块设计

### 3.1 组合根（cmd/epp）

- 只做三件事：进程级配置解析（apiAddr/token/实例身份/端口）→ 拉起 Poller 与 CellManager → 启动 gRPC/metrics/health server
- **不含**调度/流控装配逻辑（那是 Cell 内的事）；参照 `llm-d-router/cmd/epp/runner.go:1055-1202` 的装配顺序，按 Cell 化重组
- 插件注册列表在此重建（参照 `runner.go:725-729`），只注册无 K8s 依赖的内置插件 + 自研 `cluster-table-discovery`

### 3.2 Cell 管理器（pkg/cell）

```go
type CellKey string // = cluster 名

type Cell struct {
    ds      datastore.Datastore          // 数据面，常驻
    engine  atomic.Pointer[Engine]       // 政策面，热切换
    role    atomic.Value[Role]           // primary / standby
    draining atomic.Bool
}
```

- **创建**：AssignmentWatcher 发现新分配 → new datastore + discovery notifier 接线 → 等 ClusterDiscovery 首同步与 ConfigPoller 首编译 → Ready
- **角色转换矩阵**（需求文档 §3.4.1）：主→备=停止准入（drain）保留数据；备→主=开闸即服务；无分配=drain 后销毁
- **drain 语义**：旧 Engine 的流控只出不进，排空条件=队列空且 in-flight 归零，超时强杀；饱和度估计值传给新 Engine 防切换瞬间超发；`DeleteFlowControlFlowSeries` 清理指标

### 3.3 Demux（pkg/demux）

- 挂在 gRPC `Process()` 入口：读 ext-proc metadata（`llm-d.ai/inference-pool`，BFE 注入，见 FR-B1）→ `CellManager.Get(key)`
- 未命中/缺 metadata → 可重试错误 + 指标；可配 default-pool 兜底
- `RequestContext.PoolKey` 贯穿响应阶段（指标 pool label 的数据源）
- demux 层对 panic 做 recover，单 Cell 崩溃不拖垮进程（FR 风险 1 的缓解）

### 3.4 Poller 框架（pkg/innerapi + pkg/poller）

三个轮询器共享一个 client（鉴权 Token、version 增量、`Data: null` 处理、指数退避、失败指标）：

| Poller | 数据源 | 消费动作 |
|---|---|---|
| ClusterDiscovery | cluster_table | 解析全量 `Config[cluster]`，per-cluster diff（增/改/删实例），按 cluster 名分发到各 Cell 的 datastore（EndpointDiscovery notifier 语义：单 goroutine 顺序 Upsert/Delete） |
| ConfigPoller | picker_config | 取本实例被分配 cluster 的配置，per-cell 编译（复用 loader 严格解码/DAG 实例化/层序校验）→ 校验通过原子切换，失败该 cell 保留旧引擎 |
| AssignmentWatcher | assignment | 本实例角色视图 `{cluster → primary|standby}` 的变更 → 驱动 Cell 创建/角色转换/销毁 + 就绪上报 |

一致性规则：Cell 就绪需**双源齐备**（assignment 有分配 + discovery/config 数据到达）；只有 picker_config 无 assignment → 跳过 + 告警（需求 §6 风险 5）。

### 3.5 cluster-table-discovery 插件

- 实现 llm-d `EndpointDiscovery` 接口（`discovery.go:54-82`），注册为 `cluster-table-discovery`
- 注意职责划分：**插件实例属于 Cell 内**（每 Cell 一个，poller 层只负责取数与分发；或反过来 poller 直连 datastore notifier——实现时二选一，以"插件接口零侵入复用"优先）
- Weight=0 视为摘除（语义见需求 §3.1.1）；端点 ID = `{Namespace: pool名, Name: BackendConf.Name}`，MetricsHost = Addr:Port

### 3.6 热加载（Engine 编译-切换）

```
ConfigPoller 发现 cluster 配置变化
  → 编译：rawConfig + cell.ds → Engine（plugins 实例化 → profile 组装 → FC 参数注入）
  → 校验失败：engine_reloads_total{cluster,result=invalid}++，保留旧引擎
  → 校验通过：cell.engine.Store(new)；engine_reloads_total{cluster,result=success}++；engine_current_version{cluster} 更新
  → 旧引擎 drain（§3.2）
```

第二期加参数热通道（FR-C6）：结构不变时直接写活插件的原子参数，不重建引擎。

## 4. 关键时序

### 4.1 请求路径

```
BFE ── ext-proc RequestHeaders(+metadata pool=cluster-x) ──▶ EPP
  PoolDemux → Cell-x → Director.HandleRequest
    → model rewrite → FC Admit（排队，Cell 级容量域）
    → candidates（Cell-x datastore）→ Scheduler → 选中 Addr:Port
  ◀── dynamic metadata x-gateway-destination-endpoint ── BFE 直连引擎
BFE ── ResponseHeaders/Body(流式) ──▶ EPP（token 计量回收，派发标记）
```

### 4.2 配置热加载

见 §3.6。全程该 cluster 请求无失败（新请求走新引擎指针），其他 cluster 完全不受影响。

### 4.3 主备 failover

```
kill epp-A（cluster-x 的主）
  → BFE 健康检查（gRPC health，滞回：冷却 30~60s + 连续 N 次通过）
  → BFE 切 epp-B；未派发请求重试到 B，已派发不盲目重试（FR-H3）
  → epp-B 上 Cell-x 备→主：开闸即服务（热数据冷准入，FR-H5）
ai-gateway-api（可选第二期主动检测）→ assignment 翻转 → A 恢复后按矩阵降级为备
```

### 4.4 扩容（加组）

新组 {C,D} 启动 → 注册到 ai-gateway-api → assignment 下发 → C/D 建 Cell（先备角色同步数据）→ 分配器确认就绪后翻转部分 cluster 主角色 → A/B 对应 Cell 主→备 drain 保留数据 → BFE 经 server_data_conf 拿到新 EPPAddr 顺序。**已有实例零接触**（NFR-5）。

## 5. 配置模型（三个拉取接口）

| 接口 | 消费者 | 内容 | 契约文档 |
|---|---|---|---|
| cluster_table（已有） | EPP | 全量实例列表 | ai-gateway-api InnerAPI 既有定义 |
| server_data_conf.GslbBasic.EPPAddr（已有字段补语义） | BFE | 有序 `[主, 备]` 地址（`[0]`=主、`[1]`=备；BFE 按序消费 + 滞回，全部不可用回退本地均衡） | —（BFE 侧契约，EPP 不消费） |
| picker_config（新增） | EPP | `map[cluster]EndpointPickerConfig` | 《EPP配置定义说明-picker_config》（`docs/zh_cn/configuration/`） |
| assignment（新增） | EPP | 本实例 `{cluster → 角色}` + 就绪上报 | 待编写（见 §3.4） |

变更全部走 version 增量 + Data-null；EPP 侧 fail-static。

## 6. 错误处理与边界

| 故障 | 行为 |
|---|---|
| ai-gateway-api 不可用 | 三 Poller 各自退避；实例/配置/分配全部沿用最后已知状态（NFR-4：30 分钟无劣化） |
| 配置编译失败 | per-cell 隔离，旧引擎服务，指标告警（FR-C4） |
| BFE 未注入/错误 pool 名 | 拒绝 + 指标；灰度期 BFE 双写老链路 |
| 主 EPP 故障 | BFE 滞回切换备；无备（测试组）降级本地均衡 |
| 单 Cell panic | demux 层 recover，该请求失败，进程与其他 Cell 不受影响 |

## 7. 可观测性

- **端口**：gRPC 业务端口（ext-proc）/ gRPC health 端口 / metrics 端口（`/metrics` + pprof，内网收敛，NFR-8）
- **新增指标**：`engine_reloads_total{cluster,result}`、`engine_current_version{cluster}`、poller 三件套（last_sync_timestamp/failures/backoff_state）、Cell 角色活跃态（双活跃告警数据源 FR-H4）
- **复用**：llm-d 全部既有指标（调度/流控/端点/请求），不改引擎埋点（FR-O3）

## 8. 性能与扩展

- 请求路径新增开销仅 demux 一次原子 Load（NFR-1）；调度/流控与原生 EPP 同构
- 单实例容量 = f(cluster 数, 端点数, QPS)；超限即加组（NFR-5），组为扩容/故障域/反亲和的天然边界
- cluster_table 全量响应的 diff 成本 O(总实例数)，轮询间隔默认 5s 无压力；超大部署可调 15s（NFR-2 时效权衡）

## 9. 安全

- InnerAPI Token 经进程配置注入，配置分发渠道保密（与 ai-gateway-api 其他消费者同级）
- BFE↔EPP gRPC 走 TLS（FR-B4：证书校验 + 连接复用）
- pprof 默认关闭、生产开启需显式开关（FR-O6）；metrics 端口仅内网（NFR-8）

## 10. 里程碑（对应需求 §7）

| 里程碑 | 设计要点落点 |
|---|---|
| M0 spike | §3.1 最小装配：datastore + file-discovery + scheduler + ext-proc server，验证引擎可库化（G6） |
| M1 单 cluster 闭环 | §3.4 ClusterDiscovery/ConfigPoller、§3.5 插件、§4.1 链路 |
| M2 热加载 | §3.6、§3.2 drain |
| M3 多 cluster + 实例组 | §3.2 角色矩阵、§3.3 demux、§4.3/4.4、server_data_conf EPPAddr 语义 |
| M4 生产加固 | §7 告警齐套、§9 安全、FR-C6 参数通道、FR-B4 |

## 11. 参考文档索引

| 文档 | 内容 |
|---|---|
| 《ai-gateway-epp需求分析.md》（同目录） | 目标 G1-G6、FR/NFR 全量、边界 |
| 《EPP配置定义说明-picker_config.md》（`../configuration/`） | picker_config 接口契约 |
| 《详细设计/README.md》（`详细设计/`） | 实现层展开：包结构 / Cell 热加载 / Poller / Demux / 可观测与测试 |
| 上游 `github.com/llm-d/llm-d-router` 源码 | 引擎内部机制（调度/流控/数据层/插件/协议）；本仓库经 go module 引用 |
