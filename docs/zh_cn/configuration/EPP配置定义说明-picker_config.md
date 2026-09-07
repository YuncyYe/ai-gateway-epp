# ai-gateway-epp 配置定义说明（picker_config）

本文档定义 ai-gateway-epp 通过 ai-gateway-api InnerAPI `picker_config` 接口下发给 EPP 的配置结构。配置形状与 llm-d `EndpointPickerConfig`（`llm-d-router/apix/config/v1alpha1/endpointpickerconfig_types.go`）保持一致——EPP 侧复用其严格解码与校验管线，ai-gateway-api 侧可直接以该类型做 Schema 校验。

## 1. InnerAPI 承载格式

接口约定遵循 `cluster-table.md` 同款规范（version 增量、`Data: null` 表示无变化、Token 鉴权）：

```
GET /configs/epp_data/picker_config?version=<上次版本号>
```

```json
{
    "ErrNum": 200,
    "ErrMsg": "success",
    "Data": {
        "Version": "20260906120000",
        "Config": {
            "cluster-a": { /* EndpointPickerConfig，见 §3 */ },
            "cluster-b": { /* EndpointPickerConfig */ }
        }
    },
    "WorkMode": "ModeNormal"
}
```

- `Config` 是 `map[cluster名]EndpointPickerConfig`，cluster 名与 BFE cluster 名、cluster_table 的 key 完全一致
- EPP 只消费自己被分配（主/备）的 cluster 的配置；某 cluster 出现在 picker_config 但不在分配中 → 跳过并告警
- 配置未变化时 `Data: null`；EPP 拉取失败时 fail-static，沿用旧配置

**范围声明**：本文档只定义 cluster 的**调度策略配置**（"怎么调度"）。cluster 的主/备分配（"由哪个 EPP 服务"，`cluster → {group, primary}`）**不属于本文档**，由独立通道 `epp_endpoints` 下发（BFE 消费 failover 顺序，EPP 消费 Cell 角色），见需求文档 FR-H1/§3.4.1。两个配置的变更频率与消费方不同，刻意分离。

## 2. 配置的角色与生效方式

每 cluster 的配置独立编译为一个调度引擎（Engine）：**plugins 实例化 → 调度 profile 组装 → 流控参数注入**，编译成功后原子切换（详见《EPP对接ai-gateway-api配置中心与热加载方案》）。因此本配置是**声明层 API**：ai-gateway-api 只保证结构合法（JSON Schema），运行期正确性（插件引用存在、DAG 无环、层序约束）由 EPP 编译期校验兜底——编译失败该 cluster 沿用旧引擎，不影响其他 cluster。

## 3. EndpointPickerConfig 顶层结构

```jsonc
{
  "featureGates":   ["flowControl", "someGate=false"],   // 可选，特性开关
  "plugins":        [ /* 必填，插件实例声明 */ ],
  "schedulingProfiles": [ /* 必填，调度策略组合 */ ],
  "dataLayer":      { /* 数据层：发现源与采集 */ },
  "flowControl":    { /* 流控 */ },
  "requestHandler": { /* 请求解析 */ },
  "saturationDetector": { /* 已废弃，用 flowControl.saturationDetector */ },
  "parser":         { /* 已废弃，用 requestHandler.parsers */ }
}
```

| 字段 | 必填 | 说明 |
|---|---|---|
| featureGates | 否 | 特性开关列表，`"name"`/`"name=true"`/`"name=false"`，省略时用各 gate 注册默认值。**注意：含 `AllowExperimentalPlugins` 语义时不可进程内热更**（引擎只读纪律的既定边界） |
| plugins | **是** | 插件实例声明列表，全配置的核心（见 §4） |
| schedulingProfiles | **是** | 调度 profile 列表（见 §5） |
| dataLayer | 条件 | 数据层配置；使用新版数据层时必填（见 §6） |
| flowControl | 否 | 流控配置，仅 `flowControl` gate 开启时生效（见 §7） |
| requestHandler | 否 | 请求解析插件指定，缺省用默认 parser（见 §8） |
| saturationDetector / parser | 否 | **已废弃字段**，分别由 `flowControl.saturationDetector`、`requestHandler.parsers` 取代；两者同时设置时新字段优先 |

## 4. plugins：插件实例声明

```jsonc
{
  "plugins": [
    { "name": "my-filter", "type": "least-load-filter", "parameters": { /* 插件自定义 */ } },
    { "name": "kv-scorer", "type": "kv-cache-utilization-scorer", "parameters": { "weight": 0.8 } }
  ]
}
```

| 字段 | 必填 | 说明 |
|---|---|---|
| type | **是** | 插件类型，须在 EPP 插件注册表中存在（内置清单见《EPP代码分析/04-插件框架》§4.6；自定义插件经新仓库组合根注册） |
| name | 否 | 实例名，供 `pluginRef` 引用；省略时取 type 值 |
| parameters | 否 | 插件自定义参数，**由插件工厂自行解码**（`json.RawMessage`，严格模式：未知字段拒绝）；各插件参数定义见其自身文档 |

引用约束（EPP 编译期校验）：

- 所有 `pluginRef` 必须指向 plugins 列表中已声明的实例名
- 插件间初始化依赖经工厂声明的依赖关系做 DAG 拓扑排序，成环即编译失败
- 被引用但未声明的常用插件由系统默认值自动注入（如 saturation detector 默认 `utilization-detector`）

本文示例涉及的内置插件参数形状（严格解码，未知字段拒绝；均以各插件源码为准）：

| 插件 type | 参数形状 |
|---|---|
| `utilization-filter` | `{ "conditions": [ { "metric": "<active-requests\|running-requests\|waiting-queue\|kv-cache-utilization>", "maxValue": 0.9 } ], "fallbackOnEmpty": true }` |
| `kv-cache-utilization-scorer` / `queue-scorer` | `{}`（无参数） |
| `max-score-picker` | `{}` |
| `utilization-detector` | `{}` |
| `openai-parser` | `{}` |
| `cluster-table-discovery`（自研） | `{ "clusterName": "..." }`（可省略，默认取请求 demux 到的 pool 名） |

## 5. schedulingProfiles：调度策略组合

```jsonc
{
  "schedulingProfiles": [
    {
      "name": "default",
      "plugins": [
        { "pluginRef": "my-filter" },
        { "pluginRef": "kv-scorer", "weight": 2.0 },
        { "pluginRef": "max-score-picker" }
      ]
    }
  ]
}
```

| 字段 | 必填 | 说明 |
|---|---|---|
| name | **是** | profile 名；多 profile 时由 ProfileHandler 插件按模型/请求特征选择（见《EPP代码分析/03-调度系统》） |
| plugins | **是** | 插件槽位列表：filter / scorer / picker / profile-handler 按插件类型自动归槽；**`weight` 仅对 scorer 生效**，为打分组合权重（加权平均归一化） |
| 执行顺序 | — | Filter → Score（按 weight 加权）→ Pick；插件在槽内按声明顺序执行；跨插件数据依赖走 Producer/Consumer DAG |

单个 cluster 至少要有一个可用 profile；典型最小集：1 个 utilization filter + 1~2 个 scorer + max-score-picker。

## 6. dataLayer：数据层

```jsonc
{
  "dataLayer": {
    "injectDefaults": true,
    "discovery": { "endpoints": { "pluginRef": "cluster-table-discovery" } },
    "sources": [ { "pluginRef": "prometheus-source", "extractors": [ { "pluginRef": "vllm-extractor" } ] } ],
    "crossReplicaSyncerPluginRef": "memory-syncer",
    "crossReplicaSyncInterval": "1s"
  }
}
```

| 字段 | 必填 | 说明 |
|---|---|---|
| discovery.endpoints.pluginRef | 是（本项目） | 端点发现插件。本项目固定使用 `cluster-table-discovery`（ai-gateway-epp 自研，对接 cluster_table InnerAPI）；**设置后 EPP 完全绕过 K8s CRD reconciler**（`endpointpickerconfig_types.go:267-273`） |
| discovery.peers | 否 | 对端 EPP 发现插件，实例组互备场景第二期引入 |
| injectDefaults | 否 | 默认 true：自动注入默认指标 source/extractor；false 则全部手动声明 |
| sources[].pluginRef / extractors[].pluginRef | 否 | 指标采集源与提取器（source → extractor 一对多）；缺省由 injectDefaults 补齐 |
| crossReplicaSyncerPluginRef | 否 | 跨副本同步插件；实例组内成员间共享端点状态时用（采集去重，见《EPP代码分析/02-数据层与状态同步》§2.6） |
| crossReplicaSyncInterval / crossReplicaPublishTimeout | 否 | 发布节奏与单次发布超时，缺省用系统默认 |

## 7. flowControl：流控

```jsonc
{
  "flowControl": {
    "maxRequests": "1000", "maxBytes": "2Gi",
    "defaultRequestTTL": "30s", "noEndpointRequestTTL": "10m",
    "enableEviction": true,
    "saturationDetector": { "pluginRef": "utilization-detector" },
    "usageLimitPolicyPluginRef": "static-usage-limits",
    "defaultPriorityBand":  { "maxRequests": "500", "maxBytes": "1Gi" },
    "defaultNegativePriorityBand": { "maxRequests": "50" },
    "priorityBands": [
      { "priority": 2, "maxRequests": "300", "fairnessPolicyRef": "global-strict-fairness-policy" },
      { "priority": 1, "maxRequests": "200", "fairnessPolicyRef": "round-robin-fairness-policy",
        "orderingPolicyRef": "fcfs-ordering-policy" }
    ]
  }
}
```

| 字段 | 必填 | 说明 |
|---|---|---|
| maxRequests / maxBytes | 否 | 全局并发上限（跨全部优先级带）；Kubernetes Quantity 格式（`"100"`、`"1k"`、`"1Gi"`），缺省或 "0" 表示不限 |
| defaultRequestTTL | 否 | 池**有端点**时的排队预算，默认 60s；超期以可重试背压错误拒绝。显式 "0s" 禁用驱逐（等客户端断开） |
| noEndpointRequestTTL | 否 | 池**无端点**（冷启动扩容）时的排队预算，默认跟随 defaultRequestTTL； regime 切换时重新起算（语义细节见《EPP代码分析/05-流控系统》§4） |
| priorityBands | 否 | 显式优先级带；未声明的优先级回落 defaultPriorityBand 模板 |
| defaultPriorityBand / defaultNegativePriorityBand | 否 | 默认带模板；负优先级单独模板用于"可牺牲流量"（小容量，饱和时快速拒绝） |
| usageLimitPolicyPluginRef | 否 | 容量自适应策略插件；缺省静态策略（threshold=1.0，不门控） |
| saturationDetector.pluginRef | 否 | 饱和度检测插件，默认 `utilization-detector` |
| enableEviction | 否 | 需求驱动驱逐：高优先级被饱和阻塞时终止负优先级在飞请求回收容量，默认 false |

**PriorityBandConfig**（priorityBands / default 系列共用）：

| 字段 | 必填 | 说明 |
|---|---|---|
| priority | 是（priorityBands 内） | 整型优先级，越大越高 |
| maxBytes / maxRequests | 否 | 该带容量上限；缺省/"0" 用系统默认（1G / 5000），**带级上限恒存在**，要"不限"须显式设大值 |
| fairnessPolicyRef | 否 | 流间公平策略，默认 `global-strict-fairness-policy` |
| orderingPolicyRef | 否 | 流内排序策略，默认 `fcfs-ordering-policy` |

## 8. requestHandler：请求解析

```jsonc
{ "requestHandler": { "parsers": [ { "pluginRef": "openai-parser" } ] } }
```

- `parsers[].pluginRef`（必填于条目内）：解析协议消息（模型名、token 估算）的 parser 插件，默认 `openai-parser`；多 parser 按 path 最长后缀匹配选择
- 模型改写可不在配置中声明，由客户端 header `x-llm-d-model-name-rewrite` 驱动（运行时替代已废弃的 CRD 通道）

## 9. 类型与格式约定

| 类型 | 格式 | 示例 |
|---|---|---|
| resource.Quantity | Kubernetes 资源量（int64 或二进制 SI） | `"100"`、`"1k"`、`"500M"`、`"1Gi"` |
| metav1.Duration | Go duration 字符串 | `"30s"`、`"10m"`、`"0s"`（显式禁用语义） |
| json.RawMessage | 插件 parameters 原样透传，插件自行严格解码 | 见各插件文档 |
| bool/int/string | 常规 JSON | — |

## 10. 校验与错误语义

| 阶段 | 校验方 | 行为 |
|---|---|---|
| 结构校验 | ai-gateway-api（JSON Schema，即本文件的类型形状） | 结构不合法拒绝提交 |
| 引用完整性 / DAG 无环 / 层序约束（FlowControl < RequestControl < Scheduling） | EPP 编译期 | 编译失败：该 cluster 沿用旧引擎，`engine_reloads_total{cluster,result=invalid}` 递增 |
| Alpha 稳定性插件 | EPP 编译期 | 未显式允许则拒绝（引擎只读纪律边界） |

## 11. 版本与演进

- 配置形状跟随上游 `apix/config/v1alpha1`；上游新增字段经升级 go.mod 自动获得，ai-gateway-api 的 Schema 校验同步跟进
- 上游标记 Deprecated 的字段（`saturationDetector`、`parser`）在 ai-gateway-api **新建配置中禁止使用**，存量迁移后由 EPP 编译期告警
- 每 cluster 配置独立演进，互不影响——这是 per-cell 编译架构的属性

## 12. 完整示例

```json
{
  "Version": "20260906120000",
  "Config": {
    "llm-cluster-a": {
      "featureGates": ["flowControl"],
      "plugins": [
        { "name": "ep-discover", "type": "cluster-table-discovery",
          "parameters": { "clusterName": "llm-cluster-a", "pollInterval": "5s" } },
        { "name": "util-filter", "type": "utilization-filter",
          "parameters": { "conditions": [ { "metric": "kv-cache-utilization", "maxValue": 0.9 } ] } },
        { "name": "kv-scorer", "type": "kv-cache-utilization-scorer",
          "parameters": {} },
        { "name": "queue-scorer", "type": "queue-scorer",
          "parameters": {} },
        { "name": "max-score", "type": "max-score-picker", "parameters": {} },
        { "name": "util-detector", "type": "utilization-detector", "parameters": {} }
      ],
      "schedulingProfiles": [
        { "name": "default",
          "plugins": [
            { "pluginRef": "util-filter" },
            { "pluginRef": "kv-scorer", "weight": 1.0 },
            { "pluginRef": "queue-scorer", "weight": 0.5 },
            { "pluginRef": "max-score" }
          ] }
      ],
      "dataLayer": {
        "discovery": { "endpoints": { "pluginRef": "ep-discover" } }
      },
      "flowControl": {
        "defaultRequestTTL": "30s",
        "noEndpointRequestTTL": "10m",
        "enableEviction": true,
        "saturationDetector": { "pluginRef": "util-detector" },
        "priorityBands": [
          { "priority": 1, "maxRequests": "200" }
        ]
      },
      "requestHandler": { "parsers": [ { "pluginRef": "openai-parser" } ] }
    }
  }
}
```

注：`cluster-table-discovery` 为 ai-gateway-epp 自研插件（见《EPP对接InnerAPI-cluster-table改造方案》），其 parameters（apiAddr/token 等）在进程级配置给出，cluster 级配置只需引用插件名——上例中 `clusterName` 亦可省略（默认取 demux 到的 pool 名）。
