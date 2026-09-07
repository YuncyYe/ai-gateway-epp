# 详细设计-04：Demux 与请求路径

## 1. metadata 约定

| 项 | 值 |
|---|---|
| namespace | `llm-d.ai` |
| key | `inference-pool` |
| 值 | BFE cluster 名（= EPP CellKey = cluster_table 的 key，三处同名） |
| 注入方 | BFE ext-proc 客户端（`chooseBackendFromEPP` 构造 ProcessingRequest 时附带 `bal.name`，FR-B1） |
| 读取点 | gRPC `Process()` 入口（每个 RequestHeaders 消息） |

## 2. 数据结构

```go
package demux

const MetadataNamespace = "llm-d.ai"
const PoolMetadataKey   = "inference-pool"

type Router interface {
    Route(pool string) (*cell.Cell, error) // ErrUnknownPool / ErrNoPoolMetadata
}

// Server 是多 Cell 版 ext-proc 处理器。
type Server struct {
    router   Router                          // → cell.Manager
    fallback *fwkserver.ExtProcServerRunner  // 单 pool 兜底（DefaultPool 场景直连引擎）
}
```

**复用粒度注记**：不复用 llm-d `handlers.StreamingServer` 整体（其持单 ds/director，server.go:67 构造即绑定），而是复用其内部阶段处理逻辑（RequestHeaders/Body 状态机、response 收尾）组装多 Cell 版本——具体复用粒度（整体包一层 vs 提取处理函数）在 spike 中确定（详见详细设计-05 §5）。

`RequestContext` 增加 `PoolKey` 字段并贯穿响应阶段：请求路径指标的 pool label 数据源（FR-M4）。

## 3. 请求处理算法

```
Process(stream):
  pool, ok = metadata(stream, RequestHeaders)["llm-d.ai"]["inference-pool"]
  if !ok:
      pool = cfg.DefaultPool
      if pool == "": return ErrNoPoolMetadata（可重试）+ metrics demux_errors_total{reason=no-pool}
  c, ok = router.Route(pool)
  if !ok: return ErrUnknownPool + metrics demux_errors_total{reason=unknown-pool}
  defer recover():  单请求 panic 转错误返回，进程与其他 Cell 存活（故障域隔离）
  eng = c.engine.Load()
  if c.role != primary: return ErrCellDraining（可重试，BFE 应重试到备）+ metrics

  ── 以下为 llm-d 标准阶段处理，全部组件来自 eng 与 c.ds ──
  RequestHeaders: 提取 trace/context → 等 RequestBody
  RequestBody:    parser 解析模型 → model rewrite → FC Admit（排队，Cell 级容量域）
                → candidates（c.ds）→ Scheduler → 选中 Addr:Port
                → 响应 dynamic metadata x-gateway-destination-endpoint(+scores)
  ResponseHeaders/Body(流式): token 计量回收；派发标记写 metadata（FR-H3 的 BFE 重试依据）
```

请求路径时序全景（含 BFE 侧）见《ai-gateway-epp系统设计.md》§4.1。

## 4. 请求面错误语义

| 错误 | 条件 | BFE 侧表现 |
|---|---|---|
| ErrNoPoolMetadata | metadata 缺 pool 且未配 DefaultPool | ext-proc 错误 → BFE 记日志 + 回退本地均衡 |
| ErrUnknownPool | pool 无对应 Cell（未分配/未就绪） | 同上；CREATING 期间的瞬态 |
| ErrCellDraining | Cell 处于 demote/drop 过渡 | 可重试，failover 场景重试到备 |
| panic（单 Cell） | demux recover 捕获 | 该请求失败，进程存活 |

**降级总原则**：demux 层任何失败都不阻断 BFE——BFE 既有 `BalanceEpp` 回退语义（`reverseproxy.go:345-355`，EPP 失败回退本地负载均衡）是所有请求面错误的最终兜底。
