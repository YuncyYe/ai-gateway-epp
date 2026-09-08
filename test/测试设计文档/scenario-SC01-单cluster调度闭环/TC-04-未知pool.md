# TC-04 未知 pool

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-04 / 未知 pool |
| 所属场景 | SC01 单 cluster 调度闭环 |
| 版本 | v0.1 |

## 测试目的

验证 metadata 指向未在 assignment 中分配的 cluster（未知 pool）时，ext-proc 返回错误（fail-closed），且不影响已分配 cluster 的正常调度。

## 运行模式

单组件进程组：epp（真实进程）×1（instance-id `epp-sc01-tc04`）+ inference-sim（真实进程）×1（逻辑名 `a0`）+ ai-gateway-api mock + BFE 由测试代码扮演。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用。
2. mock API 已配置 assignment 全量视图（仅 `cluster-a`（本实例 primary），**不含** `cluster-ghost`）、epp_config（cluster-a 最小配置）、cluster_table（1 个 sim 后端 `a0`，Weight=50）。
3. epp 启动并就绪（health=SERVING）。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 建立 ext-proc gRPC stream，metadata 带 `llm-d.ai/inference-pool = "cluster-ghost"`（未分配的 pool 名），随后发送 RequestBody（chat 请求） | 调度调用返回错误（流上无 `x-gateway-destination-endpoint`） |
| 2 | 对 `cluster-a` 执行一次正常的 ext-proc 调度（metadata pool = `cluster-a`） | 返回非空 `x-gateway-destination-endpoint` 且等于 `a0` 的 sim 地址，已分配 cluster 不受影响 |

## 预期结果

- 全部步骤通过：未知 pool 请求被拒绝，合法 cluster 调度不受影响。
- epp 的 stderr 日志无 error 级记录（fail 时 harness 自动 dump）。

## 清理

`defer e.Close()`：Kill epp/sim 进程并 Wait，关闭 mock API，日志目录 `t.TempDir` 自动清理。
