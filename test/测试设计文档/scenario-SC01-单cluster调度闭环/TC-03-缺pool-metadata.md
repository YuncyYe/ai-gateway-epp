# TC-03 缺 pool metadata

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-03 / 缺 pool metadata |
| 所属场景 | SC01 单 cluster 调度闭环 |
| 版本 | v0.1 |

## 测试目的

验证请求未携带 `llm-d.ai/inference-pool` metadata 且未配置 default-pool 时，ext-proc 返回错误而非随机路由（FR-M2 的 fail-closed 分支），且该错误不拖垮 epp 进程与其余 cluster 的正常调度。

## 运行模式

单组件进程组：epp（真实进程）×1（instance-id `epp-sc01-tc03`）+ inference-sim（真实进程）×1（逻辑名 `a0`）+ ai-gateway-api mock + BFE 由测试代码扮演。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用。
2. mock API 已配置 assignment（cluster-a=primary）、picker_config（cluster-a 最小配置）、cluster_table（1 个 sim 后端 `a0`，Weight=50）。
3. epp 启动未配置 default-pool，health=SERVING。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 建立 ext-proc gRPC stream，RequestHeaders 的 metadata 中 `llm-d.ai/inference-pool` 为空串（即不携带有效 pool 名），随后发送 RequestBody（chat 请求） | 调度调用返回错误（流上无 `x-gateway-destination-endpoint`） |
| 2 | 错误发生后检查 epp 健康状态（health 轮询，5s 超时） | epp 进程存活，health 仍为 SERVING |
| 3 | 对 `cluster-a` 执行一次正常的 ext-proc 调度（metadata pool = `cluster-a`） | 返回非空 `x-gateway-destination-endpoint` 且等于 `a0` 的 sim 地址，正常 cluster 调度不受影响 |

## 预期结果

- 全部步骤通过：缺 metadata 请求被拒绝，进程健康，合法 pool 调度恢复正常。
- epp 的 stderr 日志无 error 级记录（fail 时 harness 自动 dump）。

## 清理

`defer e.Close()`：Kill epp/sim 进程并 Wait，关闭 mock API，日志目录 `t.TempDir` 自动清理。
