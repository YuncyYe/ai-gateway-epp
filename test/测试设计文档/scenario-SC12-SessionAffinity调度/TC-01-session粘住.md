# TC-01 session 粘住

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-01 / session 粘住 |
| 所属场景 | SC12 SessionAffinity 调度 |
| 版本 | v0.1 |

## 测试目的

验证 session-affinity-scorer（session_id 策略）端到端生效：带 `x-session-id` 的请求首次调度后把 session 绑定到被选端点，后续同 session 请求稳定粘住该端点；不带 session header 的请求不受影响（scorer 弃权，picker 自由选端点，无错误）。

## 运行模式

单组件进程组：epp（真实进程）×1 + inference-sim（真实进程）×2 + ai-gateway-api mock + BFE 由测试代码扮演。harness、mock 与二进制构建见总体说明 §1-§2。

## 前置条件

1. mock API 已配置 assignment（cluster-a=primary）、cluster_table（cluster-a 下 sub-1 挂 2 个 sim 后端 a0/b0）。
2. picker_config 下发的调度配置：`cluster-table-discovery`（clusterName=cluster-a）+ `session-affinity-scorer`（strategy=session_id，sessionIdConfig.sources=[{header: "x-session-id"}]）+ `max-score-picker`；调度 profile 引用 sa-scorer + max-score；dataLayer discovery 引用 ep-discover；requestHandler 使用 openai-parser。
3. epp 启动并就绪。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 发送带 header `x-session-id: sess-1` 的 ext-proc 请求（RequestHeaders，dynamic metadata 带 `llm-d.ai/inference-pool = "cluster-a"`，path `/v1/chat/completions`，body 为 OpenAI chat 请求，`"max_tokens": 8`） | 成功返回目标端点 |
| 2 | 对 sess-1 连续 pick 共 8 次（以首次为锚点要求整 burst 一致，未收敛则 100ms 重试，观察窗口 30s） | 8 次全部命中同一端点（`stick(sess-1, 8)` 收敛） |
| 3 | 校验粘住端点属于 sim 后端集合 {a0, b0} | 端点在候选集内 |
| 4 | 不带 `x-session-id` 发送一次 pick | 请求成功返回，端点有效（scorer 弃权、picker 自由选，无错误） |

## 预期结果

- 全部步骤通过；session 粘住端点在候选集内；无端 header 请求正常调度。
- epp 日志无 error 级记录。

## 清理

`defer e.Close(t)`：Kill epp/sim 进程并 Wait，关闭 mock API 与 gRPC 连接，`t.TempDir` 自动清理。
