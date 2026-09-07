# TC-01 KV-cache 偏好空闲端点

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-01 / KV-cache 偏好空闲端点 |
| 所属场景 | SC10 基于 vLLM 状态的差异化调度 |
| 版本 | v0.1 |

## 测试目的

在仅启用 kv-cache-utilization-scorer 的插件链下，验证 epp 的调度决策真实消费后端实时指标：高 KV-cache 使用率的后端被饿死，空闲后端独占流量。这是指标链路（sim /metrics → epp metrics-data-source → extractor → scorer）生效的首次验证（FR-D2）。

## 运行模式

单组件进程组：epp（真实进程）×1 + inference-sim（真实进程，cluster-a 下 a0/b0 两个后端，均启用 fake-metrics）×2 + ai-gateway-api mock + BFE 由测试代码扮演。对应实现 `sc10_test.go` 的 `TestTC01_KVCacheUtilizationBias`。

## 前置条件

1. 公共前置条件见总体说明 §1-§2（epp 二进制由 harness 自动构建，inference-sim 二进制可用）。
2. mock API 已配置 assignment（cluster-a=primary）、cluster_table（2 个 sim 后端 a0/b0）；picker_config 由 `WithPickerConfigFn` 下发单 scorer 配置：插件链 cluster-table-discovery + `kv-cache-utilization-scorer`（名称 "kv-scorer"）+ max-score-picker，调度 profile "default" 引用 scorer 与 max-score，requestHandler 为 openai-parser。
3. b0 通过 env 选项 `WithSimFakeMetrics` 启用 fake-metrics（初始 `{}`）；epp 启动并就绪。
4. 指标链路为异步采集（默认 50ms refresh），观察窗口 30s。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 基线：反复发送 ext-proc pick 请求（metadata `llm-d.ai/inference-pool = "cluster-a"`，path `/v1/chat/completions`，body 为 OpenAI chat 请求 `"max_tokens": 8`），每轮 5 次、最多 4 轮，在 20s 观察窗内收集命中端点 | scorer 各处输入均为 0，两后端同分随机选择：a0、b0 均被命中 |
| 2 | 对 b0 执行 `POST /admin/config`，body `{"fake-metrics": {"kv-cache-usage": 0.95}}` | 返回 200 |
| 3 | 在 10s 观察窗内抓取 b0 的 `/metrics`，断言 `vllm:kv_cache_usage_perc` | 指标值 > 0.9，确认 sim 侧 fake metrics 已生效 |
| 4 | 在 30s 观察窗内连续发送 pick 请求，每轮突发 10 次 | 10 次 pick 全部命中空闲端点 a0；b0 被饿死 |

## 预期结果

- 全部步骤通过；高负载后端 b0（kv-cache-usage=0.95）的 pick 命中数为 0。
- epp 的 stderr 日志无 error 级记录（fail 时 harness 自动 dump）。

## 清理

`defer e.Close(t)`：Kill epp/sim 进程并 Wait，关闭 mock API 与 gRPC 连接，`t.TempDir` 自动清理。
