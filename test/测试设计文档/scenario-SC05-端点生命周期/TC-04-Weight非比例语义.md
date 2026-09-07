# TC-04 Weight 非比例语义

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-04 / Weight 非比例语义 |
| 所属场景 | SC05 端点生命周期 |
| 版本 | v0.1 |

## 测试目的

验证 Weight 仅作为注册/摘除开关而非流量比例（需求分析 §3.1.1）：90/10 权重下两个后端仍都收到流量——若 Weight 是比例，10 权重的后端会从命中集合中消失。

## 运行模式

单组件进程组：epp（真实进程）×1 + inference-sim（真实进程）×2 + ai-gateway-api mock + BFE 由测试代码扮演。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用。
2. mock API 已配置 assignment（cluster-a=primary）、picker_config（cluster-a 最小配置）、cluster_table 含 a0、a1 两个后端（初始 Weight 均为 50）。
3. epp 启动并就绪（health=SERVING）。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 经 `SetClusterTable` 重下发表：a0 Weight=90、a1 Weight=10（其余字段不变） | mock 接受新表；discovery 异步应用 |
| 2 | 轮询断言（`WaitFor`，超时 30s）：每轮发 5 次 pick（metadata `llm-d.ai/inference-pool = "cluster-a"`，path `/v1/chat/completions`，chat 请求体，`max_tokens:8`，单次超时 5s），累计命中集合 | 直至命中集合大小 = 2，即 90/10 权重下 a0、a1 两个后端仍都被选中 |

## 预期结果

- 90/10 权重下两个后端均收到流量，命中集合大小为 2。
- epp 进程健康、无 panic；流量实际比例由调度策略决定，本 TC 只断言"非零 Weight 的后端均参与调度"。

## 清理

`defer e.Close()`：Kill epp/sim 进程并 Wait，关闭 mock API 与 gRPC 连接，`t.TempDir` 自动清理。
