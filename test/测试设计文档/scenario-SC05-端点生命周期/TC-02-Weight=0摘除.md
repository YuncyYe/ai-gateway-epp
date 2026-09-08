# TC-02 Weight=0 摘除

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-02 / Weight=0 摘除 |
| 所属场景 | SC05 端点生命周期 |
| 版本 | v0.1 |

## 测试目的

验证 Weight=0 的摘除语义（见 `pkg/poller/discovery.go`）：cluster_table 中某后端 Weight 置 0 后，该后端被移出调度集合，流量全部落到剩余端点；过程不重启 epp。

## 运行模式

单组件进程组：epp（真实进程）×1 + inference-sim（真实进程）×2 + ai-gateway-api mock + BFE 由测试代码扮演。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用。
2. mock API 已配置 assignment 全量视图（cluster-a → 本实例 primary）、epp_config（cluster-a 最小配置）、cluster_table 含 a0、a1 两个后端（Weight 均为 50）。
3. epp 启动并就绪（health=SERVING），两个后端均在调度集合中。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 经 `SetClusterTable` 重下发表：a0 Weight=50、a1 Weight=0（其余字段不变） | mock 接受新表；discovery 异步应用摘除 |
| 2 | 轮询验证（截止 20s，轮次间隔 200ms）：每轮发 5 次 pick（metadata `llm-d.ai/inference-pool = "cluster-a"`，path `/v1/chat/completions`，chat 请求体，`max_tokens:8`，单次超时 5s） | 每轮 5 次 pick 全部成功（err 为 nil，否则用例失败）；命中地址只允许是 a0；一旦某轮全部命中 a0（不再命中 a1）即通过 |
| 3 | 若截止 20s 仍有 pick 落到 a1 | 用例失败：`picks still route to drained backend` |

## 预期结果

- Weight=0 后 a1 不再被选中，流量稳定全部落到 a0。
- epp 进程健康、无 panic；摘除经轮询 + diff 异步生效，观察窗口 20s。

## 清理

`defer e.Close()`：Kill epp/sim 进程并 Wait，关闭 mock API 与 gRPC 连接，`t.TempDir` 自动清理。
