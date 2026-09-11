# TC-02 多 session 独立

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-02 / 多 session 独立 |
| 所属场景 | SC12 SessionAffinity 调度 |
| 版本 | v0.1 |

## 测试目的

验证多个 session 的绑定互相独立：两个 session 各自粘住某个端点，交错请求时绑定不串扰、不泄漏。

## 运行模式

单组件进程组：epp（真实进程）×1 + inference-sim（真实进程）×2 + ai-gateway-api mock + BFE 由测试代码扮演。harness、mock 与二进制构建见总体说明 §1-§2。

## 前置条件

同 TC-01：epp_config 为 cluster-table-discovery + session-affinity-scorer（session_id，header `x-session-id`）+ max-score-picker；cluster-a 挂 2 个 sim 后端 a0/b0；epp 启动并就绪。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | `stick(sess-a, 6)`：带 header `x-session-id: sess-a` 连续 6 次 pick 收敛 | sess-a 粘住某一端点（重试窗口 30s） |
| 2 | `stick(sess-b, 6)`：带 header `x-session-id: sess-b` 连续 6 次 pick 收敛 | sess-b 粘住某一端点 |
| 3 | 交错验证：`stick(sess-a, 4)` | sess-a 仍粘住原端点，绑定未被 sess-b 干扰 |
| 4 | `stick(sess-b, 4)` | sess-b 仍粘住原端点 |

## 预期结果

- 全部步骤通过；两个 session 绑定独立稳定，交错请求后均保持粘住。
- epp 日志无 error 级记录。

## 清理

`defer e.Close(t)`：Kill epp/sim 进程并 Wait，关闭 mock API 与 gRPC 连接，`t.TempDir` 自动清理。
