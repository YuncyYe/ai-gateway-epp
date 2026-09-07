# TC-02 全过滤 fail-closed

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-02 / 全过滤 fail-closed |
| 所属场景 | SC13 UtilizationFilter 调度 |
| 版本 | v0.1 |

## 测试目的

验证守门语义：当 cluster 内全部端点的 `kv-cache-utilization` 都超过阈值、且 `fallbackOnEmpty=false`（默认）时，调度**失败**而非勉强选一个过载端点；epp 进程本身保持健康（liveness 正常）；一端负载恢复后调度自动恢复并只命中该端点。

## 运行模式

单组件进程组：epp（真实进程）×1 + inference-sim（真实进程）×2 + ai-gateway-api mock + BFE 由测试代码扮演。实例 ID `epp-sc13-tc02`，cluster-a 下辖 a0、b0 两个 sim 后端。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用（缺失则 skip）。
2. mock API 已配置 assignment（cluster-a=primary）、cluster_table（cluster-a = a0、b0）、picker_config（ep-discover + util-filter + max-score-picker，无 scorer）。
3. util-filter 参数：`conditions=[{"metric":"kv-cache-utilization","maxValue":0.9}]`，`fallbackOnEmpty=false`。
4. 两个 sim 启动时均启用 fake-metrics（初始 `{}`）。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | `SetSimFakeMetrics` 向 a0 下发 `kv-cache-usage=0.95`、向 b0 下发 `kv-cache-usage=0.99`（均 >0.9 阈值） | fake-metrics 生效 |
| 2 | 轮询 PickEndpoint（pool=`cluster-a`，path=`/v1/chat/completions`，chat body `max_tokens=8`）直至返回错误 | 调度失败（err != nil，30s 观察窗）——全部候选被过滤，fail-closed |
| 3 | `e.EPP.WaitHealth(t, "liveness", 5s)` | liveness 仍为 SERVING：调度失败不影响进程存活 |
| 4 | `SetSimFakeMetrics` 将 a0 的 `kv-cache-usage` 清零，轮询 PickEndpoint | 调度恢复，且命中端点固定为 a0（b0 仍超阈值被过滤） |

## 预期结果

- 全部步骤通过：全过滤期间每次 pick 均失败；a0 恢复后 pick 成功且只命中 a0。
- epp 无 panic，stderr 无 error 级记录（fail 时 harness 自动 dump）。

## 清理

`defer e.Close(t)`：Kill epp/sim 进程并 Wait，关闭 mock API 与 gRPC 连接，`t.TempDir` 自动清理。
