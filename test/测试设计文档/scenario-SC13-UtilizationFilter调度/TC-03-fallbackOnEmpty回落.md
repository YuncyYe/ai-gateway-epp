# TC-03 fallbackOnEmpty 回落

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-03 / fallbackOnEmpty 回落 |
| 所属场景 | SC13 UtilizationFilter 调度 |
| 版本 | v0.1 |

## 测试目的

验证 `fallbackOnEmpty=true` 时 filter 的回落语义：全部端点超阈值、过滤后候选集为空时，回落到未过滤的全候选列表继续服务——请求仍被路由（超阈值端点也接受），与 TC-02 的 fail-closed 形成对照。

## 运行模式

单组件进程组：epp（真实进程）×1 + inference-sim（真实进程）×2 + ai-gateway-api mock + BFE 由测试代码扮演。实例 ID `epp-sc13-tc03`，cluster-a 下辖 a0、b0 两个 sim 后端。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用（缺失则 skip）。
2. mock API 已配置 assignment（cluster-a=primary）、cluster_table（cluster-a = a0、b0）、picker_config（ep-discover + util-filter + max-score-picker，无 scorer）。
3. util-filter 参数：`conditions=[{"metric":"kv-cache-utilization","maxValue":0.9}]`，`fallbackOnEmpty=true`（本 TC 与 TC-01/02 唯一配置差异）。
4. 两个 sim 启动时均启用 fake-metrics（初始 `{}`）。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | `SetSimFakeMetrics` 向 a0 下发 `kv-cache-usage=0.95`、向 b0 下发 `kv-cache-usage=0.99`（均 >0.9 阈值） | fake-metrics 生效 |
| 2 | 连续 5 次 PickEndpoint（pool=`cluster-a`，path=`/v1/chat/completions`，chat body `max_tokens=8`） | 5 次全部成功，且命中端点均为合法候选（a0 或 b0）——过滤集为空时回落到全候选，请求不断流 |

注：指标采集为异步路径，观察窗口 30s（WaitFor 轮询）。

## 预期结果

- 全部步骤通过：与 TC-02 相同负载下调度不失败，命中端点均在 {a0, b0} 内。
- epp 无 panic，stderr 无 error 级记录（fail 时 harness 自动 dump）。

## 清理

`defer e.Close(t)`：Kill epp/sim 进程并 Wait，关闭 mock API 与 gRPC 连接，`t.TempDir` 自动清理。
