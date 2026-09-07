# TC-02 demux 路由

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-02 / demux 路由 |
| 所属场景 | SC01 单 cluster 调度闭环 |
| 版本 | v0.1 |

## 测试目的

验证 demux（FR-M2）：同一条 ext-proc 链路上，请求按 `llm-d.ai/inference-pool` metadata 中的 pool 名路由到各自的 cluster，两个 cluster 的后端集合互不相交。

## 运行模式

单组件进程组：epp（真实进程）×1（instance-id `epp-sc01-tc02`）+ inference-sim（真实进程）×2（逻辑名 `a0`、`b0`）+ ai-gateway-api mock + BFE 由测试代码扮演。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用。
2. mock API 已配置：
   - assignment：`cluster-a`、`cluster-b` 均为 `primary`（instance 匹配 epp 的 `-instance-id`）；
   - picker_config：`cluster-a`、`cluster-b` 各一份最小可用配置；
   - cluster_table：`cluster-a.sub-1 = [a0(Weight=50)]`，`cluster-b.sub-1 = [b0(Weight=50)]`，Addr/Port 为 sim 实例动态地址。
3. epp 启动并就绪（health=SERVING）。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 对 `cluster-a` 执行 ext-proc 调度（RequestHeaders metadata `llm-d.ai/inference-pool = "cluster-a"`，path `/v1/chat/completions`，RequestBody 为 chat 请求，`"max_tokens": 8`） | 返回的 `x-gateway-destination-endpoint` 等于 `a0` 的 sim 地址 |
| 2 | 对 `cluster-b` 执行同样的 ext-proc 调度（metadata pool 为 `cluster-b`） | 返回的 `x-gateway-destination-endpoint` 等于 `b0` 的 sim 地址 |
| 3 | 对比两次选端点结果 | 两个 cluster 各落各自唯一的后端，路由集合无交叉 |

## 预期结果

- 全部步骤通过；每个 cluster 都被路由到其 cluster_table 中声明的后端。
- epp 的 stderr 日志无 error 级记录（fail 时 harness 自动 dump）。

## 清理

`defer e.Close()`：Kill epp/sim 进程并 Wait，关闭 mock API，日志目录 `t.TempDir` 自动清理。
