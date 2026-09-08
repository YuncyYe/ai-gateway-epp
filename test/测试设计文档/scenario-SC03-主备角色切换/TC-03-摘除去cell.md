# TC-03 摘除去 cell

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-03 / 摘除去 cell |
| 所属场景 | SC03 主备角色切换 |
| 版本 | v0.1 |

## 测试目的

验证从 assignment 中移除某个 cluster（保留其他 cluster）后：被移除 cluster 的调度失败、cell 被销毁且最新 assignment report 不再携带该 cluster；其余 cluster 不受影响、调度正常。

## 运行模式

单组件进程组：epp（真实进程）×1 + inference-sim（真实进程）×2 + ai-gateway-api mock + BFE 由测试代码扮演。详见总体说明 §1。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用（见总体说明 §2）。
2. mock API 已配置 cluster-a（后端 `a0`）、cluster-b（后端 `b0`）两 cluster 的 epp_config / cluster_table 最小配置，assignment 全量视图初始为 cluster-a、cluster-b → 本实例 primary。
3. epp 启动并就绪（health=SERVING）。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | `SetAssignment` 将 assignment 改为 cluster-b → 本实例 primary（移除 cluster-a） | — |
| 2 | 轮询（20s 观察窗）pick cluster-a（metadata `llm-d.ai/inference-pool = "cluster-a"`，path `/v1/chat/completions`，chat body `"max_tokens":8`，单请求超时 3s） | 返回错误，被移除 cluster 的调度失败 |
| 3 | pick cluster-b（metadata 中 inference-pool 为 `cluster-b`，其余同步骤 2，超时 10s） | 调度成功，返回端点为 cluster-b 的 sim 端点 `b0`（即 `e.ClusterSims["cluster-b"][0]`），其余 cluster 不受影响 |
| 4 | 读取 mock 的 report 计数器 `ReportCount()` | 恒为 0（就绪上报已删除，cell 销毁以下一版全量视图生效为准） |

## 预期结果

- cluster-a 被摘除后调度失败，cluster-b 调度正常；
- 全程零上报（FR-D4 消失纳入语义、FR-H1 主备角色）；
- 全部步骤通过；epp 的 stderr 日志无 error 级记录（fail 时 harness 自动 dump）。

## 清理

`defer e.Close()`：Kill epp/sim 进程并 Wait，关闭 mock API 与 gRPC 连接，`t.TempDir` 自动清理。
