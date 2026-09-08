# TC-01 新 cluster 动态纳入

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-01 / 新 cluster 动态纳入 |
| 所属场景 | SC08 cluster 数据驱动生命周期 |
| 版本 | v0.1 |

## 测试目的

验证运行中的 epp 在 epp_data/config（assignment 全量视图 + epp_config 段）与 cluster_table 同时新增一个 cluster 后，自动完成建 cell、编译引擎、发现端点并对外可调度，全程无需重启 epp（FR-D4、FR-M3）。

## 运行模式

单组件进程组：epp（真实进程）×1 + inference-sim（真实进程，先 1 后 2）+ ai-gateway-api mock + BFE 由测试代码扮演。每 TC 独立一套进程组。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用。
2. 仅下发 cluster-a：assignment 全量视图（cluster-a → 本实例 primary）、epp_config（cluster-a 最小配置，cluster-table-discovery 插件钉住 clusterName=cluster-a）、cluster_table（cluster-a 下 1 个 sim 后端 a0，Weight=50）。
3. epp 启动并就绪（health=SERVING，cluster-a cell ready）。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 新起一个 inference-sim（sim-b0，模型 `sim-model`），并将其登记为 cluster-b 的后端；mock API 全量更新两端点：assignment 全量视图增加 cluster-b → 本实例 primary，epp_config 增加 cluster-b 最小配置，cluster_table 增加 cluster-b/sub-1 下 sim-b0（Weight=50） | mock 接口下发成功 |
| 2 | 30s 观察窗口内轮询：对 epp ext-proc 分别以 `llm-d.ai/inference-pool = "cluster-b"` 和 `"cluster-a"` 发送 RequestHeaders（path `/v1/chat/completions`）+ RequestBody（OpenAI chat 请求，`"max_tokens": 8`），读取调度返回的 `x-gateway-destination-endpoint` | cluster-b 的 pick 返回 sim-b0 的 `ip:port`，cluster-a 的 pick 返回其原有 sim 后端 a0，两者均无错误（新 cluster 无需重启即可调度，原 cluster 不受影响） |
| 3 | 读取 mock 的 report 计数器 `ReportCount()` | 恒为 0（就绪上报已删除，新 cell 的建立以调度成功为准） |

## 预期结果

- 全部步骤通过；epp 的 stderr 日志无 error 级记录（fail 时 harness 自动 dump）。
- 两端点数据驱动纳入新 cluster：建 cell、编译引擎、发现端点、可调度，epp 进程全程未重启；退役 report 端点零上报。

## 清理

`defer e.Close()`：Kill epp/sim 进程并 Wait，关闭 mock API 与 gRPC 连接，`t.TempDir` 自动清理。
