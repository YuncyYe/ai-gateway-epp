# TC-02 多 cluster 独立热加载

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-02 / 多 cluster 独立热加载 |
| 所属场景 | SC02 配置热加载 |
| 版本 | v0.1 |

## 测试目的

验证多 cluster 场景下热加载的隔离性：只更新 cluster-b 的 epp_config，仅 cluster-b 的引擎换入，cluster-a 的引擎版本保持不变。

## 运行模式

单组件进程组：epp（真实进程）×1 + inference-sim（真实进程）×2 + ai-gateway-api mock + BFE 由测试代码扮演。对应实现 `sc02_test.go` 的 `TestTC02_MultiClusterIndependent`（env 标识 `epp-sc02-tc02`）。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用。
2. mock API 已配置 assignment 全量视图（cluster-a → 本实例 primary、cluster-b=primary）、两个 cluster 的 epp_config v1（最小配置）、cluster_table（cluster-a=[sim a0, Weight=50]、cluster-b=[sim b0, Weight=50]）。
3. epp 启动并就绪，`ai_epp_engine_current_version` 中 cluster-a、cluster-b 的版本值均存在且非空，分别记为 va、vb。
4. 轮询间隔 200ms，版本变化观察窗口 20s。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 从 epp metrics 分别读取 cluster-a、cluster-b 的引擎版本，记为 va、vb | 两者均非空 |
| 2 | mock 下发配置：cluster-a 仍为 v1 原文（`EppConfig("cluster-a", false)`），cluster-b 替换为 v2（插件 `max-score` 重命名为 `max-score-v2`） | mock 下发成功 |
| 3 | 每 100ms 轮询 epp metrics，等待 cluster-b 引擎版本变化（20s 截止） | cluster-b 版本从 vb 变为新值；超时则 fail 并 dump metrics 摘录、report 与 epp 日志尾部 |
| 4 | 再次读取 cluster-a 的引擎版本 | 仍等于 va，未因 cluster-b 配置更新而重编译 |

## 预期结果

- 仅 cluster-b 完成引擎换入；cluster-a 引擎版本哈希不变，实现多 cluster 热加载隔离。
- 观测指标：`ai_epp_engine_current_version{cluster="cluster-a"}` 不变、`ai_epp_engine_current_version{cluster="cluster-b"}` 前移。

## 清理

`defer e.Close()`：Kill epp/sim 进程并 Wait，关闭 mock API 与 gRPC 连接，`t.TempDir` 自动清理。
