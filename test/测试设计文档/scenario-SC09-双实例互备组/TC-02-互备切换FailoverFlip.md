# TC-02 互备切换 FailoverFlip

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-02 / 互备切换 FailoverFlip |
| 所属场景 | SC09 双实例互备组 |
| 版本 | v0.2（2026-09-08：随 EPP 调度对接改造，双活跃容忍用例由 FailoverFlip 替代——assignment 全量视图按构造每 cluster 只有一个 primary，api 侧错配双主已不可表达；failover 由 BFE 侧 EPPAddr 连接滞回驱动，EPP 零上报） |

## 测试目的

验证固定 2 实例互备组内的角色切换：assignment 全量视图翻转（cluster-a 由 A 主 B 备改为 B 主 A 备）后，新主实例热接管服务（备 cell 热数据，开闸即服务），原主实例降备并拒绝服务；全程对退役的 assignment report 端点零上报。

## 运行模式

双 epp 实例进程组：`epp-A` / `epp-B`（真实进程，同一 mock API、各自 `-instance-id`）+ inference-sim（真实进程）×1（cluster-a）+ ai-gateway-api mock（`epp_data/config` 单端点：epp_config + assignment 全量视图，两实例同一份，经 `SetAssignmentView` 直装）+ BFE 由测试代码扮演。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用。
2. mock API 已配置：epp_config（cluster-a 最小配置）、cluster_table（cluster-a 1 个 sim 后端 `cluster-a-0`，Weight=50）。
3. mock API 已安装全量视图：`cluster-a = {primary: epp-A, standby: epp-B}`。
4. epp-A、epp-B 各自启动并就绪（`WaitHealth("", 30s)` 成功）。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 对 epp-A 发 ext-proc 调度请求：RequestHeaders dynamic metadata 带 `llm-d.ai/inference-pool = "cluster-a"`，path `/v1/chat/completions`，随后发送 RequestBody（OpenAI chat 请求，`"max_tokens": 8`，模型 `sim-model`） | 返回 endpoint 为 cluster-a 的 sim 地址（超时 10s） |
| 2 | 对 epp-B 发同样的调度请求 | 请求失败，错误信息含 `not serving`（备实例热数据冷准入，Unavailable，超时 5s） |
| 3 | `SetAssignmentView` 将全量视图翻转为 `cluster-a = {primary: epp-B, standby: epp-A}` | — |
| 4 | 轮询（20s 观察窗）对 epp-B 重复 pick cluster-a | pick 成功，返回 cluster-a 的 sim 地址（failover 后新主接管） |
| 5 | 轮询（20s 观察窗）对 epp-A 重复 pick cluster-a | 返回错误，错误信息含 `not serving`（原主已降备） |
| 6 | 读取 mock 的 report 计数器 `ReportCount()` | 恒为 0（EPP 零上报） |

## 预期结果

- 视图翻转后 B 热接管服务、A 降备拒绝服务，两进程均存活；
- 全程零上报（退役 report 端点计数器为 0）；
- epp-A、epp-B 的 stderr 日志无 error 级记录。

## 清理

`defer p.Close(t)`：依次 Kill epp-A/epp-B 进程并 Wait，停止 sim 进程，关闭 mock API；`t.TempDir` 自动清理日志目录。
