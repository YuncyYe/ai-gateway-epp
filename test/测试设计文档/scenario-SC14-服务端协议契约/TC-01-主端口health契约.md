# TC-01 主端口 health 契约

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-01 / 主端口 health 契约 |
| 所属场景 | SC14 服务端协议契约 |
| 版本 | v0.1 |

## 测试目的

验证 gRPC health 服务同时挂载在 ext-proc 主端口（GRPCAddr，数据端口）与独立 health 端口（HealthAddr），且两端口共享同一就绪门控（assignment 首同步 + 全部 assigned cell ready）——这是 BFE failover 健康检查朝主端口 `grpc.health.v1.Check` 判活的契约。同时验证 health 注册不影响主端口的数据面调度。

## 运行模式

单组件进程组：epp（真实进程）×1 + inference-sim（真实进程）×1 + ai-gateway-api mock + BFE 由测试代码扮演。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用。
2. mock API 已 `SetFail(true)`（InnerAPI 故障态），使 epp 就绪门控保持在 NOT_SERVING，消除启动后就绪过快来不及断言的时序不确定性。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 启动 epp（`-instance-id epp-sc14-tc01`），mock 保持 `SetFail(true)`；对 GRPCAddr 与 HealthAddr 分别发起单次 gRPC health Check（service 为空串） | 两端口均非 SERVING（err 或非 SERVING 状态），即未就绪阶段双端口判活一致 |
| 2 | 启动 inference-sim（模型 `sim-model`，实例 `sim-a0`）；mock 下发三视图：cluster_table（cluster-a / sub-1，后端 `cluster-a-a0` = sim 地址，Weight=50）、assignment（cluster-a=primary）、picker_config（`PickerConfig("cluster-a", false)`，flowControl 关闭）；随后 `SetFail(false)` | mock 接口接受配置 |
| 3 | `WaitHealth` 等待 epp 就绪（health 端口 SERVING，30s 超时） | epp 完成首同步并就绪 |
| 4 | 对 GRPCAddr（主端口）发起 health Check，service 分别为空串与 `liveness` | 两者均返回 SERVING：主端口就绪判活与 liveness 常 SERVING 均成立 |
| 5 | 以明文 ext-proc 客户端向 GRPCAddr 发送 pick：`llm-d.ai/inference-pool = "cluster-a"`，path `/v1/chat/completions`，body 为 `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc14"}],"max_tokens":8}`（10s 超时） | 调度成功，返回的 `x-gateway-destination-endpoint` 等于 sim 地址，即主端口数据面不受 health 注册影响 |

## 预期结果

- 未就绪时双端口 health Check 均非 SERVING；就绪后主端口 service="" 与 service="liveness" 均为 SERVING。
- 主端口 pick 命中 sim 后端；epp 日志无异常。

## 清理

`defer epp.Proc.Stop(t)` / `defer sim.Stop(t)`：Kill epp/sim 进程并 Wait，关闭 mock API，`t.TempDir` 自动清理。
