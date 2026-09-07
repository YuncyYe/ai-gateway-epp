# TC-01 故障期 fail-static

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-01 / 故障期 fail-static |
| 所属场景 | SC04 InnerAPI 故障 fail-static |
| 版本 | v0.1 |

## 测试目的

验证 ai-gateway-api InnerAPI 全部不可用（全部版本化端点返回 500）时，epp 以 fail-static 方式基于最后同步的配置与端点状态继续提供调度服务：pick 仍命中 sim 后端，进程不退出、liveness 保持 SERVING。

## 运行模式

单组件进程组：epp（真实进程）×1 + inference-sim（真实进程）×1 + ai-gateway-api mock + BFE 由测试代码扮演。每 TC 独立一套进程组与 mock。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用（见总体说明 §2，缺失时 `t.Skip`）。
2. mock API 已配置 assignment（cluster-a=primary）、picker_config（cluster-a 最小配置）、cluster_table（1 个 sim 后端 a0）；epp 启动并就绪，已首拉成功。
3. 环境 ID `epp-sc04-tc01`，全部监听 `127.0.0.1` 动态端口。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 调用 mock `SetFail(true)`：assignment / picker_config / cluster_table / report 等全部版本化端点返回 500 | mock 进入故障态 |
| 2 | 以 `WaitFor`（10s 超时）轮询发送 ext-proc pick：RequestHeaders 的 dynamic metadata 带 `llm-d.ai/inference-pool = "cluster-a"`，path `/v1/chat/completions`，随后发 RequestBody（`{"model":"sim-model",...,"max_tokens":8}`），单次请求超时 3s | 轮询窗口内 pick 成功，返回的 `x-gateway-destination-endpoint` 为 a0 的 `ip:port`（期间偶发错误在 `WaitFor` 内重试，不立即判失败） |
| 3 | 查询 epp health 的 liveness（5s 超时） | 返回 SERVING：进程未因 InnerAPI 故障退出 |

## 预期结果

- 故障期间调度仍基于最后同步的集群视图工作，pick 命中 a0。
- epp 进程存活，liveness=SERVING。

## 清理

`defer e.Close()`：Kill epp/sim 进程并 Wait，关闭 mock API 与 gRPC 连接，`t.TempDir` 自动清理。
