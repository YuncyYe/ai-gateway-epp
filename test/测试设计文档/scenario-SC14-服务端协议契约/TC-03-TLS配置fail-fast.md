# TC-03 TLS 配置 fail-fast

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-03 / TLS 配置 fail-fast |
| 所属场景 | SC14 服务端协议契约 |
| 版本 | v0.1 |

## 测试目的

验证 TLS 参数只配其一（只传 `-grpc-tls-cert` 不传 `-grpc-tls-key`）时 epp 启动 fail-fast：进程秒级内非零退出，并在日志中输出明确错误信息，而不是静默降级为明文或挂起。

## 运行模式

单组件进程组：epp（真实进程，启动失败断言）×1 + ai-gateway-api mock。不启动 inference-sim。

## 前置条件

1. harness 已构建 epp 二进制。
2. `common.WriteSelfSignedCert` 生成自签证书与 key（本用例只取 certFile）。
3. mock API 正常监听（epp 启动参数需要 `-api-addr`）。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | `StartProcess` 直接拉起 epp：`-api-addr <mock>`、`-instance-id epp-sc14-tc03`、`-grpc-tls-cert <certFile>`（**故意不传 key**），grpc/health/metrics 端口均取 `FindFreePort`，`-bind-address 127.0.0.1`；goroutine 中 `Cmd.Wait()` 并记录启动耗时 | 进程非零退出，耗时在秒级（远低于 15s 超时上限） |
| 2 | 调用 `proc.Stop(t)` 关闭日志句柄（进程已退出，Kill/Wait 为空操作；保证 Windows 下 t.TempDir 可清理日志文件） | 无报错 |
| 3 | 读取 epp 日志文件全文 | 日志包含 `grpc-tls-cert and grpc-tls-key must be set together` |

## 预期结果

- 进程非零退出且 15s 内发生（实际秒级）。
- 日志含明确的 fail-fast 错误信息。

## 清理

`proc.Stop(t)` 后 `t.TempDir` 自动清理；mock API `defer api.Close()`。
