# TC-02 gRPC TLS 全链路

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-02 / gRPC TLS 全链路 |
| 所属场景 | SC14 服务端协议契约 |
| 版本 | v0.1 |

## 测试目的

验证 `-grpc-tls-cert`/`-grpc-tls-key` 同时配置时，ext-proc 与 health 两个 gRPC server 均启用 TLS：明文 dial 被拒、TLS 健康检查与 TLS pick 正常，两端口同配生效。

## 运行模式

单组件进程组：epp（真实进程，TLS 启动）×1 + inference-sim（真实进程）×1 + ai-gateway-api mock + BFE 由测试代码扮演。

## 前置条件

1. harness 已构建 epp 二进制；inference-sim 二进制可用。
2. `common.WriteSelfSignedCert` 生成 ECDSA 自签证书（CN=ai-gateway-epp-test，SAN=127.0.0.1），写 cert/key PEM 到临时目录，并返回客户端校验用 `*x509.CertPool`。
3. mock API 已下发两端点：cluster_table（cluster-a / sub-1，后端 `cluster-a-a0` = sim 地址，Weight=50）、assignment 全量视图（cluster-a → 本实例 primary）、epp_config（`EppConfig("cluster-a", false)`）。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 以 `-grpc-tls-cert <certFile> -grpc-tls-key <keyFile>` 启动 epp（`-instance-id epp-sc14-tc02`）；用 `CheckHealthTLS` 轮询等待 HealthAddr 就绪（30s 超时，间隔 100ms），再验证 GRPCAddr 就绪（5s 超时） | 两端口 TLS health Check（service=""）均返回 SERVING |
| 2 | 以明文 `PickEndpoint` 向 GRPCAddr 发送 pick：`llm-d.ai/inference-pool = "cluster-a"`，path `/v1/chat/completions`，body 为 `{"model":"sim-model","messages":[{"role":"user","content":"hello from sc14"}],"max_tokens":8}`（3s 超时） | 调用失败——明文 ext-proc dial 被 TLS server 拒绝 |
| 3 | 以明文 `CheckHealth` 对 GRPCAddr 发起 health Check | 调用失败——明文 health Check 同样被拒 |
| 4 | 以 `PickEndpointTLS`（transport credentials 使用自签证书 CertPool）向 GRPCAddr 发送相同 pick（10s 超时） | 调度成功，返回的 `x-gateway-destination-endpoint` 等于 sim 地址，即 TLS 数据面全链路正常 |

## 预期结果

- TLS 就绪后两端口 health Check 均 SERVING。
- 明文 pick 与明文 health Check 均失败；TLS pick 命中 sim 后端。
- epp 日志无异常。

## 清理

`defer epp.Proc.Stop(t)` / `defer sim.Stop(t)`：Kill epp/sim 进程并 Wait，关闭 mock API，`t.TempDir` 自动清理。
