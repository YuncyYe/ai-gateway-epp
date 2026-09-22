# TC-08 本地模式零 InnerAPI 访问

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-08 / 本地模式零 InnerAPI 访问 |
| 所属场景 | SC15 本地配置文件加载 |
| 版本 | v0.1 |

## 测试目的

验证本地文件模式下 EPP **完全不连接 ai-gateway-api InnerAPI**：即使 `-api-addr` 指向一个真实可用的 mock 服务，EPP 也不会向 `epp_data/config`、`gslb_data/cluster_table` 发起任何请求，退役的 `assignment/report` 计数也保持 0（对应设计文档"不再连接 InnerAPI，仅依赖本地文件即可启动"）。

## 运行模式

单组件进程组：epp（真实进程）×1 + 进程内 ai-gateway-api mock（仅作请求计数器）。对应实现（拟）`TestTC08_NoInnerAPITraffic`（env 标识 `epp-sc15-tc08`）。

## 前置条件

1. mock API 已启动（不配置任何 epp_config/assignment/cluster_table，仅计数）。
2. 本地配置目录三份文件齐备，可正常加载。
3. epp 以 `--local-config-dir=<dir> -api-addr=<mock.Addr> -instance-id=<本实例>` 启动。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 启动 epp，`WaitHealth("", 30s)` | readiness SERVING（配置来自本地文件） |
| 2 | ext-proc pick `pool="cluster-a"` 成功 | 本地 cluster_table 端点生效，服务正常 |
| 3 | 持续服务窗口（≥5×pollInterval，如 2s）后读取 mock 计数 | `RequestCount("/configs/epp_data/config")` = 0、`RequestCount("/configs/gslb_data/cluster_table")` = 0、`ReportCount()` = 0 |
| 4 | 断言进程内未创建 InnerAPI 客户端路径的副作用：`/metrics` 中两 poller `last_sync_timestamp` > 0（说明由本地源驱动） | 本地源驱动 |

## 预期结果

- 本地模式下对 InnerAPI 三类路径的请求数均为 0，证明完全脱离控制面。
- 注意：本 TC 会显式启动 mock，属于"只用于计零"的对照，不违反"不访问控制面"的目标。

## 清理

`defer e.Close()`：停止 epp 并关闭 mock。