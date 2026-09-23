# TC-07 epp_data_config.json 非法 JSON fail-static

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-07 / `epp_data_config.json` 非法 JSON fail-static |
| 所属场景 | SC15 本地配置文件加载 |
| 版本 | v0.1 |

## 测试目的

验证本地文件 JSON 解析失败时同样 fail-static：`LocalFileSource.Fetch` 返回 decode error → poller 不更新状态、退避重试；进程存活、不就绪、无 cell/引擎；替换为合法内容后自愈。与 TC-06 的区别在于失败分支为 **JSON 解码错误**（文件存在但内容损坏）而非 **读文件错误**。

## 运行模式

同 TC-06（epp 真实进程 ×1，无 mock/sim；低层原语启动）。对应实现（拟）`TestTC07_BadJSON`（env 标识 `epp-sc15-tc07`）。

## 前置条件

- 本地目录含 `cluster_table.json`、`epp_pool.json`。
- `epp_data_config.json` 存在但内容非法，例如 `{ this is not json }`。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 启动 epp，等待 gRPC 监听 | 进程存活 |
| 2 | `Check("liveness")` / `Check("")` | liveness SERVING / readiness NOT_SERVING |
| 3 | 抓取 metrics：`ai_epp_poller_failures_total{poller="epp_data"}` > 0、`ai_epp_poller_backoff_state{poller="epp_data"}` = 1 | 解码失败并退避 |
| 4 | 断言无 `ai_epp_engine_current_version{cluster="cluster-a"}`、无 cell | 损坏内容未成为 sticky 状态 |
| 5 | 用合法内容覆盖 `epp_data_config.json` | 文件落盘 |
| 6 | `WaitHealth("", 45s)` 后断言引擎版本非空、`cell_state primary/primary`=1、`backoff_state` = 0 | 自愈成功 |
| 7 | ext-proc pick `pool="cluster-a"` | 返回 `127.0.0.1:portA` |

## 预期结果

- 损坏 JSON 期间 fail-static：不加载、不建 cell、退避重试、进程存活。
- 修复文件后自动恢复，行为与 TC-06 一致。

## 清理

`defer e.Close()`。