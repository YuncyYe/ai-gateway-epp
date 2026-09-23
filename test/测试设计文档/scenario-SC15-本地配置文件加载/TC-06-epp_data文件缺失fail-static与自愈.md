# TC-06 epp_data_config.json 缺失 fail-static 与自愈

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-06 / `epp_data_config.json` 缺失 fail-static 与自愈 |
| 所属场景 | SC15 本地配置文件加载 |
| 版本 | v0.1 |

## 测试目的

验证本地文件读取失败时继承 poller 的 fail-static 语义：文件不存在 → `Fetch` 返回 error → 不更新状态、退避重试；进程不退出（liveness SERVING）、不就绪（readiness NOT_SERVING）、无 cell/引擎；补齐合法文件后自动恢复。

## 运行模式

同 TC-01（epp 真实进程 ×1，无 mock/sim）。对应实现（拟）`TestTC06_MissingEppData`（env 标识 `epp-sc15-tc06`）。本 TC 需要控制"先缺失、后补齐"，使用低层原语启动（等待 gRPC 监听，不等 readiness）。

## 前置条件

- 本地目录**仅含** `cluster_table.json`、`epp_pool.json`，**不含** `epp_data_config.json`。
- `cluster_table.json`：`cluster-a.sub-1 = [portA]`。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 启动 epp，等待 gRPC 监听 | 进程存活、可连接 |
| 2 | `Check("liveness")` | SERVING（进程未退出） |
| 3 | `Check("")` | NOT_SERVING（未就绪） |
| 4 | 抓取 metrics：`ai_epp_poller_failures_total{poller="epp_data"}` > 0 且 `ai_epp_poller_backoff_state{poller="epp_data"}` = 1 | epp_data 拉取失败并退避 |
| 5 | 断言无 `ai_epp_engine_current_version{cluster="cluster-a"}`、无 `ai_epp_cell_state{cluster="cluster-a",...}` | 未建 cell、未编译引擎 |
| 6 | 写入合法 `epp_data_config.json`（assignment primary=本实例） | 文件落盘 |
| 7 | `WaitHealth("", 45s)` | readiness 恢复 SERVING（退避上限 30s，窗口放宽） |
| 8 | 断言 `ai_epp_engine_current_version{cluster="cluster-a"}` 非空、`cell_state primary/primary`=1、`ai_epp_poller_backoff_state{poller="epp_data"}` = 0 | 配置补齐后自动加载生效、退避清零 |
| 9 | ext-proc pick `pool="cluster-a"` | 返回 `127.0.0.1:portA` |

## 预期结果

- 缺失文件期间 fail-static：进程存活、不就绪、无状态更新、退避重试。
- 补齐后自动自愈：就绪、建 cell、编译引擎、退避清零、可调度。
- 观测：`ai_epp_poller_failures_total`、`ai_epp_poller_backoff_state`、`ai_epp_engine_current_version`、health。

## 清理

`defer e.Close()`。