# TC-09 epp_pool.json 不参与运行时

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-09 / `epp_pool.json` 不参与运行时 |
| 所属场景 | SC15 本地配置文件加载 |
| 版本 | v0.1 |

## 测试目的

验证 `epp_pool.json` 按 2026-09-14 设计文档所述"仅为参考/文档用途，EPP 不直接消费"：缺少该文件不影响启动与调度；文件中不含当前 `instance-id` 也不会触发任何运行时校验而阻断加载（角色完全由 `epp_data_config.json` 的 `assignment` 决定）。

> 说明：2026-09-20 文档提出 reload 时校验 `instance-id` 是否在 pool 中，当前实现尚未落地；若后续落地，本 TC 需改为"pool 不含本实例 → reload 失败、旧配置保持"。见场景说明 §8。

## 运行模式

同 TC-01（epp 真实进程 ×1，无 mock/sim）。对应实现（拟）`TestTC09_PoolFileNotConsumed`（env 标识 `epp-sc15-tc09`）。

## 前置条件

- 本地目录含 `cluster_table.json`、`epp_data_config.json`，**不含** `epp_pool.json`。
- `assignment["cluster-a"] = {primary: 本实例, standby: null}`。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 启动并 `WaitHealth("", 30s)` | readiness SERVING（无 pool 文件也可启动） |
| 2 | 断言引擎版本非空、`cell_state primary/primary`=1、`assignment_no_match`=0 | 角色/引擎来自 epp_data_config，与 pool 文件无关 |
| 3 | ext-proc pick `pool="cluster-a"` | 返回 `127.0.0.1:portA` |
| 4 | 写入 `epp_pool.json`，其 `groups[].instances` 只含一个**无关**实例 id（不含本实例） | 文件落盘 |
| 5 | 等待 ≥5×pollInterval（如 2s）后再断言 readiness、cell、pick | 行为完全不变（无运行时校验、不影响调度） |

## 预期结果

- 缺失 `epp_pool.json` 不影响启动与调度；pool 内容（含与本实例不匹配）不参与运行时，验证"EPP 不消费 pool 文件"。

## 清理

`defer e.Close()`。