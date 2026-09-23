# TC-04 assignment 本地自匹配（standby 热备）

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-04 / assignment 本地自匹配（standby 热备） |
| 所属场景 | SC15 本地配置文件加载 |
| 版本 | v0.1 |

## 测试目的

验证本地 `epp_data_config.json` 的 `assignment` 段按实例 id 自匹配：当本实例在某 cluster 中为 standby 时，EPP 建立热备 cell（数据面保持热、引擎仍编译），readiness 正常，但 ext-proc 对该 cluster 拒绝服务（`cell is not serving`）。

## 运行模式

同 TC-01（epp 真实进程 ×1，无 mock/sim）。对应实现（拟）`TestTC04_StandbyRole`（env 标识 `epp-sc15-tc04`）。

## 前置条件

- `assignment["cluster-a"] = {primary: "peer-instance", standby: "<本实例>"}`。
- 其余同 TC-01（`epp_config["cluster-a"]` 最小配置、`cluster_table` 单端点）。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 启动并 `WaitHealth("", 30s)` | readiness SERVING（热备 cell 也 Ready） |
| 2 | 断言 `ai_epp_cell_state{cluster="cluster-a",role="standby",state="standby"}` = 1 | 本实例以 standby 角色建 cell |
| 3 | 断言 `ai_epp_engine_current_version{cluster="cluster-a"}` 非空 | 热备 cell 仍编译引擎（数据面热） |
| 4 | 断言 `ai_epp_assignment_no_match` = 0 | standby 角色属于自匹配成功 |
| 5 | ext-proc pick `pool="cluster-a"` | 返回错误且包含 `not serving`（拒绝服务） |
| 6 | `Check("liveness")` | SERVING（拒绝数据面不影响存活） |

## 预期结果

- 本地 assignment 的 standby 角色被正确解析并应用（对应 mock 模式 SC03-TC01 的角色切换语义，在本地文件模式下成立）。
- 区分 TC-01 的 primary 自匹配：role/state 标签为 standby，且不提供选点。

## 清理

`defer e.Close()`。