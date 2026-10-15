# TC-04 assignment 本地自匹配（未分配组）

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-04 / assignment 本地自匹配（未分配组） |
| 所属场景 | SC15 本地配置文件加载 |
| 版本 | v0.2（2026-10-14：由"standby 热备"改为"未分配组"，assignment 契约 `{primary, standby}` → `{group}`，角色概念删除） |

## 测试目的

验证本地 `epp_data_config.json` 的 `assignment` 段按**组名**自匹配：当 cluster 被分配到其他组时，本实例不建 cell、不编译引擎，readiness 保持 NOT_SERVING（无 cell 可就绪），`ai_epp_assignment_no_match` 指标为 1，ext-proc 对该 cluster 拒绝服务。

## 运行模式

单组件进程组（本地文件模式）：epp（真实进程）×1 + 本地配置目录（保留回环端点，不访问推理服务）。BFE 由测试代码扮演。详见总体说明 §1。

## 前置条件

1. harness 已构建 epp 二进制；
2. 配置目录：`cluster_table.json`（cluster-a 一个保留回环端点）、`epp_data_config.json`（`epp_config["cluster-a"]` 最小配置；`assignment["cluster-a"] = {group: "other-group"}`）、`epp_pool.json`（参考文件）；
3. epp 以 `--local-config-dir=<dir>` 启动。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | `WaitHealth("liveness", 30s)` | liveness SERVING（进程存活） |
| 2 | 抓取 `/metrics`：`ai_epp_engine_current_version{cluster="cluster-a"}`、`ai_epp_cell_state{cluster="cluster-a",state="active"}`、`ai_epp_assignment_no_match` | 引擎版本为空、cell_state=0、no_match=1（组名未命中任何分配） |
| 3 | 检查 readiness（`CheckHealth("")`） | NOT_SERVING（无 cell 可就绪） |
| 4 | pick cluster-a（3s 超时） | 被拒绝（本实例无该 cluster 的 cell） |

## 预期结果

- 本地 assignment 的组名不匹配时：不建 cell、不编译引擎、no_match=1、readiness NOT_SERVING、pick 拒绝；
- 与 TC-01（组名命中）形成正负对照。

## 清理

停止 epp 进程（`env.Proc.Stop(t)`）。
