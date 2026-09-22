# TC-03 多 cluster 本地加载与隔离

| 项 | 内容 |
|---|---|
| 用例编号 / 名称 | TC-03 / 多 cluster 本地加载与隔离 |
| 所属场景 | SC15 本地配置文件加载 |
| 版本 | v0.1 |

## 测试目的

验证本地目录可同时承载多个 cluster 的配置：各 cluster 的 `epp_config` 独立编译、assignment 各自生效、`cluster_table` 端点各自注册，ext-proc 按 `inference-pool` 元数据分别命中各自端点集，互不串扰。

## 运行模式

同 TC-01（epp 真实进程 ×1，无 mock/sim）。对应实现（拟）`TestTC03_MultiCluster`（env 标识 `epp-sc15-tc03`）。

## 前置条件

本地配置目录：

- `cluster_table.json`：`cluster-a.sub-1 = [portA]`、`cluster-b.sub-1 = [portB]`（不同保留端口）。
- `epp_data_config.json`：`epp_config` 含 cluster-a、cluster-b 两份最小配置（各自 `clusterName` 指向自身）；`assignment` 两个 cluster 均为本实例 primary。
- `epp_pool.json` 占位。

## 测试步骤

| # | 操作 | 预期 |
|---|---|---|
| 1 | 启动并 `WaitHealth("", 30s)` | readiness SERVING |
| 2 | 断言 `ai_epp_engine_current_version{cluster="cluster-a"}` 与 `{cluster="cluster-b"}` 均非空 | 两份 epp_config 均编译 |
| 3 | 断言 `ai_epp_cell_state` 中 cluster-a、cluster-b 均为 `role="primary",state="primary"` = 1 | 两 cluster 均建主 cell |
| 4 | ext-proc pick `pool="cluster-a"` | 返回 `127.0.0.1:portA` |
| 5 | ext-proc pick `pool="cluster-b"` | 返回 `127.0.0.1:portB` |

## 预期结果

- 多 cluster 本地配置全部加载且相互隔离（对应 mock 模式 SC01-TC02 的 demux 行为，在本地文件模式下成立）。

## 清理

`defer e.Close()`。