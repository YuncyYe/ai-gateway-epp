# SC15-TC10 预置配置文件（testdata）

本目录是**随仓库跟踪的预置配置**，供 `TestTC10_PreplacedConfigDir` 使用：EPP 以
`--local-config-dir <本目录>` 启动，直接读取这三份文件（不再由测试临时生成）。

- 结构参考运行样例 `ai-gateway-epp/conf/epp_data_conf/`，但 `epp_data_config.json` 是**独立维护的完整形态配置**：
  - `featureGates: ["flowControl"]`；插件链：`cluster-table-discovery`、`utilization-filter` +
    `utilization-detector`（saturation-detector）、`kv-cache-utilization-scorer`、`session-affinity-scorer`、
    `queue-scorer`、`prefix-cache-scorer`、`max-score-picker`、`openai-parser`；含 `flowControl` 区块
    （maxRequests/priorityBands/saturationDetector）；
  - cluster 名 `cluster-a`（`cluster-table-discovery.parameters.clusterName` 指向自身）；
  - `assignment.group = epp-sc15-tc10`（与本用例 `-epp-group` 一致）；
  - `cluster_table` 端点 `127.0.0.1:18981`（保留回环端口，本场景**不访问后端**）。

⚠️ 该文件与用例存在**内容耦合**：`TestTC10_PreplacedConfigDir` 断言
`ai_epp_engine_current_version{cluster="cluster-a"}` == `sha256(epp_config["cluster-a"] 的原始字节)`
（`cell.HashConfig` 的算法）。改动本文件即可，无需改动用例（哈希随内容自动变化）；
但若改 `cluster` 名或 `assignment.group`，需同步用例中的 cluster 名与 `-epp-group`。

文件清单：

| 文件 | 作用 | EPP 是否消费 |
|---|---|---|
| `cluster_table.json` | 后端端点集（ClusterDiscovery） | 是 |
| `epp_data_config.json` | `epp_config` 引擎编译 + `assignment` 组匹配 | 是 |
| `epp_pool.json` | 实例池定义（组名与本用例 `-epp-group` 对应） | 否（仅参考） |

> 请勿在测试中改写本目录内容（用例断言依赖其冻结取值）；需要变体的用例请用
> `t.TempDir()` 生成配置（见 TC-05/TC-06/TC-07）。