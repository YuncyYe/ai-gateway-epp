# EPP 本地配置文件加载设计（ai-gateway-epp 侧）

> 面向开发者的方案沟通文档，聚焦**关键变化与决策**；实现细节与逐行改造见同目录 `design-changes.md`。

## 1. 背景

EPP 进程的全部集群级配置当前只经 InnerAPI 从 ai-gateway-api 控制面拉取：

- **cluster_table**：后端模型服务集群的 RS 列表（Name/Addr/Port/Weight）；
- **epp_data/config**：per-cluster 调度配置（`epp_config`）+ cluster→主备实例角色全量视图（`assignment`）。

纯 InnerAPI 模式带来以下不便：

- **本地调试**：必须部署完整 ai-gateway-api 控制面才能启动 EPP，单步调试成本高；
- **集成测试**：需要 mock HTTP 服务，测试代码复杂；
- **CI**：流水线需额外维护一个 InnerAPI 服务。

## 2. 目标

新增 `--local-config-dir` 启动参数，指向包含本地 JSON 配置文件的目录。当该参数**非空**时，EPP 从本地文件加载集群级配置，**不连接 InnerAPI**，仅依赖本地文件即可启动。

**默认（参数为空）时，全部路径与行为保持不变。**

## 3. 非目标

- 不做文件监听（inotify）自动热加载；
- 不做单文件 UI / 管理界面配置；
- 不改变 InnerAPI 模式下的任何行为；
- 不修改轮询框架本身；
- 生产环境仍以 InnerAPI 热加载为准。

## 4. 方案总览

利用现有 `Source[T]` 取数接口这一扩展点：`--local-config-dir` 设置时，用本地文件源替换 InnerAPI 客户端作为数据源；下游的取数-消费流程、Cell 生命周期、Engine 编译-切换链路**完全复用**。

```
        --local-config-dir 为空（默认，生产）
   ┌──────────────────────────────────────────────┐
   │  InnerAPI 客户端 → Source.Fetch(version)      │
   │  → 对端 ai-gateway-api InnerAPI               │
   └──────────────────────────────────────────────┘

        --local-config-dir 非空（调试/测试/CI）
   ┌──────────────────────────────────────────────┐
   │  本地文件源 → Source.Fetch(version)           │
   │  → 读取本地 JSON 文件（无 version 语义）       │
   └──────────────────────────────────────────────┘
                       │
                       ▼
        两个消费器 Handle（ClusterDiscovery / EppDataWatcher）
        → Cell 创建/角色转换/销毁 与 Engine 编译-切换（复用不变）
```

## 5. 关键变化

1. **新增进程级开关**：`--local-config-dir`（环境变量 `AI_GATEWAY_EPP_LOCAL_CONFIG_DIR`）。非空即进入本地文件模式。
2. **数据源替换而非新增链路**：本地模式仅替换 `Source[T]`（取数）：以本地文件源读取 `cluster_table.json`、`epp_data_config.json`；`ClusterDiscovery` 与 `EppDataWatcher` 仍作为消费器（`Handle`）驱动 Cell 与 Engine，只是不再持有 InnerAPI 客户端。
3. **本地目录约定三份文件**：`cluster_table.json`、`epp_data_config.json` 由 EPP 消费；`epp_pool.json` 仅供调试参考、EPP 不消费。
4. **文件格式跳过 envelope**：文件内容直接对应 InnerAPI 响应的 `Data.Config` 层（不含 `ErrNum/ErrMsg/Data/Version/WorkMode`）。`epp_data_config.json` = 本文档配置契约中的 `{ "epp_config": ..., "assignment": ... }`。
5. **版本语义不存在**：本地文件无 server-side version，取数恒返回 `changed=true`，`newVersion` 为空；每轮询周期（默认 5s）重新读取，**修改文件后在下一个轮询周期内生效（无需重启）**，但不引入 inotify 或 HTTP reload 入口。
6. **失败语义沿用 fail-static**：文件缺失或 JSON 非法 → 该轮失败、不更新状态、指数退避重试、**沿用上一份成功配置**，与 InnerAPI 模式一致。

## 6. 关键决策

| 决策 | 说明 |
|------|------|
| 复用 `Source[T]` 扩展点，不改轮询框架 | 取数与消费解耦，本地模式只替换数据源，改动面小、风险低，InnerAPI 路径零影响。 |
| 复用既有消费器 `Handle` | `ClusterDiscovery`/`EppDataWatcher` 的"取数+消费"职责本就分离；本地模式以无客户端方式创建，仅提供消费逻辑，避免复制一套 Cell/Engine 装配。 |
| `changed` 恒为 `true` | 本地文件无增量语义，每轮视为一次重读，保证编辑文件后自动生效，无需额外版本管理。 |
| 不引入文件监听（inotify） | 轮询周期（默认 5s）已满足调试需要，保持实现简单。 |
| 不做 HTTP reload 端点 | 本期范围仅本地加载；如后续需要，同一目录契约可平滑扩展（另见 `2026-09-20-reload-local-conf/`）。 |
| `epp_pool.json` 不参与运行时 | EPP 只经 `assignment` 段获取角色；池文件仅作调试参考，`id` 应与 assignment 中实例 id 保持一致，由开发者自行保证。 |
| 跳过 envelope | 本地文件从 `Data.Config` 层开始，便于手写；与 InnerAPI 完整响应格式的差异在配置契约文档中明确。 |

## 7. 配置目录契约

| 文件名 | 对应 InnerAPI 端点 | 内容 | EPP 是否消费 |
|--------|--------------------|------|:---:|
| `cluster_table.json` | `GET /configs/gslb_data/cluster_table` | `map[cluster]map[subCluster][]BackendConf`（RS 列表） | **是** |
| `epp_data_config.json` | `GET /configs/epp_data/config` | `{ "epp_config": ..., "assignment": ... }` | **是** |
| `epp_pool.json` | `GET /open-api/v1/epp-pool` | EPP 实例池定义（实例组 + 实例列表） | 否（仅参考） |

文件字段级格式与完整示例见配置契约文档《EPP配置定义说明-epp_config.md》§1.2，本文不重复。

## 8. 使用方式

```bash
# 准备目录（放入三份 JSON）
mkdir -p /tmp/epp-local-config

# 方式 1：命令行参数
./epp --local-config-dir=/tmp/epp-local-config \
      --instance-id=epp-id1 \
      --grpc-port=9002 --health-port=9003 --metrics-port=9090 \
      --default-pool=cluster_epp_sim

# 方式 2：环境变量
export AI_GATEWAY_EPP_LOCAL_CONFIG_DIR=/tmp/epp-local-config
./epp --instance-id=epp-id1
```

## 9. 影响范围

| 模块 | 位置 | 变化 |
|------|------|------|
| 进程级配置 | `cmd/epp/config.go` | 新增 `LocalConfigDir` 字段与 `--local-config-dir` flag |
| 组装逻辑 | `cmd/epp/main.go` | 按 `LocalConfigDir` 二选一组装数据源（本地文件源 / InnerAPI 源） |
| 本地文件源 | `pkg/poller/local_source.go`（新增） | 提供 `LocalFileSource` 通用文件源与 cluster_table 专用文件源 |
| 单元测试 | `pkg/poller/local_source_test.go`（新增） | 覆盖正常加载、文件缺失、JSON 非法、version 被忽略 |

不改动：`pkg/poller/poller.go` 轮询框架、`pkg/cell/`、`pkg/innerapi/`、插件注册与 Engine 编译链路。

## 10. 风险与边界

| 风险 / 边界 | 说明 | 缓解 |
|------|------|------|
| 本地文件与 InnerAPI 数据格式不一致 | 本地文件从 `Data.Config` 层开始，跳过 envelope，与 InnerAPI 完整响应存在差异 | 在配置契约文档中明确两种模式的 JSON schema，并以单元测试覆盖 |
| 本地模式下 `APIAddr`/`APIToken` 未使用 | 二者仍有默认值，不会导致启动失败 | 本地模式跳过 InnerAPI 客户端创建，不校验、不连接 |
| `epp_pool.json` 不被消费 | EPP 仅经 assignment 获取角色 | 文档标注为调试参考；由开发者保证池文件 id 与 assignment 一致 |
| 编辑后生效时机 | 依赖轮询周期，非即时 | 调试场景可接受；如需即时触发可后续引入 reload 入口 |
| 生产误用本地模式 | 配置文件漂移、无统一管控 | 本地模式定位调试/测试/CI；生产部署不下发该参数 |

## 11. 关联文档

- 本改造详细设计变更：`docs/zh_cn/modifications/2026-09-14-local-config-file/design-changes.md`
- 配置契约权威定义：`docs/zh_cn/configuration/EPP配置定义说明-epp_config.md`（§1.2 本地文件模式）
- 系统设计：`docs/zh_cn/sys_design/ai-gateway-epp系统设计.md`（§3.7 本地配置文件模式）
- 后续演进（本地配置 reload，另行设计）：`docs/zh_cn/modifications/2026-09-20-reload-local-conf/design-changes.md`