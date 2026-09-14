# EPP 本地配置文件加载（ai-gateway-epp 侧改造）设计变更

## 1. 背景与动机

### 1.1 现状

EPP 进程的全部集群级配置通过 InnerAPI 从 ai-gateway-api 控制面拉取，涉及两条拉取链路：

| 拉取链路 | InnerAPI 端点 | 消费者 | 配置内容 |
|----------|--------------|--------|---------|
| cluster_table | `GET /configs/gslb_data/cluster_table` | `ClusterDiscovery`（`pkg/poller/discovery.go`） | 后端模型服务集群的 RS 列表（Name/Addr/Port/Weight） |
| epp_data/config | `GET /configs/epp_data/config` | `EppDataWatcher`（`pkg/poller/epp_data.go`） | `epp_config`（per-cluster 调度参数）+ `assignment`（cluster→主备实例角色全量视图） |

此外，EPP 实例池信息由 ai-gateway-api 侧 OpenAPI `/open-api/v1/epp-pool` 管理（实例组 + 实例列表），EPP 进程不直接消费该端点，而是通过 `epp_data/config` 的 `assignment` 段间接引用实例 id。

### 1.2 问题

纯 InnerAPI 拉取模式在以下场景存在不便：

- **本地调试**：开发和调试时需要部署完整的 ai-gateway-api 控制面才能启动 EPP，极大增加单步调试和问题排查的成本
- **集成测试**：单元测试和集成测试需要 mock HTTP 服务，增加测试代码复杂度
- **CI 环境**：流水线中运行 EPP 集成测试时，需要额外维护一个完整的 InnerAPI 服务

### 1.3 目标态

新增 `--local-config-dir` 启动参数，指向包含三份 JSON 配置文件的本地目录。当该参数非空时，EPP 从本地文件加载配置，不再连接 InnerAPI，仅依赖本地文件即可启动。

## 2. 关键代码锚点（现状）

- 进程级配置解析：`cmd/epp/config.go:55-84`（`parseConfig`，所有 flag 定义）
- InnerAPI 客户端：`pkg/innerapi/client.go:70-147`（`Client` / `Get` / envelope 解析 / version 增量）
- cluster_table 消费：`pkg/poller/discovery.go:31,67-99`（`ClusterTablePath` / `ClusterDiscovery.Fetch`）
- epp_data 消费：`pkg/poller/epp_data.go:66-73`（`EppDataWatcher.Fetch`，直接 `client.Get`）
- 轮询框架：`pkg/poller/poller.go:55-57`（`Source[T]` 接口），`:103-144`（Start 循环）
- 组装：`cmd/epp/main.go:66,76-83`（`innerapi.NewClient` / 两 poller 创建）

## 3. 设计

### 3.1 总体方案

利用现有 `Source[T]` 接口扩展点，新增 `LocalFileSource[T]` 实现。当 `--local-config-dir` 设置时，`cmd/epp/main.go` 组装阶段用 `LocalFileSource` 替换 `innerapi.Client`，轮询框架无改动。

```
            ┌─ --local-config-dir 为空（默认）───────────────┐
            │  innerapi.Client → Source[T].Fetch(version)     │
            │  → 对端 HTTP InnerAPI ──────────────────────────┤
            │                                                 │
            │  --local-config-dir 非空（调试模式）─────────────┤
            │  LocalFileSource[T] → Source[T].Fetch(version)   │
            │  → 读本地 JSON 文件（无 version 语义）───────────┤
            └─────────────────────────────────────────────────┘
```

### 3.2 `Source[T]` 接口适配

```go
// pkg/poller/poller.go（现状）
type Source[T any] interface {
    Fetch(ctx context.Context, version string) (changed bool, newVersion string, data T, err error)
}
```

`LocalFileSource[T]` 的行为约定：

| 方法 | 行为 |
|------|------|
| `Fetch` | 每次调用均读取文件 → JSON 反序列化，`changed` 始终返回 `true`（文件模式下无增量语义，每次重载），`newVersion` 返回空字符串，`data` 为文件内容 |
| 版本控制 | 不适用（本地文件无 server-side version） |
| 错误处理 | 文件不存在或 JSON 解析失败 → 返回 error，继承 fail-static 语义（poller 不更新版本、退避重试） |

### 3.3 新增 CLI 参数

在 `cmd/epp/config.go` 的 `Config` 结构体新增字段，并在 `parseConfig` 中注册 flag：

```go
// 新增字段
LocalConfigDir string  // 本地配置目录，非空时从文件加载所有集群级配置

// flag 注册
flag.StringVar(&cfg.LocalConfigDir, "local-config-dir",
    os.Getenv("AI_GATEWAY_EPP_LOCAL_CONFIG_DIR"),
    "load cluster-level configs from local directory instead of InnerAPI (empty = use InnerAPI)")
```

## 4. 改造点明细

### 4.1 新增 `pkg/poller/local_source.go`

实现 `LocalFileSource[T any]`，满足 `Source[T]` 接口：

```go
// LocalFileSource loads config data from a local JSON file on every Fetch call.
// It implements Source[T] for use with the generic Poller framework.
type LocalFileSource[T any] struct {
    path string
}

func NewLocalFileSource[T any](dir, filename string) *LocalFileSource[T] {
    return &LocalFileSource[T]{path: filepath.Join(dir, filename)}
}

func (s *LocalFileSource[T]) Fetch(ctx context.Context, version string) (bool, string, T, error) {
    var data T
    raw, err := os.ReadFile(s.path)
    if err != nil {
        return false, "", data, fmt.Errorf("local source read %s: %w", s.path, err)
    }
    if err := json.Unmarshal(raw, &data); err != nil {
        return false, "", data, fmt.Errorf("local source decode %s: %w", s.path, err)
    }
    return true, "", data, nil
}
```

**关键设计决策**：
- `changed` 始终返回 `true`，因为本地文件模式下每次重载
- 不加文件监听（watch/inotify）：调试场景手动修改文件重启 EPP 即可，保持简单
- 不加缓存/去重：文件内容由开发者控制，重复读取开销可忽略

### 4.2 文件命名与格式

`--local-config-dir` 指向的目录下需包含以下文件：

| 文件名 | 对应 API 端点 | 用途 | EPP 是否直接消费 |
|--------|--------------|------|:---:|
| `cluster_table.json` | `GET /configs/gslb_data/cluster_table` | 后端模型服务集群 RS 列表 | **是**（`ClusterDiscovery`） |
| `epp_data_config.json` | `GET /configs/epp_data/config` | per-cluster 调度参数 + assignment | **是**（`EppDataWatcher`） |
| `epp_pool.json` | `GET /open-api/v1/epp-pool` | EPP 实例池定义（实例组 + 实例列表） | 否（仅校验/文档用途） |

文件 JSON 格式直接对应 `innerapi.Client.Get` 解码的 `Config` 字段层级（**跳过外部 envelope**，直接从 `Data.Config` 层开始）：

**`cluster_table.json`** 格式（对应 `clusterTableConfig` 类型，即 `map[string]map[string][]BackendConf`）：

```json
{
  "cluster_epp_sim": {
    "cluster_epp_sim": [
      {
        "Name": "127.0.0.1_8981",
        "Addr": "127.0.0.1",
        "Port": 8981,
        "Weight": 50
      },
      {
        "Name": "127.0.0.1_8982",
        "Addr": "127.0.0.1",
        "Port": 8982,
        "Weight": 50
      },
      {
        "Name": "127.0.0.1_8983",
        "Addr": "127.0.0.1",
        "Port": 8983,
        "Weight": 50
      }
    ]
  }
}
```

**`epp_data_config.json`** 格式（对应 `innerapi.EppDataConfig` 类型）：

```json
{
  "epp_config": {
    "cluster_epp_sim": {
      "featureGates": ["flowControl"],
      "plugins": [
        {
          "name": "ep-discover",
          "type": "cluster-table-discovery",
          "parameters": { "clusterName": "cluster_epp_sim" }
        },
        {
          "name": "util-filter",
          "type": "utilization-filter",
          "parameters": {
            "conditions": [{ "maxValue": 0.9, "metric": "kv-cache-utilization" }]
          }
        },
        { "name": "kv-scorer", "type": "kv-cache-utilization-scorer", "parameters": {} },
        { "name": "queue-scorer", "type": "queue-scorer", "parameters": {} },
        { "name": "prefix-scorer", "type": "prefix-cache-scorer", "parameters": {} },
        { "name": "max-score", "type": "max-score-picker", "parameters": {} },
        { "name": "openai-parser", "type": "openai-parser", "parameters": {} }
      ],
      "schedulingProfiles": [{
        "name": "default",
        "plugins": [
          { "pluginRef": "util-filter", "weight": null },
          { "pluginRef": "kv-scorer", "weight": 0.6 },
          { "pluginRef": "queue-scorer", "weight": 0.6 },
          { "pluginRef": "prefix-scorer", "weight": 1 },
          { "pluginRef": "max-score", "weight": null }
        ]
      }],
      "dataLayer": {
        "discovery": { "endpoints": { "pluginRef": "ep-discover" } }
      },
      "flowControl": {
        "defaultRequestTTL": "1m0s",
        "noEndpointRequestTTL": "1m0s"
      },
      "requestHandler": {
        "parsers": [{ "pluginRef": "openai-parser" }]
      }
    }
  },
  "assignment": {
    "cluster_epp_sim": {
      "primary": "epp-id1",
      "standby": null
    }
  }
}
```

**`epp_pool.json`** 格式（对应 OpenAPI `/open-api/v1/epp-pool` 的 Data 层，不带 envelope）：

```json
{
  "name": "EPP.pool",
  "groups": [
    {
      "name": "epp-local",
      "instances": [
        { "id": "epp-id1", "host": "127.0.0.1", "port": 9002 }
      ]
    }
  ]
}
```

### 4.3 改造 `cmd/epp/main.go` 组装逻辑

现状（`main.go:66,76-83`）：

```go
client := innerapi.NewClient(cfg.APIAddr, cfg.APIToken, cfg.PollTimeout)
// ...
discoverySource := poller.NewClusterDiscovery(client, hub, ...)
eppDataWatcher := poller.NewEppDataWatcher(client, cfg.InstanceID, manager, logger)
```

目标态：

```go
useLocal := cfg.LocalConfigDir != ""
var discoverySrc poller.Source[map[string][]fwkdl.EndpointMetadata]
var eppDataSrc poller.Source[innerapi.EppDataConfig]

if useLocal {
    discoverySrc = poller.NewLocalFileSource[map[string][]fwkdl.EndpointMetadata](cfg.LocalConfigDir, "cluster_table.json")
    eppDataSrc = poller.NewLocalFileSource[innerapi.EppDataConfig](cfg.LocalConfigDir, "epp_data_config.json")
} else {
    client := innerapi.NewClient(cfg.APIAddr, cfg.APIToken, cfg.PollTimeout)
    discoverySrc = poller.NewClusterDiscovery(client, hub, ...)
    eppDataSrc = poller.NewEppDataWatcher(client, cfg.InstanceID, manager, logger)
}
```

注意：`ClusterDiscovery` 和 `EppDataWatcher` 本身已实现 `Source[T]` 接口，在 `local-config-dir` 模式下被 `LocalFileSource` 直接替代，不再创建 `innerapi.Client`、`ClusterDiscovery` 和 `EppDataWatcher` 实例。

### 4.4 `Config` 结构体字段新增

`cmd/epp/config.go:28-53` 新增：

```go
// 新增字段
LocalConfigDir string  // 本地配置目录，非空时所有集群级配置从文件加载
```

## 5. 涉及文件清单

| 文件 | 改造点 |
|------|--------|
| `cmd/epp/config.go` | `Config` 新增 `LocalConfigDir`；`parseConfig` 注册 `--local-config-dir` flag |
| `pkg/poller/local_source.go`（**新增**） | `LocalFileSource[T]` 实现 `Source[T]` 接口 |
| `cmd/epp/main.go` | `run()` 中根据 `cfg.LocalConfigDir` 选择 `LocalFileSource` 或 `innerapi.Client` 组装路径 |
| `pkg/poller/local_source_test.go`（**新增**） | `LocalFileSource` 单元测试 |

## 6. 使用示例

### 6.1 创建本地配置目录

```bash
mkdir -p /tmp/epp-local-config
# 向目录中放入 cluster_table.json、epp_data_config.json、epp_pool.json
```

### 6.2 启动 EPP（本地文件模式）

```bash
# 方式 1：命令行参数
./epp --local-config-dir=/tmp/epp-local-config \
      --instance-id=epp-id1 \
      --grpc-port=9002 \
      --health-port=9003 \
      --metrics-port=9090 \
      --default-pool=cluster_epp_sim

# 方式 2：环境变量
export AI_GATEWAY_EPP_LOCAL_CONFIG_DIR=/tmp/epp-local-config
./epp --instance-id=epp-id1
```

### 6.3 启动 EPP（InnerAPI 模式，默认，行为不变）

```bash
./epp --api-addr=http://172.19.1.222:8183/inner-api/v1 \
      --api-token=my-token \
      --instance-id=epp-0
```

## 7. 测试计划

### 7.1 单元测试（`pkg/poller/local_source_test.go`）

- `TestLocalFileSource_Fetch`：文件存在、JSON 合法 → `changed=true`，`data` 正确解码
- `TestLocalFileSource_FileNotFound`：文件不存在 → 返回 error（poller 会退避重试）
- `TestLocalFileSource_BadJSON`：JSON 非法 → 返回 error
- `TestLocalFileSource_VersionIgnored`：version 参数被忽略（始终 `changed=true`，`newVersion=""`）

### 7.2 集成验证

- 本地文件模式启动 EPP，验证：
  - cluster_table 中的 RS 地址被正确发现（`discovery` poller `FirstSync` 关闭）
  - epp_data_config 中的 assignment 自匹配正确（`EppDataWatcher` `FirstSync` 关闭）
  - Cell 创建成功、Ready 状态正确
  - gRPC 服务端口正常监听
- InnerAPI 模式（`--local-config-dir` 为空）行为不变（回归验证）

## 8. 风险与注意事项

| 风险 | 说明 | 缓解 |
|------|------|------|
| 本地文件与 InnerAPI 数据格式不一致 | `cluster_table.json` 和 `epp_data_config.json` 的文件格式从 `Data.Config` 层开始，跳过 envelope；与 InnerAPI 完整响应格式存在差异 | 文档明确两种模式的 JSON schema；新增单元测试覆盖两种格式 |
| 本地文件模式下 `APIAddr`/`APIToken` 仍为必填但未使用 | 当前 `parseConfig` 中 `APIAddr` 有默认值 `http://127.0.0.1:8181/inner-api/v1`，不会报错 | 在 `main.go` 中当 `LocalConfigDir` 非空时跳过 `innerapi.NewClient`，不影响启动 |
| `epp_pool.json` 不被 EPP 进程消费 | EPP 只通过 `epp_data_config.json` 的 `assignment` 段获取角色信息，`epp_pool.json` 仅作为参考/校验文件 | 文档注明用途（调试参考），不作运行时校验；由开发者自行保证 `epp_pool.json` 中的 id 与 `epp_data_config.json` 中的 `assignment` 一致 |
| 无热加载 | 本地文件模式下修改配置需重启 EPP | 本地调试场景可接受；生产环境仍使用 InnerAPI 的热加载模式 |

## 9. 非目标

- 不支持文件监听（inotify）自动热加载——本地开发场景手动重启足够
- 不支持单文件 UI / 管理界面配置
- 不改变 InnerAPI 模式下的任何行为（`--local-config-dir` 为空时全部路径不变）
- 不修改轮询框架（`pkg/poller/poller.go`）