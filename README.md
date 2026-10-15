# ai-gateway-epp

ai-gateway 的 Endpoint Picker（EPP）：基于 [llm-d-router](https://github.com/llm-d/llm-d-router) 引擎包重新组合的多 cluster 调度进程。与上游 EPP 的差异：

- **无 Kubernetes 依赖**：实例列表来自 ai-gateway-api InnerAPI 的 cluster_table，调度配置与主备角色来自 `epp_data/config` 接口（`epp_config` + `assignment` 全量视图两段）——CRD reconciler 全部不存在。
- **一个进程服务多个 cluster**：每个被分配的 cluster 对应一个 Cell（数据面常驻：datastore + 指标采集；政策面热切换：调度引擎按配置版本原子换指针），ext-proc 请求按 BFE 注入的 `llm-d.ai/inference-pool` metadata 路由到对应 Cell。
- **配置热加载**：`epp_config` 变更编译为新引擎后原子切换，旧引擎 drain（FC 队列逐出 + 在飞请求等待上限），单 cluster 编译失败不影响其他 cluster。

## 目录结构

```
cmd/epp/            组合根：启动参数、插件注册、server 装配
pkg/cell/           Cell/Engine/Manager：生命周期、角色转换、编译热切换、drain
pkg/poller/         通用轮询框架 + ClusterDiscovery/EppDataWatcher（epp_config 编译切换 + assignment 全量视图自匹配）
pkg/innerapi/       InnerAPI HTTP client（信封解析、version 增量、Token 鉴权）
pkg/demux/          按 pool metadata 路由 ext-proc stream 到 Cell 的 StreamingServer
pkg/eppplugin/      cluster-table-discovery 发现插件（Hub → diff → datastore）
test/integration/   fake InnerAPI 端到端测试
```

## 构建与测试

```
make build     # 构建到 output/bin/epp（注入 VERSION 与 git commit），并同步 output/conf
make release   # 交叉编译 amd64/arm64 并打包 dist/epp_<version>_linux_<arch>.tar.gz（包内 bin/epp + conf/）
make docker    # 镜像内多阶段构建本地镜像（ai-gateway-epp:v<version> / :latest）
make clean     # 清理 output/ 与 dist/
go test ./...
```

产物布局（`make build`）：

```
output/
├── bin/epp        # 可执行文件
└── conf/          # 运行配置（由仓库 conf/ 拷贝；本地模式 --local-config-dir ./output/conf）
```

也可以 `go build ./...`，但直接构建出的二进制不含版本信息；`epp -v` 查看版本，`epp -V` 额外查看 go 版本与 git commit。

go.mod 通过 `replace` 指向本地 llm-d-router 工作区；需要 go1.26+。

## 运行

```
# 生产：配置来自 ai-gateway-api InnerAPI
./output/bin/epp -epp-group epp-a \
    -api-addr http://api-server:8181/inner-api/v1 \
    -grpc-port 9002 -health-port 9003 -metrics-port 9090

# 本地调试：读取本地配置目录（跳过 InnerAPI）
./output/bin/epp -epp-group epp-local -local-config-dir ./output/conf/epp_data_conf
```

`-epp-group` 是实例身份（assignment 自匹配用，须与 `/epp-pool` 组名一致）；**必填，无缺省值**——未设置时进程拒绝启动（fail-fast），避免实例静默不服务。

环境变量：`AI_GATEWAY_API_TOKEN`（InnerAPI 鉴权）、`AI_GATEWAY_EPP_GROUP`（组名）、`NAMESPACE`（endpoint pool 标签）。

## 设计文档

见 `ai-gateway-epp/docs/zh_cn/`（需求分析、系统设计、`configuration/EPP配置定义说明-epp_config.md`、详细设计、`modifications/2026-09-08-epp-scheduling-integration/`）。
