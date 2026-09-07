# ai-gateway-epp 集成测试

参照 `bfe/tests/integration` 的组织方式。被测组件为**真实独立进程**：ai-gateway-epp（本仓库构建的二进制）与 vLLM 模拟器（[llm-d-inference-sim](https://github.com/llm-d/llm-d-inference-sim)）；ai-gateway-api 为测试进程内的 HTTP mock，BFE 角色由测试代码以 ext-proc gRPC 客户端扮演。

## 目录结构

```
test/
├── common/                  # 公共 harness
│   ├── process_env.go       # epp / inference-sim 二进制编译缓存、启动、停止、就绪等待
│   ├── mock_ai_gateway_api.go  # ai-gateway-api InnerAPI mock（assignment/picker_config/cluster_table/report）
│   ├── sim_backend.go       # inference-sim 实例池管理（多后端、命中统计）
│   └── util.go              # FindFreePort / WaitForTCP 等
├── implementation/          # 测试代码，每个场景一个独立包
│   └── scenario-SCxx-<slug>/
│       └── scxx_xxx_test.go # TestTCxx_* 与测试设计文档 TC 编号一一对应
└── 测试设计文档/             # 中文设计文档（场景说明 + 逐 TC 用例）
    ├── 测试场景总体说明.md
    └── scenario-SCxx-<中文名>/
```

## 运行

```bash
go test ./test/... -v                                    # 全部场景
go test ./test/... -v -p 4                               # 降低包级并行（机器负载高时更稳）
go test ./test/implementation/scenario-SC01-.../ -v      # 单场景
go test ./test/implementation/scenario-SC01-.../ -run TestTC01 -v
```

> 注意：每个场景包会真实拉起 epp + sim 子进程，11 个包默认全并行时
> Windows 上偶发 30s 就绪窗口超时（重跑即过，失败位置不固定）。负载
> 较高的机器建议 `-p 4`，CI 可用 `-p 2`。

## 约定

- 仅标准库 `testing`（集成测试不引入 testify/ginkgo）。
- 每个 TC 独立环境：新起全套 epp + sim 进程与 mock API，结束即清理（`t.TempDir` + Kill/Wait）。
- epp 二进制由 harness 自动构建：每个测试包（独立测试进程）在进程独有临时目录中构建一份私有副本（go build cache 保证后续构建很快），并行包之间不共享可执行文件路径，无需手工准备。
- inference-sim 二进制查找顺序：`INFERENCE_SIM_BIN` 环境变量 → sibling checkout `../llm-d-inference-sim/bin/` → 缓存于 `test/.sim-bin/`（均不存在时跳过依赖 sim 的场景并 `t.Skip` 说明）。
- 端口全部 `FindFreePort` 动态分配，监听地址强制 `127.0.0.1`。
