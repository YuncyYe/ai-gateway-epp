# 详细设计-02：Cell 管理与热加载

## 1. 数据结构

```go
package cell

type Key string // = cluster 名

type Role int
const (
    RoleNone Role = iota
    RoleStandby              // 热数据冷准入
    RolePrimary              // 服务态
)

// Cell 数据面常驻；政策面经指针热切换。
type Cell struct {
    Key    Key
    ds     datastore.Datastore            // llm-d 引擎，常驻
    epf    datalayer.EndpointFactory      // 采集 goroutine 宿主
    engine atomic.Pointer[Engine]         // 政策面
    role   atomic.Int32                   // Role
    ready  chan struct{}                 // 首同步完成关闭（health 聚合用）
    drain  *drainState                   // 进行中的 drain（引擎或 Cell 级）
}

type Engine struct {
    Version    string                     // 配置版本（数据指纹，用于指标）
    Scheduler  scheduling.Scheduler
    Director   requestcontrol.Director
    Admission  requestcontrol.AdmissionController
    FC         *fccontroller.FlowController // drain 的主要对象
    Plugins    fwkplugin.Handle
    Saturation flowcontrol.SaturationDetector
}

type Manager struct {
    cells sync.Map // Key → *Cell
    // 依赖注入（便于测试替换）
    newDatastore   func(key Key) datastore.Datastore
    compileEngine  func(key Key, raw json.RawMessage, ds datastore.Datastore) (*Engine, error)
    drainScheduler *DrainScheduler
}

func (m *Manager) Get(key Key) (*Cell, bool)
func (m *Manager) Ensure(key Key, role Role) (*Cell, error)   // 幂等创建/角色调整
func (m *Manager) Demote(key Key)                              // 主→备（drain 引擎，保留数据面）
func (m *Manager) Promote(key Key)                             // 备→主（开闸）
func (m *Manager) Drop(key Key)                                // 无分配：drain → 释放数据面
func (m *Manager) SwapEngine(key Key, eng *Engine)             // 热加载切换
```

## 2. 角色转换矩阵

| 旧角色 \ 新角色 | 主 | 备 | 无分配 |
|---|---|---|---|
| 主 | 不变 | 停止准入（drain），保留数据 | drain 后销毁 |
| 备 | 开闸即服务 | 不变 | 销毁 |

`Demote` 与 `Drop` 的区别：Demote 保留数据面（该实例通常正是该 cluster 的新备）；Drop 连数据面一起释放。

## 3. 状态机

### 3.1 Cell 生命周期

```
            assignment 新增            discovery+config 双源齐备
   (无) ──────────────▶ CREATING ───────────────────────▶ STANDBY
                                                        │          ▲
                              promote（开闸）           ▼          │ demote（drain 引擎）
                                            PRIMARY ──────────────┘
   任一状态 ──assignment 移除──▶ DRAINING（停止准入，等 in-flight 归零）──▶ (无，释放数据面)
```

- `CREATING` 期间到达的请求：返回可重试错误（BFE 短暂回退本地均衡）
- `DRAINING` 有全局超时（`EngineDrainTimeout`，默认 60s），超时强杀并记指标
- 状态变迁全部由 EppDataWatcher 驱动（assignment 段驱动角色/生命周期，epp_config 段补引擎就绪条件），两段同 version 快照内一起应用

### 3.2 Engine 状态（Cell 内）

```
ACTIVE ⇄ DRAINING（SwapEngine 触发；排空条件=FC 队列空 && in-flight==0）
```

**FC 容量连续性**：drain 开始时将 saturation detector 当前估计快照传给后继 Engine 作初值，防切换瞬间超发。

## 4. 编译切换算法（热加载核心路径）

```
输入：cluster 新配置 cfg（json.RawMessage）
eng, err = compileEngine(cluster, cfg, cell.ds)   // loader 管线：严格解码/DAG 实例化/层序校验
if err:
    metrics engine_reloads_total{cluster, invalid}++
    log；cell 保留旧引擎；return            // 失败隔离：不影响其他 cell
cell.engine.Store(eng)                          // 原子指针切换，新请求立即生效
metrics engine_reloads_total{cluster, success}++
metrics engine_current_version{cluster} = eng.Version
drainScheduler.Schedule(cell, oldEng)           // 异步 drain，不阻塞后续
```

compileEngine 内部（复用 llm-d loader，函数化自 `cmd/epp/runner` 的 phase two）：`严格解码 rawConfig → 插件 DAG 拓扑实例化（注入 cell.ds 相关句柄）→ 组装 Scheduler/Director/Admission/FC → 校验（引用完整性/层序/Alpha gate）`。

## 5. Drain 语义

- 旧 FC **只出不进**（新请求走新引擎指针，不会进入旧队列）
- 排空条件：FC 队列为空 **且** in-flight 请求归零；超时强杀（计量丢失靠 metrics TTL 自愈，FR-H3 既定语义）
- 排空后收尾：`DeleteFlowControlFlowSeries` 清理旧 flow 序列指标；PluginState 依赖 llm-d janitor TTL 自然回收（不主动清理）
- DrainScheduler 为有限 goroutine 池（上限可配），避免批量再平衡时 drain 风暴

## 6. 并发模型

| 执行体 | 数量 | 所有权 |
|---|---|---|
| gRPC stream handler | 每请求 1 goroutine | 只读 engine 指针 + cell.ds（引擎内部 COW，无锁读） |
| Poller goroutine | 2（discovery/epp_data） | 各自 Source 串行；写路径只到 Manager API |
| drain goroutine | DrainScheduler 池（上限 N） | 独占旧 Engine，直到排空 |
| 采集 goroutine | 每端点 1（llm-d collector 托管于 cell.ds 生命周期） | 随 Cell 数据面生灭 |

纪律：Cell 的 `engine` 指针是唯一的跨 goroutine 可变点；`role/state` 用 atomic；Cell 集合用 sync.Map；端点/队列状态留在 llm-d 引擎内部（其 actor 模型单写者假设在 Cell 内成立——**每 Cell 独立 FC，绝不跨 Cell 共享流控组件**）。
