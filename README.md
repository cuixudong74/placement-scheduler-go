# placement-scheduler-go

一套**声明式工作负载调度控制器**的内存实现：持续把声明的资源需求调和到有限的执行节点上。
不依赖任何外部服务，全部状态在内存中维护，可直接以单元测试驱动。

核心包：`pkg/scheduler`。

## 能力总览

| 能力 | 入口 |
| --- | --- |
| 声明式负载提交（资源需求 + 放置约束） | `Controller.Submit` |
| 周期调和 / 事件触发调和 | `Controller.Run`（周期 + 事件双触发） |
| 节点新增 / 失联 / 恢复 / 下线 / 移除 | `AddNode` / `NodeLost` / `NodeRecovered` / `DrainNode` / `RemoveNode` |
| 确定性调度决策 | `Scheduler.ScheduleCycle` |
| 按优先级抢占（带防饥饿老化） | `Config.EnablePreemption` + 老化参数 |
| 版本校验的绑定写入（乐观锁） | `Store.BindWorkload` / `UpdateWorkloadStatus` |
| 权限校验与审计 | `Authorizer` / `AuditLog`（所有敏感操作） |

## 数据模型

- **Workload**：`Resources`（如 `{"cpu": 2, "mem": 1024}`）、`Priority`（越大越优先）、
  `Constraints`（见下）、以及由控制器维护的状态（`Phase`、`NodeID`、`Reason`、`Attempts`、
  `Preemptions`、`Version`）。
- **Node**：`Capacity`、`Allocated`（账目，由 Store 原子维护）、`Labels`、`Status`
  （`Ready / Lost / Draining / Removed`）、`Version`。
- **PlacementConstraints**：
  - `NodeSelector`：节点标签全等匹配；
  - `RequiredAffinity` / `RequiredAntiAffinity`：按 `TopologyKey` 拓扑域统计，
    要求域内存在 / 不存在带匹配标签的其他负载；
  - `Spread`：同一拓扑域内同 `Group` 负载数不超过 `MaxPerDomain`
    （负载用标签 `spread-group` 声明所属分组）。

## 调度语义

### 挂起与拒绝是可区分的

约束不可满足、信息缺失或声明非法时**绝不静默调度**，负载状态会带上明确原因码：

| Reason | 含义 | 是否可恢复 |
| --- | --- | --- |
| `InsufficientResources` | 容量不足（可能触发抢占或等待扩容） | 条件满足后自动重调度 |
| `ConstraintConflict` | 节点选择器 / 亲和 / 反亲和 / 分散约束冲突 | 环境变化后自动重调度 |
| `MissingTopologyInfo` | 节点缺少分散/亲和所需的拓扑标签 | 补全标签后自动重调度 |
| `NoReadyNode` | 无可用节点 | 节点上线后自动重调度 |
| `NodeLost` / `NodeDraining` / `Preempted` / `EvictedByAdmin` | 被驱逐的各类原因 | 自动重新排队 |
| `InvalidSpec` | 声明非法（如负资源、分散约束缺字段） | **永久拒绝**（`Failed`），不再调度 |

处于 `Pending / Evicted` 的负载在每个调度周期都会被重新评估（受退避窗口约束），
条件满足后自动进入调度，无需人工干预。

### 确定性

相同输入与相同节点集合下，调度结果**逐条一致**。稳定排序依据：

1. **候选负载排序**：有效优先级降序 → 创建时间升序 → ID 升序；
2. **节点选择**：放置后剩余容量更小者优先（best-fit，按资源名字典序逐项比较），
   完全并列时按节点 ID 升序；
3. **抢占方案**：受害者数升序 → 受害者有效优先级总和升序 → 节点 ID 升序；
   受害者内部按有效优先级升序 → ID 升序驱逐。

所有遍历（节点、负载、映射）在比较前都显式排序，Go map 遍历顺序与提交先后
都不影响最终归属。唯一的随机源是退避抖动，其种子由 `Config.RandomSeed` 固定，
且只影响重试时机、不影响归属。测试 `TestDeterministicPlacement` 在固定种子、
不同提交顺序下验证结果可复现。

### 抢占与防饥饿

- 仅当抢占者的**基础优先级**严格高于受害者的**有效优先级**时才允许驱逐，
  且只驱逐到刚好放得下为止；抢占后约束（如亲和）在新状态上重新校验。
- **防饥饿老化**：负载每失败 `AgingEvery` 次调度尝试，有效优先级提升 `AgingStep`。
  老化只抬高受害者的被抢占门槛（不抬高抢占者的抢占能力），因此：
  - 任何负载被同一批高优负载抢占的次数有限；
  - 不会出现两个负载互相抢占的乒乓；
  - 被抢占者自动重新排队，容量释放后按有效优先级优先获得调度。
- 抢占通过 `Store` 的原子解绑完成，账目不会超卖。

### 节点生命周期

- **新增**：触发重调度，挂起负载自动收敛；
- **失联（`NodeLost`）**：节点标记 `Lost`（不再参与调度），其上负载被幂等驱逐并重新排队；
- **恢复（`NodeRecovered`）**：节点上报实际运行集合，控制器以 Store 绑定关系为唯一事实
  来源进行对账，返回可解释的 `ReconcileReport`：
  - `Orphans`：节点在运行但 Store 未绑定给它的负载（应停止）；
  - `Missing`：Store 绑定给它但未运行的负载（应启动）；
  - `Aligned`：双方一致；
- **下线（`DrainNode`）**：标记 `Draining`，驱逐全部负载，新绑定被拒绝；
- **移除（`RemoveNode`）**：终态 `Removed`。

驱逐与重新绑定均为**幂等**操作；绑定关系是负载上的单值字段，
**同一负载在任何时刻不会同时绑定到两个节点**。

### 并发与账目一致性

- 所有写入带**版本校验**（乐观锁）：`BindWorkload` 同时校验负载与节点版本，
  冲突返回 `ErrVersionConflict`，失败方按退避策略重读重试，不覆盖他人更新；
- `BindWorkload` 在同一把锁内完成“容量校验 + 扣减账目 + 绑定”，
  因此 `Allocated <= Capacity` 恒成立，并发绑定同一节点不会超卖，
  同一负载的并发绑定最多一个生效（重复绑定同一节点幂等成功，不重复扣减）；
- 释放的容量立即可被重新分配；调度周期之间以 Store 最新状态为准，账目始终自洽。

### 权限与审计

- 所有敏感操作（提交、绑定、抢占、驱逐、节点变更/移除）都经过 `Authorizer` 校验；
  内置角色：`admin`（全部）、`scheduler`（绑定/抢占/驱逐）、`submitter`（提交）、
  `node-agent`（节点上报）；`system:controller` 等内置身份可配置为免鉴权；
- 每个操作（无论放行或拒绝）都追加一条 `AuditEvent`
  （时间、操作者、动作、目标、是否放行、细节），越权请求被拒绝且可追溯。

## 配置项（`Config`）

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `ScheduleInterval` | 1s | 调度周期（`Controller.Run` 的节拍） |
| `MaxRetries` | 5 | 绑定版本冲突的最大重试次数 |
| `BackoffBase` | 0 | 退避基数，第 n 次重试等待 `BackoffBase << n` + 抖动 |
| `BackoffMax` | 30s | 退避上限 |
| `AgingStep` | 1 | 防饥饿：每个老化周期有效优先级提升量 |
| `AgingEvery` | 10 | 老化周期（按调度尝试次数计） |
| `EnablePreemption` | false | 是否允许按优先级抢占 |
| `RandomSeed` | 0 | 退避抖动随机种子（固定种子可复现） |

时钟与睡眠函数可注入（`NewScheduler(..., now, sleep)`），测试可用假时钟获得完全可复现的行为。

## 快速开始

```go
store := scheduler.NewStore()
audit := scheduler.NewAuditLog(nil)
authz := &scheduler.Authorizer{SystemIdentities: map[string]bool{"system:controller": true}}
sched := scheduler.NewScheduler(store, scheduler.Config{
    ScheduleInterval: time.Second,
    EnablePreemption: true,
    RandomSeed:       42,
}, authz, audit, nil, nil)
c := scheduler.NewController(store, sched, authz, audit)

go c.Run(ctx) // 周期 + 事件双触发调和

admin := scheduler.Principal{Name: "ops", Roles: []string{"admin"}}
_ = c.AddNode(admin, scheduler.Node{ID: "n1", Capacity: scheduler.ResourceList{"cpu": 8}})
_ = c.Submit(admin, scheduler.Workload{
    ID: "web-1", Priority: 10,
    Resources: scheduler.ResourceList{"cpu": 2},
})
```

## 测试

```sh
go test ./...          # 全部测试
go test ./... -race    # 含竞态检测
```

覆盖：约束冲突与可区分原因、抢占与防饥饿、节点失联/恢复对账、下线驱逐、
并发绑定（同一负载/同一节点）、版本冲突、账目自洽、固定种子下的确定性复现、
权限拒绝与审计追溯、退避重试。
