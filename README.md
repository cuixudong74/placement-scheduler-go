# placement-scheduler-go

一套纯内存的声明式工作负载调度控制器：持续把声明的资源需求调和到有限的执行节点上。
不依赖任何外部服务，所有状态保存在进程内，适合本地运行与测试。

## 架构

```
pkg/types       核心声明式类型（Workload / Node / Constraints / Phase / Reason）
pkg/store       线程安全内存存储，绑定写入带 ResourceVersion 乐观并发校验
pkg/scheduler   确定性调度器：过滤（约束）→ 打分（容量）→ 抢占规划
pkg/controller  调和循环：队列排序、绑定重试退避、节点生命周期、防饥饿老化
pkg/audit       权限策略（RBAC）与审计日志
pkg/config      调度周期、重试次数与退避策略配置
test/           场景化自动化测试
```

## 调度语义

### 负载生命周期

`Pending → Bound`，异常路径：`Pending → Rejected`（终态）、`Bound → Evicted → Pending → Bound`。

- **Rejected（拒绝，终态）**：声明本身非法或自相矛盾，永不调度。原因可区分：
  - `InvalidSpec`：负资源请求、缺少 topologyKey、MaxPerDomain < 1 等；
  - `ConflictingConstraints`：同一 (labelSelector, topologyKey) 同时出现在亲和与反亲和中。
- **Pending（挂起，可恢复）**：当前无法调度但条件满足后自动重进调度。原因可区分：
  - `UnsatisfiableConstraints`：无 Ready 节点满足硬约束；
  - `MissingTopologyInfo`：节点缺少约束所需的拓扑标签；
  - `InsufficientResources`：约束可行但容量不足（含抢占后仍不足）；
  - `Preempted` / `NodeLost` / `NodeDraining`：被抢占或节点事件导致重新排队。

### 约束

- `NodeSelector`：节点标签等值匹配；
- `RequiredAffinity` / `RequiredAntiAffinity`：按 topologyKey 划域，域内必须存在/不得存在匹配标签的已绑定负载；
- `Spread`：同组（GroupKey，缺省为负载名）副本在每个拓扑域内不超过 MaxPerDomain。

### 确定性

相同输入与相同节点集合下，调度结果逐条一致。稳定排序依据：

- 节点遍历：按名称升序；
- 待调度队列：按（有效优先级降序，名称升序）；
- 候选节点选择：按（放置后剩余资源字典序降序——资源名排序后逐项比较，节点名升序）；
- 抢占受害者：按（有效优先级升序，名称升序）；多节点方案按（受害者数升序，节点名升序）选择。

实现中不依赖 map 遍历顺序与任何运行时随机源。

### 抢占与防饥饿

- 仅当负载的**有效优先级**严格更低时才可被抢占；
- 有效优先级 = 声明优先级 + min(被抢占次数, 10)（老化）；
- 被抢占负载自动重新排队，容量可用后重新绑定；老化使同优先级后来者无法反复抢占同一受害者，杜绝永久饥饿；
- 抢占先驱逐受害者再绑定，账目由绑定关系现算，不会超卖。

### 并发与账目

- `Bind(workload, resourceVersion, node)` 为乐观并发写入：版本不匹配返回 `ErrConflict`，
  容量不足返回 `ErrCapacity`，已绑定他节点返回 `ErrBound`，调用方按配置退避重试，
  每次重试重新读取版本，绝不覆盖他人写入；
- 对已绑定到同一节点的重复 Bind 是幂等成功；同一负载任何时刻不会同时绑定两个节点；
- 节点已分配量 = 绑定到该节点的负载请求之和（单一事实来源现算），
  释放即可被重新分配，无独立账本，不会重复扣减或泄漏；
- 节点失联（Lost）/下线（Draining）触发驱逐与重调度，驱逐幂等；
  恢复（Ready）后账目天然对齐，并写入可解释的审计记录。

### 权限与审计

- 所有调度、绑定、抢占、驱逐动作经 `audit.Policy` 校验并写入审计日志；
- 内置 `RolePolicy`：`viewer` 只读，`operator` 可调度/绑定/解绑，`admin` 额外可抢占/驱逐，未知 actor 一律拒绝；
- 越权请求被拒绝且留下 `Allowed=false` 的审计记录（含 actor、动作、负载、节点、原因）。

## 配置项（pkg/config.Config）

| 字段 | 默认 | 说明 |
| --- | --- | --- |
| `ScheduleInterval` | 100ms | 调度周期（`Controller.Run` 的调和间隔） |
| `MaxBindRetries` | 5 | 绑定版本冲突的最大重试次数 |
| `RetryBackoffBase` | 1ms | 退避基数，第 n 次重试等待 `base << n` |
| `RetryBackoffMax` | 100ms | 退避上限 |
| `PreemptionEnabled` | true | 是否允许高优抢占低优 |

## 运行测试

```sh
go test ./...          # 全部测试
go test ./... -race    # 含竞态检测
```

测试覆盖：约束冲突与拒绝原因、缺失拓扑信息后的自动恢复调度、抢占与重新排队、
老化防饥饿、节点失联/恢复/下线、并发绑定唯一胜者、账目自洽、越权拒绝与审计、
固定种子不同提交顺序下的确定性复现。
