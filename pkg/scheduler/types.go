// Package scheduler 实现一套声明式工作负载的内存调度控制器。
package scheduler

import "time"

// ResourceList 描述一组命名资源的数量，如 {"cpu": 4, "memory": 8192}。
type ResourceList map[string]int64

// Add 返回 r 与 o 逐项相加后的新 ResourceList。
func (r ResourceList) Add(o ResourceList) ResourceList {
	out := make(ResourceList, len(r)+len(o))
	for k, v := range r {
		out[k] = v
	}
	for k, v := range o {
		out[k] += v
	}
	return out
}

// Sub 返回 r 与 o 逐项相减后的新 ResourceList。
func (r ResourceList) Sub(o ResourceList) ResourceList {
	out := make(ResourceList, len(r))
	for k, v := range r {
		out[k] = v
	}
	for k, v := range o {
		out[k] -= v
	}
	return out
}

// Fits 报告 usage + req 是否不超过 capacity。
func Fits(capacity, usage, req ResourceList) bool {
	for k, v := range req {
		if usage[k]+v > capacity[k] {
			return false
		}
	}
	return true
}

// NonNegative 报告所有资源项是否非负。
func (r ResourceList) NonNegative() bool {
	for _, v := range r {
		if v < 0 {
			return false
		}
	}
	return true
}

// WorkloadPhase 是工作负载的生命周期阶段。
type WorkloadPhase string

const (
	PhasePending WorkloadPhase = "Pending" // 等待调度
	PhaseBound   WorkloadPhase = "Bound"   // 已绑定到节点
	PhaseEvicted WorkloadPhase = "Evicted" // 被驱逐/抢占，等待重新入队
	PhaseFailed  WorkloadPhase = "Failed"  // 永久拒绝，不再调度
)

// Reason 是可区分的挂起/拒绝原因码。
type Reason string

const (
	ReasonNone                  Reason = ""
	ReasonInsufficientResources Reason = "InsufficientResources" // 资源不足（可抢占或等待）
	ReasonConstraintConflict    Reason = "ConstraintConflict"    // 亲和/反亲和/分散约束不可满足
	ReasonMissingTopologyInfo   Reason = "MissingTopologyInfo"   // 分散约束缺少拓扑信息
	ReasonNoReadyNode           Reason = "NoReadyNode"           // 无可用节点
	ReasonNodeLost              Reason = "NodeLost"              // 所在节点失联
	ReasonNodeDraining          Reason = "NodeDraining"          // 节点下线驱逐
	ReasonPreempted             Reason = "Preempted"             // 被高优负载抢占
	ReasonEvictedByAdmin        Reason = "EvictedByAdmin"        // 管理操作驱逐
	ReasonInvalidSpec           Reason = "InvalidSpec"           // 声明非法，永久拒绝
	ReasonRetryExhausted        Reason = "RetryExhausted"        // 重试次数耗尽
)

// MatchExpression 是一个简单的标签匹配表达式（In / NotIn / Exists）。
type MatchExpression struct {
	Key      string
	Operator string // "In" | "NotIn" | "Exists"
	Values   []string
}

// AffinityTerm 描述对“其他工作负载”的亲和/反亲和要求：
// 目标节点上（不）应运行带有匹配标签的负载。
type AffinityTerm struct {
	LabelSelector []MatchExpression
	TopologyKey   string // 在该拓扑域内统计，如 "zone"
}

// SpreadConstraint 描述按拓扑键分散的约束。
type SpreadConstraint struct {
	TopologyKey  string // 节点标签键，如 "zone"
	MaxPerDomain int    // 同一拓扑域内本负载同类（同 SpreadGroup）最多实例数
	Group        string // 参与分散统计的分组标签（通常等于负载所属服务名）
}

// PlacementConstraints 是全部放置约束。
type PlacementConstraints struct {
	NodeSelector         map[string]string // 节点标签必须全部匹配
	RequiredAffinity     []AffinityTerm    // 必须满足：域内存在匹配负载
	RequiredAntiAffinity []AffinityTerm    // 必须满足：域内不存在匹配负载
	Spread               []SpreadConstraint
}

// Workload 是声明式工作负载。
type Workload struct {
	ID          string
	Labels      map[string]string
	Priority    int64 // 越大越优先
	Resources   ResourceList
	Constraints PlacementConstraints

	Phase       WorkloadPhase
	NodeID      string // 仅当 Phase==Bound 有效；任何时刻最多绑定一个节点
	Reason      Reason
	Message     string
	Attempts    int // 已调度尝试次数（用于退避与防饥饿老化）
	Preemptions int // 被抢占次数

	Version   uint64 // 乐观锁版本，每次写入递增
	CreatedAt time.Time
	UpdatedAt time.Time
}

// NodeStatus 是节点生命周期状态。
type NodeStatus string

const (
	NodeReady    NodeStatus = "Ready"    // 可调度
	NodeLost     NodeStatus = "Lost"     // 失联，不可调度，等待恢复
	NodeDraining NodeStatus = "Draining" // 下线中，驱逐存量，不再接收新绑定
	NodeRemoved  NodeStatus = "Removed"  // 已移除（终态）
)

// Node 是执行节点。
type Node struct {
	ID        string
	Labels    map[string]string
	Capacity  ResourceList
	Allocated ResourceList // 已分配账目，由 Store 原子维护，Allocated <= Capacity 恒成立
	Status    NodeStatus
	Version   uint64
}

// EffectivePriority 计算考虑老化（防饥饿）后的有效优先级：
// 每等待 AgingEvery 次调度周期提升 AgingStep。
func (w *Workload) EffectivePriority(agingStep int64, agingEvery int) int64 {
	if agingEvery <= 0 {
		return w.Priority
	}
	return w.Priority + agingStep*int64(w.Attempts/agingEvery)
}
