// Package types 定义调度系统的核心声明式类型。
package types

// ResourceList 表示一组命名资源的数量，如 {"cpu": 4, "memory": 8192}。
type ResourceList map[string]int64

// Add 返回 a+b 的副本。
func (a ResourceList) Add(b ResourceList) ResourceList {
	out := make(ResourceList, len(a))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] += v
	}
	return out
}

// Sub 返回 a-b 的副本（允许出现负值，由调用方校验）。
func (a ResourceList) Sub(b ResourceList) ResourceList {
	out := make(ResourceList, len(a))
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] -= v
	}
	return out
}

// Fits 判断 usage+req 是否不超过 capacity。
func Fits(capacity, usage, req ResourceList) bool {
	for k, v := range req {
		if usage[k]+v > capacity[k] {
			return false
		}
	}
	return true
}

// Clone 返回深拷贝。
func (a ResourceList) Clone() ResourceList {
	out := make(ResourceList, len(a))
	for k, v := range a {
		out[k] = v
	}
	return out
}

// Phase 是工作负载的生命周期阶段。
type Phase string

const (
	PhasePending  Phase = "Pending"  // 等待调度
	PhaseBound    Phase = "Bound"    // 已绑定到节点
	PhaseEvicted  Phase = "Evicted"  // 被驱逐/抢占，等待重新调度
	PhaseRejected Phase = "Rejected" // 声明本身非法，永不调度
)

// Reason 是可区分的挂起/拒绝原因。
type Reason string

const (
	ReasonNone                     Reason = ""
	ReasonInsufficientResources    Reason = "InsufficientResources"    // 资源不足，可被抢占或等待
	ReasonUnsatisfiableConstraints Reason = "UnsatisfiableConstraints" // 当前无节点满足约束
	ReasonConflictingConstraints   Reason = "ConflictingConstraints"   // 约束自相矛盾
	ReasonMissingTopologyInfo      Reason = "MissingTopologyInfo"      // 缺少拓扑/标签信息
	ReasonPreempted                Reason = "Preempted"                // 被高优负载抢占
	ReasonNodeLost                 Reason = "NodeLost"                 // 所在节点失联
	ReasonNodeDraining             Reason = "NodeDraining"             // 所在节点下线
	ReasonInvalidSpec              Reason = "InvalidSpec"              // 声明非法，拒绝
)

// AffinityTerm 描述一条（反）亲和规则：
// 在 TopologyKey 相同的拓扑域内，必须（不得）存在匹配 LabelSelector 的已绑定负载。
type AffinityTerm struct {
	LabelSelector map[string]string
	TopologyKey   string
}

// SpreadConstraint 描述分散约束：每个拓扑域内本负载所属组（同 GroupKey）的副本数不超过 MaxPerDomain。
type SpreadConstraint struct {
	TopologyKey  string
	MaxPerDomain int
	GroupKey     string // 为空时按负载自身 Name 分组
}

// Constraints 是负载声明的放置约束集合。
type Constraints struct {
	NodeSelector         map[string]string
	RequiredAffinity     []AffinityTerm
	RequiredAntiAffinity []AffinityTerm
	Spread               []SpreadConstraint
}

// Workload 是声明式工作负载。
type Workload struct {
	Name        string
	Labels      map[string]string
	Priority    int32
	Requests    ResourceList
	Constraints Constraints

	Phase   Phase
	Node    string
	Reason  Reason
	Message string

	Generation      int64 // 声明版本，用户每次修改 +1
	ResourceVersion int64 // 存储版本，每次写入 +1，用于乐观并发控制
}

// NodeState 是节点生命周期状态。
type NodeState string

const (
	NodeReady    NodeState = "Ready"
	NodeLost     NodeState = "Lost"
	NodeDraining NodeState = "Draining"
	NodeOffline  NodeState = "Offline"
)

// Node 是执行节点。
type Node struct {
	Name     string
	Labels   map[string]string
	Capacity ResourceList
	State    NodeState

	ResourceVersion int64
}
