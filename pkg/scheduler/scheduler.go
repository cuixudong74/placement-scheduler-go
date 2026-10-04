// Package scheduler 实现确定性调度与抢占。
//
// 确定性保证：所有遍历都基于按名称排序的切片；候选节点按
// （放置后剩余资源字典序降序，节点名升序）稳定排序；抢占受害者按
// （优先级升序，名称升序）选取。相同输入必得相同 Decision。
package scheduler

import (
	"fmt"
	"sort"

	"github.com/cuixudong74/placement-scheduler-go/pkg/store"
	"github.com/cuixudong74/placement-scheduler-go/pkg/types"
)

// Decision 是一次调度结论。
type Decision struct {
	Workload  string
	Node      string   // 为空表示未调度
	Preempted []string // 需要驱逐的低优负载（按确定顺序）
	Reason    types.Reason
	Message   string
}

// Scheduler 在 Store 快照上做确定性调度。
type Scheduler struct {
	Store             *store.Store
	PreemptionEnabled bool

	// EffectivePriority 返回负载用于排队与抢占资格判定的有效优先级
	// （控制器用其实现老化防饥饿）；为 nil 时使用声明优先级。
	EffectivePriority func(w types.Workload) int32
}

// New 创建调度器。
func New(s *store.Store) *Scheduler { return &Scheduler{Store: s, PreemptionEnabled: true} }

// snapshot 是一次调度决策所需的只近视图。
type snapshot struct {
	nodes     []types.Node // 按名称排序
	bound     []types.Workload
	allocated map[string]types.ResourceList
}

func (sc *Scheduler) snap() snapshot {
	nodes := sc.Store.ListNodes()
	bound := []types.Workload{}
	for _, w := range sc.Store.ListWorkloads() {
		if w.Phase == types.PhaseBound {
			bound = append(bound, w)
		}
	}
	alloc := make(map[string]types.ResourceList, len(nodes))
	for _, n := range nodes {
		alloc[n.Name] = types.ResourceList{}
	}
	for _, w := range bound {
		if _, ok := alloc[w.Node]; ok {
			alloc[w.Node] = alloc[w.Node].Add(w.Requests)
		}
	}
	return snapshot{nodes: nodes, bound: bound, allocated: alloc}
}

// cand 是一个通过硬约束过滤的候选节点；free==nil 表示容量不足需抢占。
type cand struct {
	node types.Node
	free types.ResourceList // 放置后的剩余资源
}

// ScheduleOne 为单个负载做调度决策（不提交绑定）。

// effPrio 返回有效优先级（含老化），未配置时用声明优先级。
func (sc *Scheduler) effPrio(w types.Workload) int32 {
	if sc.EffectivePriority != nil {
		return sc.EffectivePriority(w)
	}
	return w.Priority
}

func (sc *Scheduler) ScheduleOne(w types.Workload) Decision {
	s := sc.snap()
	d := Decision{Workload: w.Name}

	var feasible []cand
	anyMissing := false
	anyFiltered := false
	anyReady := false
	for _, n := range s.nodes {
		if n.State != types.NodeReady {
			continue
		}
		anyReady = true
		fr := s.checkConstraints(w, n)
		if !fr.ok {
			anyFiltered = true
			if fr.missing {
				anyMissing = true
			}
			continue
		}
		c := cand{node: n}
		if types.Fits(n.Capacity, s.allocated[n.Name], w.Requests) {
			c.free = n.Capacity.Sub(s.allocated[n.Name].Add(w.Requests))
		}
		feasible = append(feasible, c)
	}

	// 1) 直接可放置：按（剩余资源字典序降序，名称升序）稳定排序选最优。
	var direct []cand
	for _, c := range feasible {
		if c.free != nil {
			direct = append(direct, c)
		}
	}
	if len(direct) > 0 {
		sort.SliceStable(direct, func(i, j int) bool {
			if c := compareResourceList(direct[i].free, direct[j].free); c != 0 {
				return c > 0
			}
			return direct[i].node.Name < direct[j].node.Name
		})
		d.Node = direct[0].node.Name
		return d
	}

	// 2) 无直接容量：尝试抢占（仅在存在约束可行的候选时）。
	if len(feasible) > 0 {
		if sc.PreemptionEnabled {
			nodes := make([]types.Node, 0, len(feasible))
			for _, c := range feasible {
				nodes = append(nodes, c.node)
			}
			if victims, node, ok := sc.planPreemption(s, w, nodes); ok {
				d.Node = node
				d.Preempted = victims
				return d
			}
		}
		d.Reason = types.ReasonInsufficientResources
		d.Message = "no feasible node has enough capacity even after preempting lower-priority workloads"
		return d
	}

	// 3) 约束层面不可行：区分缺信息与不可满足。
	switch {
	case anyMissing:
		d.Reason = types.ReasonMissingTopologyInfo
		d.Message = "some ready nodes lack topology labels required by constraints"
	case anyFiltered || !anyReady:
		d.Reason = types.ReasonUnsatisfiableConstraints
		d.Message = "no ready node satisfies the placement constraints"
	default:
		d.Reason = types.ReasonInsufficientResources
		d.Message = "no ready node available"
	}
	return d
}

// filterResult 记录单个节点的过滤结论。
type filterResult struct {
	ok      bool
	missing bool // 缺少拓扑信息
	why     string
}

// checkConstraints 校验硬约束（不含容量）。
func (s snapshot) checkConstraints(w types.Workload, n types.Node) filterResult {
	for k, v := range w.Constraints.NodeSelector {
		if n.Labels[k] != v {
			return filterResult{why: fmt.Sprintf("nodeSelector %s=%s mismatch", k, v)}
		}
	}
	// 亲和：每个 term 要求同拓扑域内存在匹配负载。
	for _, t := range w.Constraints.RequiredAffinity {
		dom, ok := n.Labels[t.TopologyKey]
		if !ok {
			return filterResult{missing: true, why: "node missing topologyKey " + t.TopologyKey}
		}
		if !s.existsMatchingInDomain(t, w.Name, t.TopologyKey, dom) {
			return filterResult{why: "required affinity not satisfied in domain " + dom}
		}
	}
	// 反亲和：同拓扑域内不得存在匹配负载。
	for _, t := range w.Constraints.RequiredAntiAffinity {
		dom, ok := n.Labels[t.TopologyKey]
		if !ok {
			return filterResult{missing: true, why: "node missing topologyKey " + t.TopologyKey}
		}
		if s.existsMatchingInDomain(t, w.Name, t.TopologyKey, dom) {
			return filterResult{why: "anti-affinity violated in domain " + dom}
		}
	}
	// 分散：放入后组内副本数不得超过 MaxPerDomain。
	for _, sp := range w.Constraints.Spread {
		dom, ok := n.Labels[sp.TopologyKey]
		if !ok {
			return filterResult{missing: true, why: "node missing topologyKey " + sp.TopologyKey}
		}
		group := sp.GroupKey
		if group == "" {
			group = w.Name
		}
		count := 0
		for _, b := range s.bound {
			if b.Labels["group"] != group && b.Name != group {
				continue
			}
			bn := s.nodeOf(b.Node)
			if bn == nil {
				continue
			}
			if bn.Labels[sp.TopologyKey] == dom {
				count++
			}
		}
		if count+1 > sp.MaxPerDomain {
			return filterResult{why: fmt.Sprintf("spread limit %d reached in domain %s", sp.MaxPerDomain, dom)}
		}
	}
	return filterResult{ok: true}
}

func (s snapshot) nodeOf(name string) *types.Node {
	for i := range s.nodes {
		if s.nodes[i].Name == name {
			return &s.nodes[i]
		}
	}
	return nil
}

func (s snapshot) existsMatchingInDomain(t types.AffinityTerm, self, topoKey, dom string) bool {
	for _, b := range s.bound {
		if b.Name == self {
			continue
		}
		if !matchSelector(b.Labels, t.LabelSelector) {
			continue
		}
		bn := s.nodeOf(b.Node)
		if bn != nil && bn.Labels[topoKey] == dom {
			return true
		}
	}
	return false
}

func matchSelector(labels, sel map[string]string) bool {
	for k, v := range sel {
		if labels[k] != v {
			return false
		}
	}
	return true
}

// planPreemption 在约束可行的节点上规划抢占。
// 选择规则：受害者数量最少者优先，并列取节点名升序；
// 单节点内受害者按（优先级升序，名称升序）选取，只取优先级严格更低的负载。
func (sc *Scheduler) planPreemption(s snapshot, w types.Workload, nodes []types.Node) ([]string, string, bool) {
	type plan struct {
		node    types.Node
		victims []types.Workload
	}
	var plans []plan
	for _, n := range nodes {
		var victims []types.Workload
		for _, b := range s.bound {
			if b.Node == n.Name && sc.effPrio(b) < sc.effPrio(w) {
				victims = append(victims, b)
			}
		}
		sort.SliceStable(victims, func(i, j int) bool {
			if sc.effPrio(victims[i]) != sc.effPrio(victims[j]) {
				return sc.effPrio(victims[i]) < sc.effPrio(victims[j])
			}
			return victims[i].Name < victims[j].Name
		})
		used := s.allocated[n.Name]
		var picked []types.Workload
		for _, v := range victims {
			if types.Fits(n.Capacity, used, w.Requests) {
				break
			}
			used = used.Sub(v.Requests)
			picked = append(picked, v)
		}
		if types.Fits(n.Capacity, used, w.Requests) {
			plans = append(plans, plan{node: n, victims: picked})
		}
	}
	if len(plans) == 0 {
		return nil, "", false
	}
	sort.SliceStable(plans, func(i, j int) bool {
		if len(plans[i].victims) != len(plans[j].victims) {
			return len(plans[i].victims) < len(plans[j].victims)
		}
		return plans[i].node.Name < plans[j].node.Name
	})
	names := make([]string, 0, len(plans[0].victims))
	for _, v := range plans[0].victims {
		names = append(names, v.Name)
	}
	return names, plans[0].node.Name, true
}

// compareResourceList 按资源名排序后字典序比较，保证多资源比较确定。
func compareResourceList(a, b types.ResourceList) int {
	keys := map[string]struct{}{}
	for k := range a {
		keys[k] = struct{}{}
	}
	for k := range b {
		keys[k] = struct{}{}
	}
	ks := make([]string, 0, len(keys))
	for k := range keys {
		ks = append(ks, k)
	}
	sort.Strings(ks)
	for _, k := range ks {
		if a[k] != b[k] {
			if a[k] < b[k] {
				return -1
			}
			return 1
		}
	}
	return 0
}
