// Package store 提供带版本校验的内存状态存储。
//
// 资源账目采用单一事实来源：节点已分配量始终由“绑定到该节点的负载
// 请求之和”现算，不存在独立账本，因此不会重复扣减或泄漏。
package store

import (
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/cuixudong74/placement-scheduler-go/pkg/types"
)

// 存储层错误，供调用方区分重试策略。
var (
	ErrNotFound     = errors.New("not found")
	ErrConflict     = errors.New("resource version conflict")
	ErrCapacity     = errors.New("insufficient capacity")
	ErrNodeNotReady = errors.New("node not ready")
	ErrInvalidState = errors.New("invalid state transition")
	ErrBound        = errors.New("workload already bound to another node")
)

// Store 是线程安全的内存状态存储，负载写入带 ResourceVersion 校验。
type Store struct {
	mu        sync.Mutex
	nodes     map[string]types.Node
	workloads map[string]types.Workload
}

// New 创建空存储。
func New() *Store {
	return &Store{
		nodes:     make(map[string]types.Node),
		workloads: make(map[string]types.Workload),
	}
}

// AddNode 注册节点（同名覆盖并保留 ResourceVersion 递增语义）。
func (s *Store) AddNode(n types.Node) error {
	if n.Name == "" {
		return fmt.Errorf("%w: node name empty", ErrInvalidState)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.nodes[n.Name]; ok {
		n.ResourceVersion = old.ResourceVersion + 1
	} else {
		n.ResourceVersion = 1
		if n.State == "" {
			n.State = types.NodeReady
		}
	}
	s.nodes[n.Name] = n
	return nil
}

// UpdateNodeState 变更节点状态（Ready/Lost/Draining/Offline）。
func (s *Store) UpdateNodeState(name string, state types.NodeState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[name]
	if !ok {
		return fmt.Errorf("%w: node %q", ErrNotFound, name)
	}
	if n.State == state {
		return nil // 幂等
	}
	n.State = state
	n.ResourceVersion++
	s.nodes[name] = n
	return nil
}

// RemoveNode 删除节点；仍有负载绑定时拒绝。
func (s *Store) RemoveNode(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.nodes[name]; !ok {
		return fmt.Errorf("%w: node %q", ErrNotFound, name)
	}
	for _, w := range s.workloads {
		if w.Phase == types.PhaseBound && w.Node == name {
			return fmt.Errorf("%w: node %q still has bound workloads", ErrInvalidState, name)
		}
	}
	delete(s.nodes, name)
	return nil
}

// GetNode 读取节点。
func (s *Store) GetNode(name string) (types.Node, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[name]
	return n, ok
}

// ListNodes 返回按名称排序的节点列表（确定性遍历）。
func (s *Store) ListNodes() []types.Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]types.Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// SubmitWorkload 提交工作负载。
//
// 声明非法（负资源、空拓扑键等）或约束自相矛盾时，负载被置为
// Phase=Rejected 并携带可区分原因（InvalidSpec / ConflictingConstraints），
// 永不进入调度；这属于“拒绝”而非错误，故返回 nil。
func (s *Store) SubmitWorkload(w types.Workload) error {
	if w.Name == "" {
		return fmt.Errorf("%w: workload name empty", ErrInvalidState)
	}
	w.Generation++
	w.ResourceVersion = 1
	w.Phase = types.PhasePending
	w.Node = ""
	w.Reason = types.ReasonNone
	w.Message = ""

	if msg := validateSpec(w); msg != "" {
		w.Phase = types.PhaseRejected
		w.Reason = types.ReasonInvalidSpec
		w.Message = msg
	} else if msg := detectConflict(w.Constraints); msg != "" {
		w.Phase = types.PhaseRejected
		w.Reason = types.ReasonConflictingConstraints
		w.Message = msg
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.workloads[w.Name]; ok {
		// 重复提交同名负载：保留版本递增，重新进入调度。
		w.ResourceVersion = old.ResourceVersion + 1
	}
	s.workloads[w.Name] = w
	return nil
}

// validateSpec 校验声明合法性，返回空串表示合法。
func validateSpec(w types.Workload) string {
	for k, v := range w.Requests {
		if v < 0 {
			return fmt.Sprintf("negative request for resource %q", k)
		}
	}
	for _, t := range w.Constraints.RequiredAffinity {
		if t.TopologyKey == "" {
			return "affinity term missing topologyKey"
		}
	}
	for _, t := range w.Constraints.RequiredAntiAffinity {
		if t.TopologyKey == "" {
			return "anti-affinity term missing topologyKey"
		}
	}
	for _, sp := range w.Constraints.Spread {
		if sp.TopologyKey == "" {
			return "spread constraint missing topologyKey"
		}
		if sp.MaxPerDomain < 1 {
			return "spread constraint maxPerDomain must be >= 1"
		}
	}
	return ""
}

// detectConflict 检测自相矛盾的约束：同一 (selector, topologyKey)
// 同时出现在亲和与反亲和中时，任何放置都不可能满足。
func detectConflict(c types.Constraints) string {
	for _, a := range c.RequiredAffinity {
		for _, r := range c.RequiredAntiAffinity {
			if a.TopologyKey == r.TopologyKey && selectorEqual(a.LabelSelector, r.LabelSelector) {
				return fmt.Sprintf("affinity and anti-affinity conflict on topologyKey %q", a.TopologyKey)
			}
		}
	}
	return ""
}

func selectorEqual(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if bv, ok := b[k]; !ok || bv != v {
			return false
		}
	}
	return true
}

// GetWorkload 读取负载。
func (s *Store) GetWorkload(name string) (types.Workload, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workloads[name]
	return w, ok
}

// ListWorkloads 返回按名称排序的全部负载（确定性顺序）。
func (s *Store) ListWorkloads() []types.Workload {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]types.Workload, 0, len(s.workloads))
	for _, w := range s.workloads {
		out = append(out, w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// Allocated 计算节点已分配资源（由绑定关系现算，单一事实来源）。
func (s *Store) Allocated(node string) types.ResourceList {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.allocatedLocked(node)
}

func (s *Store) allocatedLocked(node string) types.ResourceList {
	out := types.ResourceList{}
	for _, w := range s.workloads {
		if w.Phase == types.PhaseBound && w.Node == node {
			out = out.Add(w.Requests)
		}
	}
	return out
}

// Bind 以乐观并发方式把负载绑定到节点：
//   - 负载 ResourceVersion 必须匹配，否则 ErrConflict（调用方重试）；
//   - 已绑定到同一节点时幂等成功；已绑定到其他节点返回 ErrBound，
//     保证同一负载任何时刻不会同时绑定两个节点；
//   - 节点必须存在且 Ready，否则 ErrNodeNotReady；
//   - 容量必须足够，否则 ErrCapacity（不扣减任何账目）。
func (s *Store) Bind(workloadName string, workloadRV int64, node string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workloads[workloadName]
	if !ok {
		return fmt.Errorf("%w: workload %q", ErrNotFound, workloadName)
	}
	if w.ResourceVersion != workloadRV {
		return fmt.Errorf("%w: workload %q", ErrConflict, workloadName)
	}
	if w.Phase == types.PhaseBound {
		if w.Node == node {
			return nil // 幂等重放
		}
		return fmt.Errorf("%w: %q bound to %q", ErrBound, workloadName, w.Node)
	}
	if w.Phase == types.PhaseRejected {
		return fmt.Errorf("%w: workload %q rejected", ErrInvalidState, workloadName)
	}
	n, ok := s.nodes[node]
	if !ok {
		return fmt.Errorf("%w: node %q", ErrNotFound, node)
	}
	if n.State != types.NodeReady {
		return fmt.Errorf("%w: node %q is %s", ErrNodeNotReady, node, n.State)
	}
	if !types.Fits(n.Capacity, s.allocatedLocked(node), w.Requests) {
		return fmt.Errorf("%w: node %q", ErrCapacity, node)
	}
	w.Phase = types.PhaseBound
	w.Node = node
	w.Reason = types.ReasonNone
	w.Message = ""
	w.ResourceVersion++
	s.workloads[workloadName] = w
	return nil
}

// Unbind 解除绑定（幂等）：已绑定则释放并置为 Evicted，未绑定则仅记录原因。
func (s *Store) Unbind(workloadName string, reason types.Reason) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workloads[workloadName]
	if !ok {
		return fmt.Errorf("%w: workload %q", ErrNotFound, workloadName)
	}
	if w.Phase != types.PhaseBound {
		if w.Reason == reason {
			return nil // 幂等
		}
		w.Reason = reason
		w.ResourceVersion++
		s.workloads[workloadName] = w
		return nil
	}
	w.Phase = types.PhaseEvicted
	w.Node = ""
	w.Reason = reason
	w.ResourceVersion++
	s.workloads[workloadName] = w
	return nil
}

// MarkPending 把负载置为 Pending 并记录原因（幂等），等待条件满足后重调度。
func (s *Store) MarkPending(workloadName string, reason types.Reason, message string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workloads[workloadName]
	if !ok {
		return fmt.Errorf("%w: workload %q", ErrNotFound, workloadName)
	}
	if w.Phase == types.PhaseBound || w.Phase == types.PhaseRejected {
		return fmt.Errorf("%w: workload %q is %s", ErrInvalidState, workloadName, w.Phase)
	}
	if w.Phase == types.PhasePending && w.Reason == reason && w.Message == message {
		return nil // 幂等
	}
	w.Phase = types.PhasePending
	w.Reason = reason
	w.Message = message
	w.ResourceVersion++
	s.workloads[workloadName] = w
	return nil
}

// DeleteWorkload 删除负载：若已绑定先释放账目，再从存储移除（幂等）。
func (s *Store) DeleteWorkload(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.workloads[name]; !ok {
		return nil // 幂等
	}
	delete(s.workloads, name)
	return nil
}
