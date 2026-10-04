package scheduler

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// ErrVersionConflict 表示乐观锁版本冲突，写入方应按策略重试。
var ErrVersionConflict = errors.New("version conflict")

// ErrInsufficientCapacity 表示节点容量不足，拒绝扣减（防超卖）。
var ErrInsufficientCapacity = errors.New("insufficient node capacity")

// ErrNotFound 表示目标对象不存在。
var ErrNotFound = errors.New("not found")

// Store 是内存状态存储，所有写操作带版本校验并在同一把锁内完成，
// 保证资源账目自洽：Allocated 永不超过 Capacity，释放可被重新分配。
type Store struct {
	mu        sync.Mutex
	workloads map[string]*Workload
	nodes     map[string]*Node
}

func NewStore() *Store {
	return &Store{
		workloads: make(map[string]*Workload),
		nodes:     make(map[string]*Node),
	}
}

// --- 节点 ---

// UpsertNode 新增或更新节点（容量/标签/状态）。新增时版本从 1 开始。
func (s *Store) UpsertNode(n Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if n.ID == "" {
		return fmt.Errorf("node id required")
	}
	if n.Capacity == nil {
		n.Capacity = ResourceList{}
	}
	if n.Allocated == nil {
		n.Allocated = ResourceList{}
	}
	if existing, ok := s.nodes[n.ID]; ok {
		// 保留既有分配账目与版本
		n.Allocated = existing.Allocated
		n.Version = existing.Version + 1
	} else {
		if n.Status == "" {
			n.Status = NodeReady
		}
		n.Version = 1
	}
	cp := n
	s.nodes[n.ID] = &cp
	return nil
}

// SetNodeStatus 带版本校验地更新节点状态。
func (s *Store) SetNodeStatus(nodeID string, status NodeStatus, expectedVersion uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok {
		return ErrNotFound
	}
	if n.Version != expectedVersion {
		return ErrVersionConflict
	}
	n.Status = status
	n.Version++
	return nil
}

// RemoveNode 将节点标记为 Removed（保留账目以便审计/对账）。
func (s *Store) RemoveNode(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok {
		return ErrNotFound
	}
	n.Status = NodeRemoved
	n.Version++
	return nil
}

// GetNode 返回节点快照。
func (s *Store) GetNode(nodeID string) (Node, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.nodes[nodeID]
	if !ok {
		return Node{}, false
	}
	return *n, true
}

// ListNodes 返回按 ID 排序的节点快照（确定性遍历）。
func (s *Store) ListNodes() []Node {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Node, 0, len(s.nodes))
	for _, n := range s.nodes {
		out = append(out, *n)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// --- 工作负载 ---

// SubmitWorkload 写入新负载（已存在同 ID 则报错，避免覆盖他人写入）。
func (s *Store) SubmitWorkload(w Workload) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if w.ID == "" {
		return fmt.Errorf("workload id required")
	}
	if _, ok := s.workloads[w.ID]; ok {
		return fmt.Errorf("workload %q already exists", w.ID)
	}
	if w.Phase == "" {
		w.Phase = PhasePending
	}
	w.Version = 1
	cp := w
	s.workloads[w.ID] = &cp
	return nil
}

// UpdateWorkloadStatus 带版本校验地更新负载状态字段（不改变绑定）。
func (s *Store) UpdateWorkloadStatus(id string, expectedVersion uint64, mutate func(w *Workload)) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workloads[id]
	if !ok {
		return ErrNotFound
	}
	if w.Version != expectedVersion {
		return ErrVersionConflict
	}
	mutate(w)
	w.Version++
	return nil
}

// BindWorkload 原子地完成“校验容量 + 扣减节点账目 + 绑定负载”，
// 同时校验负载与节点双方的版本，冲突则整体不生效。
// 同一负载在任何时刻只能绑定到一个节点：若已绑定到其他节点则报错。
func (s *Store) BindWorkload(workloadID, nodeID string, expectedWorkloadVersion, expectedNodeVersion uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workloads[workloadID]
	if !ok {
		return ErrNotFound
	}
	n, ok := s.nodes[nodeID]
	if !ok {
		return ErrNotFound
	}
	if w.Version != expectedWorkloadVersion || n.Version != expectedNodeVersion {
		return ErrVersionConflict
	}
	if w.Phase == PhaseBound {
		if w.NodeID == nodeID {
			return nil // 幂等：重复绑定同一节点视为成功
		}
		return fmt.Errorf("workload %q already bound to node %q", w.ID, w.NodeID)
	}
	if n.Status != NodeReady {
		return fmt.Errorf("node %q not ready (%s)", n.ID, n.Status)
	}
	if !Fits(n.Capacity, n.Allocated, w.Resources) {
		return ErrInsufficientCapacity
	}
	n.Allocated = n.Allocated.Add(w.Resources)
	n.Version++
	w.Phase = PhaseBound
	w.NodeID = nodeID
	w.Reason = ReasonNone
	w.Message = ""
	w.Version++
	return nil
}

// UnbindWorkload 幂等地解除绑定并释放节点账目。
// 重复调用、或对未绑定负载调用均返回 nil。
func (s *Store) UnbindWorkload(workloadID string, reason Reason, message string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workloads[workloadID]
	if !ok {
		return ErrNotFound
	}
	if w.Phase != PhaseBound {
		return nil // 幂等
	}
	if n, ok := s.nodes[w.NodeID]; ok {
		n.Allocated = n.Allocated.Sub(w.Resources)
		n.Version++
	}
	w.Phase = PhaseEvicted
	w.NodeID = ""
	w.Reason = reason
	w.Message = message
	w.Version++
	return nil
}

// GetWorkload 返回负载快照。
func (s *Store) GetWorkload(id string) (Workload, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workloads[id]
	if !ok {
		return Workload{}, false
	}
	return *w, true
}

// ListWorkloads 返回按 ID 排序的负载快照（确定性遍历）。
func (s *Store) ListWorkloads() []Workload {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Workload, 0, len(s.workloads))
	for _, w := range s.workloads {
		out = append(out, *w)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}
