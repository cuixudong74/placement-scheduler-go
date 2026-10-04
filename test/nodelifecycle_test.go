package test

import (
	"testing"

	"github.com/cuixudong74/placement-scheduler-go/pkg/types"
)

// 节点失联：其上负载被驱逐并重新调度到健康节点；
// 驱逐幂等，任何时刻负载不得同时绑定两个节点。
func TestNodeLostTriggersEvictionAndReschedule(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 4, nil))
	_ = c.OnNodeAdded("tester", node("n2", 4, nil))

	_ = s.SubmitWorkload(wl("w1", 0, 2, types.Constraints{}))
	c.Reconcile("tester")
	w1, _ := s.GetWorkload("w1")
	if w1.Phase != types.PhaseBound {
		t.Fatalf("w1 phase=%s, want Bound", w1.Phase)
	}
	firstNode := w1.Node

	// 失联：w1 应被驱逐并迁移到另一节点。
	lost := firstNode
	if err := c.OnNodeLost("tester", lost); err != nil {
		t.Fatal(err)
	}
	w1, _ = s.GetWorkload("w1")
	if w1.Phase != types.PhaseBound {
		t.Fatalf("w1 phase=%s, want Bound on survivor", w1.Phase)
	}
	if w1.Node == lost {
		t.Fatalf("w1 still on lost node %s", lost)
	}
	// 任何时刻只有一个绑定：Node 字段是单值，且失联节点账目为 0。
	if alloc := s.Allocated(lost); len(alloc) != 0 {
		t.Fatalf("lost node allocated=%v, want empty", alloc)
	}

	// 驱逐幂等：重复 OnNodeLost 不产生错误、不改变状态。
	if err := c.OnNodeLost("tester", lost); err != nil {
		t.Fatalf("repeated OnNodeLost should be idempotent: %v", err)
	}
	w1b, _ := s.GetWorkload("w1")
	if w1b.Phase != w1.Phase || w1b.Node != w1.Node || w1b.ResourceVersion != w1.ResourceVersion {
		t.Fatalf("idempotent OnNodeLost changed workload: %+v -> %+v", w1, w1b)
	}
}

// 节点恢复：状态对齐，可重新接受调度，审计给出可解释结论。
func TestNodeRecoveryAlignsState(t *testing.T) {
	c, s, log := newController()
	_ = c.OnNodeAdded("tester", node("n1", 4, nil))

	_ = s.SubmitWorkload(wl("w1", 0, 2, types.Constraints{}))
	c.Reconcile("tester")
	if got, _ := s.GetWorkload("w1"); got.Node != "n1" {
		t.Fatalf("w1 node=%q, want n1", got.Node)
	}

	// 失联又恢复（期间无其他节点，负载处于待调度）。
	if err := c.OnNodeLost("tester", "n1"); err != nil {
		t.Fatal(err)
	}
	w1, _ := s.GetWorkload("w1")
	if w1.Phase == types.PhaseBound {
		t.Fatal("w1 should be evicted from lost node")
	}
	if err := c.OnNodeRecovered("tester", "n1"); err != nil {
		t.Fatal(err)
	}

	// 恢复后负载重新绑定，账目对齐。
	w1, _ = s.GetWorkload("w1")
	if w1.Phase != types.PhaseBound || w1.Node != "n1" {
		t.Fatalf("w1 phase=%s node=%q, want Bound/n1 after recovery", w1.Phase, w1.Node)
	}
	if alloc := s.Allocated("n1"); alloc["cpu"] != 2 {
		t.Fatalf("allocated=%v, want cpu=2", alloc)
	}
	// 可解释结论：审计中存在恢复说明。
	found := false
	for _, r := range log.Records() {
		if r.Node == "n1" && r.Reason != "" && r.Action == "schedule" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected an audit record explaining recovery alignment")
	}
}

// 节点下线（Draining）：负载被驱逐迁移，且不再向该节点调度。
func TestNodeDraining(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 4, nil))
	_ = c.OnNodeAdded("tester", node("n2", 4, nil))

	_ = s.SubmitWorkload(wl("w1", 0, 4, types.Constraints{}))
	c.Reconcile("tester")
	w1, _ := s.GetWorkload("w1")
	if w1.Phase != types.PhaseBound {
		t.Fatalf("w1 phase=%s", w1.Phase)
	}

	if err := c.OnNodeDraining("tester", w1.Node); err != nil {
		t.Fatal(err)
	}
	w1, _ = s.GetWorkload("w1")
	if w1.Phase != types.PhaseBound {
		t.Fatalf("w1 phase=%s, want Bound on surviving node", w1.Phase)
	}
	n, _ := s.GetNode(w1.Node)
	if n.State != types.NodeReady {
		t.Fatalf("w1 re-bound to non-ready node %s (%s)", w1.Node, n.State)
	}

	// 新负载也不得调度到 Draining 节点。
	_ = s.SubmitWorkload(wl("w2", 0, 4, types.Constraints{}))
	c.Reconcile("tester")
	w2, _ := s.GetWorkload("w2")
	if w2.Phase == types.PhaseBound {
		n2, _ := s.GetNode(w2.Node)
		if n2.State != types.NodeReady {
			t.Fatalf("w2 bound to draining node %s", w2.Node)
		}
	}
}

// 失联节点上的负载被驱逐后，失联期间的“幻影占用”不得影响恢复后的账目：
// 恢复后节点账目只由当前绑定关系决定。
func TestNodeLostAccountingAlignedOnRecovery(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 4, nil))
	_ = c.OnNodeAdded("tester", node("n2", 4, nil))

	_ = s.SubmitWorkload(wl("w1", 0, 3, types.Constraints{}))
	c.Reconcile("tester")
	w1, _ := s.GetWorkload("w1")
	lost := w1.Node

	if err := c.OnNodeLost("tester", lost); err != nil {
		t.Fatal(err)
	}
	// 失联期间账目已释放（负载迁走）。
	if alloc := s.Allocated(lost); alloc["cpu"] != 0 {
		t.Fatalf("lost node allocated=%v, want 0", alloc)
	}
	if err := c.OnNodeRecovered("tester", lost); err != nil {
		t.Fatal(err)
	}
	// 恢复后节点为空，可接受新负载。
	_ = s.SubmitWorkload(wl("w2", 0, 4, types.Constraints{}))
	c.Reconcile("tester")
	w2, _ := s.GetWorkload("w2")
	if w2.Phase != types.PhaseBound {
		t.Fatalf("w2 phase=%s, want Bound (recovered node must accept full capacity)", w2.Phase)
	}
	total := s.Allocated("n1")["cpu"] + s.Allocated("n2")["cpu"]
	if total != 3+4 {
		t.Fatalf("total allocated=%d, want 7 (no leak, no double count)", total)
	}
}
