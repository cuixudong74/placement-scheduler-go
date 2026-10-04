package test

import (
	"testing"

	"github.com/cuixudong74/placement-scheduler-go/pkg/types"
)

// 容量不足时高优负载抢占低优负载；被抢占者重新排队，
// 扩容后最终获得调度；全程资源账目不超卖。
func TestPreemptionAndReschedule(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 4, nil))

	low := wl("low", 0, 3, types.Constraints{})
	_ = s.SubmitWorkload(low)
	c.Reconcile("tester")
	if got, _ := s.GetWorkload("low"); got.Phase != types.PhaseBound {
		t.Fatalf("low phase=%s, want Bound", got.Phase)
	}

	// 高优负载需要 4cpu，节点只剩 1cpu：必须抢占 low。
	high := wl("high", 10, 4, types.Constraints{})
	_ = s.SubmitWorkload(high)
	c.Reconcile("tester")

	highGot, _ := s.GetWorkload("high")
	lowGot, _ := s.GetWorkload("low")
	if highGot.Phase != types.PhaseBound || highGot.Node != "n1" {
		t.Fatalf("high phase=%s node=%q, want Bound/n1", highGot.Phase, highGot.Node)
	}
	if lowGot.Phase == types.PhaseBound {
		t.Fatal("low should have been preempted")
	}
	if lowGot.Reason != types.ReasonPreempted {
		t.Fatalf("low reason=%s, want Preempted", lowGot.Reason)
	}
	if c.PreemptionCount("low") != 1 {
		t.Fatalf("preemption count=%d, want 1", c.PreemptionCount("low"))
	}

	// 不超卖：n1 上只有 high（4/4）。
	if alloc := s.Allocated("n1"); alloc["cpu"] != 4 {
		t.Fatalf("allocated=%v, want cpu=4", alloc)
	}

	// 被抢占者重新排队并最终获得调度：扩容后下一轮调和自动绑定。
	_ = c.OnNodeAdded("tester", node("n2", 4, nil))
	lowGot, _ = s.GetWorkload("low")
	if lowGot.Phase != types.PhaseBound || lowGot.Node != "n2" {
		t.Fatalf("low phase=%s node=%q, want Bound/n2", lowGot.Phase, lowGot.Node)
	}
	if alloc := s.Allocated("n1"); alloc["cpu"] != 4 {
		t.Fatalf("n1 allocated=%v, want cpu=4 (no oversell)", alloc)
	}
	if alloc := s.Allocated("n2"); alloc["cpu"] != 1*3 {
		t.Fatalf("n2 allocated=%v, want cpu=3", alloc)
	}
}

// 同优先级不得互相抢占。
func TestNoPreemptionAmongEquals(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 4, nil))

	_ = s.SubmitWorkload(wl("a", 5, 3, types.Constraints{}))
	c.Reconcile("tester")
	_ = s.SubmitWorkload(wl("b", 5, 4, types.Constraints{}))
	c.Reconcile("tester")

	a, _ := s.GetWorkload("a")
	b, _ := s.GetWorkload("b")
	if a.Phase != types.PhaseBound {
		t.Fatalf("a phase=%s, want Bound (must not be preempted by equal priority)", a.Phase)
	}
	if b.Phase != types.PhasePending || b.Reason != types.ReasonInsufficientResources {
		t.Fatalf("b phase=%s reason=%s, want Pending/InsufficientResources", b.Phase, b.Reason)
	}
}

// 防饥饿：被抢占后负载的有效优先级随抢占次数老化提升，
// 同优先级的后来者不得再次抢占它，因此不会永久饥饿。
func TestNoStarvationViaAging(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 4, nil))

	_ = s.SubmitWorkload(wl("victim", 0, 4, types.Constraints{}))
	c.Reconcile("tester")
	if got, _ := s.GetWorkload("victim"); got.Phase != types.PhaseBound {
		t.Fatalf("victim phase=%s, want Bound", got.Phase)
	}

	// 第一波：优先级 1 的攻击者抢占 victim（有效优先级 0 < 1）。
	_ = s.SubmitWorkload(wl("attacker-1", 1, 4, types.Constraints{}))
	c.Reconcile("tester")
	if got, _ := s.GetWorkload("victim"); got.Phase == types.PhaseBound {
		t.Fatal("victim should be preempted by higher priority attacker-1")
	}
	if n := c.PreemptionCount("victim"); n != 1 {
		t.Fatalf("preemption count=%d, want 1", n)
	}

	// 攻击者完成退出，victim 重新排队并绑定（被抢占者可恢复）。
	if err := s.DeleteWorkload("attacker-1"); err != nil {
		t.Fatal(err)
	}
	c.Reconcile("tester")
	if got, _ := s.GetWorkload("victim"); got.Phase != types.PhaseBound {
		t.Fatalf("victim phase=%s, want Bound after attacker left", got.Phase)
	}

	// 第二波：同为优先级 1 的攻击者到达。victim 有效优先级已老化为 1，
	// 不再满足“严格更低”的抢占条件，必须留在节点上。
	_ = s.SubmitWorkload(wl("attacker-2", 1, 4, types.Constraints{}))
	c.Reconcile("tester")
	victim, _ := s.GetWorkload("victim")
	attacker, _ := s.GetWorkload("attacker-2")
	if victim.Phase != types.PhaseBound {
		t.Fatalf("victim phase=%s, want Bound (aging must prevent repeated preemption)", victim.Phase)
	}
	if attacker.Phase != types.PhasePending || attacker.Reason != types.ReasonInsufficientResources {
		t.Fatalf("attacker-2 phase=%s reason=%s, want Pending/InsufficientResources",
			attacker.Phase, attacker.Reason)
	}
	if n := c.PreemptionCount("victim"); n != 1 {
		t.Fatalf("preemption count=%d, want still 1", n)
	}
	if alloc := s.Allocated("n1"); alloc["cpu"] > 4 {
		t.Fatalf("oversell: allocated=%v", alloc)
	}

	// 严格更高优先级（2 > 1）仍然可以抢占：老化不破坏优先级语义。
	_ = s.DeleteWorkload("attacker-2")
	_ = s.SubmitWorkload(wl("attacker-3", 2, 4, types.Constraints{}))
	c.Reconcile("tester")
	if got, _ := s.GetWorkload("victim"); got.Phase == types.PhaseBound {
		t.Fatal("victim should yield to strictly higher effective priority")
	}
	if n := c.PreemptionCount("victim"); n != 2 {
		t.Fatalf("preemption count=%d, want 2", n)
	}
}

// 抢占不得造成账目超卖：抢占后节点分配量必须等于幸存负载之和。
func TestPreemptionAccounting(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 10, nil))

	for _, w := range []types.Workload{
		wl("p0-a", 0, 4, types.Constraints{}),
		wl("p0-b", 0, 4, types.Constraints{}),
	} {
		_ = s.SubmitWorkload(w)
	}
	c.Reconcile("tester")

	_ = s.SubmitWorkload(wl("p9", 9, 8, types.Constraints{}))
	c.Reconcile("tester")

	alloc := s.Allocated("n1")
	if alloc["cpu"] > 10 {
		t.Fatalf("oversell: allocated=%v > 10", alloc)
	}
	// p9 需要 8，必须驱逐两个 p0 才能放下。
	p9, _ := s.GetWorkload("p9")
	if p9.Phase != types.PhaseBound {
		t.Fatalf("p9 phase=%s, want Bound", p9.Phase)
	}
	if alloc["cpu"] != 8 {
		t.Fatalf("allocated=%v, want exactly 8 (both victims evicted)", alloc)
	}
}
