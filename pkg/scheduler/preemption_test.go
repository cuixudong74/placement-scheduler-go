package scheduler

import (
	"testing"
)

func TestPreemptionEvictsLowerPriority(t *testing.T) {
	c, store, _ := newTestController(Config{EnablePreemption: true})
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 4}})
	submit(t, c, Workload{ID: "low", Priority: 1, Resources: ResourceList{"cpu": 3}})
	c.sched.ScheduleCycle(systemActor)
	if w := mustWorkload(t, store, "low"); w.Phase != PhaseBound {
		t.Fatalf("low should be bound, got %s", w.Phase)
	}

	// 高优负载到达，容量不足 -> 抢占 low
	submit(t, c, Workload{ID: "high", Priority: 10, Resources: ResourceList{"cpu": 3}})
	c.sched.ScheduleCycle(systemActor)

	high := mustWorkload(t, store, "high")
	if high.Phase != PhaseBound || high.NodeID != "n1" {
		t.Fatalf("high should be bound to n1, got %s/%s", high.Phase, high.NodeID)
	}
	low := mustWorkload(t, store, "low")
	if low.Phase != PhaseEvicted && low.Phase != PhasePending {
		t.Fatalf("low should be evicted/pending, got %s", low.Phase)
	}
	if low.Reason != ReasonPreempted && low.Reason != ReasonInsufficientResources {
		t.Fatalf("low reason should be Preempted, got %s", low.Reason)
	}
	if low.Preemptions != 1 {
		t.Fatalf("low.Preemptions should be 1, got %d", low.Preemptions)
	}
	// 账目不得超卖
	n, _ := store.GetNode("n1")
	if n.Allocated["cpu"] > n.Capacity["cpu"] {
		t.Fatalf("oversell: allocated %v > capacity %v", n.Allocated, n.Capacity)
	}
	if n.Allocated["cpu"] != 3 {
		t.Fatalf("allocated should be exactly 3 (high only), got %v", n.Allocated)
	}
	// 被抢占者重新排队：扩容后最终获得调度
	addNode(t, c, Node{ID: "n2", Capacity: ResourceList{"cpu": 4}})
	c.sched.ScheduleCycle(systemActor)
	low = mustWorkload(t, store, "low")
	if low.Phase != PhaseBound {
		t.Fatalf("preempted low should eventually be rescheduled, got %s (%s)", low.Phase, low.Message)
	}
}

func TestPreemptionNotAllowedForEqualOrLowerPriority(t *testing.T) {
	c, store, _ := newTestController(Config{EnablePreemption: true})
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 2}})
	submit(t, c, Workload{ID: "first", Priority: 5, Resources: ResourceList{"cpu": 2}})
	c.sched.ScheduleCycle(systemActor)
	// 同优先级不能抢占
	submit(t, c, Workload{ID: "second", Priority: 5, Resources: ResourceList{"cpu": 2}})
	c.sched.ScheduleCycle(systemActor)
	if w := mustWorkload(t, store, "second"); w.Phase == PhaseBound {
		t.Fatalf("equal priority must not preempt")
	}
	if w := mustWorkload(t, store, "first"); w.Phase != PhaseBound {
		t.Fatalf("first should remain bound, got %s", w.Phase)
	}
}

func TestPreemptionDisabled(t *testing.T) {
	c, store, _ := newTestController(Config{EnablePreemption: false})
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 2}})
	submit(t, c, Workload{ID: "low", Priority: 1, Resources: ResourceList{"cpu": 2}})
	c.sched.ScheduleCycle(systemActor)
	submit(t, c, Workload{ID: "high", Priority: 100, Resources: ResourceList{"cpu": 2}})
	c.sched.ScheduleCycle(systemActor)
	if w := mustWorkload(t, store, "high"); w.Phase != PhasePending {
		t.Fatalf("preemption disabled: high should stay pending, got %s", w.Phase)
	}
	if w := mustWorkload(t, store, "low"); w.Phase != PhaseBound {
		t.Fatalf("low should remain bound")
	}
}

// 防饥饿：老化提升有效优先级，等待足够久的低优负载
// 最终不再被更高基础优先级的负载抢占。
func TestAgingPreventsStarvation(t *testing.T) {
	cfg := Config{EnablePreemption: true, AgingStep: 1, AgingEvery: 1}
	c, store, _ := newTestController(cfg)
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 2}})
	submit(t, c, Workload{ID: "old", Priority: 1, Resources: ResourceList{"cpu": 2}})
	c.sched.ScheduleCycle(systemActor)
	if w := mustWorkload(t, store, "old"); w.Phase != PhaseBound {
		t.Fatalf("old should be bound")
	}
	// 让 old 积累等待老化：先被抢占再反复重试失败
	submit(t, c, Workload{ID: "p2", Priority: 5, Resources: ResourceList{"cpu": 2}})
	c.sched.ScheduleCycle(systemActor) // old 被 p2 抢占
	if w := mustWorkload(t, store, "old"); w.Phase == PhaseBound {
		t.Fatalf("old should have been preempted by p2")
	}
	// old 反复尝试（每次失败 Attempts+1，有效优先级随老化上升）
	for i := 0; i < 10; i++ {
		c.sched.ScheduleCycle(systemActor)
	}
	old := mustWorkload(t, store, "old")
	if old.EffectivePriority(1, 1) <= 5 {
		t.Fatalf("old effective priority should have aged above 5, got %d", old.EffectivePriority(1, 1))
	}
	// 释放容量让 old 重新绑定
	if err := c.Evict(systemActor, "p2"); err != nil {
		t.Fatal(err)
	}
	c.sched.ScheduleCycle(systemActor)
	old = mustWorkload(t, store, "old")
	if old.Phase != PhaseBound {
		t.Fatalf("old should be re-bound after capacity freed, got %s", old.Phase)
	}
	// 此时一个基础优先级 5 的新负载无法再抢占老化后的 old
	submit(t, c, Workload{ID: "p3", Priority: 5, Resources: ResourceList{"cpu": 2}})
	c.sched.ScheduleCycle(systemActor)
	old = mustWorkload(t, store, "old")
	if old.Phase != PhaseBound {
		t.Fatalf("aged old must not be preempted by equal-effective priority, got %s", old.Phase)
	}
	if w := mustWorkload(t, store, "p3"); w.Phase == PhaseBound {
		t.Fatalf("p3 should not have preempted aged old")
	}
}

// 被抢占的负载不会永久饥饿：持续有更高优负载到达时，
// 老化使其最终获得并保住位置。
func TestPreemptedWorkloadEventuallyWins(t *testing.T) {
	cfg := Config{EnablePreemption: true, AgingStep: 2, AgingEvery: 1}
	c, store, _ := newTestController(cfg)
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 2}})
	submit(t, c, Workload{ID: "victim", Priority: 1, Resources: ResourceList{"cpu": 2}})
	c.sched.ScheduleCycle(systemActor)

	// 连续 3 个更高优负载轮流抢占/占用
	for _, id := range []string{"x1", "x2", "x3"} {
		submit(t, c, Workload{ID: id, Priority: 3, Resources: ResourceList{"cpu": 2}})
		c.sched.ScheduleCycle(systemActor)
		// 占位的负载被管理驱逐，释放容量，模拟其完成
		for _, w := range store.ListWorkloads() {
			if w.Phase == PhaseBound && w.ID != "victim" {
				_ = c.Evict(systemActor, w.ID)
			}
		}
		c.sched.ScheduleCycle(systemActor)
	}
	// victim 老化后应能绑定
	v := mustWorkload(t, store, "victim")
	if v.Phase != PhaseBound {
		t.Fatalf("victim should eventually be bound, got %s (%s)", v.Phase, v.Message)
	}
	// 且有效优先级已高于 3，新的 priority=3 负载无法再抢占它
	submit(t, c, Workload{ID: "x4", Priority: 3, Resources: ResourceList{"cpu": 2}})
	c.sched.ScheduleCycle(systemActor)
	if v := mustWorkload(t, store, "victim"); v.Phase != PhaseBound {
		t.Fatalf("aged victim must not be preempted again")
	}
}
