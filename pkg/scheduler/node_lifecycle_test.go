package scheduler

import (
	"testing"
)

func TestNodeLostEvictsAndReschedules(t *testing.T) {
	// 单节点环境保证 w1 确定性落在 n1
	c2, store2, _ := newTestController(Config{})
	addNode(t, c2, Node{ID: "n1", Capacity: ResourceList{"cpu": 4}})
	submit(t, c2, Workload{ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 1}})
	c2.sched.ScheduleCycle(systemActor)

	if err := c2.NodeLost(systemActor, "n1"); err != nil {
		t.Fatal(err)
	}
	w := mustWorkload(t, store2, "w1")
	if w.Phase != PhaseEvicted || w.Reason != ReasonNodeLost {
		t.Fatalf("w1 should be evicted with NodeLost, got %s/%s", w.Phase, w.Reason)
	}
	n1, _ := store2.GetNode("n1")
	if n1.Allocated["cpu"] != 0 {
		t.Fatalf("lost node allocated should be released, got %v", n1.Allocated)
	}
	// 失联节点不可调度
	addNode(t, c2, Node{ID: "n2", Capacity: ResourceList{"cpu": 4}})
	c2.sched.ScheduleCycle(systemActor)
	w = mustWorkload(t, store2, "w1")
	if w.Phase != PhaseBound || w.NodeID != "n2" {
		t.Fatalf("w1 should be rescheduled to n2, got %s/%s", w.Phase, w.NodeID)
	}
}

func TestNodeRecoveryReconcile(t *testing.T) {
	// 通过 nodeSelector 把 w1/w2 固定在 n1
	c2, store2, _ := newTestController(Config{})
	addNode(t, c2, Node{ID: "n1", Labels: map[string]string{"rack": "r1"}, Capacity: ResourceList{"cpu": 4}})
	submit(t, c2, Workload{ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 1},
		Constraints: PlacementConstraints{NodeSelector: map[string]string{"rack": "r1"}}})
	submit(t, c2, Workload{ID: "w2", Priority: 1, Resources: ResourceList{"cpu": 1},
		Constraints: PlacementConstraints{NodeSelector: map[string]string{"rack": "r1"}}})
	c2.sched.ScheduleCycle(systemActor)

	// n1 失联，负载被驱逐；随后 n2 上线接管
	if err := c2.NodeLost(systemActor, "n1"); err != nil {
		t.Fatal(err)
	}
	addNode(t, c2, Node{ID: "n2", Labels: map[string]string{"rack": "r1"}, Capacity: ResourceList{"cpu": 4}})
	c2.sched.ScheduleCycle(systemActor)
	for _, id := range []string{"w1", "w2"} {
		if w := mustWorkload(t, store2, id); w.NodeID != "n2" {
			t.Fatalf("%s should be on n2, got %q", id, w.NodeID)
		}
	}
	// n1 恢复，上报它本地仍在运行 w1（残留）与 w2（残留）
	rep, err := c2.NodeRecovered(systemActor, "n1", []string{"w1", "w2"})
	if err != nil {
		t.Fatal(err)
	}
	// Store 中 w1/w2 均绑定 n2，n1 上的是孤儿，应被停止
	if len(rep.Orphans) != 2 {
		t.Fatalf("expected 2 orphans, got %v", rep.Orphans)
	}
	if len(rep.Missing) != 0 || len(rep.Aligned) != 0 {
		t.Fatalf("unexpected missing/aligned: %v / %v", rep.Missing, rep.Aligned)
	}
	// 任何时刻不得双绑：w1/w2 只能绑定 n2
	for _, id := range []string{"w1", "w2"} {
		w := mustWorkload(t, store2, id)
		if w.NodeID != "n2" || w.Phase != PhaseBound {
			t.Fatalf("%s double-bound or lost: %s/%s", id, w.Phase, w.NodeID)
		}
	}
	// 恢复后 n1 可重新参与调度
	submit(t, c2, Workload{ID: "w3", Priority: 1, Resources: ResourceList{"cpu": 1}})
	c2.sched.ScheduleCycle(systemActor)
	w3 := mustWorkload(t, store2, "w3")
	if w3.Phase != PhaseBound {
		t.Fatalf("w3 should be scheduled after recovery, got %s", w3.Phase)
	}
}

func TestRecoveryWithAlignedWorkloads(t *testing.T) {
	c, store, _ := newTestController(Config{})
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 4}})
	submit(t, c, Workload{ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 1}})
	c.sched.ScheduleCycle(systemActor)
	// 节点短暂失联又恢复，期间负载未被改绑（无其他节点）：
	// 失联即驱逐语义下 w1 已重新排队，恢复后重调度回 n1
	if err := c.NodeLost(systemActor, "n1"); err != nil {
		t.Fatal(err)
	}
	rep, err := c.NodeRecovered(systemActor, "n1", []string{"w1"})
	if err != nil {
		t.Fatal(err)
	}
	// w1 在 Store 中已解绑，节点上的 w1 是孤儿
	if len(rep.Orphans) != 1 || rep.Orphans[0] != "w1" {
		t.Fatalf("expected w1 orphan, got %v", rep.Orphans)
	}
	c.sched.ScheduleCycle(systemActor)
	if w := mustWorkload(t, store, "w1"); w.Phase != PhaseBound || w.NodeID != "n1" {
		t.Fatalf("w1 should rebind to recovered n1, got %s/%s", w.Phase, w.NodeID)
	}
	// 再次对账应对齐
	rep2, _ := c.NodeRecovered(systemActor, "n1", []string{"w1"})
	if len(rep2.Aligned) != 1 || len(rep2.Orphans) != 0 {
		t.Fatalf("expected aligned after rebind, got %+v", rep2)
	}
}

func TestDrainNodeEvictsAndRejectsNewBinds(t *testing.T) {
	c, store, _ := newTestController(Config{})
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 4}})
	addNode(t, c, Node{ID: "n2", Capacity: ResourceList{"cpu": 4}})
	submit(t, c, Workload{ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 1},
		Constraints: PlacementConstraints{NodeSelector: map[string]string{}}})
	c.sched.ScheduleCycle(systemActor)
	// 下线 w1 所在节点
	w1 := mustWorkload(t, store, "w1")
	victim := w1.NodeID
	if err := c.DrainNode(systemActor, victim); err != nil {
		t.Fatal(err)
	}
	w1 = mustWorkload(t, store, "w1")
	if w1.Phase != PhaseEvicted || w1.Reason != ReasonNodeDraining {
		t.Fatalf("w1 should be evicted with NodeDraining, got %s/%s", w1.Phase, w1.Reason)
	}
	// 重调度只能去另一个节点
	c.sched.ScheduleCycle(systemActor)
	w1 = mustWorkload(t, store, "w1")
	if w1.Phase != PhaseBound || w1.NodeID == victim {
		t.Fatalf("w1 must not rebind to draining node, got %s/%s", w1.Phase, w1.NodeID)
	}
	// 直接对 Draining 节点绑定应被拒绝
	n, _ := store.GetNode(victim)
	w2 := Workload{ID: "w2", Priority: 1, Resources: ResourceList{"cpu": 1}}
	_ = c.Submit(systemActor, w2)
	w2s := mustWorkload(t, store, "w2")
	if err := store.BindWorkload("w2", victim, w2s.Version, n.Version); err == nil {
		t.Fatalf("bind to draining node must fail")
	}
}

func TestEvictionIsIdempotent(t *testing.T) {
	c, store, _ := newTestController(Config{})
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 4}})
	submit(t, c, Workload{ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 2}})
	c.sched.ScheduleCycle(systemActor)
	// 重复驱逐/解绑均成功且不重复释放账目
	for i := 0; i < 3; i++ {
		if err := c.Evict(systemActor, "w1"); err != nil {
			t.Fatalf("evict %d: %v", i, err)
		}
	}
	n, _ := store.GetNode("n1")
	if n.Allocated["cpu"] != 0 {
		t.Fatalf("allocated should be 0 after idempotent evicts, got %v", n.Allocated)
	}
	// 重复 NodeLost 也幂等
	if err := c.NodeLost(systemActor, "n1"); err != nil {
		t.Fatal(err)
	}
	if err := c.NodeLost(systemActor, "n1"); err != nil {
		t.Fatalf("repeated NodeLost should be idempotent: %v", err)
	}
	n, _ = store.GetNode("n1")
	if n.Allocated["cpu"] != 0 {
		t.Fatalf("allocated must stay 0, got %v", n.Allocated)
	}
}

func TestNoDoubleBinding(t *testing.T) {
	c, store, _ := newTestController(Config{})
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 4}})
	addNode(t, c, Node{ID: "n2", Capacity: ResourceList{"cpu": 4}})
	submit(t, c, Workload{ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 1}})
	c.sched.ScheduleCycle(systemActor)
	w := mustWorkload(t, store, "w1")
	first := w.NodeID
	other := "n2"
	if first == "n2" {
		other = "n1"
	}
	// 已绑定负载再绑定到另一节点必须失败
	n2, _ := store.GetNode(other)
	if err := store.BindWorkload("w1", other, w.Version, n2.Version); err == nil {
		t.Fatalf("binding bound workload to another node must fail")
	}
	// 重复绑定同一节点幂等成功，不重复扣减
	n1, _ := store.GetNode(first)
	if err := store.BindWorkload("w1", first, w.Version, n1.Version); err != nil {
		t.Fatalf("re-bind to same node should be idempotent: %v", err)
	}
	n1, _ = store.GetNode(first)
	if n1.Allocated["cpu"] != 1 {
		t.Fatalf("allocated must remain 1, got %v", n1.Allocated)
	}
}
