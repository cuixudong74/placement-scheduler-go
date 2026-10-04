package scheduler

import (
	"testing"
	"time"
)

var systemActor = Principal{Name: "system:controller"}

func newTestController(cfg Config) (*Controller, *Store, *AuditLog) {
	store := NewStore()
	audit := NewAuditLog(nil)
	authz := &Authorizer{SystemIdentities: map[string]bool{"system:controller": true}}
	sched := NewScheduler(store, cfg, authz, audit, nil, func(time.Duration) {})
	return NewController(store, sched, authz, audit), store, audit
}

func submit(t *testing.T, c *Controller, w Workload) {
	t.Helper()
	if err := c.Submit(systemActor, w); err != nil {
		t.Fatalf("submit %s: %v", w.ID, err)
	}
}

func addNode(t *testing.T, c *Controller, n Node) {
	t.Helper()
	if err := c.AddNode(systemActor, n); err != nil {
		t.Fatalf("add node %s: %v", n.ID, err)
	}
}

func mustWorkload(t *testing.T, s *Store, id string) Workload {
	t.Helper()
	w, ok := s.GetWorkload(id)
	if !ok {
		t.Fatalf("workload %s not found", id)
	}
	return w
}

func TestBasicSchedulingBindsWorkload(t *testing.T) {
	c, store, _ := newTestController(Config{})
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 4}})
	submit(t, c, Workload{ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 2}})

	placed := c.sched.ScheduleCycle(systemActor)
	if len(placed) != 1 || placed[0] != "w1" {
		t.Fatalf("expected [w1] placed, got %v", placed)
	}
	w := mustWorkload(t, store, "w1")
	if w.Phase != PhaseBound || w.NodeID != "n1" {
		t.Fatalf("w1 should be bound to n1, got %s/%s", w.Phase, w.NodeID)
	}
	n, _ := store.GetNode("n1")
	if n.Allocated["cpu"] != 2 {
		t.Fatalf("n1 allocated should be 2, got %v", n.Allocated)
	}
}

func TestInsufficientResourcesIsPendingNotSilent(t *testing.T) {
	c, store, _ := newTestController(Config{})
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 2}})
	submit(t, c, Workload{ID: "big", Priority: 1, Resources: ResourceList{"cpu": 8}})

	c.sched.ScheduleCycle(systemActor)
	w := mustWorkload(t, store, "big")
	if w.Phase != PhasePending || w.Reason != ReasonInsufficientResources {
		t.Fatalf("expected Pending/InsufficientResources, got %s/%s", w.Phase, w.Reason)
	}
	// 条件满足（扩容）后自动重新进入调度
	addNode(t, c, Node{ID: "n2", Capacity: ResourceList{"cpu": 16}})
	c.sched.ScheduleCycle(systemActor)
	w = mustWorkload(t, store, "big")
	if w.Phase != PhaseBound || w.NodeID != "n2" {
		t.Fatalf("expected bound to n2 after capacity added, got %s/%s", w.Phase, w.NodeID)
	}
}

func TestNodeSelectorConflict(t *testing.T) {
	c, store, _ := newTestController(Config{})
	addNode(t, c, Node{ID: "n1", Labels: map[string]string{"disk": "hdd"}, Capacity: ResourceList{"cpu": 4}})
	submit(t, c, Workload{
		ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 1},
		Constraints: PlacementConstraints{NodeSelector: map[string]string{"disk": "ssd"}},
	})
	c.sched.ScheduleCycle(systemActor)
	w := mustWorkload(t, store, "w1")
	if w.Phase != PhasePending || w.Reason != ReasonConstraintConflict {
		t.Fatalf("expected Pending/ConstraintConflict, got %s/%s (%s)", w.Phase, w.Reason, w.Message)
	}
}

func TestAntiAffinityConflictThenAutoReschedule(t *testing.T) {
	c, store, _ := newTestController(Config{})
	addNode(t, c, Node{ID: "n1", Labels: map[string]string{"zone": "a"}, Capacity: ResourceList{"cpu": 8}})
	anti := PlacementConstraints{
		RequiredAntiAffinity: []AffinityTerm{{
			LabelSelector: []MatchExpression{{Key: "app", Operator: "In", Values: []string{"web"}}},
			TopologyKey:   "zone",
		}},
	}
	submit(t, c, Workload{ID: "w1", Labels: map[string]string{"app": "web"}, Priority: 1, Resources: ResourceList{"cpu": 1}})
	submit(t, c, Workload{ID: "w2", Labels: map[string]string{"app": "web"}, Priority: 1, Resources: ResourceList{"cpu": 1}, Constraints: anti})

	c.sched.ScheduleCycle(systemActor)
	w2 := mustWorkload(t, store, "w2")
	if w2.Phase != PhasePending || w2.Reason != ReasonConstraintConflict {
		t.Fatalf("w2 expected Pending/ConstraintConflict, got %s/%s", w2.Phase, w2.Reason)
	}
	// 新增另一 zone 的节点后，w2 自动被调度
	addNode(t, c, Node{ID: "n2", Labels: map[string]string{"zone": "b"}, Capacity: ResourceList{"cpu": 8}})
	c.sched.ScheduleCycle(systemActor)
	w2 = mustWorkload(t, store, "w2")
	if w2.Phase != PhaseBound || w2.NodeID != "n2" {
		t.Fatalf("w2 expected bound to n2, got %s/%s", w2.Phase, w2.NodeID)
	}
}

func TestSpreadConstraintAndMissingTopology(t *testing.T) {
	c, store, _ := newTestController(Config{})
	spread := PlacementConstraints{Spread: []SpreadConstraint{{TopologyKey: "zone", MaxPerDomain: 1, Group: "web"}}}
	// 节点缺少 zone 标签 -> 缺少必要信息，不得静默调度
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 8}})
	submit(t, c, Workload{ID: "w1", Labels: map[string]string{"spread-group": "web"}, Priority: 1, Resources: ResourceList{"cpu": 1}, Constraints: spread})
	c.sched.ScheduleCycle(systemActor)
	w1 := mustWorkload(t, store, "w1")
	if w1.Phase != PhasePending || w1.Reason != ReasonMissingTopologyInfo {
		t.Fatalf("w1 expected Pending/MissingTopologyInfo, got %s/%s", w1.Phase, w1.Reason)
	}

	// 两个同组负载、单 zone 单节点：第二个违反 MaxPerDomain=1
	c2, store2, _ := newTestController(Config{})
	addNode(t, c2, Node{ID: "n1", Labels: map[string]string{"zone": "a"}, Capacity: ResourceList{"cpu": 8}})
	submit(t, c2, Workload{ID: "a", Labels: map[string]string{"spread-group": "web"}, Priority: 1, Resources: ResourceList{"cpu": 1}, Constraints: spread})
	submit(t, c2, Workload{ID: "b", Labels: map[string]string{"spread-group": "web"}, Priority: 1, Resources: ResourceList{"cpu": 1}, Constraints: spread})
	c2.sched.ScheduleCycle(systemActor)
	wb := mustWorkload(t, store2, "b")
	if wb.Phase != PhasePending || wb.Reason != ReasonConstraintConflict {
		t.Fatalf("b expected Pending/ConstraintConflict, got %s/%s (%s)", wb.Phase, wb.Reason, wb.Message)
	}
}

func TestInvalidSpecIsRejectedPermanently(t *testing.T) {
	c, store, _ := newTestController(Config{})
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 4}})
	submit(t, c, Workload{ID: "bad", Priority: 1, Resources: ResourceList{"cpu": -1}})
	c.sched.ScheduleCycle(systemActor)
	w := mustWorkload(t, store, "bad")
	if w.Phase != PhaseFailed || w.Reason != ReasonInvalidSpec {
		t.Fatalf("expected Failed/InvalidSpec, got %s/%s", w.Phase, w.Reason)
	}
	// 后续周期不再尝试
	if placed := c.sched.ScheduleCycle(systemActor); len(placed) != 0 {
		t.Fatalf("failed workload must not be rescheduled, placed=%v", placed)
	}
}

func TestNoReadyNode(t *testing.T) {
	c, store, _ := newTestController(Config{})
	submit(t, c, Workload{ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 1}})
	c.sched.ScheduleCycle(systemActor)
	w := mustWorkload(t, store, "w1")
	if w.Phase != PhasePending || w.Reason != ReasonNoReadyNode {
		t.Fatalf("expected Pending/NoReadyNode, got %s/%s", w.Phase, w.Reason)
	}
}
