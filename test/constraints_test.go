package test

import (
	"testing"

	"github.com/cuixudong74/placement-scheduler-go/pkg/audit"
	"github.com/cuixudong74/placement-scheduler-go/pkg/config"
	"github.com/cuixudong74/placement-scheduler-go/pkg/controller"
	"github.com/cuixudong74/placement-scheduler-go/pkg/store"
	"github.com/cuixudong74/placement-scheduler-go/pkg/types"
)

func newController() (*controller.Controller, *store.Store, *audit.Log) {
	s := store.New()
	log := audit.NewLog()
	c := controller.New(s, config.Default(), audit.AllowAll{}, log)
	return c, s, log
}

func node(name string, cpu int64, labels map[string]string) types.Node {
	return types.Node{
		Name:     name,
		Labels:   labels,
		Capacity: types.ResourceList{"cpu": cpu},
		State:    types.NodeReady,
	}
}

func wl(name string, prio int32, cpu int64, c types.Constraints) types.Workload {
	return types.Workload{
		Name:        name,
		Priority:    prio,
		Requests:    types.ResourceList{"cpu": cpu},
		Constraints: c,
	}
}

// 约束自相矛盾：同一 (selector, topologyKey) 同时要求亲和与反亲和，
// 必须被拒绝且原因可区分，永不进入调度。
func TestConflictingConstraintsRejected(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 8, map[string]string{"zone": "a"}))

	term := types.AffinityTerm{LabelSelector: map[string]string{"app": "db"}, TopologyKey: "zone"}
	w := wl("w-conflict", 0, 1, types.Constraints{
		RequiredAffinity:     []types.AffinityTerm{term},
		RequiredAntiAffinity: []types.AffinityTerm{term},
	})
	if err := s.SubmitWorkload(w); err != nil {
		t.Fatal(err)
	}
	c.Reconcile("tester")

	got, _ := s.GetWorkload("w-conflict")
	if got.Phase != types.PhaseRejected {
		t.Fatalf("phase = %s, want Rejected", got.Phase)
	}
	if got.Reason != types.ReasonConflictingConstraints {
		t.Fatalf("reason = %s, want ConflictingConstraints", got.Reason)
	}
}

// 非法声明（负资源请求）被拒绝，原因与约束冲突可区分。
func TestInvalidSpecRejected(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 8, nil))

	w := wl("w-bad", 0, -1, types.Constraints{})
	if err := s.SubmitWorkload(w); err != nil {
		t.Fatal(err)
	}
	c.Reconcile("tester")

	got, _ := s.GetWorkload("w-bad")
	if got.Phase != types.PhaseRejected || got.Reason != types.ReasonInvalidSpec {
		t.Fatalf("phase=%s reason=%s, want Rejected/InvalidSpec", got.Phase, got.Reason)
	}
}

// 缺少拓扑信息：节点没有约束要求的 topologyKey 标签，
// 负载挂起且原因可区分；补上标签后自动进入调度。
func TestMissingTopologyInfoThenScheduled(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 8, map[string]string{})) // 无 zone 标签

	w := wl("w-needtopo", 0, 1, types.Constraints{
		RequiredAntiAffinity: []types.AffinityTerm{
			{LabelSelector: map[string]string{"app": "db"}, TopologyKey: "zone"},
		},
	})
	_ = s.SubmitWorkload(w)
	c.Reconcile("tester")

	got, _ := s.GetWorkload("w-needtopo")
	if got.Phase != types.PhasePending || got.Reason != types.ReasonMissingTopologyInfo {
		t.Fatalf("phase=%s reason=%s, want Pending/MissingTopologyInfo", got.Phase, got.Reason)
	}

	// 条件满足：节点补上拓扑标签后，下一轮调和自动调度成功。
	_ = c.OnNodeAdded("tester", node("n1", 8, map[string]string{"zone": "a"}))
	got, _ = s.GetWorkload("w-needtopo")
	if got.Phase != types.PhaseBound || got.Node != "n1" {
		t.Fatalf("phase=%s node=%q, want Bound/n1", got.Phase, got.Node)
	}
}

// 约束当前不可满足（无匹配节点）：挂起并给出原因；
// 新增满足条件的节点后自动调度。
func TestUnsatisfiableThenSatisfied(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 8, map[string]string{"disk": "hdd"}))

	w := wl("w-ssd", 0, 1, types.Constraints{NodeSelector: map[string]string{"disk": "ssd"}})
	_ = s.SubmitWorkload(w)
	c.Reconcile("tester")

	got, _ := s.GetWorkload("w-ssd")
	if got.Phase != types.PhasePending || got.Reason != types.ReasonUnsatisfiableConstraints {
		t.Fatalf("phase=%s reason=%s, want Pending/UnsatisfiableConstraints", got.Phase, got.Reason)
	}

	_ = c.OnNodeAdded("tester", node("n2", 8, map[string]string{"disk": "ssd"}))
	got, _ = s.GetWorkload("w-ssd")
	if got.Phase != types.PhaseBound || got.Node != "n2" {
		t.Fatalf("phase=%s node=%q, want Bound/n2", got.Phase, got.Node)
	}
}

// 反亲和实际生效：同域已有匹配负载时不得放入。
func TestAntiAffinityEnforced(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 8, map[string]string{"zone": "a"}))
	_ = c.OnNodeAdded("tester", node("n2", 8, map[string]string{"zone": "b"}))

	db := wl("db-1", 0, 1, types.Constraints{})
	db.Labels = map[string]string{"app": "db"}
	_ = s.SubmitWorkload(db)

	web := wl("web-1", 0, 1, types.Constraints{
		RequiredAntiAffinity: []types.AffinityTerm{
			{LabelSelector: map[string]string{"app": "db"}, TopologyKey: "zone"},
		},
	})
	_ = s.SubmitWorkload(web)
	c.Reconcile("tester")

	dbGot, _ := s.GetWorkload("db-1")
	webGot, _ := s.GetWorkload("web-1")
	if dbGot.Phase != types.PhaseBound || webGot.Phase != types.PhaseBound {
		t.Fatalf("db=%s web=%s, want both Bound", dbGot.Phase, webGot.Phase)
	}
	if dbGot.Node == webGot.Node {
		t.Fatalf("anti-affinity violated: both on %s", dbGot.Node)
	}
}

// 分散约束：同组副本在每个拓扑域内不得超过 MaxPerDomain。
func TestSpreadConstraint(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 8, map[string]string{"zone": "a"}))
	_ = c.OnNodeAdded("tester", node("n2", 8, map[string]string{"zone": "a"}))
	_ = c.OnNodeAdded("tester", node("n3", 8, map[string]string{"zone": "b"}))

	spread := types.Constraints{Spread: []types.SpreadConstraint{
		{TopologyKey: "zone", MaxPerDomain: 1, GroupKey: "g1"},
	}}
	for _, name := range []string{"r1", "r2", "r3"} {
		w := wl(name, 0, 1, spread)
		w.Labels = map[string]string{"group": "g1"}
		_ = s.SubmitWorkload(w)
	}
	c.Reconcile("tester")

	zones := map[string]int{}
	bound, pending := 0, 0
	for _, name := range []string{"r1", "r2", "r3"} {
		got, _ := s.GetWorkload(name)
		if got.Phase == types.PhaseBound {
			bound++
			n, _ := s.GetNode(got.Node)
			zones[n.Labels["zone"]]++
		} else {
			pending++
			if got.Reason != types.ReasonUnsatisfiableConstraints {
				t.Fatalf("%s reason=%s, want UnsatisfiableConstraints", name, got.Reason)
			}
		}
	}
	if bound != 2 || pending != 1 {
		t.Fatalf("bound=%d pending=%d, want 2/1", bound, pending)
	}
	for z, cnt := range zones {
		if cnt > 1 {
			t.Fatalf("spread violated in zone %s: %d replicas", z, cnt)
		}
	}
}
