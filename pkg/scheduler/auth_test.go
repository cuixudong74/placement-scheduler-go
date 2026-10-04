package scheduler

import (
	"errors"
	"testing"
)

func TestPermissionDeniedIsAudited(t *testing.T) {
	c, _, audit := newTestController(Config{})
	nobody := Principal{Name: "mallory"} // 无任何角色
	err := c.Submit(nobody, Workload{ID: "w1", Resources: ResourceList{"cpu": 1}})
	var denied *ErrPermissionDenied
	if !errors.As(err, &denied) {
		t.Fatalf("expected ErrPermissionDenied, got %v", err)
	}
	// 审计可追溯
	found := false
	for _, e := range audit.Events() {
		if e.Actor == "mallory" && e.Action == ActionSubmitWorkload && !e.Allowed {
			found = true
		}
	}
	if !found {
		t.Fatalf("denied request must be audited, events=%v", audit.Events())
	}
}

func TestRoleBasedPermissions(t *testing.T) {
	c, store, audit := newTestController(Config{})
	submitter := Principal{Name: "ci", Roles: []string{"submitter"}}
	nodeAgent := Principal{Name: "agent-1", Roles: []string{"node-agent"}}

	// submitter 可提交负载，但不能操作节点
	if err := c.Submit(submitter, Workload{ID: "w1", Resources: ResourceList{"cpu": 1}}); err != nil {
		t.Fatalf("submitter should submit: %v", err)
	}
	if err := c.AddNode(submitter, Node{ID: "n1", Capacity: ResourceList{"cpu": 4}}); err == nil {
		t.Fatalf("submitter must not add nodes")
	}
	// node-agent 可上报节点，但不能提交负载
	if err := c.AddNode(nodeAgent, Node{ID: "n1", Capacity: ResourceList{"cpu": 4}}); err != nil {
		t.Fatalf("node-agent should add node: %v", err)
	}
	if err := c.Submit(nodeAgent, Workload{ID: "w2", Resources: ResourceList{"cpu": 1}}); err == nil {
		t.Fatalf("node-agent must not submit workloads")
	}
	// 调度周期以系统身份执行绑定，应被放行并审计
	placed := c.sched.ScheduleCycle(systemActor)
	if len(placed) != 1 {
		t.Fatalf("expected 1 placed, got %v", placed)
	}
	if w := mustWorkload(t, store, "w1"); w.Phase != PhaseBound {
		t.Fatalf("w1 should be bound by system identity")
	}
	bindAudited := false
	for _, e := range audit.Events() {
		if e.Action == ActionBind && e.Target == "w1" && e.Allowed {
			bindAudited = true
		}
	}
	if !bindAudited {
		t.Fatalf("bind action must be audited")
	}
}

func TestPreemptionAndEvictionAudited(t *testing.T) {
	c, _, audit := newTestController(Config{EnablePreemption: true})
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 2}})
	submit(t, c, Workload{ID: "low", Priority: 1, Resources: ResourceList{"cpu": 2}})
	c.sched.ScheduleCycle(systemActor)
	submit(t, c, Workload{ID: "high", Priority: 9, Resources: ResourceList{"cpu": 2}})
	c.sched.ScheduleCycle(systemActor)
	if err := c.Evict(systemActor, "high"); err != nil {
		t.Fatal(err)
	}
	var preempt, evict bool
	for _, e := range audit.Events() {
		if e.Action == ActionPreempt && e.Target == "high" && e.Allowed {
			preempt = true
		}
		if e.Action == ActionEvict && e.Allowed {
			evict = true
		}
	}
	if !preempt || !evict {
		t.Fatalf("preempt(%v) and evict(%v) must be audited", preempt, evict)
	}
}
