package test

import (
	"testing"

	"github.com/cuixudong74/placement-scheduler-go/pkg/audit"
	"github.com/cuixudong74/placement-scheduler-go/pkg/config"
	"github.com/cuixudong74/placement-scheduler-go/pkg/controller"
	"github.com/cuixudong74/placement-scheduler-go/pkg/store"
	"github.com/cuixudong74/placement-scheduler-go/pkg/types"
)

func newRoleController() (*controller.Controller, *store.Store, *audit.Log) {
	s := store.New()
	log := audit.NewLog()
	policy := audit.RolePolicy{Roles: map[string]string{
		"alice": audit.RoleAdmin,
		"bob":   audit.RoleOperator,
		"carol": audit.RoleViewer,
	}}
	c := controller.New(s, config.Default(), policy, log)
	return c, s, log
}

// 越权请求必须被拒绝且可追溯。
func TestUnauthorizedActionsDeniedAndAudited(t *testing.T) {
	c, s, log := newRoleController()
	_ = c.OnNodeAdded("alice", node("n1", 4, nil))
	_ = s.SubmitWorkload(wl("w1", 0, 1, types.Constraints{}))

	// viewer 无任何写权限：调度被拒绝，负载保持 Pending。
	c.Reconcile("carol")
	if got, _ := s.GetWorkload("w1"); got.Phase == types.PhaseBound {
		t.Fatal("viewer must not be able to schedule")
	}

	// operator 可以调度绑定。
	c.Reconcile("bob")
	if got, _ := s.GetWorkload("w1"); got.Phase != types.PhaseBound {
		t.Fatalf("operator should schedule: phase=%s", got.Phase)
	}

	// operator 无权驱逐：节点下线操作中的驱逐被拒绝。
	_ = s.SubmitWorkload(wl("w2", 0, 1, types.Constraints{}))
	c.Reconcile("bob")
	if err := c.OnNodeLost("bob", "n1"); err != nil {
		t.Fatal(err)
	}
	// 驱逐被拒绝，w1 仍绑定在 n1（虽然节点 Lost）。
	if got, _ := s.GetWorkload("w1"); got.Phase != types.PhaseBound {
		t.Fatalf("operator evict must be denied: w1 phase=%s", got.Phase)
	}

	// admin 可以驱逐。
	if err := c.OnNodeLost("alice", "n1"); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.GetWorkload("w1"); got.Phase == types.PhaseBound {
		t.Fatal("admin evict should succeed")
	}

	// 可追溯：每一次越权都有审计记录。
	var denied, total int
	for _, r := range log.Records() {
		total++
		if !r.Allowed {
			denied++
			if r.Reason == "" {
				t.Fatal("denied record must carry a reason")
			}
		}
	}
	if denied == 0 {
		t.Fatal("expected denied audit records for unauthorized attempts")
	}
	if total == 0 {
		t.Fatal("expected audit records")
	}
}

// 未知 actor 一律拒绝。
func TestUnknownActorDenied(t *testing.T) {
	c, s, log := newRoleController()
	_ = c.OnNodeAdded("alice", node("n1", 4, nil))
	_ = s.SubmitWorkload(wl("w1", 0, 1, types.Constraints{}))

	c.Reconcile("mallory")
	if got, _ := s.GetWorkload("w1"); got.Phase == types.PhaseBound {
		t.Fatal("unknown actor must be denied")
	}
	found := false
	for _, r := range log.Records() {
		if r.Actor == "mallory" && !r.Allowed {
			found = true
		}
	}
	if !found {
		t.Fatal("expected denied audit record for unknown actor")
	}
}
