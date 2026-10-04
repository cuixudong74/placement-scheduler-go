package scheduler

import (
	"context"
	"testing"
	"time"
)

// 调和循环：事件触发 + 周期触发都能驱动调度。
func TestControllerRunLoopSchedules(t *testing.T) {
	c, store, _ := newTestController(Config{ScheduleInterval: 10 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go c.Run(ctx)

	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 4}})
	submit(t, c, Workload{ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 1}})

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if w, ok := store.GetWorkload("w1"); ok && w.Phase == PhaseBound {
			return // 成功：事件或周期触发完成了调度
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("controller run loop did not schedule w1 in time")
}

// 退避可配置：失败负载在退避窗口内不被重试。
func TestBackoffDefersRetry(t *testing.T) {
	store := NewStore()
	audit := NewAuditLog(nil)
	now := time.Unix(1700000000, 0)
	clock := &now
	authz := &Authorizer{SystemIdentities: map[string]bool{"system:controller": true}}
	sched := NewScheduler(store, Config{
		BackoffBase: time.Hour, // 很大的退避，便于观察
	}, authz, audit, func() time.Time { return *clock }, func(time.Duration) {})
	c := NewController(store, sched, authz, audit)

	submit(t, c, Workload{ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 1}})
	// 无节点 -> 失败并进入退避
	c.sched.ScheduleCycle(systemActor)
	w1 := mustWorkload(t, store, "w1")
	if w1.Attempts != 1 {
		t.Fatalf("attempts should be 1, got %d", w1.Attempts)
	}
	// 退避窗口内即使有节点也不调度
	addNode(t, c, Node{ID: "n1", Capacity: ResourceList{"cpu": 4}})
	c.sched.ScheduleCycle(systemActor)
	if w := mustWorkload(t, store, "w1"); w.Phase == PhaseBound {
		t.Fatalf("w1 should be deferred by backoff")
	}
	// 时钟前进超过退避窗口后自动重试成功
	*clock = now.Add(2 * time.Hour)
	c.sched.ScheduleCycle(systemActor)
	if w := mustWorkload(t, store, "w1"); w.Phase != PhaseBound {
		t.Fatalf("w1 should be scheduled after backoff window, got %s", w.Phase)
	}
}
