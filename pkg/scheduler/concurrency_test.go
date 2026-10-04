package scheduler

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

// 并发绑定同一负载：只有一个成功，资源不被重复扣减。
func TestConcurrentBindSameWorkload(t *testing.T) {
	store := NewStore()
	_ = store.UpsertNode(Node{ID: "n1", Capacity: ResourceList{"cpu": 8}})
	_ = store.UpsertNode(Node{ID: "n2", Capacity: ResourceList{"cpu": 8}})
	_ = store.SubmitWorkload(Workload{ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 2}})

	const goroutines = 16
	var wg sync.WaitGroup
	successes := make(chan string, goroutines)
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			node := "n1"
			if i%2 == 1 {
				node = "n2"
			}
			w, _ := store.GetWorkload("w1")
			n, _ := store.GetNode(node)
			if err := store.BindWorkload("w1", node, w.Version, n.Version); err == nil {
				successes <- node
			}
		}(i)
	}
	wg.Wait()
	close(successes)
	var won []string
	for s := range successes {
		won = append(won, s)
	}
	// 可能多个“成功”：第一个真实绑定 + 后续对同一节点的幂等成功。
	// 但最终只能绑定到一个节点，且账目只扣减一次。
	w, _ := store.GetWorkload("w1")
	if w.Phase != PhaseBound {
		t.Fatalf("w1 should be bound")
	}
	for _, s := range won {
		if s != w.NodeID {
			t.Fatalf("concurrent bind succeeded on different node %s (bound to %s)", s, w.NodeID)
		}
	}
	total := int64(0)
	for _, n := range store.ListNodes() {
		total += n.Allocated["cpu"]
	}
	if total != 2 {
		t.Fatalf("resource must be deducted exactly once, got total %d", total)
	}
}

// 并发绑定不同负载到同一节点：账目永不超过容量。
func TestConcurrentBindSameNodeNoOversell(t *testing.T) {
	store := NewStore()
	_ = store.UpsertNode(Node{ID: "n1", Capacity: ResourceList{"cpu": 4}})
	const workers = 8
	for i := 0; i < workers; i++ {
		_ = store.SubmitWorkload(Workload{ID: fmt.Sprintf("w%d", i), Priority: 1, Resources: ResourceList{"cpu": 1}})
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id string) {
			defer wg.Done()
			// 模拟调度器的重试循环：冲突则重读版本再试
			for attempt := 0; attempt < 20; attempt++ {
				w, _ := store.GetWorkload(id)
				n, _ := store.GetNode("n1")
				err := store.BindWorkload(id, "n1", w.Version, n.Version)
				if err == ErrVersionConflict {
					continue
				}
				return // 成功或容量不足
			}
			t.Errorf("bind %s: retries exhausted", id)
		}(fmt.Sprintf("w%d", i))
	}
	wg.Wait()
	n, _ := store.GetNode("n1")
	if n.Allocated["cpu"] > n.Capacity["cpu"] {
		t.Fatalf("oversell: allocated %d > capacity %d", n.Allocated["cpu"], n.Capacity["cpu"])
	}
	// 容量 4，恰好 4 个绑定成功
	bound := 0
	for _, w := range store.ListWorkloads() {
		if w.Phase == PhaseBound {
			bound++
		}
	}
	if bound != 4 || n.Allocated["cpu"] != 4 {
		t.Fatalf("expected exactly 4 bound, got %d (allocated %d)", bound, n.Allocated["cpu"])
	}
}

// 释放的资源必须可被重新分配。
func TestReleasedResourcesReusable(t *testing.T) {
	store := NewStore()
	_ = store.UpsertNode(Node{ID: "n1", Capacity: ResourceList{"cpu": 2}})
	_ = store.SubmitWorkload(Workload{ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 2}})
	w, _ := store.GetWorkload("w1")
	n, _ := store.GetNode("n1")
	if err := store.BindWorkload("w1", "n1", w.Version, n.Version); err != nil {
		t.Fatal(err)
	}
	if err := store.UnbindWorkload("w1", ReasonEvictedByAdmin, "test"); err != nil {
		t.Fatal(err)
	}
	_ = store.SubmitWorkload(Workload{ID: "w2", Priority: 1, Resources: ResourceList{"cpu": 2}})
	w2, _ := store.GetWorkload("w2")
	n, _ = store.GetNode("n1")
	if err := store.BindWorkload("w2", "n1", w2.Version, n.Version); err != nil {
		t.Fatalf("released resources must be reusable: %v", err)
	}
}

// 版本冲突：过期版本的写入被拒绝，且不覆盖他人更新。
func TestVersionConflictDoesNotOverwrite(t *testing.T) {
	store := NewStore()
	_ = store.SubmitWorkload(Workload{ID: "w1", Priority: 1, Resources: ResourceList{"cpu": 1}})
	w, _ := store.GetWorkload("w1")
	// 第一次写入成功
	if err := store.UpdateWorkloadStatus("w1", w.Version, func(cur *Workload) {
		cur.Message = "writer-A"
	}); err != nil {
		t.Fatal(err)
	}
	// 持旧版本的写入者 B 必须失败
	if err := store.UpdateWorkloadStatus("w1", w.Version, func(cur *Workload) {
		cur.Message = "writer-B"
	}); err != ErrVersionConflict {
		t.Fatalf("expected ErrVersionConflict, got %v", err)
	}
	cur, _ := store.GetWorkload("w1")
	if cur.Message != "writer-A" {
		t.Fatalf("stale writer must not overwrite, got %q", cur.Message)
	}
}

// 并发调度周期与提交/节点变更交叉进行：账目始终自洽。
func TestConcurrentScheduleCyclesConsistent(t *testing.T) {
	c, store, _ := newTestController(Config{EnablePreemption: true, ScheduleInterval: time.Hour})
	for i := 0; i < 4; i++ {
		addNode(t, c, Node{ID: fmt.Sprintf("n%d", i), Capacity: ResourceList{"cpu": 4}})
	}
	var wg sync.WaitGroup
	stop := make(chan struct{})
	// 并发调度循环
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					c.sched.ScheduleCycle(systemActor)
				}
			}
		}()
	}
	// 并发提交负载
	for i := 0; i < 30; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = c.Submit(systemActor, Workload{
				ID: fmt.Sprintf("w%d", i), Priority: int64(i % 5),
				Resources: ResourceList{"cpu": 1},
			})
		}(i)
	}
	// 并发驱逐（制造抢占/释放压力）
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_ = c.Evict(systemActor, fmt.Sprintf("w%d", i))
		}(i)
	}
	// 等待提交完成后停止调度循环
	done := make(chan struct{})
	go func() {
		for {
			if len(store.ListWorkloads()) >= 30 {
				close(done)
				return
			}
		}
	}()
	<-done
	close(stop)
	wg.Wait()
	// 最终对账：每个节点 allocated <= capacity，且等于其绑定负载之和
	assertAccountingConsistent(t, store)
}

func assertAccountingConsistent(t *testing.T, store *Store) {
	t.Helper()
	sum := map[string]ResourceList{}
	for _, w := range store.ListWorkloads() {
		if w.Phase == PhaseBound {
			sum[w.NodeID] = sum[w.NodeID].Add(w.Resources)
		}
	}
	for _, n := range store.ListNodes() {
		used := sum[n.ID]
		for k, v := range n.Allocated {
			if used[k] != v {
				t.Fatalf("node %s resource %s: allocated %d != sum of bound %d", n.ID, k, v, used[k])
			}
			if v > n.Capacity[k] {
				t.Fatalf("node %s resource %s oversold: %d > %d", n.ID, k, v, n.Capacity[k])
			}
		}
		if !n.Allocated.NonNegative() {
			t.Fatalf("node %s negative allocated: %v", n.ID, n.Allocated)
		}
	}
}
