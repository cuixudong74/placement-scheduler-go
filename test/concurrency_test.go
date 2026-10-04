package test

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/cuixudong74/placement-scheduler-go/pkg/store"
	"github.com/cuixudong74/placement-scheduler-go/pkg/types"
)

// 并发绑定同一节点：容量只允许部分成功，失败者收到可区分错误，
// 已分配绝不超过容量（不超卖）。
func TestConcurrentBindSameNodeNoOversell(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 4, nil))

	const n = 8
	for i := 0; i < n; i++ {
		_ = s.SubmitWorkload(wl(fmt.Sprintf("w%d", i), 0, 1, types.Constraints{}))
	}

	var wg sync.WaitGroup
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("w%d", i)
			w, _ := s.GetWorkload(name)
			errs[i] = s.Bind(name, w.ResourceVersion, "n1")
		}(i)
	}
	wg.Wait()

	succeeded := 0
	for i, err := range errs {
		if err == nil {
			succeeded++
		} else if !errors.Is(err, store.ErrCapacity) && !errors.Is(err, store.ErrConflict) {
			t.Fatalf("w%d unexpected error: %v", i, err)
		}
	}
	if succeeded != 4 {
		t.Fatalf("succeeded=%d, want exactly 4 (capacity)", succeeded)
	}
	if alloc := s.Allocated("n1"); alloc["cpu"] != 4 {
		t.Fatalf("allocated=%v, want cpu=4 (no oversell)", alloc)
	}
}

// 并发绑定同一负载到不同节点：只能有一个成功，
// 任何时刻负载不得同时绑定两个节点。
func TestConcurrentBindSameWorkloadSingleWinner(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 4, nil))
	_ = c.OnNodeAdded("tester", node("n2", 4, nil))
	_ = s.SubmitWorkload(wl("w", 0, 1, types.Constraints{}))

	const racers = 16
	var wg sync.WaitGroup
	errs := make([]error, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			node := "n1"
			if i%2 == 1 {
				node = "n2"
			}
			// 模拟并发调度器：各自读取版本后尝试绑定。
			w, _ := s.GetWorkload("w")
			errs[i] = s.Bind("w", w.ResourceVersion, node)
		}(i)
	}
	wg.Wait()

	// 成功可能多于一次：对同一节点的幂等重放也返回成功。
	// 关键不变量：负载最终只绑定到一个节点，账目只记一份。
	w, _ := s.GetWorkload("w")
	if w.Phase != types.PhaseBound {
		t.Fatalf("phase=%s, want Bound", w.Phase)
	}
	winner := w.Node
	for i, err := range errs {
		node := "n1"
		if i%2 == 1 {
			node = "n2"
		}
		if err == nil && node != winner {
			t.Fatalf("racer %d bound to %s but workload is on %s: double binding!", i, node, winner)
		}
	}
	total := s.Allocated("n1")["cpu"] + s.Allocated("n2")["cpu"]
	if total != 1 {
		t.Fatalf("total allocated=%d, want 1 (no double count)", total)
	}
}

// 版本冲突的重试不得覆盖他人更新：过期版本的写入必须失败。
func TestStaleVersionRejected(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 4, nil))
	_ = c.OnNodeAdded("tester", node("n2", 4, nil))
	_ = s.SubmitWorkload(wl("w", 0, 1, types.Constraints{}))

	w, _ := s.GetWorkload("w")
	staleRV := w.ResourceVersion

	// 写入者 A 先成功。
	if err := s.Bind("w", staleRV, "n1"); err != nil {
		t.Fatal(err)
	}
	// 写入者 B 持过期版本：必须冲突失败，不得覆盖 A 的绑定。
	if err := s.Bind("w", staleRV, "n2"); !errors.Is(err, store.ErrConflict) {
		t.Fatalf("stale bind err=%v, want ErrConflict", err)
	}
	w, _ = s.GetWorkload("w")
	if w.Node != "n1" {
		t.Fatalf("node=%q, want n1 (A's write must not be overwritten)", w.Node)
	}
}

// 释放的资源必须可被重新分配。
func TestReleasedResourcesReallocatable(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 2, nil))

	_ = s.SubmitWorkload(wl("w1", 0, 2, types.Constraints{}))
	c.Reconcile("tester")
	if got, _ := s.GetWorkload("w1"); got.Phase != types.PhaseBound {
		t.Fatalf("w1 phase=%s", got.Phase)
	}

	// 删除 w1 释放容量，w2 必须能复用。
	if err := s.DeleteWorkload("w1"); err != nil {
		t.Fatal(err)
	}
	_ = s.SubmitWorkload(wl("w2", 0, 2, types.Constraints{}))
	c.Reconcile("tester")
	w2, _ := s.GetWorkload("w2")
	if w2.Phase != types.PhaseBound || w2.Node != "n1" {
		t.Fatalf("w2 phase=%s node=%q, want Bound/n1 (released capacity reusable)", w2.Phase, w2.Node)
	}
	if alloc := s.Allocated("n1"); alloc["cpu"] != 2 {
		t.Fatalf("allocated=%v, want cpu=2", alloc)
	}
}

// 并发调和与并发绑定交织：最终账目必须自洽。
func TestConcurrentReconcileAndBindConsistent(t *testing.T) {
	c, s, _ := newController()
	_ = c.OnNodeAdded("tester", node("n1", 8, nil))
	_ = c.OnNodeAdded("tester", node("n2", 8, nil))

	const n = 10
	for i := 0; i < n; i++ {
		_ = s.SubmitWorkload(wl(fmt.Sprintf("w%d", i), 0, 1, types.Constraints{}))
	}

	var wg sync.WaitGroup
	// 一个 goroutine 持续调和。
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
				c.Reconcile("tester")
			}
		}
	}()
	// 多个 goroutine 并发尝试直接绑定（会与调和竞争）。
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			name := fmt.Sprintf("w%d", i)
			w, _ := s.GetWorkload(name)
			_ = s.Bind(name, w.ResourceVersion, "n1") // 输赢皆可，只要最终自洽
		}(i)
	}
	wg.Wait()
	close(stop)
	<-done
	c.Reconcile("tester") // 收尾调和

	// 自洽校验：每个节点不超容量；每个 Bound 负载恰好计一次；
	// 全部负载最终都被绑定（容量足够）。
	for _, nd := range s.ListNodes() {
		alloc := s.Allocated(nd.Name)
		if alloc["cpu"] > nd.Capacity["cpu"] {
			t.Fatalf("node %s oversold: %v > %v", nd.Name, alloc, nd.Capacity)
		}
	}
	boundCount := 0
	for _, w := range s.ListWorkloads() {
		if w.Phase == types.PhaseBound {
			boundCount++
		}
	}
	if boundCount != n {
		t.Fatalf("bound=%d, want %d", boundCount, n)
	}
	total := s.Allocated("n1")["cpu"] + s.Allocated("n2")["cpu"]
	if total != n {
		t.Fatalf("total allocated=%d, want %d", total, n)
	}
}
