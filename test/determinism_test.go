package test

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/cuixudong74/placement-scheduler-go/pkg/types"
)

// buildScenario 构造一组带约束的节点与负载。
func buildScenario() ([]types.Node, []types.Workload) {
	nodes := []types.Node{
		node("n1", 4, map[string]string{"zone": "a", "disk": "ssd"}),
		node("n2", 4, map[string]string{"zone": "a", "disk": "hdd"}),
		node("n3", 8, map[string]string{"zone": "b", "disk": "ssd"}),
		node("n4", 2, map[string]string{"zone": "b", "disk": "hdd"}),
	}
	mk := func(name string, prio int32, cpu int64, sel map[string]string) types.Workload {
		return wl(name, prio, cpu, types.Constraints{NodeSelector: sel})
	}
	workloads := []types.Workload{
		mk("w1", 0, 2, map[string]string{"disk": "ssd"}),
		mk("w2", 5, 3, nil),
		mk("w3", 0, 1, map[string]string{"zone": "b"}),
		mk("w4", 3, 2, nil),
		mk("w5", 0, 1, nil),
		mk("w6", 7, 4, map[string]string{"zone": "a"}),
		mk("w7", 0, 2, nil),
	}
	return nodes, workloads
}

func placement(s interface {
	ListWorkloads() []types.Workload
}) string {
	var out []string
	for _, w := range s.ListWorkloads() {
		out = append(out, fmt.Sprintf("%s->%s(%s)", w.Name, w.Node, w.Phase))
	}
	sort.Strings(out)
	return fmt.Sprintf("%v", out)
}

// 确定性：相同输入、不同提交顺序（固定种子的不同洗牌）、
// 不同 map 遍历顺序下，最终归属逐条一致。
func TestDeterministicPlacement(t *testing.T) {
	nodes, workloads := buildScenario()

	var baseline string
	for trial := 0; trial < 5; trial++ {
		c, s, _ := newController()
		for _, n := range nodes {
			if err := c.OnNodeAdded("tester", n); err != nil {
				t.Fatal(err)
			}
		}
		// 用固定种子的不同排列提交，模拟提交顺序差异。
		rng := rand.New(rand.NewSource(int64(1000 + trial)))
		perm := rng.Perm(len(workloads))
		for _, idx := range perm {
			if err := s.SubmitWorkload(workloads[idx]); err != nil {
				t.Fatal(err)
			}
		}
		c.Reconcile("tester")

		got := placement(s)
		if trial == 0 {
			baseline = got
			continue
		}
		if got != baseline {
			t.Fatalf("trial %d placement differs:\nfirst: %s\ngot:   %s", trial, baseline, got)
		}
	}
}

// 抢占决策确定性：相同场景重复执行，受害者集合与目标节点完全一致。
func TestDeterministicPreemption(t *testing.T) {
	var baseline string
	for trial := 0; trial < 3; trial++ {
		c, s, _ := newController()
		_ = c.OnNodeAdded("tester", node("n1", 6, nil))
		_ = c.OnNodeAdded("tester", node("n2", 6, nil))
		for _, w := range []types.Workload{
			wl("low-a", 0, 2, types.Constraints{}),
			wl("low-b", 1, 2, types.Constraints{}),
			wl("low-c", 0, 2, types.Constraints{}),
			wl("mid-a", 3, 2, types.Constraints{}),
		} {
			_ = s.SubmitWorkload(w)
		}
		c.Reconcile("tester")
		_ = s.SubmitWorkload(wl("high", 9, 5, types.Constraints{}))
		c.Reconcile("tester")

		got := placement(s)
		if trial == 0 {
			baseline = got
			continue
		}
		if got != baseline {
			t.Fatalf("trial %d preemption differs:\nfirst: %s\ngot:   %s", trial, baseline, got)
		}
	}
}
