package scheduler

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"
)

// runScenario 用给定输入跑一遍完整调度，返回 负载->节点 的归属映射。
func runScenario(seed int64, nodeOrder, workloadOrder []int) map[string]string {
	store := NewStore()
	audit := NewAuditLog(nil)
	authz := &Authorizer{SystemIdentities: map[string]bool{"system:controller": true}}
	base := time.Unix(1700000000, 0)
	clock := func() time.Time { return base }
	sched := NewScheduler(store, Config{
		EnablePreemption: true,
		RandomSeed:       seed,
	}, authz, audit, clock, func(time.Duration) {})
	c := NewController(store, sched, authz, audit)

	for _, i := range nodeOrder {
		_ = c.AddNode(systemActor, Node{
			ID:       fmt.Sprintf("node-%d", i),
			Labels:   map[string]string{"zone": fmt.Sprintf("z%d", i%3)},
			Capacity: ResourceList{"cpu": int64(4 + i%4), "mem": int64(1024 * (i + 2))},
		})
	}
	for _, i := range workloadOrder {
		_ = c.Submit(systemActor, Workload{
			ID:        fmt.Sprintf("wl-%02d", i),
			Labels:    map[string]string{"app": fmt.Sprintf("app-%d", i%4)},
			Priority:  int64(i % 5),
			Resources: ResourceList{"cpu": int64(1 + i%3), "mem": int64(256 * (1 + i%2))},
			CreatedAt: base.Add(time.Duration(i) * time.Second),
		})
	}
	// 跑多个周期直到稳定（含抢占重排队）
	for round := 0; round < 20; round++ {
		sched.ScheduleCycle(systemActor)
	}
	out := map[string]string{}
	for _, w := range store.ListWorkloads() {
		if w.Phase == PhaseBound {
			out[w.ID] = w.NodeID
		} else {
			out[w.ID] = "<" + string(w.Phase) + ":" + string(w.Reason) + ">"
		}
	}
	return out
}

// 确定性：相同输入 + 固定种子，无论节点/负载的提交顺序如何、
// 运行多少次，最终归属逐条一致。
func TestDeterministicPlacement(t *testing.T) {
	const nodes, workloads = 6, 24
	seed := int64(42)

	identity := func(n int) []int {
		out := make([]int, n)
		for i := range out {
			out[i] = i
		}
		return out
	}
	shuffled := func(n int, s int64) []int {
		out := identity(n)
		rand.New(rand.NewSource(s)).Shuffle(n, func(i, j int) { out[i], out[j] = out[j], out[i] })
		return out
	}

	reference := runScenario(seed, identity(nodes), identity(workloads))
	// 不同提交顺序 + 不同洗牌种子（输入集合相同）
	for trial := int64(1); trial <= 5; trial++ {
		got := runScenario(seed, shuffled(nodes, trial), shuffled(workloads, trial*7))
		if len(got) != len(reference) {
			t.Fatalf("trial %d: workload count mismatch", trial)
		}
		// CreatedAt 与提交顺序无关（按 i 构造），ID 排序保证候选顺序稳定
		for id, want := range reference {
			if got[id] != want {
				t.Fatalf("trial %d: %s placed on %s, want %s\nref=%v\ngot=%v", trial, id, got[id], want, reference, got)
			}
		}
	}
}

// 排序依据明确性：候选按 (有效优先级降, 创建时间升, ID 升) 排序。
func TestCandidateOrderingIsExplicit(t *testing.T) {
	s := NewScheduler(NewStore(), Config{AgingStep: 1, AgingEvery: 10}, nil, nil, nil, nil)
	base := time.Unix(1700000000, 0)
	ws := []Workload{
		{ID: "c", Priority: 1, CreatedAt: base},
		{ID: "a", Priority: 5, CreatedAt: base.Add(2 * time.Second)},
		{ID: "b", Priority: 5, CreatedAt: base.Add(time.Second)},
		{ID: "d", Priority: 5, CreatedAt: base.Add(time.Second)}, // 与 b 同优先级同时间，按 ID
	}
	s.sortCandidates(ws)
	got := []string{ws[0].ID, ws[1].ID, ws[2].ID, ws[3].ID}
	want := []string{"b", "d", "a", "c"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("candidate order = %v, want %v", got, want)
		}
	}
}

// 节点选择确定性：best-fit 后按节点 ID 决胜，与输入顺序无关。
func TestNodeSelectionDeterministic(t *testing.T) {
	mk := func(order []string) []Node {
		caps := map[string]int64{"n1": 8, "n2": 4, "n3": 4}
		var out []Node
		for _, id := range order {
			out = append(out, Node{ID: id, Status: NodeReady, Capacity: ResourceList{"cpu": caps[id]}})
		}
		return out
	}
	pick := func(nodes []Node) string {
		sorted := append([]Node(nil), nodes...)
		sort.SliceStable(sorted, func(i, j int) bool {
			return nodeLess(ResourceList{"cpu": 2}, sorted[i], sorted[j])
		})
		return sorted[0].ID
	}
	// best-fit：容量 4 的节点剩余更少，n2/n3 并列时按 ID 取 n2
	a := pick(mk([]string{"n1", "n2", "n3"}))
	b := pick(mk([]string{"n3", "n1", "n2"}))
	c := pick(mk([]string{"n2", "n3", "n1"}))
	if a != "n2" || b != "n2" || c != "n2" {
		t.Fatalf("node selection must be order-independent best-fit, got %s %s %s", a, b, c)
	}
}
