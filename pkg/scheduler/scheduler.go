package scheduler

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// Config 是调度器可配置项。
type Config struct {
	ScheduleInterval time.Duration // 调度周期
	MaxRetries       int           // 绑定冲突等失败的最大重试次数
	BackoffBase      time.Duration // 退避基数：第 n 次重试等待 BackoffBase << n + jitter
	BackoffMax       time.Duration // 退避上限
	AgingStep        int64         // 防饥饿：每 AgingEvery 次尝试有效优先级提升量
	AgingEvery       int           // 老化周期（按调度尝试次数计）
	EnablePreemption bool          // 是否允许按优先级抢占
	RandomSeed       int64         // 退避抖动的随机种子（固定种子可复现）
}

func (c *Config) withDefaults() Config {
	out := *c
	if out.ScheduleInterval <= 0 {
		out.ScheduleInterval = time.Second
	}
	if out.MaxRetries <= 0 {
		out.MaxRetries = 5
	}
	if out.BackoffMax <= 0 {
		out.BackoffMax = 30 * time.Second
	}
	if out.AgingStep == 0 {
		out.AgingStep = 1
	}
	if out.AgingEvery <= 0 {
		out.AgingEvery = 10
	}
	return out
}

// backoff 计算第 attempt 次重试的退避时长（带抖动，抖动源由种子确定）。
func (s *Scheduler) backoff(attempt int) time.Duration {
	d := s.cfg.BackoffBase
	for i := 0; i < attempt && d < s.cfg.BackoffMax; i++ {
		d *= 2
	}
	if d > s.cfg.BackoffMax {
		d = s.cfg.BackoffMax
	}
	if d > 0 && s.rand != nil {
		d += time.Duration(s.rand.Int63n(int64(d)))
	}
	return d
}

// Scheduler 是确定性调度器：相同输入与节点集合下决策逐条一致。
// 调度周期通过内置互斥锁串行化，并发调用 ScheduleCycle 是安全的；
// 资源账目的最终一致性由 Store 的锁与版本校验保证。
type Scheduler struct {
	mu    sync.Mutex // 串行化调度周期与退避表/随机源访问
	store *Store
	cfg   Config
	authz *Authorizer
	audit *AuditLog
	rand  *rand.Rand
	now   func() time.Time
	sleep func(time.Duration)

	retryAt map[string]time.Time // 负载 ID -> 下次允许重试时间（退避）
}

func NewScheduler(store *Store, cfg Config, authz *Authorizer, audit *AuditLog, now func() time.Time, sleep func(time.Duration)) *Scheduler {
	if now == nil {
		now = time.Now
	}
	if sleep == nil {
		sleep = time.Sleep
	}
	c := cfg.withDefaults()
	return &Scheduler{
		store: store, cfg: c, authz: authz, audit: audit,
		rand: rand.New(rand.NewSource(c.RandomSeed)),
		now:  now, sleep: sleep,
		retryAt: make(map[string]time.Time),
	}
}

// Config 返回生效配置。
func (s *Scheduler) Config() Config { return s.cfg }

// --- 确定性排序 ---

// sortCandidates 稳定排序依据：有效优先级降序 -> 创建时间升序 -> ID 升序。
func (s *Scheduler) sortCandidates(ws []Workload) {
	sort.SliceStable(ws, func(i, j int) bool {
		pi := ws[i].EffectivePriority(s.cfg.AgingStep, s.cfg.AgingEvery)
		pj := ws[j].EffectivePriority(s.cfg.AgingStep, s.cfg.AgingEvery)
		if pi != pj {
			return pi > pj
		}
		if !ws[i].CreatedAt.Equal(ws[j].CreatedAt) {
			return ws[i].CreatedAt.Before(ws[j].CreatedAt)
		}
		return ws[i].ID < ws[j].ID
	})
}

// nodeLess 节点选择依据：放置后剩余容量更小者优先（best-fit，按资源名
// 字典序逐项比较），完全并列时按节点 ID 升序。与遍历顺序无关。
func nodeLess(req ResourceList, a, b Node) bool {
	keys := map[string]bool{}
	for k := range a.Capacity {
		keys[k] = true
	}
	for k := range b.Capacity {
		keys[k] = true
	}
	sorted := make([]string, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Strings(sorted)
	for _, k := range sorted {
		ra := a.Capacity[k] - a.Allocated[k] - req[k]
		rb := b.Capacity[k] - b.Allocated[k] - req[k]
		if ra != rb {
			return ra < rb
		}
	}
	return a.ID < b.ID
}

// --- 单负载调度 ---

func validateSpec(w *Workload) *ConstraintError {
	if w.ID == "" {
		return &ConstraintError{ReasonInvalidSpec, "empty workload id"}
	}
	if !w.Resources.NonNegative() {
		return &ConstraintError{ReasonInvalidSpec, "negative resource request"}
	}
	for _, sc := range w.Constraints.Spread {
		if sc.TopologyKey == "" || sc.Group == "" || sc.MaxPerDomain <= 0 {
			return &ConstraintError{ReasonInvalidSpec, "spread constraint requires TopologyKey, Group and MaxPerDomain > 0"}
		}
	}
	return nil
}

// feasibleNodes 返回满足资源与全部约束的 Ready 节点（已按 nodeLess 排序），
// 以及当没有可行节点时最具区分度的失败原因。
func (s *Scheduler) feasibleNodes(w *Workload, nodes []Node, bound []Workload, nodeByID map[string]Node) ([]Node, *ConstraintError) {
	var feasible []Node
	ready := 0
	sawCapacity := false
	var lastErr *ConstraintError
	for i := range nodes {
		n := &nodes[i]
		if n.Status != NodeReady {
			continue
		}
		ready++
		if cerr := checkAll(w, n, bound, nodeByID); cerr != nil {
			lastErr = cerr
			continue
		}
		if !Fits(n.Capacity, n.Allocated, w.Resources) {
			sawCapacity = true
			continue
		}
		feasible = append(feasible, *n)
	}
	if len(feasible) > 0 {
		sort.SliceStable(feasible, func(i, j int) bool { return nodeLess(w.Resources, feasible[i], feasible[j]) })
		return feasible, nil
	}
	switch {
	case ready == 0:
		return nil, &ConstraintError{ReasonNoReadyNode, "no ready node available"}
	case sawCapacity:
		return nil, &ConstraintError{ReasonInsufficientResources, "no node has enough free resources"}
	case lastErr != nil:
		return nil, lastErr
	default:
		return nil, &ConstraintError{ReasonConstraintConflict, "no feasible node"}
	}
}

// preemptionPlan 描述一次抢占方案。
type preemptionPlan struct {
	node    Node
	victims []Workload
}

// planPreemption 为 w 寻找最优抢占方案：
// 驱逐节点上“有效优先级”严格低于 w“基础优先级”的负载直至放得下。
// 注意不对称设计：抢占门槛看抢占者的基础优先级、看受害者的有效
// （老化后）优先级。老化只用于保护受害者——等待越久的负载越难被
// 抢占，从而避免两个负载互相抢占乒乓，也保证任何负载被同一批高优
// 负载抢占的次数有限（防饥饿）。
// 方案排序依据：受害者数升序 -> 受害者有效优先级总和升序 -> 节点 ID 升序。
func (s *Scheduler) planPreemption(w *Workload, nodes []Node, bound []Workload, nodeByID map[string]Node) *preemptionPlan {
	wp := w.Priority
	var plans []preemptionPlan
	for i := range nodes {
		n := &nodes[i]
		if n.Status != NodeReady {
			continue
		}
		// 收集该节点上严格更低优先级的负载
		var victims []Workload
		used := n.Allocated
		for j := range bound {
			b := &bound[j]
			if b.NodeID != n.ID || b.ID == w.ID {
				continue
			}
			bp := b.EffectivePriority(s.cfg.AgingStep, s.cfg.AgingEvery)
			if bp < wp {
				victims = append(victims, *b)
			}
		}
		// 确定性驱逐顺序：有效优先级升序 -> ID 升序
		sort.SliceStable(victims, func(a, b int) bool {
			pa := victims[a].EffectivePriority(s.cfg.AgingStep, s.cfg.AgingEvery)
			pb := victims[b].EffectivePriority(s.cfg.AgingStep, s.cfg.AgingEvery)
			if pa != pb {
				return pa < pb
			}
			return victims[a].ID < victims[b].ID
		})
		var chosen []Workload
		for _, v := range victims {
			if Fits(n.Capacity, used, w.Resources) {
				break
			}
			chosen = append(chosen, v)
			used = used.Sub(v.Resources)
		}
		if !Fits(n.Capacity, used, w.Resources) {
			continue // 抢占全部低优负载也放不下
		}
		// 约束须在抢占后的状态上重新校验（亲和可能因驱逐被破坏）
		remaining := make([]Workload, 0, len(bound))
		evicted := map[string]bool{}
		for _, v := range chosen {
			evicted[v.ID] = true
		}
		for _, b := range bound {
			if !evicted[b.ID] {
				remaining = append(remaining, b)
			}
		}
		if cerr := checkAll(w, n, remaining, nodeByID); cerr != nil {
			continue
		}
		plans = append(plans, preemptionPlan{node: *n, victims: chosen})
	}
	if len(plans) == 0 {
		return nil
	}
	sort.SliceStable(plans, func(i, j int) bool {
		if len(plans[i].victims) != len(plans[j].victims) {
			return len(plans[i].victims) < len(plans[j].victims)
		}
		si := int64(0)
		for _, v := range plans[i].victims {
			si += v.EffectivePriority(s.cfg.AgingStep, s.cfg.AgingEvery)
		}
		sj := int64(0)
		for _, v := range plans[j].victims {
			sj += v.EffectivePriority(s.cfg.AgingStep, s.cfg.AgingEvery)
		}
		if si != sj {
			return si < sj
		}
		return plans[i].node.ID < plans[j].node.ID
	})
	return &plans[0]
}

// bindWithRetry 带版本校验地绑定，冲突时按退避策略重试，
// 重试重新读取最新状态，不覆盖其他写入者的更新。
func (s *Scheduler) bindWithRetry(w *Workload, nodeID string) error {
	var lastErr error
	for attempt := 0; attempt <= s.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			s.sleep(s.backoff(attempt))
		}
		cur, ok := s.store.GetWorkload(w.ID)
		if !ok {
			return ErrNotFound
		}
		if cur.Phase == PhaseBound {
			if cur.NodeID == nodeID {
				return nil // 已被（其他写入者）绑定到目标节点，幂等成功
			}
			return fmt.Errorf("workload %q already bound to %q", w.ID, cur.NodeID)
		}
		node, ok := s.store.GetNode(nodeID)
		if !ok {
			return ErrNotFound
		}
		err := s.store.BindWorkload(w.ID, nodeID, cur.Version, node.Version)
		if err == nil {
			return nil
		}
		lastErr = err
		if err == ErrVersionConflict {
			continue // 他人并发写入，重读重试
		}
		return err // 容量不足/节点不就绪等，重试无意义
	}
	return fmt.Errorf("bind retries exhausted: %w", lastErr)
}

// scheduleOne 尝试调度单个负载；actor 用于权限与审计。
// 返回是否成功绑定。
func (s *Scheduler) scheduleOne(actor Principal, w *Workload, nodes []Node, bound []Workload, nodeByID map[string]Node) bool {
	if cerr := validateSpec(w); cerr != nil {
		_ = s.store.UpdateWorkloadStatus(w.ID, w.Version, func(cur *Workload) {
			cur.Phase = PhaseFailed
			cur.Reason = cerr.Reason
			cur.Message = cerr.Message
		})
		return false
	}
	feasible, cerr := s.feasibleNodes(w, nodes, bound, nodeByID)
	if len(feasible) > 0 {
		target := feasible[0]
		if err := guard(s.authz, s.audit, actor, ActionBind, w.ID, "bind to "+target.ID); err != nil {
			s.markPending(w, ReasonNoReadyNode, err.Error())
			return false
		}
		if err := s.bindWithRetry(w, target.ID); err == nil {
			delete(s.retryAt, w.ID)
			return true
		}
		s.deferRetry(w)
		return false
	}
	// 资源不足时尝试抢占
	if s.cfg.EnablePreemption && cerr.Reason == ReasonInsufficientResources {
		if plan := s.planPreemption(w, nodes, bound, nodeByID); plan != nil {
			if err := guard(s.authz, s.audit, actor, ActionPreempt, w.ID, fmt.Sprintf("preempt %d victims on %s", len(plan.victims), plan.node.ID)); err != nil {
				s.markPending(w, cerr.Reason, err.Error())
				return false
			}
			for _, v := range plan.victims {
				_ = s.store.UnbindWorkload(v.ID, ReasonPreempted, fmt.Sprintf("preempted by %q", w.ID))
				s.bumpPreemptions(v.ID)
			}
			if err := s.bindWithRetry(w, plan.node.ID); err == nil {
				delete(s.retryAt, w.ID)
				return true
			}
			s.deferRetry(w)
			return false
		}
	}
	s.markPending(w, cerr.Reason, cerr.Message)
	s.deferRetry(w)
	return false
}

func (s *Scheduler) bumpPreemptions(id string) {
	w, ok := s.store.GetWorkload(id)
	if !ok {
		return
	}
	_ = s.store.UpdateWorkloadStatus(id, w.Version, func(cur *Workload) {
		cur.Preemptions++
	})
}

func (s *Scheduler) markPending(w *Workload, r Reason, msg string) {
	cur, ok := s.store.GetWorkload(w.ID)
	if !ok {
		return
	}
	_ = s.store.UpdateWorkloadStatus(w.ID, cur.Version, func(cw *Workload) {
		if cw.Phase != PhaseBound {
			cw.Phase = PhasePending
			cw.Reason = r
			cw.Message = msg
		}
	})
}

// deferRetry 记录退避时间并递增尝试计数（驱动老化防饥饿）。
func (s *Scheduler) deferRetry(w *Workload) {
	cur, ok := s.store.GetWorkload(w.ID)
	if !ok {
		return
	}
	attempts := cur.Attempts + 1
	_ = s.store.UpdateWorkloadStatus(w.ID, cur.Version, func(cw *Workload) {
		cw.Attempts = attempts
	})
	s.retryAt[w.ID] = s.now().Add(s.backoff(attempts))
}

// ScheduleCycle 执行一个调度周期，返回本轮成功绑定的负载 ID（确定性顺序）。
func (s *Scheduler) ScheduleCycle(actor Principal) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	nodes := s.store.ListNodes()
	nodeByID := make(map[string]Node, len(nodes))
	for _, n := range nodes {
		nodeByID[n.ID] = n
	}
	all := s.store.ListWorkloads()
	var bound []Workload
	var candidates []Workload
	now := s.now()
	for _, w := range all {
		switch w.Phase {
		case PhaseBound:
			bound = append(bound, w)
		case PhasePending, PhaseEvicted:
			if t, ok := s.retryAt[w.ID]; ok && now.Before(t) {
				continue // 退避中
			}
			candidates = append(candidates, w)
		}
	}
	s.sortCandidates(candidates)
	var placed []string
	for i := range candidates {
		w := &candidates[i]
		if s.scheduleOne(actor, w, nodes, bound, nodeByID) {
			placed = append(placed, w.ID)
		}
		// 每个候选处理后都刷新本轮内视图（绑定可能成功，也可能
		// 发生了抢占驱逐），保证同周期内账目自洽：
		// 节点账目与绑定集合均以 Store 最新状态为准。
		nodes = s.store.ListNodes()
		for _, n := range nodes {
			nodeByID[n.ID] = n
		}
		bound = bound[:0]
		for _, b := range s.store.ListWorkloads() {
			if b.Phase == PhaseBound {
				bound = append(bound, b)
			}
		}
	}
	return placed
}
