package scheduler

import (
	"context"
	"fmt"
	"sort"
	"sync"
)

// ReconcileReport 是节点恢复后的对账结论，可解释、可执行。
type ReconcileReport struct {
	NodeID  string
	Orphans []string // 节点实际运行、但 Store 未绑定到该节点的负载：节点应停止
	Missing []string // Store 绑定到该节点、但节点未运行的负载：节点应启动
	Aligned []string // 双方一致的负载
	Note    string
}

// Controller 是调度控制器：驱动周期调和，处理节点生命周期事件。
type Controller struct {
	store  *Store
	sched  *Scheduler
	authz  *Authorizer
	audit  *AuditLog
	system Principal // 控制器内置身份（免鉴权）

	trigger chan struct{}
	once    sync.Once
}

func NewController(store *Store, sched *Scheduler, authz *Authorizer, audit *AuditLog) *Controller {
	return &Controller{
		store: store, sched: sched, authz: authz, audit: audit,
		system:  Principal{Name: "system:controller"},
		trigger: make(chan struct{}, 1),
	}
}

// poke 异步触发一次调度周期（非阻塞）。
func (c *Controller) poke() {
	select {
	case c.trigger <- struct{}{}:
	default:
	}
}

// Run 运行调和循环，直到 ctx 取消。周期与事件都会触发调度。
func (c *Controller) Run(ctx context.Context) {
	interval := c.sched.Config().ScheduleInterval
	t := newTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C():
			c.sched.ScheduleCycle(c.system)
		case <-c.trigger:
			c.sched.ScheduleCycle(c.system)
		}
	}
}

// --- 工作负载 API ---

// Submit 提交声明式负载。越权会被拒绝并记录审计。
func (c *Controller) Submit(actor Principal, w Workload) error {
	if err := guard(c.authz, c.audit, actor, ActionSubmitWorkload, w.ID, "submit"); err != nil {
		return err
	}
	if w.CreatedAt.IsZero() {
		w.CreatedAt = c.sched.now()
	}
	if err := c.store.SubmitWorkload(w); err != nil {
		return err
	}
	c.poke()
	return nil
}

// Evict 管理性驱逐一个负载（幂等）。
func (c *Controller) Evict(actor Principal, workloadID string) error {
	if err := guard(c.authz, c.audit, actor, ActionEvict, workloadID, "admin evict"); err != nil {
		return err
	}
	if err := c.store.UnbindWorkload(workloadID, ReasonEvictedByAdmin, "evicted by "+actor.Name); err != nil {
		return err
	}
	c.poke()
	return nil
}

// --- 节点生命周期 API ---

// AddNode 新增或更新节点并触发重调度。
func (c *Controller) AddNode(actor Principal, n Node) error {
	if err := guard(c.authz, c.audit, actor, ActionNodeUpdate, n.ID, "upsert node"); err != nil {
		return err
	}
	if err := c.store.UpsertNode(n); err != nil {
		return err
	}
	c.poke()
	return nil
}

// NodeLost 标记节点失联：驱逐其上的负载（幂等），负载重新排队。
func (c *Controller) NodeLost(actor Principal, nodeID string) error {
	if err := guard(c.authz, c.audit, actor, ActionNodeUpdate, nodeID, "mark lost"); err != nil {
		return err
	}
	n, ok := c.store.GetNode(nodeID)
	if !ok {
		return ErrNotFound
	}
	if err := c.store.SetNodeStatus(nodeID, NodeLost, n.Version); err != nil {
		return err
	}
	c.evictNodeWorkloads(nodeID, ReasonNodeLost, "node lost")
	c.poke()
	return nil
}

// NodeRecovered 标记节点恢复，并与节点上报的实际运行集合对账。
// 返回可解释的对账结论；Store 中的绑定关系是唯一事实来源，
// 因此任何时刻同一负载不会同时绑定到两个节点。
func (c *Controller) NodeRecovered(actor Principal, nodeID string, actualWorkloadIDs []string) (ReconcileReport, error) {
	rep := ReconcileReport{NodeID: nodeID}
	if err := guard(c.authz, c.audit, actor, ActionNodeUpdate, nodeID, "mark recovered"); err != nil {
		return rep, err
	}
	n, ok := c.store.GetNode(nodeID)
	if !ok {
		return rep, ErrNotFound
	}
	if err := c.store.SetNodeStatus(nodeID, NodeReady, n.Version); err != nil {
		return rep, err
	}
	// 对账：以 Store 绑定关系为准
	actual := map[string]bool{}
	for _, id := range actualWorkloadIDs {
		actual[id] = true
	}
	boundHere := map[string]bool{}
	for _, w := range c.store.ListWorkloads() {
		if w.Phase == PhaseBound && w.NodeID == nodeID {
			boundHere[w.ID] = true
		}
	}
	for id := range actual {
		if boundHere[id] {
			rep.Aligned = append(rep.Aligned, id)
		} else {
			// 节点运行着 Store 未绑定给它的负载（多为失联期间被驱逐
			// 并改绑他处的残留），节点应停止它们。
			rep.Orphans = append(rep.Orphans, id)
		}
	}
	for id := range boundHere {
		if !actual[id] {
			rep.Missing = append(rep.Missing, id)
		}
	}
	sort.Strings(rep.Orphans)
	sort.Strings(rep.Missing)
	sort.Strings(rep.Aligned)
	rep.Note = fmt.Sprintf("node %s recovered: %d aligned, %d orphans to stop, %d missing to start",
		nodeID, len(rep.Aligned), len(rep.Orphans), len(rep.Missing))
	if c.audit != nil {
		c.audit.Record(actor.Name, ActionNodeUpdate, nodeID, true, rep.Note)
	}
	c.poke()
	return rep, nil
}

// DrainNode 下线节点：标记 Draining（不再接收新绑定），驱逐全部负载。
func (c *Controller) DrainNode(actor Principal, nodeID string) error {
	if err := guard(c.authz, c.audit, actor, ActionNodeUpdate, nodeID, "drain"); err != nil {
		return err
	}
	n, ok := c.store.GetNode(nodeID)
	if !ok {
		return ErrNotFound
	}
	if err := c.store.SetNodeStatus(nodeID, NodeDraining, n.Version); err != nil {
		return err
	}
	c.evictNodeWorkloads(nodeID, ReasonNodeDraining, "node draining")
	c.poke()
	return nil
}

// RemoveNode 移除节点（要求先 Drain）。触发重调度。
func (c *Controller) RemoveNode(actor Principal, nodeID string) error {
	if err := guard(c.authz, c.audit, actor, ActionNodeRemove, nodeID, "remove"); err != nil {
		return err
	}
	if err := c.store.RemoveNode(nodeID); err != nil {
		return err
	}
	c.poke()
	return nil
}

// evictNodeWorkloads 驱逐节点上全部已绑定负载（幂等）。
func (c *Controller) evictNodeWorkloads(nodeID string, reason Reason, msg string) {
	for _, w := range c.store.ListWorkloads() {
		if w.Phase == PhaseBound && w.NodeID == nodeID {
			_ = c.store.UnbindWorkload(w.ID, reason, msg)
			if c.audit != nil {
				c.audit.Record("system:controller", ActionEvict, w.ID, true, msg)
			}
		}
	}
}
