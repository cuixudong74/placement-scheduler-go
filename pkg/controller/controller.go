// Package controller 提供持续调和的调度控制器。
//
// 调和顺序确定：待调度队列按（有效优先级降序，名称升序）排序。
// 有效优先级 = 声明优先级 + min(被抢占次数, 10)，被抢占者随抢占次数
// 老化提升，保证不会永久饥饿。
package controller

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/cuixudong74/placement-scheduler-go/pkg/audit"
	"github.com/cuixudong74/placement-scheduler-go/pkg/config"
	"github.com/cuixudong74/placement-scheduler-go/pkg/scheduler"
	"github.com/cuixudong74/placement-scheduler-go/pkg/store"
	"github.com/cuixudong74/placement-scheduler-go/pkg/types"
)

// Controller 持续把声明调和到节点上。
type Controller struct {
	Store     *store.Store
	Scheduler *scheduler.Scheduler
	Audit     *audit.Log
	Policy    audit.Policy
	Config    config.Config

	mu           sync.Mutex
	preemptCount map[string]int // 负载名 -> 被抢占次数（老化依据）
	reconcileMu  sync.Mutex     // 串行化调和循环
}

// New 创建控制器。
func New(s *store.Store, cfg config.Config, policy audit.Policy, log *audit.Log) *Controller {
	c := &Controller{
		Store:        s,
		Audit:        log,
		Policy:       policy,
		Config:       cfg,
		preemptCount: map[string]int{},
	}
	c.Scheduler = scheduler.New(s)
	c.Scheduler.PreemptionEnabled = cfg.PreemptionEnabled
	// 抢占资格与受害者选择均使用老化后的有效优先级。
	c.Scheduler.EffectivePriority = func(w types.Workload) int32 { return c.effectivePriority(w) }
	return c
}

// effectivePriority 计算老化后的有效优先级。
func (c *Controller) effectivePriority(w types.Workload) int32 {
	c.mu.Lock()
	n := c.preemptCount[w.Name]
	c.mu.Unlock()
	if n > 10 {
		n = 10
	}
	return w.Priority + int32(n)
}

func (c *Controller) notePreempted(name string) {
	c.mu.Lock()
	c.preemptCount[name]++
	c.mu.Unlock()
}

// PreemptionCount 返回负载被抢占次数（测试与可解释性用）。
func (c *Controller) PreemptionCount(name string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.preemptCount[name]
}

// queue 返回待调度负载，按（有效优先级降序，名称升序）确定排序。
func (c *Controller) queue() []types.Workload {
	var q []types.Workload
	for _, w := range c.Store.ListWorkloads() {
		if w.Phase == types.PhasePending || w.Phase == types.PhaseEvicted {
			q = append(q, w)
		}
	}
	sort.SliceStable(q, func(i, j int) bool {
		pi, pj := c.effectivePriority(q[i]), c.effectivePriority(q[j])
		if pi != pj {
			return pi > pj
		}
		return q[i].Name < q[j].Name
	})
	return q
}

// Reconcile 执行一轮调和：为所有待调度负载做决策并提交绑定。
func (c *Controller) Reconcile(actor string) {
	c.reconcileMu.Lock()
	defer c.reconcileMu.Unlock()
	if !c.Audit.Check(c.Policy, actor, audit.ActionSchedule, "", "") {
		return // 越权：已记录审计，直接拒绝
	}
	for _, w := range c.queue() {
		c.scheduleOne(actor, w)
	}
}

// scheduleOne 决策并提交单个负载。
func (c *Controller) scheduleOne(actor string, w types.Workload) {
	d := c.Scheduler.ScheduleOne(w)
	if d.Node == "" {
		_ = c.Store.MarkPending(w.Name, d.Reason, d.Message)
		c.Audit.Note(actor, audit.ActionSchedule, w.Name, "", fmt.Sprintf("pending: %s: %s", d.Reason, d.Message))
		return
	}
	// 先驱逐受害者，再绑定（驱逐与绑定均幂等）。
	for _, victim := range d.Preempted {
		if !c.Audit.Check(c.Policy, actor, audit.ActionPreempt, victim, d.Node) {
			continue // 越权驱逐被拒绝，受害者保留，绑定大概率因容量失败而重排队
		}
		if err := c.Store.Unbind(victim, types.ReasonPreempted); err == nil {
			c.notePreempted(victim)
		}
	}
	if err := c.bindWithRetry(actor, w.Name, d.Node); err != nil {
		_ = c.Store.MarkPending(w.Name, types.ReasonInsufficientResources, err.Error())
	}
}

// bindWithRetry 按配置的重试次数与指数退避提交绑定。
// 每次重试都重新读取 ResourceVersion，绝不覆盖他人写入。
func (c *Controller) bindWithRetry(actor, workload, node string) error {
	if !c.Audit.Check(c.Policy, actor, audit.ActionBind, workload, node) {
		return fmt.Errorf("bind denied for actor %q", actor)
	}
	var err error
	for attempt := 0; attempt <= c.Config.MaxBindRetries; attempt++ {
		if attempt > 0 {
			time.Sleep(c.Config.RetryBackoffBase << min(attempt-1, 10))
		}
		cur, ok := c.Store.GetWorkload(workload)
		if !ok {
			return fmt.Errorf("workload %q disappeared", workload)
		}
		if cur.Phase == types.PhaseBound {
			if cur.Node == node {
				return nil // 已被（他人）绑定到目标节点，幂等成功
			}
			return fmt.Errorf("workload %q already bound to %q", workload, cur.Node)
		}
		err = c.Store.Bind(workload, cur.ResourceVersion, node)
		switch {
		case err == nil:
			c.Audit.Note(actor, audit.ActionBind, workload, node, "bound")
			return nil
		case errors.Is(err, store.ErrConflict):
			continue // 版本冲突：退避后重读重试
		default:
			return err // 容量/节点状态等错误：交给下一轮调和重新决策
		}
	}
	return fmt.Errorf("bind retries exhausted: %w", err)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// Run 按配置的调度周期持续调和，直到 ctx 取消。
func (c *Controller) Run(ctx context.Context, actor string) {
	t := time.NewTicker(c.Config.ScheduleInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			c.Reconcile(actor)
		}
	}
}

// OnNodeAdded 节点新增触发重调度。
func (c *Controller) OnNodeAdded(actor string, n types.Node) error {
	if err := c.Store.AddNode(n); err != nil {
		return err
	}
	c.Reconcile(actor)
	return nil
}

// OnNodeLost 节点失联：标记 Lost，驱逐其上负载（幂等）并触发重调度。
func (c *Controller) OnNodeLost(actor string, name string) error {
	return c.evictNode(actor, name, types.NodeLost, types.ReasonNodeLost)
}

// OnNodeDraining 节点下线：标记 Draining，驱逐其上负载并触发重调度。
func (c *Controller) OnNodeDraining(actor string, name string) error {
	return c.evictNode(actor, name, types.NodeDraining, types.ReasonNodeDraining)
}

func (c *Controller) evictNode(actor string, name string, state types.NodeState, reason types.Reason) error {
	if err := c.Store.UpdateNodeState(name, state); err != nil {
		return err
	}
	for _, w := range c.Store.ListWorkloads() {
		if w.Phase == types.PhaseBound && w.Node == name {
			if !c.Audit.Check(c.Policy, actor, audit.ActionEvict, w.Name, name) {
				continue // 越权驱逐被拒绝并留痕
			}
			if err := c.Store.Unbind(w.Name, reason); err != nil {
				return err
			}
		}
	}
	c.Reconcile(actor)
	return nil
}

// OnNodeRecovered 节点恢复：置回 Ready 并触发重调度。
// 失联期间被驱逐的负载已重新排队或绑定他处，账目由绑定关系现算，
// 因此恢复后天然对齐；审计记录给出可解释结论。
func (c *Controller) OnNodeRecovered(actor string, name string) error {
	if err := c.Store.UpdateNodeState(name, types.NodeReady); err != nil {
		return err
	}
	relocated := 0
	for _, w := range c.Store.ListWorkloads() {
		if w.Reason == types.ReasonNodeLost {
			relocated++
		}
	}
	c.Audit.Note(actor, audit.ActionSchedule, "", name,
		fmt.Sprintf("node recovered; %d workloads were relocated during outage; books aligned", relocated))
	c.Reconcile(actor)
	return nil
}
