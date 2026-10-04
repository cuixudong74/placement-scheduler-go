// Package audit 提供权限校验与审计记录。
package audit

import (
	"sync"
	"time"
)

// Action 是被审计的操作类型。
type Action string

const (
	ActionSchedule Action = "schedule"
	ActionBind     Action = "bind"
	ActionPreempt  Action = "preempt"
	ActionEvict    Action = "evict"
	ActionUnbind   Action = "unbind"
)

// Record 是一条审计记录。
type Record struct {
	Time     time.Time
	Actor    string
	Action   Action
	Workload string
	Node     string
	Allowed  bool
	Reason   string
}

// Policy 决定 actor 是否允许执行 action。
type Policy interface {
	Allow(actor string, action Action) bool
}

// AllowAll 允许一切（默认）。
type AllowAll struct{}

// Allow 实现 Policy。
func (AllowAll) Allow(actor string, action Action) bool { return true }

// 角色常量。
const (
	RoleAdmin    = "admin"
	RoleOperator = "operator"
	RoleViewer   = "viewer"
)

// RolePolicy 按角色授权：viewer 无任何写权限，operator 可调度/绑定/解绑，
// 仅 admin 可抢占与驱逐。未知 actor 一律拒绝。
type RolePolicy struct {
	Roles map[string]string // actor -> role
}

// Allow 实现 Policy。
func (p RolePolicy) Allow(actor string, action Action) bool {
	role, ok := p.Roles[actor]
	if !ok {
		return false
	}
	switch role {
	case RoleAdmin:
		return true
	case RoleOperator:
		switch action {
		case ActionSchedule, ActionBind, ActionUnbind:
			return true
		}
		return false
	default: // viewer 及未知角色
		return false
	}
}

// Log 是线程安全的内存审计日志。
type Log struct {
	mu      sync.Mutex
	records []Record
	now     func() time.Time // 可注入时钟，便于测试
}

// NewLog 创建空日志。
func NewLog() *Log { return &Log{now: time.Now} }

// SetClock 注入时钟（测试用）。
func (l *Log) SetClock(now func() time.Time) { l.now = now }

// Check 校验权限并写入审计记录；越权返回 false 且记录 Allowed=false。
func (l *Log) Check(p Policy, actor string, action Action, workload, node string) bool {
	allowed := p.Allow(actor, action)
	rec := Record{
		Time:     l.now(),
		Actor:    actor,
		Action:   action,
		Workload: workload,
		Node:     node,
		Allowed:  allowed,
	}
	if !allowed {
		rec.Reason = "permission denied"
	}
	l.mu.Lock()
	l.records = append(l.records, rec)
	l.mu.Unlock()
	return allowed
}

// Note 写入一条不带权限校验的信息性审计记录（如调度结果说明）。
func (l *Log) Note(actor string, action Action, workload, node, reason string) {
	l.mu.Lock()
	l.records = append(l.records, Record{
		Time: l.now(), Actor: actor, Action: action,
		Workload: workload, Node: node, Allowed: true, Reason: reason,
	})
	l.mu.Unlock()
}

// Records 返回全部审计记录副本。
func (l *Log) Records() []Record {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Record, len(l.records))
	copy(out, l.records)
	return out
}
