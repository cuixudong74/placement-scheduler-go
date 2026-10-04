package scheduler

import (
	"fmt"
	"sync"
	"time"
)

// Action 是需要权限校验的操作类型。
type Action string

const (
	ActionSubmitWorkload Action = "workload:submit"
	ActionBind           Action = "schedule:bind"
	ActionPreempt        Action = "schedule:preempt"
	ActionEvict          Action = "schedule:evict"
	ActionNodeUpdate     Action = "node:update"
	ActionNodeRemove     Action = "node:remove"
)

// Principal 是请求发起方身份。
type Principal struct {
	Name  string
	Roles []string
}

// ErrPermissionDenied 表示越权请求被拒绝。
type ErrPermissionDenied struct {
	Actor  string
	Action Action
	Target string
}

func (e *ErrPermissionDenied) Error() string {
	return fmt.Sprintf("permission denied: %s cannot %s on %s", e.Actor, e.Action, e.Target)
}

// rolePermissions 是内置的角色-权限映射。
var rolePermissions = map[string][]Action{
	"admin":      {ActionSubmitWorkload, ActionBind, ActionPreempt, ActionEvict, ActionNodeUpdate, ActionNodeRemove},
	"scheduler":  {ActionBind, ActionPreempt, ActionEvict},
	"submitter":  {ActionSubmitWorkload},
	"node-agent": {ActionNodeUpdate},
}

// Authorizer 基于角色做权限校验。
type Authorizer struct {
	// SystemIdentities 是免校验的内置身份（如控制器自身）。
	SystemIdentities map[string]bool
}

// Can 报告 principal 是否有权对 target 执行 action。
func (a *Authorizer) Can(p Principal, action Action, target string) bool {
	if a != nil && a.SystemIdentities[p.Name] {
		return true
	}
	for _, r := range p.Roles {
		for _, allowed := range rolePermissions[r] {
			if allowed == action {
				return true
			}
		}
	}
	return false
}

// AuditEvent 是一条审计记录。
type AuditEvent struct {
	Time    time.Time
	Actor   string
	Action  Action
	Target  string
	Allowed bool
	Detail  string
}

// AuditLog 是追加式内存审计日志。
type AuditLog struct {
	mu     sync.Mutex
	events []AuditEvent
	now    func() time.Time
}

func NewAuditLog(now func() time.Time) *AuditLog {
	if now == nil {
		now = time.Now
	}
	return &AuditLog{now: now}
}

// Record 追加一条审计记录。
func (l *AuditLog) Record(actor string, action Action, target string, allowed bool, detail string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.events = append(l.events, AuditEvent{
		Time: l.now(), Actor: actor, Action: action, Target: target, Allowed: allowed, Detail: detail,
	})
}

// Events 返回全部审计记录快照。
func (l *AuditLog) Events() []AuditEvent {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]AuditEvent, len(l.events))
	copy(out, l.events)
	return out
}

// guard 校验权限并写审计；拒绝时返回 *ErrPermissionDenied。
func guard(authz *Authorizer, audit *AuditLog, p Principal, action Action, target, detail string) error {
	allowed := true
	if authz != nil {
		allowed = authz.Can(p, action, target)
	}
	if audit != nil {
		audit.Record(p.Name, action, target, allowed, detail)
	}
	if !allowed {
		return &ErrPermissionDenied{Actor: p.Name, Action: action, Target: target}
	}
	return nil
}
