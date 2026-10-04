package scheduler

import "fmt"

// ConstraintError 携带可区分的原因码，供调度器写入负载状态。
type ConstraintError struct {
	Reason  Reason
	Message string
}

func (e *ConstraintError) Error() string { return fmt.Sprintf("%s: %s", e.Reason, e.Message) }

func matchesExpressions(labels map[string]string, exprs []MatchExpression) bool {
	for _, e := range exprs {
		v, ok := labels[e.Key]
		switch e.Operator {
		case "In":
			if !ok || !contains(e.Values, v) {
				return false
			}
		case "NotIn":
			if ok && contains(e.Values, v) {
				return false
			}
		case "Exists":
			if !ok {
				return false
			}
		default:
			return false
		}
	}
	return true
}

func contains(xs []string, x string) bool {
	for _, y := range xs {
		if y == x {
			return true
		}
	}
	return false
}

// checkNodeSelector 校验节点标签选择器。
func checkNodeSelector(w *Workload, n *Node) *ConstraintError {
	for k, v := range w.Constraints.NodeSelector {
		if n.Labels[k] != v {
			return &ConstraintError{
				Reason:  ReasonConstraintConflict,
				Message: fmt.Sprintf("node %q label %q=%q, want %q", n.ID, k, n.Labels[k], v),
			}
		}
	}
	return nil
}

// domainOf 返回节点在给定拓扑键下的域值。
func domainOf(n *Node, topologyKey string) (string, bool) {
	v, ok := n.Labels[topologyKey]
	return v, ok
}

// checkAffinity 校验亲和/反亲和。
// bound 是当前所有已绑定负载（含其节点），用于在拓扑域内统计。
func checkAffinity(w *Workload, n *Node, bound []Workload, nodes map[string]Node) *ConstraintError {
	for _, term := range w.Constraints.RequiredAffinity {
		dom, ok := domainOf(n, term.TopologyKey)
		if !ok {
			return &ConstraintError{
				Reason:  ReasonMissingTopologyInfo,
				Message: fmt.Sprintf("node %q missing topology label %q", n.ID, term.TopologyKey),
			}
		}
		found := false
		for i := range bound {
			b := &bound[i]
			if b.ID == w.ID {
				continue
			}
			bn, ok := nodes[b.NodeID]
			if !ok {
				continue
			}
			if bn.Labels[term.TopologyKey] == dom && matchesExpressions(b.Labels, term.LabelSelector) {
				found = true
				break
			}
		}
		if !found {
			return &ConstraintError{
				Reason:  ReasonConstraintConflict,
				Message: fmt.Sprintf("affinity: no matching workload in domain %s=%s", term.TopologyKey, dom),
			}
		}
	}
	for _, term := range w.Constraints.RequiredAntiAffinity {
		dom, ok := domainOf(n, term.TopologyKey)
		if !ok {
			return &ConstraintError{
				Reason:  ReasonMissingTopologyInfo,
				Message: fmt.Sprintf("node %q missing topology label %q", n.ID, term.TopologyKey),
			}
		}
		for i := range bound {
			b := &bound[i]
			if b.ID == w.ID {
				continue
			}
			bn, ok := nodes[b.NodeID]
			if !ok {
				continue
			}
			if bn.Labels[term.TopologyKey] == dom && matchesExpressions(b.Labels, term.LabelSelector) {
				return &ConstraintError{
					Reason:  ReasonConstraintConflict,
					Message: fmt.Sprintf("anti-affinity: workload %q already in domain %s=%s", b.ID, term.TopologyKey, dom),
				}
			}
		}
	}
	return nil
}

// checkSpread 校验分散约束：把 w 放到 n 后，其所在域内同组负载数不得超过 MaxPerDomain。
func checkSpread(w *Workload, n *Node, bound []Workload, nodes map[string]Node) *ConstraintError {
	for _, sc := range w.Constraints.Spread {
		if sc.TopologyKey == "" || sc.Group == "" {
			return &ConstraintError{
				Reason:  ReasonInvalidSpec,
				Message: "spread constraint requires TopologyKey and Group",
			}
		}
		dom, ok := domainOf(n, sc.TopologyKey)
		if !ok {
			return &ConstraintError{
				Reason:  ReasonMissingTopologyInfo,
				Message: fmt.Sprintf("node %q missing topology label %q", n.ID, sc.TopologyKey),
			}
		}
		count := 0
		for i := range bound {
			b := &bound[i]
			if b.ID == w.ID {
				continue
			}
			if b.Labels["spread-group"] != sc.Group {
				continue
			}
			bn, ok := nodes[b.NodeID]
			if !ok {
				continue
			}
			if bn.Labels[sc.TopologyKey] == dom {
				count++
			}
		}
		if count+1 > sc.MaxPerDomain {
			return &ConstraintError{
				Reason:  ReasonConstraintConflict,
				Message: fmt.Sprintf("spread: domain %s=%s already has %d of group %q (max %d)", sc.TopologyKey, dom, count, sc.Group, sc.MaxPerDomain),
			}
		}
	}
	return nil
}

// checkAll 对单个节点执行全部约束检查，返回第一个违例。
func checkAll(w *Workload, n *Node, bound []Workload, nodes map[string]Node) *ConstraintError {
	if err := checkNodeSelector(w, n); err != nil {
		return err
	}
	if err := checkAffinity(w, n, bound, nodes); err != nil {
		return err
	}
	if err := checkSpread(w, n, bound, nodes); err != nil {
		return err
	}
	return nil
}
