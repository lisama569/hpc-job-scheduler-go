package scheduler

import (
	"fmt"
	"sort"
)

// 计划内维护（maintenance）与排空（drain）相关逻辑。
//
// 节点状态流转：
//
//	Online ──BeginMaintenance──▶ Draining ──DrainNode（清空作业）──▶ Drained
//	  ▲                              │                                │
//	  └────────CancelMaintenance─────┴────────CancelMaintenance───────┤
//	  ▲                                                               │
//	  └──────────────────────EndMaintenance────────────────────────────┘
//
// 任意非故障状态都可被 FailNode 打断为 Failed；处于维护流程中的节点
// 故障后保留维护意图，RecoverNode 时回到 Drained 而非 Online。

// BeginMaintenance 把在线节点切到维护中：不再接收新调度，已有作业先保留。
// 已处于维护流程或故障中的节点返回错误，不产生副作用。
func (s *Scheduler) BeginMaintenance(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cluster.Node(nodeID); !ok {
		return ErrUnknownNode
	}
	if !s.cluster.beginMaintenance(nodeID) {
		n, _ := s.cluster.Node(nodeID)
		return fmt.Errorf("scheduler: cannot begin maintenance on node %q (state %s)", nodeID, n.State)
	}
	return nil
}

// DrainNode 显式排空维护中节点：受影响作业尽量平滑迁移到其他在线节点，
// 放不下则退回等待；作业组整体迁移或整体等待。资源与配额对每个作业
// 只结算一次。排空干净后节点置为已排空（真正下线）。
//
// 迁移不触发抢占：只尝试在其他在线节点上空闲分配，避免维护动作引发
// 连锁抢占；放不下的作业按有界重试退回队列，由后续调度循环按原有规则处理。
func (s *Scheduler) DrainNode(nodeID string) ([]Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, ok := s.cluster.Node(nodeID)
	if !ok {
		return nil, ErrUnknownNode
	}
	if n.State != NodeDraining {
		return nil, fmt.Errorf("scheduler: cannot drain node %q (state %s)", nodeID, n.State)
	}

	// 收集该节点上的运行中作业，按 ID 升序确定处理集合。
	var affected []*Job
	for _, j := range s.jobs {
		if j.State == StateRunning && j.AssignedNode == nodeID {
			affected = append(affected, j)
		}
	}
	sort.Slice(affected, func(a, b int) bool { return affected[a].ID < affected[b].ID })

	handled := map[string]bool{}
	var decisions []Decision

	// 先整体处理作业组（按 GroupID 升序），再处理独立作业（按 ID 升序）：
	// 只要组里有成员在受影响节点上，整组一起搬或一起等。
	groups := map[string][]*Job{}
	var gangIDs, soloIDs []string
	for _, j := range affected {
		if j.GroupID != "" {
			if _, seen := groups[j.GroupID]; !seen {
				gangIDs = append(gangIDs, j.GroupID)
			}
			groups[j.GroupID] = append(groups[j.GroupID], j)
		} else {
			soloIDs = append(soloIDs, j.ID)
		}
	}
	sort.Strings(gangIDs)
	sort.Strings(soloIDs)

	for _, gid := range gangIDs {
		// 整组快照包含全部仍在运行的成员（可能分布在多个节点上），
		// 保证不会出现同组一半运行、一半排队的中间状态。
		var members []*Job
		for _, j := range s.jobs {
			if j.GroupID == gid && j.State == StateRunning {
				members = append(members, j)
			}
		}
		sort.Slice(members, func(a, b int) bool { return members[a].ID < members[b].ID })
		decisions = append(decisions, s.drainGroupLocked(gid, members, nodeID)...)
		for _, m := range members {
			handled[m.ID] = true
		}
	}
	for _, id := range soloIDs {
		if handled[id] {
			continue
		}
		decisions = append(decisions, s.drainOneLocked(s.jobs[id], nodeID))
	}

	// 所有运行中作业都已迁走或退回，节点占用归零，真正下线。
	s.cluster.markDrained(nodeID)
	decisions = append(decisions, Decision{JobID: nodeID, Action: "node-drained",
		Reason: fmt.Sprintf("node %s drained and taken offline", nodeID)})
	return decisions, nil
}

// drainOneLocked 排空单个独立作业：先在其他在线节点试迁移；
// 放不下则把原占用的资源与配额恰好结算一次后退回队列（有界重试）。
func (s *Scheduler) drainOneLocked(j *Job, nodeID string) Decision {
	if dest, ok := s.cluster.tryAllocate(j.Req); ok {
		// 平滑迁移：源端释放一次、目标端占用一次，配额保持不变
		// （作业始终 Running，不应发生配额漂移）。
		s.cluster.release(nodeID, j.Req)
		j.AssignedNode = dest
		return Decision{j.ID, "migrated",
			fmt.Sprintf("drained from %s to %s with %+v (quota unchanged)", nodeID, dest, j.Req)}
	}
	s.cluster.release(nodeID, j.Req)
	s.quota[j.Owner] = s.quota[j.Owner].Sub(j.Req)
	return s.requeueOrFailLocked(j, fmt.Sprintf("drained off node %s; no healthy node fits", nodeID))
}

// drainGroupLocked 整体排空一个作业组：
// 先把整组占用从各节点摘下，再在其他在线节点上从零试排（first-fit，顺序确定）。
// 全部成员同时落地才整体迁移（被排空节点处于 Draining，天然不会被选中）；
// 放不下则把占用原样恢复，随后整组的资源与配额各结算一次后整体退回，
// 重试结论全组一致。配额在成功路径上全程不变（无配额漂移）。
func (s *Scheduler) drainGroupLocked(groupID string, members []*Job, nodeID string) []Decision {
	orig := make(map[string]string, len(members))
	for _, m := range members {
		orig[m.ID] = m.AssignedNode
		s.cluster.release(m.AssignedNode, m.Req)
	}

	type alloc struct {
		job    *Job
		nodeID string
	}
	var trials []alloc
	fit := true
	for _, m := range members {
		dest, ok := s.cluster.tryAllocate(m.Req)
		if !ok {
			fit = false
			break
		}
		trials = append(trials, alloc{m, dest})
	}
	if fit {
		for _, a := range trials {
			a.job.AssignedNode = a.nodeID
		}
		var ds []Decision
		for _, a := range trials {
			ds = append(ds, Decision{a.job.ID, "migrated",
				fmt.Sprintf("gang %q drained together: moved to %s with %+v (quota unchanged)",
					groupID, a.nodeID, a.job.Req)})
		}
		return ds
	}

	// 放不下：回滚本轮试分配，并把整组占用恢复到原节点，
	// 再统一走"退回队列"路径，保证每个作业的资源最终只结算一次。
	for _, a := range trials {
		s.cluster.release(a.nodeID, a.job.Req)
	}
	for _, m := range members {
		s.cluster.restore(orig[m.ID], m.Req)
		m.AssignedNode = orig[m.ID]
	}
	cause := fmt.Sprintf("gang %q drained off node %s; group does not fit simultaneously", groupID, nodeID)
	exhausted := false
	for _, m := range members {
		if m.Retries+1 > m.MaxRetries {
			exhausted = true
		}
	}
	var ds []Decision
	for _, m := range members {
		s.cluster.release(m.AssignedNode, m.Req) // 每个作业恰好释放一次
		s.quota[m.Owner] = s.quota[m.Owner].Sub(m.Req)
		if exhausted {
			m.AssignedNode = ""
			m.Retries++
			m.State = StateFailed
			ds = append(ds, Decision{m.ID, "failed",
				fmt.Sprintf("%s and retries exhausted (%d/%d); whole group failed",
					cause, m.Retries, m.MaxRetries)})
			continue
		}
		ds = append(ds, s.requeueOrFailLocked(m, cause))
	}
	return ds
}

// CancelMaintenance 取消维护：维护中或已排空的节点回到在线态，
// 立即重新参与后续调度循环（其上作业若仍在运行则继续保留）。
func (s *Scheduler) CancelMaintenance(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cluster.Node(nodeID); !ok {
		return ErrUnknownNode
	}
	if !s.cluster.cancelMaintenance(nodeID) {
		n, _ := s.cluster.Node(nodeID)
		return fmt.Errorf("scheduler: cannot cancel maintenance on node %q (state %s)", nodeID, n.State)
	}
	return nil
}

// EndMaintenance 结束维护：已排空、检修完成的节点回到在线态，
// 下一轮调度即可重新使用。
func (s *Scheduler) EndMaintenance(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cluster.Node(nodeID); !ok {
		return ErrUnknownNode
	}
	if !s.cluster.endMaintenance(nodeID) {
		n, _ := s.cluster.Node(nodeID)
		return fmt.Errorf("scheduler: cannot end maintenance on node %q (state %s)", nodeID, n.State)
	}
	return nil
}
