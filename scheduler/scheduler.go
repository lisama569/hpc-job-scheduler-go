package scheduler

import (
	"errors"
	"fmt"
	"sort"
	"sync"
)

// ErrJobExists 表示重复提交相同 ID 的作业。
var ErrJobExists = errors.New("scheduler: job already exists")

// ErrUnknownJob 表示操作的作业不存在。
var ErrUnknownJob = errors.New("scheduler: unknown job")

// ErrJobNotRunning 表示对非运行中的作业执行了 Complete。
var ErrJobNotRunning = errors.New("scheduler: job is not running")

// Decision 记录一次调度判定，供日志与测试断言使用。
type Decision struct {
	JobID  string
	Action string // "allocated" / "wait" / "rejected" / "preempted-victim" / "requeued" / "failed"
	Reason string // 人类可读的判定依据
}

// Scheduler 是并发安全的作业调度器。
// 所有对外可见状态（作业状态、节点占用、配额）均在同一把互斥锁内变更，
// 并发提交/调度/故障注入下保持一致。
type Scheduler struct {
	mu      sync.Mutex
	cluster *Cluster
	jobs    map[string]*Job
	queue   *readyQueue
	quota   map[string]Resources // Owner -> 当前占用（仅 Running 作业计入）
	limits  map[string]Resources // Owner -> 配额上限，缺省表示不限
}

// New 创建调度器；limits 为各归属方的配额上限（可为 nil 表示不限）。
func New(c *Cluster, limits map[string]Resources) *Scheduler {
	if limits == nil {
		limits = map[string]Resources{}
	}
	return &Scheduler{
		cluster: c,
		jobs:    map[string]*Job{},
		queue:   newReadyQueue(),
		quota:   map[string]Resources{},
		limits:  limits,
	}
}

// Submit 提交作业：校验容量与依赖，拒绝原因可区分；通过则入队。
// 被拒绝的作业也会登记在册（状态 Rejected），便于查询与审计。
func (s *Scheduler) Submit(j *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.submitLocked(j)
}

func (s *Scheduler) submitLocked(j *Job) error {
	if _, dup := s.jobs[j.ID]; dup {
		return ErrJobExists
	}
	jc := *j // 拷贝，调用方后续修改不影响调度器内部状态
	j = &jc
	j.State = StatePending
	j.RejectWhy = RejectNone
	j.Retries = 0
	j.AssignedNode = ""
	s.jobs[j.ID] = j

	switch {
	case !s.cluster.fitsAnyNode(j.Req):
		s.rejectLocked(j, RejectExceedsCapacity)
	case s.hasCycleLocked(j):
		s.rejectLocked(j, RejectDependencyCycle)
	default:
		bad := s.unsatisfiableDepLocked(j)
		if bad != "" {
			s.rejectLocked(j, RejectDependencyUnsatisfiable)
		}
	}
	if j.State == StateRejected {
		return nil
	}
	s.queue.Push(j)
	return nil
}

// rejectLocked 将作业置为 Rejected 并记录可区分的原因。
func (s *Scheduler) rejectLocked(j *Job, why RejectReason) {
	j.State = StateRejected
	j.RejectWhy = why
}

// hasCycleLocked 检测自依赖环（依赖必须指向已提交作业，故唯一可能的环是自环）。
func (s *Scheduler) hasCycleLocked(j *Job) bool {
	for _, d := range j.DependsOn {
		if d == j.ID {
			return true
		}
	}
	return false
}

// unsatisfiableDepLocked 返回第一个永不可满足的依赖 ID；全部可满足时返回空串。
// 依赖必须引用已提交且未被拒绝/失败的作业。
func (s *Scheduler) unsatisfiableDepLocked(j *Job) string {
	for _, d := range j.DependsOn {
		dep, ok := s.jobs[d]
		if !ok || dep.State == StateRejected || dep.State == StateFailed {
			return d
		}
	}
	return ""
}

// SubmitArray 提交一个作业数组：元素互不影响，任一元素被拒不影响其余元素；
// 配额按 Owner 独立核算。返回成功入队或被登记的元素 ID 列表。
func (s *Scheduler) SubmitArray(owner, prefix string, count int, tmpl Job) ([]string, error) {
	if count <= 0 {
		return nil, fmt.Errorf("scheduler: array count must be positive, got %d", count)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := make([]string, 0, count)
	for i := 0; i < count; i++ {
		j := tmpl
		j.ID = fmt.Sprintf("%s-%d", prefix, i)
		j.Owner = owner
		j.ArrayIndex = i
		if err := s.submitLocked(&j); err != nil {
			return ids, err
		}
		ids = append(ids, j.ID)
	}
	return ids, nil
}

// RunCycle 执行一轮调度：按确定性顺序为队列中的作业分配资源。
// 组调度整体成功或整体等待，失败不留残留占用；
// 普通作业在资源不足时按确定规则抢占低优先级作业。
// 返回本轮全部判定，顺序确定。
func (s *Scheduler) RunCycle() []Decision {
	s.mu.Lock()
	defer s.mu.Unlock()

	var decisions []Decision
	drained := make([]*Job, 0, s.queue.Len())
	for {
		j := s.queue.Pop()
		if j == nil {
			break
		}
		drained = append(drained, j)
	}
	processed := map[string]bool{}
	for _, j := range drained {
		if processed[j.ID] {
			continue
		}
		if j.GroupID != "" {
			decisions = append(decisions, s.scheduleGroupLocked(j.GroupID, drained, processed)...)
			continue
		}
		processed[j.ID] = true
		decisions = append(decisions, s.scheduleOneLocked(j, true))
	}
	return decisions
}

// scheduleGroupLocked 组调度：组内全部 Pending 成员整体获得资源，或整体继续等待。
// 任一成员的依赖未满足或资源/配额不足时，整组等待且不留残留占用。
func (s *Scheduler) scheduleGroupLocked(groupID string, drained []*Job, processed map[string]bool) []Decision {
	var members []*Job
	for _, j := range drained {
		if j.GroupID == groupID && !processed[j.ID] {
			members = append(members, j)
			processed[j.ID] = true
		}
	}
	var decisions []Decision

	// 依赖检查：任一成员依赖未就绪则整组等待；依赖永不可满足则整组拒绝。
	for _, m := range members {
		if bad := s.unsatisfiableDepLocked(m); bad != "" {
			for _, m2 := range members {
				s.rejectLocked(m2, RejectDependencyUnsatisfiable)
				decisions = append(decisions, Decision{m2.ID, "rejected",
					fmt.Sprintf("gang member %s has unsatisfiable dependency %q; whole group rejected", m.ID, bad)})
			}
			return decisions
		}
		if !s.depsReadyLocked(m) {
			for _, m2 := range members {
				s.queue.Push(m2)
			}
			return append(decisions, Decision{groupID, "wait",
				fmt.Sprintf("gang member %s waiting on dependencies; whole group waits", m.ID)})
		}
	}

	// 配额试算：按归属方汇总整组需求。
	need := map[string]Resources{}
	for _, m := range members {
		need[m.Owner] = need[m.Owner].Add(m.Req)
	}
	for owner, n := range need {
		if !s.quotaOKLocked(owner, n) {
			for _, m := range members {
				s.queue.Push(m)
			}
			return append(decisions, Decision{groupID, "wait",
				fmt.Sprintf("gang quota exceeded for owner %q: need %+v, used %+v, limit %+v",
					owner, n, s.quota[owner], s.limits[owner])})
		}
	}

	// 资源试分配：全部成功才提交，否则逐个回滚，不留残留。
	type alloc struct {
		job    *Job
		nodeID string
	}
	var placed []alloc
	for _, m := range members {
		nodeID, ok := s.cluster.tryAllocate(m.Req)
		if !ok {
			for _, p := range placed {
				s.cluster.release(p.nodeID, p.job.Req)
			}
			for _, m := range members {
				s.queue.Push(m)
			}
			return append(decisions, Decision{groupID, "wait",
				fmt.Sprintf("gang of %d tasks does not fit simultaneously (rolled back %d trial allocations)",
					len(members), len(placed))})
		}
		placed = append(placed, alloc{m, nodeID})
	}
	for _, p := range placed {
		s.commitLocked(p.job, p.nodeID)
		decisions = append(decisions, Decision{p.job.ID, "allocated",
			fmt.Sprintf("gang allocated on node %s with %+v", p.nodeID, p.job.Req)})
	}
	return decisions
}

// scheduleOneLocked 调度单个作业；allowPreempt 控制是否可触发抢占。
func (s *Scheduler) scheduleOneLocked(j *Job, allowPreempt bool) Decision {
	if bad := s.unsatisfiableDepLocked(j); bad != "" {
		s.rejectLocked(j, RejectDependencyUnsatisfiable)
		return Decision{j.ID, "rejected", fmt.Sprintf("dependency %q can never succeed", bad)}
	}
	if !s.depsReadyLocked(j) {
		s.queue.Push(j)
		return Decision{j.ID, "wait", "dependencies not all succeeded yet"}
	}
	if !s.quotaOKLocked(j.Owner, j.Req) {
		s.queue.Push(j)
		return Decision{j.ID, "wait",
			fmt.Sprintf("quota exceeded for owner %q: need %+v, used %+v, limit %+v",
				j.Owner, j.Req, s.quota[j.Owner], s.limits[j.Owner])}
	}
	if nodeID, ok := s.cluster.tryAllocate(j.Req); ok {
		s.commitLocked(j, nodeID)
		return Decision{j.ID, "allocated", fmt.Sprintf("allocated on node %s with %+v", nodeID, j.Req)}
	}
	if allowPreempt {
		if d, ok := s.preemptLocked(j); ok {
			return d
		}
	}
	s.queue.Push(j)
	return Decision{j.ID, "wait",
		fmt.Sprintf("insufficient free resources: need %+v, cluster free %+v", j.Req, s.cluster.totalFree())}
}

// preemptLocked 按确定规则为 j 回收资源：
// 候选牺牲者为优先级严格更低的运行中作业，按 (优先级升序, ID 升序) 逐个抢占，
// 直到 j 能放下；全部抢占仍放不下则整体回滚，视同资源不足。
// 被抢占作业按有界重试重新入队，重试耗尽则置为 Failed。
func (s *Scheduler) preemptLocked(j *Job) (Decision, bool) {
	var victims []*Job
	for _, o := range s.jobs {
		if o.State == StateRunning && o.Priority < j.Priority {
			victims = append(victims, o)
		}
	}
	if len(victims) == 0 {
		return Decision{}, false
	}
	sort.Slice(victims, func(a, b int) bool {
		if victims[a].Priority != victims[b].Priority {
			return victims[a].Priority < victims[b].Priority
		}
		return victims[a].ID < victims[b].ID
	})

	var preempted []*Job
	for _, v := range victims {
		s.cluster.release(v.AssignedNode, v.Req)
		s.quota[v.Owner] = s.quota[v.Owner].Sub(v.Req)
		preempted = append(preempted, v)
		if nodeID, ok := s.cluster.tryAllocate(j.Req); ok {
			s.commitLocked(j, nodeID)
			for _, p := range preempted {
				s.requeueOrFailLocked(p, "preempted")
			}
			return Decision{j.ID, "allocated",
				fmt.Sprintf("preempted %d lower-priority job(s) and allocated on node %s", len(preempted), nodeID)}, true
		}
	}
	// 回滚：恢复全部牺牲者，如同抢占从未发生。
	for _, p := range preempted {
		s.cluster.restore(p.AssignedNode, p.Req)
		s.quota[p.Owner] = s.quota[p.Owner].Add(p.Req)
	}
	return Decision{}, false
}

// requeueOrFailLocked 实现有界重试：重试次数未达上限则重新入队，否则置为 Failed。
func (s *Scheduler) requeueOrFailLocked(j *Job, cause string) Decision {
	j.AssignedNode = ""
	j.Retries++
	if j.Retries > j.MaxRetries {
		j.State = StateFailed
		return Decision{j.ID, "failed",
			fmt.Sprintf("%s and retries exhausted (%d/%d)", cause, j.Retries, j.MaxRetries)}
	}
	j.State = StatePending
	s.queue.Push(j)
	return Decision{j.ID, "requeued",
		fmt.Sprintf("%s, retry %d/%d", cause, j.Retries, j.MaxRetries)}
}

// commitLocked 提交一次分配：置运行态并计入配额。
func (s *Scheduler) commitLocked(j *Job, nodeID string) {
	j.State = StateRunning
	j.AssignedNode = nodeID
	s.quota[j.Owner] = s.quota[j.Owner].Add(j.Req)
}

// depsReadyLocked 报告作业的全部依赖是否已成功。
func (s *Scheduler) depsReadyLocked(j *Job) bool {
	for _, d := range j.DependsOn {
		dep, ok := s.jobs[d]
		if !ok || dep.State != StateSucceeded {
			return false
		}
	}
	return true
}

// quotaOKLocked 报告 owner 在追加 need 后是否仍不超限。
func (s *Scheduler) quotaOKLocked(owner string, need Resources) bool {
	limit, ok := s.limits[owner]
	if !ok {
		return true
	}
	return s.quota[owner].Add(need).Fits(limit)
}

// Complete 将运行中的作业标记为成功并释放资源与配额。
func (s *Scheduler) Complete(jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return ErrUnknownJob
	}
	if j.State != StateRunning {
		return ErrJobNotRunning
	}
	s.cluster.release(j.AssignedNode, j.Req)
	s.quota[j.Owner] = s.quota[j.Owner].Sub(j.Req)
	j.AssignedNode = ""
	j.State = StateSucceeded
	return nil
}

// FailNode 模拟节点故障：节点上作业的资源只释放一次，
// 按有界重试重新入队或置为失败，不产生重复计数或状态丢失。
func (s *Scheduler) FailNode(nodeID string) ([]Decision, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.cluster.failNode(nodeID) {
		return nil, fmt.Errorf("scheduler: cannot fail node %q (unknown or already down)", nodeID)
	}
	// 收集该节点上的运行中作业，按 ID 排序保证判定顺序确定。
	var affected []*Job
	for _, j := range s.jobs {
		if j.State == StateRunning && j.AssignedNode == nodeID {
			affected = append(affected, j)
		}
	}
	sort.Slice(affected, func(a, b int) bool { return affected[a].ID < affected[b].ID })

	var decisions []Decision
	for _, j := range affected {
		s.cluster.release(nodeID, j.Req) // 恰好释放一次
		s.quota[j.Owner] = s.quota[j.Owner].Sub(j.Req)
		decisions = append(decisions, s.requeueOrFailLocked(j, fmt.Sprintf("node %s failed", nodeID)))
	}
	return decisions, nil
}

// RecoverNode 恢复节点为健康状态。
func (s *Scheduler) RecoverNode(nodeID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.cluster.recoverNode(nodeID) {
		return fmt.Errorf("scheduler: cannot recover node %q (unknown or already healthy)", nodeID)
	}
	return nil
}

// Job 返回作业当前状态的快照。
func (s *Scheduler) Job(jobID string) (Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok {
		return Job{}, ErrUnknownJob
	}
	return *j, nil
}

// QuotaUsed 返回归属方当前占用的配额。
func (s *Scheduler) QuotaUsed(owner string) Resources {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.quota[owner]
}

// Pending 返回当前排队作业 ID（按出队顺序）。
func (s *Scheduler) Pending() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := s.queue.Snapshot()
	ids := make([]string, 0, len(snap))
	for _, j := range snap {
		ids = append(ids, j.ID)
	}
	return ids
}
