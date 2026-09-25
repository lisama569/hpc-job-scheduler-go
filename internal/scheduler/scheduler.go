// Package scheduler is the single-writer scheduling core: admission,
// dependency checks, quota, gang co-scheduling, array handling, bounded
// preemption retry and node-failure recovery. All public methods are
// serialized by one mutex so externally visible state is always consistent.
package scheduler

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"

	"github.com/lisama569/hpc-job-scheduler-go/internal/cluster"
	"github.com/lisama569/hpc-job-scheduler-go/internal/domain"
	"github.com/lisama569/hpc-job-scheduler-go/internal/queue"
	"github.com/lisama569/hpc-job-scheduler-go/internal/quota"
)

// Config tunes the scheduling core.
type Config struct {
	// MaxRequeueRetries bounds how many times one job may be requeued after a
	// preemption or a node failure before it is reported FAILED.
	MaxRequeueRetries int
}

// DefaultConfig returns safe defaults.
func DefaultConfig() Config { return Config{MaxRequeueRetries: 3} }

// placedTask is the scheduler's own reverse index: node -> what runs on it.
type placedTask struct {
	jobID  string
	taskID string
	res    domain.Resource
}

// Scheduler is the serialized scheduling core.
type Scheduler struct {
	mu sync.Mutex

	cfg     Config
	cluster *cluster.Cluster
	quota   *quota.Ledger
	pending *queue.Queue

	jobs  map[string]*domain.Job
	order []string // submission order, for stable snapshots

	// groupMembers maps a gang GroupID to the complete, immutable member set.
	groupMembers map[string][]string
	// placements: nodeID -> tasks currently running on the node.
	placements map[string][]placedTask
	// jobNodes: jobID -> node IDs hosting its tasks (1..n).
	jobNodes map[string][]string

	tick      int
	decisions []domain.Decision
	log       io.Writer
}

// New builds a Scheduler. A nil logger disables explanatory logging.
func New(cfg Config, log io.Writer) *Scheduler {
	if cfg.MaxRequeueRetries < 0 {
		cfg.MaxRequeueRetries = 0
	}
	return &Scheduler{
		cfg:          cfg,
		cluster:      cluster.New(),
		quota:        quota.NewLedger(),
		pending:      queue.New(),
		jobs:         make(map[string]*domain.Job),
		groupMembers: make(map[string][]string),
		placements:   make(map[string][]placedTask),
		jobNodes:     make(map[string][]string),
		log:          log,
	}
}

// AddNode registers a simulated node.
func (s *Scheduler) AddNode(n domain.Node) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cluster.AddNode(n)
}

// SetQuota declares an owner entitlement.
func (s *Scheduler) SetQuota(owner string, r domain.Resource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.quota.SetQuota(owner, r)
}

// Submit admits jobs into the pending queue. Input is logged so every test
// run shows exactly what was submitted and in what order.
func (s *Scheduler) Submit(jobs ...*domain.Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, j := range jobs {
		if j == nil {
			return fmt.Errorf("scheduler: nil job submitted")
		}
		if _, exists := s.jobs[j.ID]; exists {
			return fmt.Errorf("scheduler: duplicate job id %q", j.ID)
		}
		if len(j.Tasks) == 0 {
			return fmt.Errorf("scheduler: job %q has no tasks", j.ID)
		}
		if j.RetriesLeft == 0 {
			j.RetriesLeft = s.cfg.MaxRequeueRetries
		}
		j.State = domain.StatePending
		j.Reason = domain.ReasonNone
		s.logf("submit job %s input: owner=%s priority=%d group=%s array=%s#%d res=%v deps=[%s] tasks=%d retries=%d",
			j.ID, j.Owner, j.Priority, dash(j.GroupID), dash(j.ArrayID), j.Index,
			j.TotalResources(), strings.Join(j.DependsOn, ","), len(j.Tasks), j.RetriesLeft)
		s.jobs[j.ID] = j
		s.order = append(s.order, j.ID)
		if j.GroupID != "" {
			s.groupMembers[j.GroupID] = append(s.groupMembers[j.GroupID], j.ID)
		}
		s.pending.Enqueue(j, s.tick)
	}
	return nil
}

// RunPass performs one deterministic scheduling round. The queue is scanned in
// priority order; each job is either a singleton scheduling unit or part of a
// gang unit that must place atomically. New decisions are returned.
func (s *Scheduler) RunPass() []domain.Decision {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tick++
	s.logf("--- scheduling pass tick=%d pending=%d running-index=%d", s.tick, s.pending.Len(), len(s.jobNodes))

	var fresh []domain.Decision
	record := func(d domain.Decision) {
		s.decisions = append(s.decisions, d)
		fresh = append(fresh, d)
	}

	// Snapshot this pass's ordering exactly once. Jobs that are committed or
	// rejected vanish from the queue mid-scan; preemption may re-enqueue
	// victims, which must wait for the *next* pass (no cascading preemption
	// within one pass -> deterministic).
	ordered := s.pending.PeekAll()
	seenGroups := map[string]bool{}
	for _, head := range ordered {
		if head.State != domain.StatePending {
			continue // committed/rejected/preempted earlier in this pass
		}
		if head.GroupID != "" {
			if seenGroups[head.GroupID] {
				continue // group already handled when its earlier member was head
			}
			seenGroups[head.GroupID] = true
		}
		u := s.collectUnit(head)
		d := s.evaluateUnit(u)
		record(d)
		// Running -> commitUnit removed members; Rejected -> remove members;
		// Pending (wait) -> leave untouched for the next pass.
		if d.State == domain.StateRejected {
			ids := make([]string, 0, len(u.jobs))
			for _, j := range u.jobs {
				ids = append(ids, j.ID)
			}
			s.pending.RemoveMany(ids)
		}
	}
	return fresh
}

// unit is one atomic scheduling candidate.
type unit struct {
	jobs []*domain.Job
	gang bool
}

// collectUnit gathers the head job plus, for a gang job, every member of its
// group. Members are deterministically sorted by ID within the unit.
func (s *Scheduler) collectUnit(head *domain.Job) unit {
	if head.GroupID == "" {
		return unit{jobs: []*domain.Job{head}, gang: false}
	}
	ids := append([]string(nil), s.groupMembers[head.GroupID]...)
	sort.Strings(ids)
	jobs := make([]*domain.Job, 0, len(ids))
	for _, id := range ids {
		if j, ok := s.jobs[id]; ok {
			jobs = append(jobs, j)
		}
	}
	return unit{jobs: jobs, gang: true}
}

// evaluateUnit applies the full decision ladder to one unit:
// dependencies -> hard capacity -> quota -> trial placement -> preemption.
// It never partially mutates state: either the unit commits fully or nothing
// is reserved.
func (s *Scheduler) evaluateUnit(u unit) domain.Decision {
	label := "singleton"
	if u.gang {
		label = "gang(" + u.jobs[0].GroupID + ")"
	}
	var memberIDs []string
	minPrio := u.jobs[0].Priority
	totalRes := domain.Resource{}
	for _, j := range u.jobs {
		memberIDs = append(memberIDs, j.ID)
		if j.Priority < minPrio {
			minPrio = j.Priority
		}
		totalRes = totalRes.Add(j.TotalResources())
	}
	s.logf("evaluate %s members=[%s] totalRes=%v minPrio=%d", label, strings.Join(memberIDs, ","), totalRes, minPrio)

	// 1) Dependencies.
	for _, j := range u.jobs {
		for _, dep := range j.DependsOn {
			d, ok := s.jobs[dep]
			if !ok || d.State == domain.StateFailed || d.State == domain.StateRejected {
				return s.rejectUnit(u, domain.ReasonDependencyFailed,
					fmt.Sprintf("dependency %q failed/unknown (state=%v)", dep, stateOf(d)))
			}
			if d.State != domain.StateCompleted {
				return s.waitUnit(u, domain.ReasonDependencyNotMet,
					fmt.Sprintf("dependency %q not completed (state=%v)", dep, d.State))
			}
		}
	}

	// Gang rule: the *entire* declared group must currently be PENDING,
	// otherwise the gang cannot take the all-or-nothing decision.
	if u.gang {
		for _, j := range u.jobs {
			if j.State != domain.StatePending {
				return s.waitUnit(u, domain.ReasonGroupCoScheduling,
					fmt.Sprintf("member %s is %s, gang cannot place atomically", j.ID, j.State))
			}
		}
	}

	// 2) Hard per-node capacity: any task larger than every node is impossible.
	for _, j := range u.jobs {
		for _, t := range j.Tasks {
			if !s.cluster.HasAnyNodeThatFits(t.Resources) {
				return s.rejectUnit(u, domain.ReasonExceedsNodeCapacity,
					fmt.Sprintf("task %s needs %v but no up node has that capacity", t.ID, t.Resources))
			}
		}
	}

	// 3) Hard per-job quota ceiling: a single job that alone exceeds its
	// owner's entitlement can never run and is rejected permanently. Transient
	// saturation (existing usage + this unit > quota) is decided during the
	// trial placement and yields a retriable wait instead.
	quotas := s.quota.SnapshotQuota()
	for _, j := range u.jobs {
		if q, ok := quotas[j.Owner]; ok && !q.Fits(j.TotalResources()) {
			return s.rejectUnit(u, domain.ReasonQuotaExceeded,
				fmt.Sprintf("job %s owner %s requests %v above quota ceiling %v", j.ID, j.Owner, j.TotalResources(), q))
		}
	}
	// For a gang the unit is indivisible: if the members of one owner together
	// exceed that owner's ceiling, the whole gang is permanently unplaceable.
	if u.gang {
		unitOwnerUse := map[string]domain.Resource{}
		for _, j := range u.jobs {
			unitOwnerUse[j.Owner] = unitOwnerUse[j.Owner].Add(j.TotalResources())
		}
		for owner, need := range unitOwnerUse {
			if q, ok := quotas[owner]; ok && !q.Fits(need) {
				return s.rejectUnit(u, domain.ReasonGroupCoScheduling,
					fmt.Sprintf("gang members of owner %s jointly need %v above quota ceiling %v; group can never co-place", owner, need, q))
			}
		}
	}

	// 4) Trial placement on the live fleet.
	plan, err := s.planUnit(u, nil, nil)
	if err == nil {
		s.logf("judgment: %s placeable as-is plan=%s", label, planSummary(plan))
		s.commitUnit(u, plan)
		return s.runningDecision(u, plan, "placed without preemption")
	}
	s.logf("judgment: %s cannot place directly: %v", label, err)

	// Transient quota saturation is a wait, distinguishable from hard ceiling.
	if err == errQuotaNow {
		return s.waitUnit(u, domain.ReasonQuotaExceeded,
			"owner quota is currently saturated; will retry as jobs complete")
	}

	// 5) Deterministic preemption, allowed only against strictly lower
	// priority running jobs. Victim groups are atomic: a running gang is
	// preempted as a whole, never split into a partially running remainder.
	plan, victims, perr := s.tryPreempt(u, minPrio)
	if perr != nil {
		s.logf("judgment: preemption cannot free enough: %v", perr)
		if u.gang {
			return s.waitUnit(u, domain.ReasonGroupCoScheduling,
				"gang cannot be placed atomically and preemption is insufficient")
		}
		return s.waitUnit(u, domain.ReasonInsufficientResource,
			"no node has enough free resources and no lower-priority victim helps")
	}

	s.logf("judgment: preempt victims=[%s] then place plan=%s", strings.Join(victims, ","), planSummary(plan))
	s.applyPreemption(victims)
	s.commitUnit(u, plan)
	return s.runningDecision(u, plan, fmt.Sprintf("placed after preempting [%s]", strings.Join(victims, ",")))
}

// CompleteJob marks a running job finished and releases everything it held.
func (s *Scheduler) CompleteJob(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[jobID]
	if !ok || j.State != domain.StateRunning {
		return false
	}
	s.releaseJob(j)
	j.State = domain.StateCompleted
	j.Reason = domain.ReasonNone
	j.Message = "completed"
	d := domain.Decision{JobID: j.ID, State: domain.StateCompleted, Message: j.Message}
	s.decisions = append(s.decisions, d)
	s.logf("complete job %s; resources released", j.ID)
	return true
}

// FailNode simulates a node crash. The node's counter is zeroed by the cluster
// exactly once; displaced jobs are atomically requeued (or FAILED once their
// bounded retry budget is exhausted). Sibling tasks of the same job and, for
// gang groups, *every running member of the group* are evacuated together so a
// gang is never left partially running. Nothing is double-counted: resources
// on the crashed node were already zeroed by the cluster, healthy-node tasks
// are released through the normal path.
func (s *Scheduler) FailNode(nodeID string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1) Directly displaced jobs.
	direct := map[string]bool{}
	for _, pt := range s.placements[nodeID] {
		direct[pt.jobID] = true
	}
	// 2) Expand to whole running gangs: co-scheduling cannot survive with a
	// subset of members.
	evict := map[string]bool{}
	for id := range direct {
		evict[id] = true
		if g := s.jobs[id].GroupID; g != "" {
			for _, mid := range s.groupMembers[g] {
				if s.jobs[mid].State == domain.StateRunning {
					evict[mid] = true
				}
			}
		}
	}
	ids := make([]string, 0, len(evict))
	for id := range evict {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	s.logf("node failure: node=%s direct=[%s] evacuating=[%s]", nodeID,
		joinSorted(direct), strings.Join(ids, ","))

	// Zero the failed node counters first; releaseJobExcept skips it.
	s.cluster.FailNode(nodeID)

	for _, id := range ids {
		j := s.jobs[id]
		// Evacuated gang siblings may not sit on the failed node; release
		// whatever they hold on healthy nodes.
		s.releaseJobExcept(j, nodeID)
		s.removeFromIndexes(j.ID, nodeID)
		for _, t := range j.Tasks {
			t.NodeID = ""
		}
		j.RetriesLeft--
		if j.RetriesLeft >= 0 {
			j.State = domain.StatePending
			j.Reason = domain.ReasonNodeUnavailable
			if direct[id] {
				j.Message = fmt.Sprintf("requeued after node %s failure; retries left=%d", nodeID, j.RetriesLeft)
			} else {
				j.Message = fmt.Sprintf("gang evacuated after node %s failure; retries left=%d", nodeID, j.RetriesLeft)
			}
			s.pending.Enqueue(j, s.tick)
			s.logf("requeue job %s after failure of %s (retries left=%d)", j.ID, nodeID, j.RetriesLeft)
			s.decisions = append(s.decisions, domain.Decision{
				JobID: j.ID, State: domain.StatePending,
				Reason: domain.ReasonNodeUnavailable, Message: j.Message,
			})
		} else {
			j.State = domain.StateFailed
			j.Reason = domain.ReasonNodeUnavailable
			j.Message = fmt.Sprintf("exhausted retries after node %s failure", nodeID)
			s.logf("fail job %s: retry budget exhausted after node failure", j.ID)
			s.decisions = append(s.decisions, domain.Decision{
				JobID: j.ID, State: domain.StateFailed,
				Reason: domain.ReasonNodeUnavailable, Message: j.Message,
			})
		}
	}
	delete(s.placements, nodeID)
	return ids
}

// joinSorted renders a string set in stable sorted order for logging.
func joinSorted(set map[string]bool) string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}
	sort.Strings(out)
	return strings.Join(out, ",")
}

// RestoreNode brings a node back.
func (s *Scheduler) RestoreNode(nodeID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cluster.RestoreNode(nodeID)
	s.logf("node restored: %s", nodeID)
}

// Job returns a copy of one job.
func (s *Scheduler) Job(id string) (domain.Job, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return domain.Job{}, false
	}
	return j.Clone(), true
}

// Tick returns the current logical tick counter.
func (s *Scheduler) Tick() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tick
}

// Snapshot returns a consistent deep copy of all externally visible state.
func (s *Scheduler) Snapshot() domain.Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snap := domain.Snapshot{
		Tick:       s.tick,
		Nodes:      s.cluster.Snapshot(),
		OwnerUsage: s.quota.SnapshotUsage(),
		OwnerQuota: s.quota.SnapshotQuota(),
	}
	jobs := make([]domain.Job, 0, len(s.order))
	for _, id := range s.order {
		jobs = append(jobs, s.jobs[id].Clone())
	}
	snap.Jobs = jobs
	if len(s.decisions) > 0 {
		snap.Decisions = append([]domain.Decision(nil), s.decisions...)
	}
	return snap
}

// ---------- internal helpers (callers hold s.mu) ----------

func stateOf(j *domain.Job) domain.JobState {
	if j == nil {
		return domain.JobState(-1)
	}
	return j.State
}

func dash(v string) string {
	if v == "" {
		return "-"
	}
	return v
}

func (s *Scheduler) logf(format string, args ...any) {
	if s.log != nil {
		fmt.Fprintf(s.log, "[t=%d] %s\n", s.tick, fmt.Sprintf(format, args...))
	}
}

// releaseJob frees every resource held by j and clears placement indexes.
func (s *Scheduler) releaseJob(j *domain.Job) {
	for _, nodeID := range s.jobNodes[j.ID] {
		for _, pt := range s.placements[nodeID] {
			if pt.jobID == j.ID {
				s.cluster.Release(nodeID, pt.res)
			}
		}
	}
	s.quota.Release(j.Owner, j.TotalResources())
	s.removeFromIndexes(j.ID, "")
	for _, t := range j.Tasks {
		t.NodeID = ""
	}
}

// releaseJobExcept releases j's resources on every node other than failedNode
// (whose counters were already zeroed by the cluster on failure).
func (s *Scheduler) releaseJobExcept(j *domain.Job, failedNode string) {
	for _, nodeID := range s.jobNodes[j.ID] {
		if nodeID == failedNode {
			continue
		}
		for _, pt := range s.placements[nodeID] {
			if pt.jobID == j.ID {
				s.cluster.Release(nodeID, pt.res)
			}
		}
	}
	s.quota.Release(j.Owner, j.TotalResources())
}

// removeFromIndexes deletes a job from the reverse indices. If exceptNode is
// non-empty, entries on that node are dropped wholesale by the caller.
func (s *Scheduler) removeFromIndexes(jobID, exceptNode string) {
	for nodeID, pts := range s.placements {
		if nodeID == exceptNode {
			continue
		}
		kept := pts[:0]
		for _, pt := range pts {
			if pt.jobID != jobID {
				kept = append(kept, pt)
			}
		}
		s.placements[nodeID] = kept
	}
	delete(s.jobNodes, jobID)
}

func (s *Scheduler) rejectUnit(u unit, reason domain.RejectReason, msg string) domain.Decision {
	for _, j := range u.jobs {
		j.State = domain.StateRejected
		j.Reason = reason
		j.Message = msg
	}
	s.logf("judgment: REJECT reason=%s msg=%s", reason, msg)
	return domain.Decision{JobID: u.jobs[0].ID, State: domain.StateRejected, Reason: reason, Message: msg}
}

func (s *Scheduler) waitUnit(u unit, reason domain.RejectReason, msg string) domain.Decision {
	s.logf("judgment: WAIT reason=%s msg=%s", reason, msg)
	// Pending jobs keep their externally visible state; the decision still
	// records the reason so the scheduling basis stays explainable.
	return domain.Decision{JobID: u.jobs[0].ID, State: domain.StatePending, Reason: reason, Message: msg}
}

func (s *Scheduler) runningDecision(u unit, plan []plannedTask, msg string) domain.Decision {
	var pl []domain.TaskPlacement
	for _, p := range plan {
		pl = append(pl, domain.TaskPlacement{JobID: p.jobID, TaskID: p.taskID, NodeID: p.nodeID})
	}
	return domain.Decision{JobID: u.jobs[0].ID, State: domain.StateRunning, Message: msg, Placements: pl}
}
