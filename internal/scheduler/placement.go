package scheduler

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/lisama569/hpc-job-scheduler-go/internal/domain"
)

// Sentinel causes for a failed *trial* placement. They are deliberately
// distinct from domain.Reason* because "no space now" may still be repaired by
// preemption while quota saturation is a different wait category.
var (
	errNoSpace  = errors.New("no node currently has enough free resources")
	errQuotaNow = errors.New("owner quota currently saturated")
)

// plannedTask is one task-to-node assignment produced by the trial placer.
type plannedTask struct {
	jobID  string
	taskID string
	nodeID string
	res    domain.Resource
}

func planSummary(plan []plannedTask) string {
	parts := make([]string, 0, len(plan))
	for _, p := range plan {
		parts = append(parts, fmt.Sprintf("%s/%s->%s", p.jobID, p.taskID, p.nodeID))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// simulate builds the fleet view a trial placement sees: static snapshot plus
// resources assumed freed by chosen preemption victims.
type simulate struct {
	capacity map[string]domain.Resource
	free     map[string]domain.Resource
}

func (s *Scheduler) buildSimulate(nodeFreed map[string]domain.Resource) *simulate {
	sim := &simulate{capacity: map[string]domain.Resource{}, free: map[string]domain.Resource{}}
	for _, n := range s.cluster.Snapshot() {
		// Down nodes never participate in placement: their counters were
		// zeroed on failure, which must not read as "fully free".
		if n.State != domain.NodeUp {
			continue
		}
		sim.capacity[n.ID] = n.Capacity
		free := n.Free().Add(nodeFreed[n.ID])
		sim.free[n.ID] = free
	}
	return sim
}

// planUnit tries to place every task of the unit without mutating live state.
// nodeFreed/quotaFreed simulate resources released by preempted victims.
func (s *Scheduler) planUnit(u unit, nodeFreed map[string]domain.Resource, quotaFreed map[string]domain.Resource) ([]plannedTask, error) {
	// Aggregate the unit's demand per owner for the transient quota check.
	needByOwner := map[string]domain.Resource{}
	for _, j := range u.jobs {
		needByOwner[j.Owner] = needByOwner[j.Owner].Add(j.TotalResources())
	}
	quotas := s.quota.SnapshotQuota()
	for owner, need := range needByOwner {
		if q, ok := quotas[owner]; ok {
			effective := s.quota.Usage(owner).Add(need).Sub(quotaFreed[owner])
			if !q.Fits(effective) {
				return nil, errQuotaNow
			}
		}
	}

	sim := s.buildSimulate(nodeFreed)
	reserved := map[string]domain.Resource{} // in-unit reservations per node
	plan := make([]plannedTask, 0)

	// Deterministic task order across the unit: job ID, then task index.
	type item struct {
		job  *domain.Job
		task *domain.Task
	}
	var items []item
	for _, j := range u.jobs {
		for _, t := range j.Tasks {
			items = append(items, item{j, t})
		}
	}
	sort.Slice(items, func(a, b int) bool {
		if items[a].job.ID != items[b].job.ID {
			return items[a].job.ID < items[b].job.ID
		}
		return items[a].task.ID < items[b].task.ID
	})

	for _, it := range items {
		req := it.task.Resources
		type cand struct {
			id      string
			left    domain.Resource
			leftKey int64
		}
		var cands []cand
		for id := range sim.capacity {
			avail := sim.free[id].Sub(reserved[id])
			if avail.Fits(req) {
				left := avail.Sub(req)
				cands = append(cands, cand{id: id, left: left, leftKey: left.CPU + left.Memory + left.GPU})
			}
		}
		if len(cands) == 0 {
			return nil, errNoSpace
		}
		// Best fit (smallest leftover) with a stable tie-break by node ID.
		sort.Slice(cands, func(a, b int) bool {
			if cands[a].leftKey != cands[b].leftKey {
				return cands[a].leftKey < cands[b].leftKey
			}
			return cands[a].id < cands[b].id
		})
		chosen := cands[0].id
		reserved[chosen] = reserved[chosen].Add(req)
		plan = append(plan, plannedTask{
			jobID:  it.job.ID,
			taskID: it.task.ID,
			nodeID: chosen,
			res:    req,
		})
	}
	return plan, nil
}

// tryPreempt greedily accumulates strictly lower-priority *victim groups* in a
// fixed order until the unit becomes placeable. A victim group is either a
// single non-gang job or every running member of one gang, so a co-scheduled
// group is never preempted piece by piece. It performs no mutation.
func (s *Scheduler) tryPreempt(u unit, minPrio domain.Priority) ([]plannedTask, []string, error) {
	// Enumerate atomic victim groups: visit running jobs in stable order, and
	// when a gang member is first seen, fold all its running siblings in.
	seenGang := map[string]bool{}
	var groups [][]*domain.Job
	for _, id := range s.order {
		j := s.jobs[id]
		if j.State != domain.StateRunning {
			continue
		}
		if j.GroupID != "" {
			if seenGang[j.GroupID] {
				continue
			}
			seenGang[j.GroupID] = true
			var members []*domain.Job
			groupMax := domain.Priority(-1 << 30)
			for _, mid := range s.groupMembers[j.GroupID] {
				m := s.jobs[mid]
				if m.State == domain.StateRunning {
					members = append(members, m)
					if m.Priority > groupMax {
						groupMax = m.Priority
					}
				}
			}
			// A gang is preemptable only when EVERY member is strictly lower
			// priority than the requesting unit; the highest member priority
			// gates the whole group.
			if len(members) > 0 && groupMax < minPrio {
				groups = append(groups, members)
			}
			continue
		}
		if j.Priority < minPrio {
			groups = append(groups, []*domain.Job{j})
		}
	}
	if len(groups) == 0 {
		return nil, nil, errNoSpace
	}

	// Deterministic group ordering: lowest priority first; ties broken by the
	// group's total held resources (smallest disturbance first), then by the
	// lexicographically smallest member ID.
	sort.Slice(groups, func(a, b int) bool {
		sa, sb := groupScore(groups[a]), groupScore(groups[b])
		if sa.priority != sb.priority {
			return sa.priority < sb.priority
		}
		if sa.resKey != sb.resKey {
			return sa.resKey < sb.resKey
		}
		return sa.firstID < sb.firstID
	})

	nodeFreed := map[string]domain.Resource{}
	quotaFreed := map[string]domain.Resource{}
	var victims []string

	for _, g := range groups {
		for _, v := range g {
			quotaFreed[v.Owner] = quotaFreed[v.Owner].Add(v.TotalResources())
			for _, nodeID := range s.jobNodes[v.ID] {
				for _, pt := range s.placements[nodeID] {
					if pt.jobID == v.ID {
						nodeFreed[nodeID] = nodeFreed[nodeID].Add(pt.res)
					}
				}
			}
			victims = append(victims, v.ID)
		}
		if plan, err := s.planUnit(u, nodeFreed, quotaFreed); err == nil {
			return plan, victims, nil
		}
	}
	return nil, nil, errNoSpace
}

// groupScoreKey orders preemption victim groups: highest member priority
// (the group's effective priority) lowest first, then smallest total held
// resources, then smallest member ID.
type groupScoreKey struct {
	priority domain.Priority
	resKey   int64
	firstID  string
}

func groupScore(g []*domain.Job) groupScoreKey {
	k := groupScoreKey{priority: domain.Priority(-1 << 30), firstID: "~~~"}
	var total domain.Resource
	for _, j := range g {
		if j.Priority > k.priority {
			k.priority = j.Priority
		}
		if j.ID < k.firstID {
			k.firstID = j.ID
		}
		total = total.Add(j.TotalResources())
	}
	k.resKey = total.CPU + total.Memory + total.GPU
	return k
}

// applyPreemption releases chosen victims. Each victim spends one entry of the
// same bounded retry budget used by node-failure recovery; at zero it becomes
// FAILED instead of being requeued.
func (s *Scheduler) applyPreemption(victimIDs []string) {
	for _, id := range victimIDs {
		j := s.jobs[id]
		s.releaseJob(j)
		j.RetriesLeft--
		if j.RetriesLeft >= 0 {
			j.State = domain.StatePending
			j.Reason = domain.ReasonPreempted
			j.Message = fmt.Sprintf("preempted by higher-priority job; retries left=%d", j.RetriesLeft)
			s.pending.Enqueue(j, s.tick)
			s.logf("preempt job %s (prio=%d) -> requeue, retries left=%d", j.ID, j.Priority, j.RetriesLeft)
			s.decisions = append(s.decisions, domain.Decision{
				JobID: id, State: domain.StatePending,
				Reason: domain.ReasonPreempted, Message: j.Message,
			})
		} else {
			j.State = domain.StateFailed
			j.Reason = domain.ReasonPreempted
			j.Message = "preempted and bounded retry budget exhausted"
			s.logf("preempt job %s -> FAILED (retry budget exhausted)", j.ID)
			s.decisions = append(s.decisions, domain.Decision{
				JobID: id, State: domain.StateFailed,
				Reason: domain.ReasonPreempted, Message: j.Message,
			})
		}
	}
}

// commitUnit turns a validated plan into live allocations. It runs only after
// a successful trial placement, so every Allocate succeeds; quotas and the
// reverse indexes are updated once per job/task.
func (s *Scheduler) commitUnit(u unit, plan []plannedTask) {
	planByJob := map[string][]plannedTask{}
	for _, p := range plan {
		planByJob[p.jobID] = append(planByJob[p.jobID], p)
	}
	for _, j := range u.jobs {
		pt := planByJob[j.ID]
		// Remove from the pending queue atomically for the whole unit.
		s.pending.Remove(j.ID)
		for _, p := range pt {
			task := findTask(j, p.taskID)
			if task == nil || !s.cluster.Allocate(p.nodeID, p.res) {
				// Cannot happen after a validated trial; guard invariants.
				panic(fmt.Sprintf("scheduler: validated plan failed for %s/%s on %s", j.ID, p.taskID, p.nodeID))
			}
			task.NodeID = p.nodeID
			s.placements[p.nodeID] = append(s.placements[p.nodeID], placedTask{
				jobID: j.ID, taskID: p.taskID, res: p.res,
			})
			s.jobNodes[j.ID] = append(s.jobNodes[j.ID], p.nodeID)
		}
		s.quota.Allocate(j.Owner, j.TotalResources())
		j.State = domain.StateRunning
		j.Reason = domain.ReasonNone
		j.Message = "running"
	}
}

func findTask(j *domain.Job, taskID string) *domain.Task {
	for _, t := range j.Tasks {
		if t.ID == taskID {
			return t
		}
	}
	return nil
}
