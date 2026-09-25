// Package simulator drives a deterministic single-machine reproduction of a
// multi-node cluster: fixed-seed workload generation, scheduling passes,
// completions and node failures.
package simulator

import (
	"fmt"
	"math/rand"
	"sort"

	"github.com/lisama569/hpc-job-scheduler-go/internal/domain"
	"github.com/lisama569/hpc-job-scheduler-go/internal/scheduler"
)

// WorkloadSpec configures generated workloads. All random choices derive from
// one fixed-seed RNG, so two runs with the same seed are bit-for-bit identical.
type WorkloadSpec struct {
	Owners       []string
	SingleJobs   int
	GangGroups   int
	GangSizeMin  int
	GangSizeMax  int
	Arrays       int
	ArraySizeMin int
	ArraySizeMax int

	MaxCPUPerTask int64
	MaxMemPerTask int64
	MaxGPUPerTask int64

	// DependencyRate in [0,1] is the probability that a single job depends on
	// an earlier single job.
	DependencyRate float64
	// DependentWithChance adds one dependency to the job when rng<rate.
	MaxRetries int
}

// TimelineEvent is one scripted world event.
type TimelineEvent struct {
	Tick   int
	Kind   string // "complete" | "fail" | "restore"
	JobID  string // for complete
	NodeID string // for fail/restore
}

// Simulator runs a scripted world against a scheduler with a fixed RNG seed.
type Simulator struct {
	rng       *rand.Rand
	sched     *scheduler.Scheduler
	seed      int64
	owners    []string
	generated []*domain.Job
	events    []TimelineEvent
}

// New creates a Simulator.
func New(seed int64, s *scheduler.Scheduler) *Simulator {
	return &Simulator{rng: rand.New(rand.NewSource(seed)), sched: s, seed: seed}
}

func randInt(rng *rand.Rand, lo, hi int) int {
	if hi <= lo {
		return lo
	}
	return lo + rng.Intn(hi-lo+1)
}

func (sm *Simulator) owner() string {
	return sm.owners[sm.rng.Intn(len(sm.owners))]
}

// GenerateWorkload builds reproducible jobs from spec and submits them.
func (sm *Simulator) GenerateWorkload(spec WorkloadSpec) ([]*domain.Job, error) {
	sm.owners = spec.Owners
	if len(sm.owners) == 0 {
		sm.owners = []string{"default"}
	}
	var jobs []*domain.Job
	var singles []*domain.Job
	retries := spec.MaxRetries
	if retries == 0 {
		retries = 3
	}

	res := func() domain.Resource {
		return domain.Resource{
			CPU:    int64(randInt(sm.rng, 1, int(spec.MaxCPUPerTask))),
			Memory: int64(randInt(sm.rng, 1, int(spec.MaxMemPerTask))),
			GPU:    int64(randInt(sm.rng, 0, int(spec.MaxGPUPerTask))),
		}
	}

	// Singles (possibly with dependencies on earlier singles).
	for i := 0; i < spec.SingleJobs; i++ {
		id := fmt.Sprintf("J%03d", i+1)
		j := &domain.Job{
			ID:          id,
			Owner:       sm.owner(),
			Priority:    domain.Priority(randInt(sm.rng, 1, 9)),
			Tasks:       []*domain.Task{{ID: id + ":0", Resources: res()}},
			RetriesLeft: retries,
		}
		if len(singles) > 0 && sm.rng.Float64() < spec.DependencyRate {
			dep := singles[sm.rng.Intn(len(singles))]
			j.DependsOn = []string{dep.ID}
		}
		jobs = append(jobs, j)
		singles = append(singles, j)
	}

	// Gang groups: each member owns exactly one task; all members place or
	// wait together.
	for g := 0; g < spec.GangGroups; g++ {
		size := randInt(sm.rng, spec.GangSizeMin, spec.GangSizeMax)
		groupID := fmt.Sprintf("G%02d", g+1)
		for m := 0; m < size; m++ {
			id := fmt.Sprintf("%s-M%02d", groupID, m+1)
			j := &domain.Job{
				ID:          id,
				Owner:       sm.owner(),
				Priority:    domain.Priority(randInt(sm.rng, 1, 9)),
				GroupID:     groupID,
				Tasks:       []*domain.Task{{ID: id + ":0", Resources: res()}},
				RetriesLeft: retries,
			}
			jobs = append(jobs, j)
		}
	}

	// Job arrays: independent elements; quotas roll up per owner.
	for a := 0; a < spec.Arrays; a++ {
		size := randInt(sm.rng, spec.ArraySizeMin, spec.ArraySizeMax)
		arrayID := fmt.Sprintf("A%02d", a+1)
		owner := sm.owner()
		for e := 0; e < size; e++ {
			id := fmt.Sprintf("%s-E%03d", arrayID, e+1)
			j := &domain.Job{
				ID:          id,
				Owner:       owner,
				Priority:    domain.Priority(randInt(sm.rng, 1, 9)),
				ArrayID:     arrayID,
				Index:       e,
				Tasks:       []*domain.Task{{ID: id + ":0", Resources: res()}},
				RetriesLeft: retries,
			}
			jobs = append(jobs, j)
		}
	}

	// Stable submission order for reproducible snapshots and logs.
	sort.SliceStable(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })
	sm.generated = append(sm.generated, jobs...)
	if err := sm.sched.Submit(jobs...); err != nil {
		return nil, err
	}
	return append([]*domain.Job(nil), jobs...), nil
}

// ScriptEvent adds a timeline event.
func (sm *Simulator) ScriptEvent(e TimelineEvent) { sm.events = append(sm.events, e) }

// Run executes scripted events in (tick, kind-stable) order, running one
// scheduling pass on every tick up to maxTick.
func (sm *Simulator) Run(maxTick int) {
	byTick := map[int][]TimelineEvent{}
	var ticks []int
	for _, e := range sm.events {
		if _, ok := byTick[e.Tick]; !ok {
			ticks = append(ticks, e.Tick)
		}
		byTick[e.Tick] = append(byTick[e.Tick], e)
	}
	for _, t := range ticks {
		sort.SliceStable(byTick[t], func(i, j int) bool { return byTick[t][i].Kind < byTick[t][j].Kind })
	}
	for t := 1; t <= maxTick; t++ {
		sm.sched.RunPass()
		for _, e := range byTick[t] {
			switch e.Kind {
			case "complete":
				sm.sched.CompleteJob(e.JobID)
			case "fail":
				sm.sched.FailNode(e.NodeID)
			case "restore":
				sm.sched.RestoreNode(e.NodeID)
			}
		}
	}
}

// Scheduler exposes the driven scheduler so callers can take snapshots,
// trigger failures directly or inspect reconciliation state.
func (sm *Simulator) Scheduler() *scheduler.Scheduler { return sm.sched }

// Jobs returns the generated jobs.
func (sm *Simulator) Jobs() []*domain.Job { return sm.generated }

// Seed reports the RNG seed used.
func (sm *Simulator) Seed() int64 { return sm.seed }
