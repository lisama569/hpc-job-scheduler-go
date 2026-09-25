// Command hpc-sim runs a deterministic, single-machine reproduction of the
// simulated HPC scheduler and prints its final, explainable outcome.
//
// Usage:
//
//	go run ./cmd/hpc-sim [-seed 20260925] [-ticks 10] [-trace]
//
// With a fixed seed the program is bit-for-bit reproducible: same generated
// workload, same preemption/victim decisions, same final per-job conclusion.
package main

import (
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/lisama569/hpc-job-scheduler-go/internal/domain"
	"github.com/lisama569/hpc-job-scheduler-go/internal/scheduler"
	"github.com/lisama569/hpc-job-scheduler-go/internal/simulator"
)

func main() {
	seed := flag.Int64("seed", 20260925, "fixed RNG seed for reproducible workloads")
	ticks := flag.Int("ticks", 10, "number of logical ticks to simulate")
	trace := flag.Bool("trace", true, "print job inputs and scheduling judgments")
	flag.Parse()

	var logFile *os.File
	if *trace {
		logFile = os.Stdout
	}
	s := scheduler.New(scheduler.Config{MaxRequeueRetries: 2}, logFile)

	// Four identical nodes.
	for _, id := range []string{"n1", "n2", "n3", "n4"} {
		if err := s.AddNode(domain.Node{ID: id, Capacity: domain.Resource{CPU: 12, Memory: 16, GPU: 2}}); err != nil {
			fail(err)
		}
	}
	// Per-owner entitlements, accounted independently.
	s.SetQuota("alpha", domain.Resource{CPU: 40, Memory: 80, GPU: 6})
	s.SetQuota("beta", domain.Resource{CPU: 40, Memory: 80, GPU: 6})

	sm := simulator.New(*seed, s)
	jobs, err := sm.GenerateWorkload(simulator.WorkloadSpec{
		Owners:         []string{"alpha", "beta"},
		SingleJobs:     14,
		GangGroups:     3,
		GangSizeMin:    2,
		GangSizeMax:    4,
		Arrays:         2,
		ArraySizeMin:   3,
		ArraySizeMax:   5,
		MaxCPUPerTask:  6,
		MaxMemPerTask:  8,
		MaxGPUPerTask:  1,
		DependencyRate: 0.2,
		MaxRetries:     2,
	})
	if err != nil {
		fail(err)
	}

	// Scripted world: a crash mid-run and a later restore.
	sm.ScriptEvent(simulator.TimelineEvent{Tick: 3, Kind: "fail", NodeID: "n2"})
	sm.ScriptEvent(simulator.TimelineEvent{Tick: 6, Kind: "restore", NodeID: "n2"})
	sm.Run(*ticks)

	snap := s.Snapshot()
	printReport(*seed, jobs, snap)
}

func printReport(seed int64, jobs []*domain.Job, snap domain.Snapshot) {
	fmt.Printf("\n================ FINAL REPORT (seed=%d tick=%d jobs=%d) ================\n", seed, snap.Tick, len(jobs))

	fmt.Println("-- nodes --")
	for _, n := range snap.Nodes {
		fmt.Printf("node %s state=%s cap=%s used=%s free=%s\n", n.ID, n.State, n.Capacity, n.Used, n.Free())
	}

	counts := map[domain.JobState]int{}
	for _, j := range snap.Jobs {
		counts[j.State]++
	}
	fmt.Printf("-- states -- completed=%d running=%d pending=%d rejected=%d failed=%d\n",
		counts[domain.StateCompleted], counts[domain.StateRunning], counts[domain.StatePending],
		counts[domain.StateRejected], counts[domain.StateFailed])

	fmt.Println("-- owner quota usage --")
	owners := make([]string, 0, len(snap.OwnerUsage))
	for o := range snap.OwnerUsage {
		owners = append(owners, o)
	}
	sort.Strings(owners)
	for _, o := range owners {
		fmt.Printf("owner %s usage=%s quota=%s\n", o, snap.OwnerUsage[o], snap.OwnerQuota[o])
	}

	// Explainable per-job conclusions in stable order.
	fmt.Println("-- jobs (sorted by id) --")
	ids := make([]string, 0, len(snap.Jobs))
	byID := map[string]domain.Job{}
	for _, j := range snap.Jobs {
		ids = append(ids, j.ID)
		byID[j.ID] = j
	}
	sort.Strings(ids)
	for _, id := range ids {
		j := byID[id]
		place := "-"
		if len(j.Tasks) > 0 && j.Tasks[0].NodeID != "" {
			place = j.Tasks[0].NodeID
		}
		group, array := "-", "-"
		if j.GroupID != "" {
			group = j.GroupID
		}
		if j.ArrayID != "" {
			array = fmt.Sprintf("%s#%d", j.ArrayID, j.Index)
		}
		fmt.Printf("job %-8s owner=%-5s prio=%d group=%-4s array=%-6s state=%-9s reason=%-22s node=%s %s\n",
			j.ID, j.Owner, j.Priority, group, array, j.State, j.Reason, place, j.Message)
	}

	// Final reconciliation: node usage == running demand == ledger usage.
	var nodeUsed, runningDemand, ledgerTotal domain.Resource
	for _, n := range snap.Nodes {
		nodeUsed = nodeUsed.Add(n.Used)
	}
	for _, j := range snap.Jobs {
		if j.State == domain.StateRunning {
			runningDemand = runningDemand.Add(j.TotalResources())
		}
	}
	for _, u := range snap.OwnerUsage {
		ledgerTotal = ledgerTotal.Add(u)
	}
	fmt.Println("-- reconciliation --")
	fmt.Printf("nodeUsed=%s runningDemand=%s ledgerTotal=%s\n", nodeUsed, runningDemand, ledgerTotal)
	if nodeUsed != runningDemand || nodeUsed != ledgerTotal {
		fail(fmt.Errorf("invariant violated: counters diverged"))
	}
	fmt.Println("OK: node counters, running demand and per-owner quota ledgers reconcile exactly.")
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, "hpc-sim:", err)
	os.Exit(1)
}
