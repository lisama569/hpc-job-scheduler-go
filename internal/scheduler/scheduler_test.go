package scheduler

import (
	"bytes"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/lisama569/hpc-job-scheduler-go/internal/domain"
)

// newTestScheduler wires a scheduler with the explainable log captured so the
// test can print job inputs and the scheduling basis on failure.
func newTestScheduler(t *testing.T, nodes ...domain.Node) (*Scheduler, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	s := New(DefaultConfig(), &buf)
	for _, n := range nodes {
		if err := s.AddNode(n); err != nil {
			t.Fatalf("add node: %v", err)
		}
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("scheduler trace:\n%s", buf.String())
		}
	})
	return s, &buf
}

func single(id string, prio int, res domain.Resource) *domain.Job {
	return &domain.Job{ID: id, Owner: "o-" + id, Priority: domain.Priority(prio),
		Tasks: []*domain.Task{{ID: id + ":0", Resources: res}}}
}

func mustJob(s *Scheduler, id string) domain.Job {
	j, ok := s.Job(id)
	if !ok {
		panic("missing job " + id)
	}
	return j
}

// TestRejectExceedsNodeCapacity: a task larger than every node is rejected
// with the permanent reason and distinguishable from transient shortage.
func TestRejectExceedsNodeCapacity(t *testing.T) {
	s, _ := newTestScheduler(t,
		domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 8, Memory: 16}},
		domain.Node{ID: "n2", Capacity: domain.Resource{CPU: 4, Memory: 8}},
	)
	oversized := single("oversized", 5, domain.Resource{CPU: 16})
	tight := single("tight", 5, domain.Resource{CPU: 8, Memory: 16})
	if err := s.Submit(oversized, tight); err != nil {
		t.Fatal(err)
	}
	dec := s.RunPass()
	t.Logf("input: oversized cpu=16 > max node cpu=8; tight cpu=8/mem=16 fills n1. decisions=%d", len(dec))

	if j := mustJob(s, "oversized"); j.State != domain.StateRejected || j.Reason != domain.ReasonExceedsNodeCapacity {
		t.Fatalf("oversized: state=%v reason=%v", j.State, j.Reason)
	}
	if j := mustJob(s, "tight"); j.State != domain.StateRunning {
		t.Fatalf("tight should run by filling n1 exactly: %v (%s)", j.State, j.Message)
	}
}

// TestInsufficientResourceIsTransient: when capacity exists in principle but
// nothing is free, the job stays PENDING with INSUFFICIENT_RESOURCE and runs
// after a completion.
func TestInsufficientResourceIsTransient(t *testing.T) {
	s, _ := newTestScheduler(t,
		domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 8}},
	)
	holder := single("holder", 5, domain.Resource{CPU: 8})
	waiter := single("waiter", 5, domain.Resource{CPU: 4})
	if err := s.Submit(holder, waiter); err != nil {
		t.Fatal(err)
	}
	s.RunPass()
	if j := mustJob(s, "waiter"); j.State != domain.StatePending {
		t.Fatalf("waiter pre: %v", j.State)
	}
	// Latest decision for waiter must carry the transient reason.
	last := lastDecision(s, "waiter")
	if last.Reason != domain.ReasonInsufficientResource {
		t.Fatalf("waiter reason=%s message=%s", last.Reason, last.Message)
	}
	if !s.CompleteJob("holder") {
		t.Fatal("complete holder")
	}
	s.RunPass()
	if j := mustJob(s, "waiter"); j.State != domain.StateRunning {
		t.Fatalf("waiter post completion: %v", j.State)
	}
}

// TestDependencyReasons distinguishes "not met yet" from "dependency failed".
func TestDependencyReasons(t *testing.T) {
	s, _ := newTestScheduler(t, domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 32}})
	first := single("first", 5, domain.Resource{CPU: 1})
	second := single("second", 5, domain.Resource{CPU: 1})
	second.DependsOn = []string{"first"}
	third := single("third", 5, domain.Resource{CPU: 1})
	third.DependsOn = []string{"ghost"} // unknown dependency -> failed branch
	if err := s.Submit(first, second, third); err != nil {
		t.Fatal(err)
	}
	s.RunPass()
	if j := mustJob(s, "second"); j.State != domain.StatePending || j.Reason != domain.ReasonNone {
		t.Fatalf("second while dep pending: %v/%v", j.State, j.Reason)
	}
	if d := lastDecision(s, "second"); d.Reason != domain.ReasonDependencyNotMet {
		t.Fatalf("second reason=%s msg=%s", d.Reason, d.Message)
	}
	if j := mustJob(s, "third"); j.State != domain.StateRejected || j.Reason != domain.ReasonDependencyFailed {
		t.Fatalf("third: state=%v reason=%v", j.State, j.Reason)
	}
	s.CompleteJob("first")
	s.RunPass()
	if j := mustJob(s, "second"); j.State != domain.StateRunning {
		t.Fatalf("second after dep completed: %v (%s)", j.State, j.Message)
	}
}

// TestQuotaHardCeilingAndTransient: a request above the quota ceiling is a
// reject; a request within ceiling but saturated stays pending.
func TestQuotaHardCeilingAndTransient(t *testing.T) {
	s, _ := newTestScheduler(t, domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 128}})
	s.SetQuota("o-a", domain.Resource{CPU: 10})
	over := &domain.Job{ID: "over", Owner: "o-a", Priority: 5,
		Tasks: []*domain.Task{{ID: "over:0", Resources: domain.Resource{CPU: 11}}}}
	saturator := &domain.Job{ID: "sat", Owner: "o-a", Priority: 9,
		Tasks: []*domain.Task{{ID: "sat:0", Resources: domain.Resource{CPU: 8}}}}
	blocked := &domain.Job{ID: "blocked", Owner: "o-a", Priority: 5,
		Tasks: []*domain.Task{{ID: "blocked:0", Resources: domain.Resource{CPU: 3}}}}
	if err := s.Submit(over, saturator, blocked); err != nil {
		t.Fatal(err)
	}
	s.RunPass()
	if j := mustJob(s, "over"); j.State != domain.StateRejected || j.Reason != domain.ReasonQuotaExceeded {
		t.Fatalf("over: %v/%v", j.State, j.Reason)
	}
	if j := mustJob(s, "sat"); j.State != domain.StateRunning {
		t.Fatalf("sat: %v", j.State)
	}
	if j := mustJob(s, "blocked"); j.State != domain.StatePending {
		t.Fatalf("blocked: %v", j.State)
	}
	if d := lastDecision(s, "blocked"); d.Reason != domain.ReasonQuotaExceeded {
		t.Fatalf("blocked reason=%s msg=%s", d.Reason, d.Message)
	}
	s.CompleteJob("sat")
	s.RunPass()
	if j := mustJob(s, "blocked"); j.State != domain.StateRunning {
		t.Fatalf("blocked after quota freed: %v (%s)", j.State, j.Message)
	}
}

// TestGangAllOrNothingAndNoResidue: a gang that fits only as a whole runs all
// members; a gang that cannot place atomically leaves all members pending and
// zero residual reservations.
func TestGangAllOrNothingAndNoResidue(t *testing.T) {
	s, _ := newTestScheduler(t,
		domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 6}},
		domain.Node{ID: "n2", Capacity: domain.Resource{CPU: 6}},
	)
	gang := func(group string, ids ...string) []*domain.Job {
		var js []*domain.Job
		for _, id := range ids {
			js = append(js, &domain.Job{ID: id, Owner: "gang", Priority: 5, GroupID: group,
				Tasks: []*domain.Task{{ID: id + ":0", Resources: domain.Resource{CPU: 4}}}})
		}
		return js
	}
	// g1: three tasks needing 4 cpu each, fleet has only 12 total but two nodes
	// -> still placeable (n1,n2 only hold two tasks). g1 has 3 members -> fail.
	jobs := gang("g1", "g1a", "g1b", "g1c")
	if err := s.Submit(jobs...); err != nil {
		t.Fatal(err)
	}
	s.RunPass()
	for _, id := range []string{"g1a", "g1b", "g1c"} {
		if j := mustJob(s, id); j.State != domain.StatePending {
			t.Fatalf("%s: expected pending, got %v", id, j.State)
		}
	}
	if d := lastDecision(s, "g1a"); d.Reason != domain.ReasonGroupCoScheduling {
		t.Fatalf("g1 decision reason=%s msg=%s", d.Reason, d.Message)
	}
	// No residue: both nodes must be fully free afterwards.
	for _, n := range s.cluster.Snapshot() {
		if n.Used.Any() {
			t.Fatalf("node %s carries residue %v after failed gang", n.ID, n.Used)
		}
	}

	// A 2-member gang on the same fleet must place atomically across n1,n2.
	jobs2 := gang("g2", "g2a", "g2b")
	if err := s.Submit(jobs2...); err != nil {
		t.Fatal(err)
	}
	s.RunPass()
	for _, id := range []string{"g2a", "g2b"} {
		if j := mustJob(s, id); j.State != domain.StateRunning {
			t.Fatalf("%s: expected running, got %v (%s)", id, j.State, j.Message)
		}
	}
}

// TestPreemptionRules: higher priority preempts strictly lower priority,
// deterministic victims, bounded retry -> FAILED after the budget is spent.
func TestPreemptionRules(t *testing.T) {
	s, _ := newTestScheduler(t, domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 8}})
	low := single("low", 2, domain.Resource{CPU: 8})
	if err := s.Submit(low); err != nil {
		t.Fatal(err)
	}
	s.RunPass()

	hi := single("hi", 9, domain.Resource{CPU: 8})
	if err := s.Submit(hi); err != nil {
		t.Fatal(err)
	}
	s.RunPass()
	if j := mustJob(s, "hi"); j.State != domain.StateRunning {
		t.Fatalf("hi: %v", j.State)
	}
	if j := mustJob(s, "low"); j.State != domain.StatePending || j.Reason != domain.ReasonPreempted {
		t.Fatalf("low: %v/%v", j.State, j.Reason)
	}
	if j := mustJob(s, "low"); j.RetriesLeft != 2 {
		t.Fatalf("low retries: %d", j.RetriesLeft)
	}

	// Spend the bounded retry budget: each cycle lets "low" run again, then a
	// fresh unique high-priority job preempts it. It started with retries=3
	// (first preemption ->2, second ->1, third ->0 and FAILED).
	completeNext := "hi" // the first preemptor was "hi"
	for cycle := 1; cycle <= 3; cycle++ {
		s.CompleteJob(completeNext)
		s.RunPass() // low runs again
		if j := mustJob(s, "low"); j.State != domain.StateRunning {
			t.Fatalf("cycle %d: low not re-running: %v (%s)", cycle, j.State, j.Message)
		}
		bullyID := fmt.Sprintf("bully%d", cycle)
		bully := single(bullyID, 9, domain.Resource{CPU: 8})
		if err := s.Submit(bully); err != nil {
			t.Fatal(err)
		}
		s.RunPass()
		completeNext = bullyID
	}
	j := mustJob(s, "low")
	if j.State != domain.StateFailed || j.Reason != domain.ReasonPreempted {
		t.Fatalf("low after bounded retries: state=%v reason=%v msg=%s", j.State, j.Reason, j.Message)
	}
}

// TestNodeFailureNoDoubleCount: a failure zeroes node counters, requeues the
// job once and lets it run again on a healthy node; ledger totals reconcile.
func TestNodeFailureNoDoubleCount(t *testing.T) {
	s, _ := newTestScheduler(t,
		domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 8}},
		domain.Node{ID: "n2", Capacity: domain.Resource{CPU: 8}},
	)
	job := &domain.Job{ID: "fj", Owner: "o-fj", Priority: 5,
		Tasks: []*domain.Task{{ID: "fj:0", Resources: domain.Resource{CPU: 8}}}}
	if err := s.Submit(job); err != nil {
		t.Fatal(err)
	}
	s.RunPass()
	before := mustJob(s, "fj")
	if before.State != domain.StateRunning {
		t.Fatalf("fj before failure: %v", before.State)
	}
	failedNode := before.Tasks[0].NodeID
	if failedNode == "" {
		t.Fatal("no placement recorded")
	}

	affected := s.FailNode(failedNode)
	if len(affected) != 1 || affected[0] != "fj" {
		t.Fatalf("affected=%v", affected)
	}
	// The failed node must show zero usage, and no other node may count fj.
	for _, n := range s.cluster.Snapshot() {
		if n.Used.Any() {
			t.Fatalf("node %s used=%v after failure (double count)", n.ID, n.Used)
		}
	}
	if got := s.quota.Usage("o-fj"); got.Any() {
		t.Fatalf("owner usage not released after failure: %v", got)
	}
	if j := mustJob(s, "fj"); j.State != domain.StatePending || j.Reason != domain.ReasonNodeUnavailable {
		t.Fatalf("fj after failure: %v/%v", j.State, j.Reason)
	}

	// Restore nothing; the healthy node still has capacity, fj must land there.
	s.RunPass()
	after := mustJob(s, "fj")
	if after.State != domain.StateRunning {
		t.Fatalf("fj rerun: %v (%s)", after.State, after.Message)
	}
	if after.Tasks[0].NodeID == failedNode {
		t.Fatal("fj was placed back on the still-down node")
	}
	if got := s.quota.Usage("o-fj"); got.CPU != 8 {
		t.Fatalf("owner usage after rerun: %v", got)
	}
}

// TestConcurrentConsistency hammers the scheduler from many goroutines and
// asserts that counters reconcile after every settle point.
func TestConcurrentConsistency(t *testing.T) {
	s, _ := newTestScheduler(t,
		domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 16}},
		domain.Node{ID: "n2", Capacity: domain.Resource{CPU: 16}},
	)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < 5; k++ {
				id := fmt.Sprintf("c%d-%d", g, k)
				_ = s.Submit(&domain.Job{ID: id, Owner: "o", Priority: domain.Priority((g + k) % 5),
					Tasks: []*domain.Task{{ID: id + ":0", Resources: domain.Resource{CPU: 2}}}})
				s.RunPass()
				_ = s.Snapshot()
			}
		}(g)
	}
	wg.Wait()
	// Reconcile: sum node usage == quota usage == sum of running job demand.
	var nodeUsed, runningDemand domain.Resource
	for _, n := range s.cluster.Snapshot() {
		nodeUsed = nodeUsed.Add(n.Used)
	}
	for _, j := range s.Snapshot().Jobs {
		if j.State == domain.StateRunning {
			runningDemand = runningDemand.Add(j.TotalResources())
		}
	}
	if nodeUsed != runningDemand {
		t.Fatalf("node used %v != running demand %v", nodeUsed, runningDemand)
	}
	ledgerTotal := s.quota.Usage("o")
	if ledgerTotal != nodeUsed {
		t.Fatalf("ledger %v != node used %v", ledgerTotal, nodeUsed)
	}
}

// lastDecision scans recorded decisions for the most recent one for jobID.
func lastDecision(s *Scheduler, jobID string) domain.Decision {
	var found domain.Decision
	for _, d := range s.decisions {
		if d.JobID == jobID {
			found = d
		}
	}
	return found
}

// sanity: ensure trace contains input logging.
func TestTraceMentionsInputs(t *testing.T) {
	s, buf := newTestScheduler(t, domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 4}})
	if err := s.Submit(single("tracejob", 1, domain.Resource{CPU: 1})); err != nil {
		t.Fatal(err)
	}
	s.RunPass()
	if !strings.Contains(buf.String(), "submit job tracejob") {
		t.Fatalf("trace missing job input:\n%s", buf.String())
	}
	if !strings.Contains(buf.String(), "judgment:") {
		t.Fatalf("trace missing scheduling judgment:\n%s", buf.String())
	}
}
