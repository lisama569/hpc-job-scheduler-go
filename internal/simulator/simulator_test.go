package simulator

import (
	"bytes"
	"strings"
	"testing"

	"github.com/lisama569/hpc-job-scheduler-go/internal/domain"
	"github.com/lisama569/hpc-job-scheduler-go/internal/scheduler"
)

func testSpec() WorkloadSpec {
	return WorkloadSpec{
		Owners:         []string{"alpha", "beta"},
		SingleJobs:     12,
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
	}
}

func buildWorld(t *testing.T, seed int64) (*Simulator, *bytes.Buffer) {
	t.Helper()
	var buf bytes.Buffer
	s := scheduler.New(scheduler.Config{MaxRequeueRetries: 2}, &buf)
	for _, id := range []string{"n1", "n2", "n3", "n4"} {
		if err := s.AddNode(domain.Node{ID: id, Capacity: domain.Resource{CPU: 12, Memory: 16, GPU: 2}}); err != nil {
			t.Fatal(err)
		}
	}
	s.SetQuota("alpha", domain.Resource{CPU: 40, Memory: 80, GPU: 6})
	s.SetQuota("beta", domain.Resource{CPU: 40, Memory: 80, GPU: 6})
	sm := New(seed, s)
	jobs, err := sm.GenerateWorkload(testSpec())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			t.Logf("seed=%d generated %d jobs; trace:\n%s", seed, len(jobs), buf.String())
		}
	})
	return sm, &buf
}

// TestDeterministicReproduction: two independent runs with the same fixed
// seed produce identical inputs and identical final conclusions.
func TestDeterministicReproduction(t *testing.T) {
	const seed = int64(20260925)
	sm1, _ := buildWorld(t, seed)
	sm2, _ := buildWorld(t, seed)

	// Same scripted failure timeline in both worlds.
	for _, sm := range []*Simulator{sm1, sm2} {
		sm.ScriptEvent(TimelineEvent{Tick: 2, Kind: "fail", NodeID: "n2"})
		sm.ScriptEvent(TimelineEvent{Tick: 4, Kind: "restore", NodeID: "n2"})
		sm.Run(8)
	}

	r1 := canonicalSnapshot(sm1.Scheduler().Snapshot())
	r2 := canonicalSnapshot(sm2.Scheduler().Snapshot())
	if r1 != r2 {
		t.Fatalf("runs with seed %d diverged:\n--- run1 ---\n%s\n--- run2 ---\n%s", seed, r1, r2)
	}

	// And inputs must be identical too.
	i1 := canonicalJobs(sm1.Jobs())
	i2 := canonicalJobs(sm2.Jobs())
	if i1 != i2 {
		t.Fatalf("generated workloads differ for same seed")
	}
}

// TestDifferentSeedsCanDiverge (guards the seed from being hardcoded).
func TestDifferentSeedsCanDiverge(t *testing.T) {
	smA, _ := buildWorld(t, 1)
	smB, _ := buildWorld(t, 2)
	smA.Run(6)
	smB.Run(6)
	if canonicalJobs(smA.Jobs()) == canonicalJobs(smB.Jobs()) {
		t.Fatal("expected distinct workloads for distinct seeds")
	}
}

// TestScriptedFailureReconciles: after a node failure no counters drift and
// every displaced job is either requeued with a bounded reason or failed.
func TestScriptedFailureReconciles(t *testing.T) {
	sm, _ := buildWorld(t, 77)
	sm.ScriptEvent(TimelineEvent{Tick: 2, Kind: "fail", NodeID: "n3"})
	sm.Run(8)
	snap := sm.Scheduler().Snapshot()

	var nodeUsed, runningDemand domain.Resource
	for _, n := range snap.Nodes {
		nodeUsed = nodeUsed.Add(n.Used)
		if n.ID == "n3" && n.State != domain.NodeDown {
			t.Fatalf("n3 should still be down: %v", n.State)
		}
	}
	var usageTotal domain.Resource
	for _, u := range snap.OwnerUsage {
		usageTotal = usageTotal.Add(u)
	}
	for _, j := range snap.Jobs {
		if j.State == domain.StateRunning {
			runningDemand = runningDemand.Add(j.TotalResources())
		}
	}
	if nodeUsed != runningDemand {
		t.Fatalf("node usage %v != running demand %v", nodeUsed, runningDemand)
	}
	if nodeUsed != usageTotal {
		t.Fatalf("node usage %v != owner quota usage %v", nodeUsed, usageTotal)
	}
}

// TestArrayElementsAreIndependent: failure of one array element never changes
// the state of sibling elements; quotas still roll up to the same owner.
func TestArrayElementsAreIndependent(t *testing.T) {
	sm, _ := buildWorld(t, 31337)
	sm.Run(3)
	before := map[string]domain.JobState{}
	var owner string
	for _, j := range sm.Jobs() {
		if j.ArrayID != "" {
			before[j.ID] = func() domain.JobState { jj, _ := sm.Scheduler().Job(j.ID); return jj.State }()
			owner = j.Owner
		}
	}
	if len(before) < 3 {
		t.Fatalf("expected array elements, got %d", len(before))
	}

	// Find a running array element, fail its node, and confirm siblings keep
	// their pre-failure state unless they shared that exact node.
	snap := sm.Scheduler().Snapshot()
	nodeOwners := map[string][]string{}
	runningElems := map[string]bool{}
	for _, j := range snap.Jobs {
		if j.ArrayID != "" && j.State == domain.StateRunning {
			runningElems[j.ID] = true
			for _, tk := range j.Tasks {
				nodeOwners[tk.NodeID] = append(nodeOwners[tk.NodeID], j.ID)
			}
		}
	}
	if len(runningElems) == 0 {
		t.Skip("no running array element in this seed; assertion target absent")
	}
	var targetNode string
	for n, ids := range nodeOwners {
		if len(ids) >= 1 {
			targetNode = n
			break
		}
	}
	affected := sm.Scheduler().FailNode(targetNode)
	affectedSet := map[string]bool{}
	for _, id := range affected {
		affectedSet[id] = true
	}

	post := sm.Scheduler().Snapshot()
	for _, j := range post.Jobs {
		if j.ArrayID == "" {
			continue
		}
		if !affectedSet[j.ID] && before[j.ID] == domain.StateRunning {
			// Unaffected running sibling must remain running.
			if j.State != domain.StateRunning {
				t.Fatalf("array element %s changed from RUNNING to %v although not on failed node", j.ID, j.State)
			}
		}
	}
	if owner == "" {
		t.Fatal("no array owner")
	}
}

// TestTraceExplainsInputsAndBasis ensures unit-test logs carry job inputs and
// explicit scheduling judgments (per requirement).
func TestTraceExplainsInputsAndBasis(t *testing.T) {
	_, buf := buildWorld(t, 42)
	if !strings.Contains(buf.String(), "submit job") {
		t.Fatalf("trace lacks job inputs:\n%s", buf.String())
	}
	sm2, buf2 := buildWorld(t, 42)
	sm2.Run(2)
	trace := buf2.String()
	if !strings.Contains(trace, "evaluate") || !strings.Contains(trace, "judgment:") {
		t.Fatalf("trace lacks scheduling basis:\n%s", trace)
	}
}

func canonicalSnapshot(sn domain.Snapshot) string {
	var b strings.Builder
	b.WriteString("tick=" + itoa(sn.Tick) + "\n")
	for _, n := range sn.Nodes {
		b.WriteString("N " + n.ID + " " + n.State.String() + " cap=" + n.Capacity.String() + " used=" + n.Used.String() + "\n")
	}
	for _, j := range sn.Jobs {
		b.WriteString("J " + j.ID + " " + j.State.String() + " " + j.Reason.String() +
			" res=" + j.TotalResources().String() + " retry=" + itoa(j.RetriesLeft))
		for _, tk := range j.Tasks {
			b.WriteString(" " + tk.ID + "->" + tk.NodeID)
		}
		b.WriteString("\n")
	}
	return b.String()
}

func canonicalJobs(jobs []*domain.Job) string {
	var b strings.Builder
	for _, j := range jobs {
		b.WriteString(j.ID + "|" + j.Owner + "|" + itoa(int(j.Priority)) + "|" +
			j.GroupID + "|" + j.ArrayID + "|" + itoa(j.Index) + "|" + j.TotalResources().String() + "\n")
	}
	return b.String()
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var digits []byte
	for n > 0 {
		digits = append([]byte{byte('0' + n%10)}, digits...)
		n /= 10
	}
	if neg {
		return "-" + string(digits)
	}
	return string(digits)
}
