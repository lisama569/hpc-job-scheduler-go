package scheduler

import (
	"bytes"
	"testing"

	"github.com/lisama569/hpc-job-scheduler-go/internal/domain"
)

// A 2-member gang must place together; failing one node evacuates both.
func TestGangFailureAtomic(t *testing.T) {
	var buf bytes.Buffer
	s := New(DefaultConfig(), &buf)
	s.AddNode(domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 10}})
	s.AddNode(domain.Node{ID: "n2", Capacity: domain.Resource{CPU: 10}})
	gm := func(id string) *domain.Job {
		return &domain.Job{ID: id, Owner: "g", Priority: 5, GroupID: "G",
			Tasks: []*domain.Task{{ID: id + ":0", Resources: domain.Resource{CPU: 6}}}}
	}
	if err := s.Submit(gm("m1"), gm("m2")); err != nil {
		t.Fatal(err)
	}
	s.RunPass()
	j1, _ := s.Job("m1")
	j2, _ := s.Job("m2")
	if j1.State != domain.StateRunning || j2.State != domain.StateRunning {
		t.Fatalf("gang should be running: %v %v", j1.State, j2.State)
	}
	failed := j1.Tasks[0].NodeID
	affected := s.FailNode(failed)
	if len(affected) != 2 {
		t.Fatalf("expected whole gang (2) evacuated, got %v", affected)
	}
	for _, id := range []string{"m1", "m2"} {
		if j, _ := s.Job(id); j.State != domain.StatePending {
			t.Fatalf("%s state=%v, want PENDING (gang evacuated together)", id, j.State)
		}
	}
	for _, n := range s.cluster.Snapshot() {
		if n.Used.Any() {
			t.Fatalf("node %s residue %v", n.ID, n.Used)
		}
	}
	if t.Failed() {
		t.Log("\n" + buf.String())
	}
}

// A running gang can only be preempted as a whole by a high-priority unit.
func TestGangPreemptionAtomic(t *testing.T) {
	var buf bytes.Buffer
	s := New(DefaultConfig(), &buf)
	s.AddNode(domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 10}})
	s.AddNode(domain.Node{ID: "n2", Capacity: domain.Resource{CPU: 10}})
	gm := func(id string) *domain.Job {
		return &domain.Job{ID: id, Owner: "g", Priority: 2, GroupID: "G",
			Tasks: []*domain.Task{{ID: id + ":0", Resources: domain.Resource{CPU: 8}}}}
	}
	if err := s.Submit(gm("m1"), gm("m2")); err != nil {
		t.Fatal(err)
	}
	s.RunPass()
	hi := single("hi", 9, domain.Resource{CPU: 8})
	if err := s.Submit(hi); err != nil {
		t.Fatal(err)
	}
	s.RunPass()
	j, _ := s.Job("hi")
	if j.State != domain.StateRunning {
		t.Log("\n" + buf.String())
		t.Fatalf("hi not running: %v (%s)", j.State, j.Message)
	}
	for _, id := range []string{"m1", "m2"} {
		if j, _ := s.Job(id); j.State != domain.StatePending || j.Reason != domain.ReasonPreempted {
			t.Fatalf("%s state=%v reason=%v (gang must be preempted as one)", id, j.State, j.Reason)
		}
	}
}

// A mixed-priority gang cannot be preempted by a job whose priority is not
// strictly above EVERY member: the highest-priority member gates the group.
func TestGangMixedPriorityNotPreemptable(t *testing.T) {
	var buf bytes.Buffer
	s := New(DefaultConfig(), &buf)
	s.AddNode(domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 10}})
	s.AddNode(domain.Node{ID: "n2", Capacity: domain.Resource{CPU: 10}})
	mk := func(id string, p int) *domain.Job {
		return &domain.Job{ID: id, Owner: "g", Priority: domain.Priority(p), GroupID: "G",
			Tasks: []*domain.Task{{ID: id + ":0", Resources: domain.Resource{CPU: 8}}}}
	}
	if err := s.Submit(mk("lo", 2), mk("hi", 9)); err != nil {
		t.Fatal(err)
	}
	s.RunPass()
	mid := single("mid", 8, domain.Resource{CPU: 8}) // below member hi(9)
	if err := s.Submit(mid); err != nil {
		t.Fatal(err)
	}
	s.RunPass()
	if j, _ := s.Job("mid"); j.State != domain.StatePending {
		t.Log("\n" + buf.String())
		t.Fatalf("mid must wait (gang has a prio=9 member): %v", j.State)
	}
	for _, id := range []string{"lo", "hi"} {
		if j, _ := s.Job(id); j.State != domain.StateRunning {
			t.Fatalf("%s must stay running, got %v", id, j.State)
		}
	}
}
