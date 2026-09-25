package cluster

import "testing"
import "github.com/lisama569/hpc-job-scheduler-go/internal/domain"

func TestAllocateOverflowAtomic(t *testing.T) {
	c := New()
	if err := c.AddNode(domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 4}}); err != nil {
		t.Fatal(err)
	}
	if !c.Allocate("n1", domain.Resource{CPU: 3}) {
		t.Fatal("first allocate should succeed")
	}
	if c.Allocate("n1", domain.Resource{CPU: 2}) {
		t.Fatal("overflowing allocate must fail")
	}
	n := c.Snapshot()[0]
	if n.Used.CPU != 3 {
		t.Fatalf("failed allocate mutated usage: %d", n.Used.CPU)
	}
	if c.HasAnyNodeThatFits(domain.Resource{CPU: 8}) {
		t.Fatal("no node should fit cpu=8")
	}
	if !c.HasAnyNodeThatFits(domain.Resource{CPU: 4}) {
		t.Fatal("capacity-fit ignores current usage")
	}
}

func TestFailNodeZeroesUsage(t *testing.T) {
	c := New()
	_ = c.AddNode(domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 8}})
	if !c.Allocate("n1", domain.Resource{CPU: 8}) {
		t.Fatal("allocate")
	}
	c.FailNode("n1")
	n := c.Snapshot()[0]
	if n.State != domain.NodeDown || n.Used.Any() {
		t.Fatalf("after fail: state=%v used=%v", n.State, n.Used)
	}
	if c.Allocate("n1", domain.Resource{CPU: 1}) {
		t.Fatal("down node must reject allocations")
	}
	c.RestoreNode("n1")
	if !c.Allocate("n1", domain.Resource{CPU: 8}) {
		t.Fatal("restored node should accept full capacity")
	}
}

func TestReleaseNegativePanics(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic on negative usage")
		}
	}()
	c := New()
	_ = c.AddNode(domain.Node{ID: "n1", Capacity: domain.Resource{CPU: 2}})
	c.Release("n1", domain.Resource{CPU: 1})
}
