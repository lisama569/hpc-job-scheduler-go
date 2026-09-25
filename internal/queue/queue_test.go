package queue

import "testing"
import "github.com/lisama569/hpc-job-scheduler-go/internal/domain"

func qj(id string, p domain.Priority) *domain.Job { return &domain.Job{ID: id, Priority: p} }

func TestPriorityOrdering(t *testing.T) {
	q := New()
	q.Enqueue(qj("c", 1), 0)
	q.Enqueue(qj("a", 5), 0)
	q.Enqueue(qj("b", 5), 0)
	want := []string{"a", "b", "c"}
	got := q.PeekAll()
	for i, id := range want {
		if got[i].ID != id {
			t.Fatalf("pos %d: want %s got %s", i, id, got[i].ID)
		}
	}
}

func TestTickThenIDTieBreaks(t *testing.T) {
	q := New()
	q.Enqueue(qj("late", 5), 3)
	q.Enqueue(qj("early", 5), 1)
	if q.PeekHead().ID != "early" {
		t.Fatalf("head=%s want early", q.PeekHead().ID)
	}
	if !q.Remove("early") || q.Len() != 1 {
		t.Fatal("remove failed")
	}
	if q.Remove("ghost") {
		t.Fatal("removing missing id must report false")
	}
}

func TestRemoveMany(t *testing.T) {
	q := New()
	q.Enqueue(qj("x", 1), 0)
	q.Enqueue(qj("y", 1), 0)
	q.Enqueue(qj("z", 1), 0)
	if n := q.RemoveMany([]string{"x", "z", "ghost"}); n != 2 {
		t.Fatalf("removed %d, want 2", n)
	}
	if q.PeekHead().ID != "y" || q.Len() != 1 {
		t.Fatal("remaining queue wrong")
	}
}
