// Package queue is a deterministic priority queue of pending jobs.
package queue

import (
	"sort"

	"github.com/lisama569/hpc-job-scheduler-go/internal/domain"
)

type entry struct {
	job  *domain.Job
	tick int
}

// Queue orders pending jobs by (priority desc, enqueue tick asc, job id asc)
// so scheduling decisions are fully reproducible run after run.
type Queue struct {
	entries []entry
}

// New creates an empty Queue.
func New() *Queue { return &Queue{} }

// less defines the total ordering of the queue.
func less(a, b entry) bool {
	if a.job.Priority != b.job.Priority {
		return a.job.Priority > b.job.Priority
	}
	if a.tick != b.tick {
		return a.tick < b.tick
	}
	return a.job.ID < b.job.ID
}

// Enqueue adds a job at the given logical tick and keeps total order.
func (q *Queue) Enqueue(job *domain.Job, tick int) {
	q.entries = append(q.entries, entry{job: job, tick: tick})
	// Simple insertion: N is small in the simulation and determinism matters
	// more than asymptotic complexity.
	sort.SliceStable(q.entries, func(i, j int) bool {
		return less(q.entries[i], q.entries[j])
	})
}

// EnqueuedAt returns the tick at which jobID was enqueued, or -1 if absent.
func (q *Queue) EnqueuedAt(jobID string) int {
	for _, e := range q.entries {
		if e.job.ID == jobID {
			return e.tick
		}
	}
	return -1
}

// PeekHead returns the first queued job without removing it.
func (q *Queue) PeekHead() *domain.Job {
	if len(q.entries) == 0 {
		return nil
	}
	return q.entries[0].job
}

// PeekAll returns all queued jobs in scheduling order without removing.
func (q *Queue) PeekAll() []*domain.Job {
	out := make([]*domain.Job, len(q.entries))
	for i, e := range q.entries {
		out[i] = e.job
	}
	return out
}

// Remove takes one job out of the queue and reports whether it was present.
func (q *Queue) Remove(jobID string) bool {
	for i, e := range q.entries {
		if e.job.ID == jobID {
			q.entries = append(q.entries[:i], q.entries[i+1:]...)
			return true
		}
	}
	return false
}

// RemoveMany removes several jobs at once (gang atomic commit). Missing IDs
// are skipped; it returns how many were actually removed.
func (q *Queue) RemoveMany(ids []string) int {
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	kept := q.entries[:0]
	removed := 0
	for _, e := range q.entries {
		if want[e.job.ID] {
			removed++
		} else {
			kept = append(kept, e)
		}
	}
	q.entries = kept
	return removed
}

// Len reports queue length.
func (q *Queue) Len() int { return len(q.entries) }
