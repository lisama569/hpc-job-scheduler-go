package scheduler

import "sort"

// readyQueue 是确定性的就绪队列：
// 优先级高者在前；同优先级按入队序号（先入先出），保证结果可复现。
type readyQueue struct {
	items []*Job
	seq   map[string]int // 作业 ID -> 入队序号
	next  int
}

func newReadyQueue() *readyQueue {
	return &readyQueue{seq: make(map[string]int)}
}

// Push 将作业放入队列；重复放入同一作业会被忽略。
func (q *readyQueue) Push(j *Job) {
	if _, dup := q.seq[j.ID]; dup {
		return
	}
	q.seq[j.ID] = q.next
	q.next++
	q.items = append(q.items, j)
	q.sort()
}

// Pop 取出队首作业；空队列返回 nil。
// 取出的作业可从队列再次 Push（获得新序号），用于重新入队。
func (q *readyQueue) Pop() *Job {
	if len(q.items) == 0 {
		return nil
	}
	j := q.items[0]
	q.items = q.items[1:]
	delete(q.seq, j.ID)
	return j
}

// Len 返回队列长度。
func (q *readyQueue) Len() int { return len(q.items) }

// Snapshot 按出队顺序返回当前队列内容（不修改队列）。
func (q *readyQueue) Snapshot() []*Job {
	out := make([]*Job, len(q.items))
	copy(out, q.items)
	return out
}

// sort 按确定性规则排序：优先级降序，其次入队序号升序。
func (q *readyQueue) sort() {
	sort.SliceStable(q.items, func(i, j int) bool {
		a, b := q.items[i], q.items[j]
		if a.Priority != b.Priority {
			return a.Priority > b.Priority
		}
		return q.seq[a.ID] < q.seq[b.ID]
	})
}
