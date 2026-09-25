package scheduler

import "sort"

// readyQueue 是确定性的就绪队列：
// 优先级高者在前；同优先级按提交序号（先入先出）。
type readyQueue struct {
	items []*Job
	seq   map[string]int // 作业 ID -> 入队序号，保证同优先级下 FIFO 可复现
	next  int
}

func newReadyQueue() *readyQueue { return nil }

// Push 将作业放入队列；重复放入同一作业会被忽略。
func (q *readyQueue) Push(j *Job) {}

// Pop 取出队首作业；空队列返回 nil。
func (q *readyQueue) Pop() *Job { return nil }

// Len 返回队列长度。
func (q *readyQueue) Len() int { return 0 }

// Snapshot 按出队顺序返回当前队列内容（不修改队列）。
func (q *readyQueue) Snapshot() []*Job { return nil }

// less 是队列排序规则：优先级降序，其次入队序号升序。
func less(a, b *Job, seq map[string]int) bool {
	if a.Priority != b.Priority {
		return a.Priority > b.Priority
	}
	return seq[a.ID] < seq[b.ID]
}

var _ = sort.SliceStable // 骨架占位，实现阶段使用
