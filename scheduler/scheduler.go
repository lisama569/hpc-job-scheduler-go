package scheduler

import (
	"errors"
	"sync"
)

// ErrJobExists 表示重复提交相同 ID 的作业。
var ErrJobExists = errors.New("scheduler: job already exists")

// ErrUnknownJob 表示操作的作业不存在。
var ErrUnknownJob = errors.New("scheduler: unknown job")

// Decision 记录一次调度判定，供日志与测试断言使用。
type Decision struct {
	JobID  string
	Action string // "allocated" / "wait" / "rejected" / "preempted-victim" / "requeued"
	Reason string // 人类可读的判定依据
}

// Scheduler 是并发安全的作业调度器。
type Scheduler struct {
	mu      sync.Mutex
	cluster *Cluster
	jobs    map[string]*Job
	queue   *readyQueue
	quota   map[string]Resources // Owner -> 已占用配额
	limits  map[string]Resources // Owner -> 配额上限，缺省表示不限
}

// New 创建调度器；limits 为各归属方的配额上限（可为 nil 表示不限）。
func New(c *Cluster, limits map[string]Resources) *Scheduler { return nil }

// Submit 提交作业：校验依赖与容量，拒绝原因可区分；通过则入队。
func (s *Scheduler) Submit(j *Job) error { return nil }

// RunCycle 执行一轮调度：尽可能为队列中的作业分配资源（组调度整体成功或整体等待）。
// 返回本轮的所有判定，顺序确定。
func (s *Scheduler) RunCycle() []Decision { return nil }

// Complete 将运行中的作业标记为成功并释放资源。
func (s *Scheduler) Complete(jobID string) error { return nil }

// FailNode 模拟节点故障：节点上作业按有界重试重新入队或置为失败，
// 资源只释放一次，不产生重复计数。
func (s *Scheduler) FailNode(nodeID string) ([]Decision, error) { return nil, nil }

// RecoverNode 恢复节点为健康状态。
func (s *Scheduler) RecoverNode(nodeID string) error { return nil }

// Job 返回作业当前状态的快照。
func (s *Scheduler) Job(jobID string) (Job, error) { return Job{}, ErrUnknownJob }

// QuotaUsed 返回归属方当前占用的配额。
func (s *Scheduler) QuotaUsed(owner string) Resources { return Resources{} }

// Pending 返回当前排队作业 ID（按出队顺序）。
func (s *Scheduler) Pending() []string { return nil }
