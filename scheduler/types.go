// Package scheduler 实现单机可复现的集群作业调度系统。
package scheduler

// Resources 描述一份可分配的资源向量。
type Resources struct {
	CPU   int
	MemMB int
	GPU   int
}

// Add 返回两份资源的和。
func (r Resources) Add(o Resources) Resources {
	return Resources{CPU: r.CPU + o.CPU, MemMB: r.MemMB + o.MemMB, GPU: r.GPU + o.GPU}
}

// Sub 返回 r - o（调用方保证结果非负）。
func (r Resources) Sub(o Resources) Resources {
	return Resources{CPU: r.CPU - o.CPU, MemMB: r.MemMB - o.MemMB, GPU: r.GPU - o.GPU}
}

// Fits 报告 r 是否能装入 capacity（逐维比较）。
func (r Resources) Fits(capacity Resources) bool {
	return r.CPU <= capacity.CPU && r.MemMB <= capacity.MemMB && r.GPU <= capacity.GPU
}

// IsZero 报告资源是否为空。
func (r Resources) IsZero() bool {
	return r.CPU == 0 && r.MemMB == 0 && r.GPU == 0
}

// JobState 是作业的生命周期状态。
type JobState int

const (
	StatePending   JobState = iota // 排队等待调度
	StateRunning                   // 已获得资源运行中
	StateSucceeded                 // 正常完成
	StateFailed                    // 重试耗尽或节点故障不可恢复
	StateRejected                  // 提交即被拒绝（永不可调度）
	StatePreempted                 // 被抢占，等待重新入队
)

func (s JobState) String() string {
	switch s {
	case StatePending:
		return "Pending"
	case StateRunning:
		return "Running"
	case StateSucceeded:
		return "Succeeded"
	case StateFailed:
		return "Failed"
	case StateRejected:
		return "Rejected"
	case StatePreempted:
		return "Preempted"
	default:
		return "Unknown"
	}
}

// RejectReason 区分作业被拒绝的原因。
type RejectReason int

const (
	RejectNone                    RejectReason = iota // 未被拒绝
	RejectExceedsCapacity                             // 需求超出任何节点的总容量，永远放不下
	RejectDependencyUnsatisfiable                     // 依赖的作业失败/被拒，依赖链永不可满足
	RejectDependencyCycle                             // 依赖关系存在环
)

func (r RejectReason) String() string {
	switch r {
	case RejectNone:
		return "None"
	case RejectExceedsCapacity:
		return "ExceedsCapacity"
	case RejectDependencyUnsatisfiable:
		return "DependencyUnsatisfiable"
	case RejectDependencyCycle:
		return "DependencyCycle"
	default:
		return "Unknown"
	}
}

// Job 是一个待调度作业；组调度时同 GroupID 的作业整体成功或整体等待。
type Job struct {
	ID         string
	Owner      string // 归属方，配额按 Owner 独立核算
	Priority   int    // 数值越大优先级越高
	Req        Resources
	DependsOn  []string // 依赖的作业 ID，需全部 Succeeded
	GroupID    string   // 非空表示属于某个组调度作业组
	ArrayIndex int      // 作业数组下标，-1 表示非数组元素
	MaxRetries int      // 故障/抢占后有界重试次数上限

	State        JobState
	RejectWhy    RejectReason
	Retries      int
	AssignedNode string
}

// IsArrayElement 报告作业是否为数组元素。
func (j *Job) IsArrayElement() bool { return j.ArrayIndex >= 0 }
