// Package domain defines the core data model of the simulated HPC scheduler:
// resources, nodes, jobs (single / gang group / array elements) and the
// externally visible scheduling decisions.
package domain

import "fmt"

// Resource is an additive, scalar resource vector. All values are non-negative.
type Resource struct {
	CPU    int64
	Memory int64
	GPU    int64
}

// Add returns a+b without mutating either operand.
func (a Resource) Add(b Resource) Resource {
	return Resource{CPU: a.CPU + b.CPU, Memory: a.Memory + b.Memory, GPU: a.GPU + b.GPU}
}

// Sub returns a-b without mutating either operand.
func (a Resource) Sub(b Resource) Resource {
	return Resource{CPU: a.CPU - b.CPU, Memory: a.Memory - b.Memory, GPU: a.GPU - b.GPU}
}

// Fits reports whether a can host b: every component of b <= a.
func (a Resource) Fits(b Resource) bool {
	return b.CPU <= a.CPU && b.Memory <= a.Memory && b.GPU <= a.GPU
}

// Any reports whether any component is positive.
func (a Resource) Any() bool { return a.CPU > 0 || a.Memory > 0 || a.GPU > 0 }

// String renders a resource vector for explainable logs.
func (a Resource) String() string {
	return fmt.Sprintf("(cpu=%d,mem=%d,gpu=%d)", a.CPU, a.Memory, a.GPU)
}

// NodeState describes whether a node may accept tasks.
type NodeState int

const (
	NodeUp NodeState = iota
	NodeDown
)

func (s NodeState) String() string {
	switch s {
	case NodeUp:
		return "UP"
	case NodeDown:
		return "DOWN"
	default:
		return "UNKNOWN"
	}
}

// JobState is the externally visible lifecycle state of a job.
type JobState int

const (
	StatePending   JobState = iota // queued, waiting to be scheduled
	StateRunning                   // all its tasks are placed on nodes
	StateCompleted                 // finished successfully
	StateFailed                    // aborted (e.g. node failure with no retry left)
	StateRejected                  // permanently refused at admission / scheduling
)

func (s JobState) String() string {
	switch s {
	case StatePending:
		return "PENDING"
	case StateRunning:
		return "RUNNING"
	case StateCompleted:
		return "COMPLETED"
	case StateFailed:
		return "FAILED"
	case StateRejected:
		return "REJECTED"
	default:
		return "UNKNOWN"
	}
}

// RejectReason distinguishes why a job cannot be scheduled.
type RejectReason int

const (
	ReasonNone                 RejectReason = iota
	ReasonExceedsNodeCapacity               // no single node can ever fit the task
	ReasonInsufficientResource              // capacity exists in principle, none is free now
	ReasonDependencyNotMet                  // a dependency is still pending/running
	ReasonDependencyFailed                  // a dependency failed or was rejected
	ReasonQuotaExceeded                     // owner quota would be violated
	ReasonGroupCoScheduling                 // gang cannot be placed atomically
	ReasonNodeUnavailable                   // target/node fleet cannot serve it right now
	ReasonPreempted                         // evicted by a strictly higher-priority job
)

func (r RejectReason) String() string {
	switch r {
	case ReasonNone:
		return "NONE"
	case ReasonExceedsNodeCapacity:
		return "EXCEEDS_NODE_CAPACITY"
	case ReasonInsufficientResource:
		return "INSUFFICIENT_RESOURCE"
	case ReasonDependencyNotMet:
		return "DEPENDENCY_NOT_MET"
	case ReasonDependencyFailed:
		return "DEPENDENCY_FAILED"
	case ReasonQuotaExceeded:
		return "QUOTA_EXCEEDED"
	case ReasonGroupCoScheduling:
		return "GROUP_CO_SCHEDULING"
	case ReasonNodeUnavailable:
		return "NODE_UNAVAILABLE"
	case ReasonPreempted:
		return "PREEMPTED"
	default:
		return "UNKNOWN"
	}
}

// Priority is a larger-is-more-important ordering key.
type Priority int

// Task is one schedulable unit. A single job owns one task; a gang job owns
// several tasks that must all start at the same instant.
type Task struct {
	ID        string
	Resources Resource
	NodeID    string // empty while unplaced
}

// Job is the submission unit.
type Job struct {
	ID        string
	Owner     string
	Priority  Priority
	DependsOn []string

	// GroupID marks gang (co-scheduled) jobs: every *pending* job sharing the
	// same GroupID must be placed atomically, or none of them is.
	GroupID string

	// ArrayID/Index identify job-array elements. Elements are independent
	// jobs; they only share bookkeeping for the array they belong to.
	ArrayID string
	Index   int

	Tasks []*Task

	State   JobState
	Reason  RejectReason
	Message string

	// RetriesLeft bounds how often the job may be requeued after a node
	// failure or a bounded preemption retry.
	RetriesLeft int
}

// TotalResources sums the resources of all the job's tasks.
func (j *Job) TotalResources() Resource {
	var sum Resource
	for _, t := range j.Tasks {
		sum = sum.Add(t.Resources)
	}
	return sum
}

// Clone returns a deep copy safe to hand to external readers.
func (j *Job) Clone() Job {
	cp := *j
	if j.DependsOn != nil {
		cp.DependsOn = append([]string(nil), j.DependsOn...)
	}
	if j.Tasks != nil {
		cp.Tasks = make([]*Task, len(j.Tasks))
		for i, t := range j.Tasks {
			ct := *t
			cp.Tasks[i] = &ct
		}
	}
	return cp
}

// Node is a simulated compute node.
type Node struct {
	ID       string
	Capacity Resource
	Used     Resource
	State    NodeState
}

// Free returns the still-allocatable resource vector.
func (n *Node) Free() Resource { return n.Capacity.Sub(n.Used) }

// TaskPlacement records where one task landed.
type TaskPlacement struct {
	JobID  string
	TaskID string
	NodeID string
}

// Decision is the externally visible outcome of evaluating one job.
type Decision struct {
	JobID      string
	State      JobState
	Reason     RejectReason
	Message    string
	Placements []TaskPlacement
}

// Snapshot is a consistent point-in-time view of the whole scheduler.
type Snapshot struct {
	Tick       int
	Nodes      []Node
	Jobs       []Job
	Decisions  []Decision
	OwnerUsage map[string]Resource
	OwnerQuota map[string]Resource
}
