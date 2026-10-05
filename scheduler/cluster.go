package scheduler

// NodeState 描述节点的运维状态。
type NodeState int

const (
	// NodeOnline 正常在线：接收新调度，容量计入可用资源。
	NodeOnline NodeState = iota
	// NodeDraining 计划内维护中：不再接收新调度，但已有作业先保留，
	// 等待显式排空（drain）把作业迁走或退回队列。
	NodeDraining
	// NodeDrained 排空完成、真正下线检修：不接收新调度，容量不计入可用资源。
	NodeDrained
	// NodeFailed 故障：不接收新调度，容量不计入可用资源；
	// 若故障前处于维护流程，恢复后自动回到 NodeDrained（维护意图保留）。
	NodeFailed
)

func (s NodeState) String() string {
	switch s {
	case NodeOnline:
		return "Online"
	case NodeDraining:
		return "Draining"
	case NodeDrained:
		return "Drained"
	case NodeFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// schedulable 报告节点是否可以接收新的调度分配。
// 只有在线节点算作可用容量；维护中/已排空/故障节点一律不分配。
func (s NodeState) schedulable() bool { return s == NodeOnline }

// Node 是集群中的一台模拟节点。
type Node struct {
	ID        string
	Capacity  Resources
	Allocated Resources
	State     NodeState
	// wantMaintenance 记录维护意图：维护流程中若节点再次故障，
	// 恢复后凭此标记回到 Drained，而不是误判为普通 Online。
	wantMaintenance bool
}

// Free 返回节点剩余可用资源。
func (n *Node) Free() Resources { return n.Capacity.Sub(n.Allocated) }

// Cluster 管理一组本地模拟节点。
type Cluster struct {
	nodes map[string]*Node
	order []string // 稳定的节点遍历顺序，保证可复现
}

// NewCluster 按给定节点容量构建集群（顺序即遍历顺序）。
func NewCluster(nodes ...*Node) *Cluster {
	c := &Cluster{nodes: make(map[string]*Node, len(nodes))}
	for _, n := range nodes {
		n.State = NodeOnline
		c.nodes[n.ID] = n
		c.order = append(c.order, n.ID)
	}
	return c
}

// Node 按 ID 查询节点。
func (c *Cluster) Node(id string) (*Node, bool) {
	n, ok := c.nodes[id]
	return n, ok
}

// Nodes 按稳定顺序返回全部节点。
func (c *Cluster) Nodes() []*Node {
	out := make([]*Node, 0, len(c.order))
	for _, id := range c.order {
		out = append(out, c.nodes[id])
	}
	return out
}

// tryAllocate 尝试把 req 分配到某个可调度节点，成功返回节点 ID。
// 采用"首个可放下"策略，遍历顺序固定，结果可复现。
func (c *Cluster) tryAllocate(req Resources) (string, bool) {
	for _, id := range c.order {
		n := c.nodes[id]
		if !n.State.schedulable() {
			continue
		}
		if req.Fits(n.Free()) {
			n.Allocated = n.Allocated.Add(req)
			return id, true
		}
	}
	return "", false
}

// release 释放节点上的资源；节点不存在或资源未被占用时返回 false。
func (c *Cluster) release(nodeID string, req Resources) bool {
	n, ok := c.nodes[nodeID]
	if !ok || !req.Fits(n.Allocated) {
		return false
	}
	n.Allocated = n.Allocated.Sub(req)
	return true
}

// restore 把资源精确加回指定节点（仅用于抢占回滚，保证恢复原位）。
func (c *Cluster) restore(nodeID string, req Resources) {
	if n, ok := c.nodes[nodeID]; ok {
		n.Allocated = n.Allocated.Add(req)
	}
}

// failNode 将节点标记为故障；不修改占用记录，
// 由调度器按作业逐个释放，保证资源只释放一次。
// 任何非故障状态（在线/维护中/已排空）都可能再次故障。
// 处于维护流程中的节点故障后保留维护意图，恢复时回到已排空态。
func (c *Cluster) failNode(nodeID string) bool {
	n, ok := c.nodes[nodeID]
	if !ok || n.State == NodeFailed {
		return false
	}
	if n.State == NodeDraining || n.State == NodeDrained {
		n.wantMaintenance = true
	}
	n.State = NodeFailed
	return true
}

// recoverNode 恢复故障节点：维护流程中故障的节点恢复为已排空态，
// 其余恢复为在线态。
func (c *Cluster) recoverNode(nodeID string) bool {
	n, ok := c.nodes[nodeID]
	if !ok || n.State != NodeFailed {
		return false
	}
	if n.wantMaintenance {
		n.State = NodeDrained
	} else {
		n.State = NodeOnline
	}
	return true
}

// beginMaintenance 把在线节点切到维护中（不再接收新调度，已有作业保留）。
func (c *Cluster) beginMaintenance(nodeID string) bool {
	n, ok := c.nodes[nodeID]
	if !ok || n.State != NodeOnline {
		return false
	}
	n.State = NodeDraining
	n.wantMaintenance = true
	return true
}

// markDrained 在排空完成后把节点置为已排空（真正下线检修）。
func (c *Cluster) markDrained(nodeID string) bool {
	n, ok := c.nodes[nodeID]
	if !ok || n.State != NodeDraining {
		return false
	}
	n.State = NodeDrained
	return true
}

// cancelMaintenance 取消维护：维护中或已排空的节点回到在线态并重新参与调度。
func (c *Cluster) cancelMaintenance(nodeID string) bool {
	n, ok := c.nodes[nodeID]
	if !ok || (n.State != NodeDraining && n.State != NodeDrained) {
		return false
	}
	n.State = NodeOnline
	n.wantMaintenance = false
	return true
}

// endMaintenance 结束维护：已排空（检修完成）的节点回到在线态。
func (c *Cluster) endMaintenance(nodeID string) bool {
	n, ok := c.nodes[nodeID]
	if !ok || n.State != NodeDrained {
		return false
	}
	n.State = NodeOnline
	n.wantMaintenance = false
	return true
}

// totalFree 返回全集群可调度节点的空闲资源总量。
func (c *Cluster) totalFree() Resources {
	total := Resources{}
	for _, id := range c.order {
		if n := c.nodes[id]; n.State.schedulable() {
			total = total.Add(n.Free())
		}
	}
	return total
}

// fitsAnyNode 报告 req 是否能放入某个非故障节点的全部容量
// （逐节点判定，用于识别"永远放不下"的作业）。
// 计划内维护是暂时状态：维护中的节点虽不立即参与调度，但维护结束后会回来，
// 因此其容量仍计入"可容纳性"判定，避免维护窗口内提交的作业被永久拒绝；
// 故障节点与作业失败均可能是持久的，故排除在外。
func (c *Cluster) fitsAnyNode(req Resources) bool {
	for _, id := range c.order {
		if n := c.nodes[id]; n.State != NodeFailed && req.Fits(n.Capacity) {
			return true
		}
	}
	return false
}
