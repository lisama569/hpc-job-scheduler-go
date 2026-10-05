package scheduler

// NodeState 描述节点的运维状态。
type NodeState int

const (
	NodeOnline   NodeState = iota // 正常在线，参与调度
	NodeCordoned                  // 计划内维护、尚未排空：不接新作业，已有作业保留
	NodeDrained                   // 维护排空完成：节点真正下线，无作业、无占用
	NodeFailed                    // 故障：不可用，作业已被故障处理回收
)

func (s NodeState) String() string {
	switch s {
	case NodeOnline:
		return "Online"
	case NodeCordoned:
		return "Cordoned"
	case NodeDrained:
		return "Drained"
	case NodeFailed:
		return "Failed"
	default:
		return "Unknown"
	}
}

// Node 是集群中的一台模拟节点。
type Node struct {
	ID        string
	Capacity  Resources
	Allocated Resources
	State     NodeState

	// recoverCordoned 记录节点故障时是否正处于维护（Cordoned），
	// 故障恢复后据此回到 Cordoned 而非 Online。
	recoverCordoned bool
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

// tryAllocate 尝试把 req 分配到某个在线节点，成功返回节点 ID。
// 采用"首个可放下"策略，遍历顺序固定，结果可复现。
// 维护（Cordoned/Drained）与故障节点都不是可用容量，一律跳过。
func (c *Cluster) tryAllocate(req Resources) (string, bool) {
	for _, id := range c.order {
		n := c.nodes[id]
		if n.State != NodeOnline {
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
// 在线节点与已 cordon（维护未排空）的节点都可故障；
// 后者恢复后回到 Cordoned，维护意图不因故障丢失。
// Drained 节点（已无作业、真正下线）故障无意义，返回 false。
func (c *Cluster) failNode(nodeID string) bool {
	n, ok := c.nodes[nodeID]
	if !ok {
		return false
	}
	switch n.State {
	case NodeOnline:
		n.recoverCordoned = false
	case NodeCordoned:
		n.recoverCordoned = true
	default:
		return false
	}
	n.State = NodeFailed
	return true
}

// recoverNode 恢复故障节点：故障前在线则回到 Online，
// 故障前处于维护则回到 Cordoned（维护继续，仍需排空）。
func (c *Cluster) recoverNode(nodeID string) bool {
	n, ok := c.nodes[nodeID]
	if !ok || n.State != NodeFailed {
		return false
	}
	if n.recoverCordoned {
		n.State = NodeCordoned
	} else {
		n.State = NodeOnline
	}
	n.recoverCordoned = false
	return true
}

// totalFree 返回全集群在线节点的空闲资源总量；
// 维护与故障节点的空闲容量不计入可用容量。
func (c *Cluster) totalFree() Resources {
	total := Resources{}
	for _, id := range c.order {
		if n := c.nodes[id]; n.State == NodeOnline {
			total = total.Add(n.Free())
		}
	}
	return total
}

// cordon 把在线节点切到维护（未排空）状态：节点不再接新作业，已有作业保留。
func (c *Cluster) cordon(nodeID string) bool {
	n, ok := c.nodes[nodeID]
	if !ok || n.State != NodeOnline {
		return false
	}
	n.State = NodeCordoned
	return true
}

// markDrained 把已排空作业的维护节点标记为真正下线（无作业、无占用）。
func (c *Cluster) markDrained(nodeID string) bool {
	n, ok := c.nodes[nodeID]
	if !ok || n.State != NodeCordoned {
		return false
	}
	n.State = NodeDrained
	return true
}

// uncordon 把维护中的节点（Cordoned 或 Drained）恢复为在线，
// 取消维护与维护结束复用同一路径；恢复后立即重新参与调度。
func (c *Cluster) uncordon(nodeID string) bool {
	n, ok := c.nodes[nodeID]
	if !ok || (n.State != NodeCordoned && n.State != NodeDrained) {
		return false
	}
	n.State = NodeOnline
	return true
}

// fitsAnyNode 报告 req 是否能放入任意一个在线节点的全部容量
// （逐节点判定，用于识别"永远放不下"的作业；维护/故障节点不算可用容量）。
func (c *Cluster) fitsAnyNode(req Resources) bool {
	for _, id := range c.order {
		if n := c.nodes[id]; n.State == NodeOnline && req.Fits(n.Capacity) {
			return true
		}
	}
	return false
}
