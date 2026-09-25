package scheduler

// Node 是集群中的一台模拟节点。
type Node struct {
	ID        string
	Capacity  Resources
	Allocated Resources
	Healthy   bool
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
		n.Healthy = true
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

// tryAllocate 尝试把 req 分配到某个健康节点，成功返回节点 ID。
// 采用"首个可放下"策略，遍历顺序固定，结果可复现。
func (c *Cluster) tryAllocate(req Resources) (string, bool) {
	for _, id := range c.order {
		n := c.nodes[id]
		if !n.Healthy {
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
func (c *Cluster) failNode(nodeID string) bool {
	n, ok := c.nodes[nodeID]
	if !ok || !n.Healthy {
		return false
	}
	n.Healthy = false
	return true
}

// recoverNode 恢复节点为健康状态。
func (c *Cluster) recoverNode(nodeID string) bool {
	n, ok := c.nodes[nodeID]
	if !ok || n.Healthy {
		return false
	}
	n.Healthy = true
	return true
}

// totalFree 返回全集群健康节点的空闲资源总量。
func (c *Cluster) totalFree() Resources {
	total := Resources{}
	for _, id := range c.order {
		if n := c.nodes[id]; n.Healthy {
			total = total.Add(n.Free())
		}
	}
	return total
}

// fitsAnyNode 报告 req 是否能放入任意一个健康节点的全部容量
// （逐节点判定，用于识别"永远放不下"的作业）。
func (c *Cluster) fitsAnyNode(req Resources) bool {
	for _, id := range c.order {
		if n := c.nodes[id]; n.Healthy && req.Fits(n.Capacity) {
			return true
		}
	}
	return false
}
