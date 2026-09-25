package scheduler

// Node 是集群中的一台模拟节点。
type Node struct {
	ID        string
	Capacity  Resources
	Allocated Resources
	Healthy   bool
}

// Free 返回节点剩余可用资源。
func (n *Node) Free() Resources { return Resources{} }

// Cluster 管理一组本地模拟节点。
type Cluster struct {
	nodes map[string]*Node
	order []string // 稳定的节点遍历顺序，保证可复现
}

// NewCluster 按给定节点容量构建集群（顺序即遍历顺序）。
func NewCluster(nodes ...*Node) *Cluster { return nil }

// Node 按 ID 查询节点。
func (c *Cluster) Node(id string) (*Node, bool) { return nil, false }

// Nodes 按稳定顺序返回全部节点。
func (c *Cluster) Nodes() []*Node { return nil }

// tryAllocate 尝试把 req 分配到某个健康节点，成功返回节点 ID。
func (c *Cluster) tryAllocate(req Resources) (string, bool) { return "", false }

// release 释放节点上的资源；重复释放会被忽略并返回 false。
func (c *Cluster) release(nodeID string, req Resources) bool { return false }

// failNode 将节点标记为故障并清空其占用，返回被清空的占用量。
func (c *Cluster) failNode(nodeID string) (Resources, bool) { return Resources{}, false }

// recoverNode 恢复节点为健康状态。
func (c *Cluster) recoverNode(nodeID string) bool { return false }

// totalFree 返回全集群健康节点的空闲资源总量。
func (c *Cluster) totalFree() Resources { return Resources{} }

// maxNodeCapacity 返回单节点最大容量（用于判定"永远放不下"）。
func (c *Cluster) maxNodeCapacity() Resources { return Resources{} }
