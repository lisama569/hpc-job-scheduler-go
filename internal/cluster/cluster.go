// Package cluster models the simulated node fleet: capacity accounting and
// deterministic node-failure injection.
package cluster

import (
	"fmt"
	"sort"

	"github.com/lisama569/hpc-job-scheduler-go/internal/domain"
)

// Cluster owns the nodes and their usage counters. It is intentionally
// allocation-focussed: the scheduler keeps the authoritative task placement
// index, while the cluster is the single source of truth for resource
// counters, which prevents double counting after failure recovery.
type Cluster struct {
	nodes map[string]*domain.Node
	order []string
}

// New creates an empty Cluster.
func New() *Cluster {
	return &Cluster{nodes: make(map[string]*domain.Node)}
}

// AddNode registers a node. Duplicate IDs are rejected.
func (c *Cluster) AddNode(n domain.Node) error {
	if _, ok := c.nodes[n.ID]; ok {
		return fmt.Errorf("cluster: duplicate node id %q", n.ID)
	}
	cp := n
	c.nodes[n.ID] = &cp
	c.order = append(c.order, n.ID)
	return nil
}

func (c *Cluster) node(id string) (*domain.Node, error) {
	n, ok := c.nodes[id]
	if !ok {
		return nil, fmt.Errorf("cluster: unknown node %q", id)
	}
	return n, nil
}

// CanFit reports whether req fits in the node's *free* capacity right now.
func (c *Cluster) CanFit(nodeID string, req domain.Resource) bool {
	n, err := c.node(nodeID)
	if err != nil || n.State != domain.NodeUp {
		return false
	}
	return n.Free().Fits(req)
}

// HasAnyNodeThatFits reports whether any up node could ever host req based on
// its static capacity (used resources are ignored). This distinguishes a
// permanent "exceeds node capacity" refusal from a transient shortage.
func (c *Cluster) HasAnyNodeThatFits(req domain.Resource) bool {
	for _, id := range c.order {
		n := c.nodes[id]
		if n.State == domain.NodeUp && n.Capacity.Fits(req) {
			return true
		}
	}
	return false
}

// Allocate reserves req on nodeID. It is atomic: an overflow returns false and
// leaves the node untouched.
func (c *Cluster) Allocate(nodeID string, req domain.Resource) bool {
	n, err := c.node(nodeID)
	if err != nil || n.State != domain.NodeUp {
		return false
	}
	if !n.Free().Fits(req) {
		return false
	}
	n.Used = n.Used.Add(req)
	return true
}

// Release frees req from nodeID. It tolerates calls on down nodes because
// failure recovery may release tasks whose node is already marked down.
func (c *Cluster) Release(nodeID string, req domain.Resource) {
	n, err := c.node(nodeID)
	if err != nil {
		return
	}
	n.Used = n.Used.Sub(req)
	if n.Used.CPU < 0 || n.Used.Memory < 0 || n.Used.GPU < 0 {
		// Defensive: accounting must never drift negative.
		panic(fmt.Sprintf("cluster: negative usage on node %q: %v", nodeID, n.Used))
	}
}

// FailNode marks the node down and zeroes its usage so resources are never
// double-counted while the scheduler requeues the displaced jobs. Any further
// allocation against it fails until RestoreNode.
func (c *Cluster) FailNode(nodeID string) {
	if n, err := c.node(nodeID); err == nil {
		n.State = domain.NodeDown
		n.Used = domain.Resource{}
	}
}

// RestoreNode brings a node back up (empty at restore time).
func (c *Cluster) RestoreNode(nodeID string) {
	if n, err := c.node(nodeID); err == nil {
		n.State = domain.NodeUp
		n.Used = domain.Resource{}
	}
}

// Snapshot returns a deep-copied node listing in stable (sorted id) order.
func (c *Cluster) Snapshot() []domain.Node {
	ids := append([]string(nil), c.order...)
	sort.Strings(ids)
	out := make([]domain.Node, 0, len(ids))
	for _, id := range ids {
		out = append(out, *c.nodes[id])
	}
	return out
}

// NodeIDs returns node IDs in registration order.
func (c *Cluster) NodeIDs() []string { return append([]string(nil), c.order...) }
