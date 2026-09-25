// Package quota accounts per-owner resource entitlements independently.
package quota

import "github.com/lisama569/hpc-job-scheduler-go/internal/domain"

// Ledger tracks quotas and current usage keyed by owner. Array elements are
// independent jobs, but every element is accounted under its own owner, so an
// array's quota always rolls up independently per owner.
type Ledger struct {
	quota map[string]domain.Resource
	usage map[string]domain.Resource
}

// NewLedger creates an empty quota ledger. Owners without an explicit quota
// are treated as unlimited.
func NewLedger() *Ledger {
	return &Ledger{
		quota: make(map[string]domain.Resource),
		usage: make(map[string]domain.Resource),
	}
}

// SetQuota records an owner's entitlement.
func (l *Ledger) SetQuota(owner string, r domain.Resource) { l.quota[owner] = r }

// HasQuota reports whether the owner has an explicit entitlement.
func (l *Ledger) HasQuota(owner string) bool {
	_, ok := l.quota[owner]
	return ok
}

// Usage returns a copy of an owner's current usage.
func (l *Ledger) Usage(owner string) domain.Resource { return l.usage[owner] }

// CanAllocate reports whether adding req stays within the owner's quota.
// Owners without a configured quota are unlimited.
func (l *Ledger) CanAllocate(owner string, req domain.Resource) bool {
	q, ok := l.quota[owner]
	if !ok {
		return true
	}
	return q.Fits(l.usage[owner].Add(req))
}

// Allocate adds req to owner usage. Callers must have checked CanAllocate.
func (l *Ledger) Allocate(owner string, req domain.Resource) {
	l.usage[owner] = l.usage[owner].Add(req)
}

// Release subtracts req from owner usage; it never goes negative.
func (l *Ledger) Release(owner string, req domain.Resource) {
	u := l.usage[owner].Sub(req)
	if u.CPU < 0 || u.Memory < 0 || u.GPU < 0 {
		u = domain.Resource{}
	}
	l.usage[owner] = u
}

// SnapshotUsage returns a copy for reporting.
func (l *Ledger) SnapshotUsage() map[string]domain.Resource {
	out := make(map[string]domain.Resource, len(l.usage))
	for k, v := range l.usage {
		out[k] = v
	}
	return out
}

// SnapshotQuota returns a copy for reporting.
func (l *Ledger) SnapshotQuota() map[string]domain.Resource {
	out := make(map[string]domain.Resource, len(l.quota))
	for k, v := range l.quota {
		out[k] = v
	}
	return out
}
