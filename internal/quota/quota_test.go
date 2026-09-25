package quota

import "testing"
import "github.com/lisama569/hpc-job-scheduler-go/internal/domain"

func TestQuotaEnforcementAndRelease(t *testing.T) {
	l := NewLedger()
	if !l.CanAllocate("nobody", domain.Resource{CPU: 999}) {
		t.Fatal("owners without quota are unlimited")
	}
	l.SetQuota("a", domain.Resource{CPU: 10})
	if !l.CanAllocate("a", domain.Resource{CPU: 10}) {
		t.Fatal("exact quota should fit")
	}
	l.Allocate("a", domain.Resource{CPU: 7})
	if l.CanAllocate("a", domain.Resource{CPU: 4}) {
		t.Fatal("7+4 exceeds quota")
	}
	l.Release("a", domain.Resource{CPU: 7})
	if u := l.Usage("a"); u.CPU != 0 {
		t.Fatalf("usage after release: %d", u.CPU)
	}
}

func TestOwnersAccountedIndependently(t *testing.T) {
	l := NewLedger()
	l.SetQuota("a", domain.Resource{CPU: 5})
	l.SetQuota("b", domain.Resource{CPU: 5})
	l.Allocate("a", domain.Resource{CPU: 5})
	if !l.CanAllocate("b", domain.Resource{CPU: 5}) {
		t.Fatal("owner b quota must be independent of saturated owner a")
	}
	// Over-release clamps to zero instead of going negative.
	l.Release("b", domain.Resource{CPU: 2})
	if u := l.Usage("b"); u.CPU != 0 {
		t.Fatalf("expected clamp to 0, got %d", u.CPU)
	}
}
