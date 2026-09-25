package scheduler

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// logJobs 打印作业输入，logDecisions 打印调度判定依据，满足可解释性要求。
func logJobs(t *testing.T, jobs ...*Job) {
	t.Helper()
	for _, j := range jobs {
		t.Logf("job input: id=%s owner=%s prio=%d req=%+v deps=%v group=%q arrayIdx=%d maxRetries=%d",
			j.ID, j.Owner, j.Priority, j.Req, j.DependsOn, j.GroupID, j.ArrayIndex, j.MaxRetries)
	}
}

func logDecisions(t *testing.T, ds []Decision) {
	t.Helper()
	for _, d := range ds {
		t.Logf("decision: job=%s action=%s reason=%s", d.JobID, d.Action, d.Reason)
	}
}

// checkConsistency 校验对外可见状态的一致性：
// 配额 == 各 owner 运行中作业需求之和；节点占用 == 落在其上运行中作业需求之和。
func checkConsistency(t *testing.T, s *Scheduler, c *Cluster) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()

	wantQuota := map[string]Resources{}
	wantNode := map[string]Resources{}
	for _, j := range s.jobs {
		if j.State == StateRunning {
			wantQuota[j.Owner] = wantQuota[j.Owner].Add(j.Req)
			wantNode[j.AssignedNode] = wantNode[j.AssignedNode].Add(j.Req)
		}
	}
	for owner, q := range s.quota {
		if want := wantQuota[owner]; want != q {
			t.Errorf("quota mismatch for %q: recorded %+v, want %+v", owner, q, want)
		}
	}
	for _, n := range c.Nodes() {
		if want := wantNode[n.ID]; want != n.Allocated {
			t.Errorf("node %s allocation mismatch: recorded %+v, want %+v", n.ID, n.Allocated, want)
		}
	}
}

func twoNodeCluster() *Cluster {
	return NewCluster(
		&Node{ID: "n1", Capacity: Resources{CPU: 4, MemMB: 4096}},
		&Node{ID: "n2", Capacity: Resources{CPU: 4, MemMB: 4096}},
	)
}

func TestRejectExceedsCapacity(t *testing.T) {
	s := New(twoNodeCluster(), nil)
	big := &Job{ID: "big", Owner: "alice", Req: Resources{CPU: 8}, ArrayIndex: -1}
	ok := &Job{ID: "ok", Owner: "alice", Req: Resources{CPU: 4}, ArrayIndex: -1}
	logJobs(t, big, ok)

	if err := s.Submit(big); err != nil {
		t.Fatalf("submit big: %v", err)
	}
	j, _ := s.Job("big")
	t.Logf("verdict: state=%s reason=%s", j.State, j.RejectWhy)
	if j.State != StateRejected || j.RejectWhy != RejectExceedsCapacity {
		t.Fatalf("want Rejected/ExceedsCapacity, got %s/%s", j.State, j.RejectWhy)
	}

	if err := s.Submit(ok); err != nil {
		t.Fatalf("submit ok: %v", err)
	}
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("ok"); j.State != StateRunning {
		t.Fatalf("ok job should run, got %s", j.State)
	}
}

func TestDependencyRejectionAndChain(t *testing.T) {
	s := New(twoNodeCluster(), nil)
	ghost := &Job{ID: "ghost", Owner: "a", DependsOn: []string{"missing"}, Req: Resources{CPU: 1}, ArrayIndex: -1}
	self := &Job{ID: "self", Owner: "a", DependsOn: []string{"self"}, Req: Resources{CPU: 1}, ArrayIndex: -1}
	jA := &Job{ID: "A", Owner: "a", Req: Resources{CPU: 1}, ArrayIndex: -1}
	jB := &Job{ID: "B", Owner: "a", DependsOn: []string{"A"}, Req: Resources{CPU: 1}, ArrayIndex: -1}
	logJobs(t, ghost, self, jA, jB)

	s.Submit(ghost)
	s.Submit(self)
	if j, _ := s.Job("ghost"); j.RejectWhy != RejectDependencyUnsatisfiable {
		t.Fatalf("ghost: want DependencyUnsatisfiable, got %s", j.RejectWhy)
	}
	if j, _ := s.Job("self"); j.RejectWhy != RejectDependencyCycle {
		t.Fatalf("self: want DependencyCycle, got %s", j.RejectWhy)
	}

	s.Submit(jA)
	s.Submit(jB)
	ds := s.RunCycle()
	logDecisions(t, ds)
	if j, _ := s.Job("A"); j.State != StateRunning {
		t.Fatalf("A should run, got %s", j.State)
	}
	if j, _ := s.Job("B"); j.State != StatePending {
		t.Fatalf("B should wait for A, got %s", j.State)
	}
	if err := s.Complete("A"); err != nil {
		t.Fatalf("complete A: %v", err)
	}
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("B"); j.State != StateRunning {
		t.Fatalf("B should run after A succeeded, got %s", j.State)
	}
}

func TestInsufficientResourcesWaitsNotPartial(t *testing.T) {
	s := New(twoNodeCluster(), nil)
	j1 := &Job{ID: "j1", Owner: "a", Req: Resources{CPU: 4}, ArrayIndex: -1}
	j2 := &Job{ID: "j2", Owner: "a", Req: Resources{CPU: 4}, ArrayIndex: -1}
	j3 := &Job{ID: "j3", Owner: "a", Req: Resources{CPU: 4}, ArrayIndex: -1}
	logJobs(t, j1, j2, j3)
	s.Submit(j1)
	s.Submit(j2)
	s.Submit(j3)

	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("j3"); j.State != StatePending {
		t.Fatalf("j3 should wait (only 2 nodes), got %s", j.State)
	}
	if got := s.Pending(); len(got) != 1 || got[0] != "j3" {
		t.Fatalf("pending should be [j3], got %v", got)
	}
	s.Complete("j1")
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("j3"); j.State != StateRunning {
		t.Fatalf("j3 should run after j1 completed, got %s", j.State)
	}
	checkConsistency(t, s, s.cluster)
}

func TestGangSchedulingAllOrNothing(t *testing.T) {
	c := twoNodeCluster()
	s := New(c, nil)
	// 先占满 n1，使整组无法同时放下，验证回滚不留残留。
	blocker := &Job{ID: "blocker", Owner: "x", Req: Resources{CPU: 2}, ArrayIndex: -1}
	g1 := &Job{ID: "g1", Owner: "g", GroupID: "gang", Req: Resources{CPU: 3}, ArrayIndex: -1}
	g2 := &Job{ID: "g2", Owner: "g", GroupID: "gang", Req: Resources{CPU: 3}, ArrayIndex: -1}
	logJobs(t, blocker, g1, g2)
	s.Submit(blocker)
	logDecisions(t, s.RunCycle()) // blocker 落在 n1

	s.Submit(g1)
	s.Submit(g2)
	freeBefore := c.totalFree()
	ds := s.RunCycle()
	logDecisions(t, ds)

	for _, id := range []string{"g1", "g2"} {
		if j, _ := s.Job(id); j.State != StatePending {
			t.Fatalf("gang member %s should wait, got %s", id, j.State)
		}
	}
	if got := c.totalFree(); got != freeBefore {
		t.Fatalf("gang rollback leaked resources: free before %+v, after %+v", freeBefore, got)
	}
	if q := s.QuotaUsed("g"); !q.IsZero() {
		t.Fatalf("gang rollback leaked quota: %+v", q)
	}

	// 释放 blocker 后整组应同时获得资源。
	s.Complete("blocker")
	logDecisions(t, s.RunCycle())
	for _, id := range []string{"g1", "g2"} {
		if j, _ := s.Job(id); j.State != StateRunning {
			t.Fatalf("gang member %s should run, got %s", id, j.State)
		}
	}
	checkConsistency(t, s, c)
}

func TestJobArrayQuotaPerOwner(t *testing.T) {
	c := NewCluster(&Node{ID: "n1", Capacity: Resources{CPU: 8, MemMB: 8192}})
	limits := map[string]Resources{"alice": {CPU: 4, MemMB: 4096}}
	s := New(c, limits)

	tmpl := Job{Priority: 1, Req: Resources{CPU: 2, MemMB: 2048}, ArrayIndex: -1}
	ids, err := s.SubmitArray("alice", "arr", 4, tmpl)
	if err != nil {
		t.Fatalf("submit array: %v", err)
	}
	t.Logf("array submitted: %v, per-element req=%+v, owner quota limit=%+v", ids, tmpl.Req, limits["alice"])

	logDecisions(t, s.RunCycle())
	// 配额 4 CPU，每元素 2 CPU：恰好 2 个运行，2 个因配额等待。
	running, waiting := 0, 0
	for _, id := range ids {
		j, _ := s.Job(id)
		t.Logf("element %s: state=%s", id, j.State)
		switch j.State {
		case StateRunning:
			running++
		case StatePending:
			waiting++
		}
	}
	if running != 2 || waiting != 2 {
		t.Fatalf("want 2 running + 2 waiting, got %d running + %d waiting", running, waiting)
	}
	if q := s.QuotaUsed("alice"); q.CPU != 4 {
		t.Fatalf("alice quota should be 4 CPU, got %+v", q)
	}

	// 元素互不影响：完成一个，另一个等待者补上，其余状态不变。
	s.Complete(ids[0])
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job(ids[0]); j.State != StateSucceeded {
		t.Fatalf("element 0 should be succeeded, got %s", j.State)
	}
	if q := s.QuotaUsed("alice"); q.CPU != 4 {
		t.Fatalf("alice quota should stay 4 CPU after refill, got %+v", q)
	}
	checkConsistency(t, s, c)
}

func TestNodeFailureRequeueNoDoubleCount(t *testing.T) {
	c := twoNodeCluster()
	s := New(c, nil)
	j := &Job{ID: "victim", Owner: "a", Req: Resources{CPU: 2}, MaxRetries: 1, ArrayIndex: -1}
	logJobs(t, j)
	s.Submit(j)
	logDecisions(t, s.RunCycle())

	jj, _ := s.Job("victim")
	node := jj.AssignedNode
	t.Logf("victim running on %s", node)

	ds, err := s.FailNode(node)
	if err != nil {
		t.Fatalf("fail node: %v", err)
	}
	logDecisions(t, ds)
	jj, _ = s.Job("victim")
	if jj.State != StatePending || jj.Retries != 1 {
		t.Fatalf("victim should be requeued with 1 retry, got %s retries=%d", jj.State, jj.Retries)
	}
	if q := s.QuotaUsed("a"); !q.IsZero() {
		t.Fatalf("quota double-counted after failure: %+v", q)
	}
	for _, n := range c.Nodes() {
		if !n.Allocated.IsZero() {
			t.Fatalf("node %s still holds %+v after failure release", n.ID, n.Allocated)
		}
	}

	// 重新调度到健康节点，再次故障 → 重试耗尽 → Failed。
	logDecisions(t, s.RunCycle())
	jj, _ = s.Job("victim")
	if jj.State != StateRunning {
		t.Fatalf("victim should be rescheduled, got %s", jj.State)
	}
	ds, _ = s.FailNode(jj.AssignedNode)
	logDecisions(t, ds)
	if jj, _ = s.Job("victim"); jj.State != StateFailed {
		t.Fatalf("victim should be Failed after retries exhausted, got %s", jj.State)
	}
	if q := s.QuotaUsed("a"); !q.IsZero() {
		t.Fatalf("quota leaked after final failure: %+v", q)
	}
	checkConsistency(t, s, c)
}

func TestPreemptionDeterministicAndBounded(t *testing.T) {
	c := NewCluster(&Node{ID: "n1", Capacity: Resources{CPU: 4, MemMB: 4096}})
	s := New(c, nil)
	l1 := &Job{ID: "l1", Owner: "low", Priority: 1, Req: Resources{CPU: 2}, MaxRetries: 1, ArrayIndex: -1}
	l2 := &Job{ID: "l2", Owner: "low", Priority: 2, Req: Resources{CPU: 2}, MaxRetries: 1, ArrayIndex: -1}
	hi := &Job{ID: "hi", Owner: "high", Priority: 9, Req: Resources{CPU: 2}, ArrayIndex: -1}
	logJobs(t, l1, l2, hi)
	s.Submit(l1)
	s.Submit(l2)
	logDecisions(t, s.RunCycle())

	s.Submit(hi)
	ds := s.RunCycle()
	logDecisions(t, ds)
	// 确定规则：优先级最低者先被抢占；l1(1) < l2(2)，只抢占 l1。
	if j, _ := s.Job("hi"); j.State != StateRunning {
		t.Fatalf("hi should run, got %s", j.State)
	}
	if j, _ := s.Job("l1"); j.State != StatePending || j.Retries != 1 {
		t.Fatalf("l1 should be preempted and requeued, got %v retries=%d", j.State, j.Retries)
	}
	if j, _ := s.Job("l2"); j.State != StateRunning {
		t.Fatalf("l2 should keep running, got %s", j.State)
	}
	checkConsistency(t, s, c)

	// hi 完成后 l1 恢复运行；再次故障/抢占将耗尽其重试。
	s.Complete("hi")
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("l1"); j.State != StateRunning {
		t.Fatalf("l1 should resume, got %s", j.State)
	}
	ds, _ = s.FailNode("n1")
	logDecisions(t, ds)
	if j, _ := s.Job("l1"); j.State != StateFailed {
		t.Fatalf("l1 retries exhausted, want Failed, got %s", j.State)
	}
	checkConsistency(t, s, c)
}

func TestPreemptionRollbackWhenStillNotFitting(t *testing.T) {
	c := NewCluster(&Node{ID: "n1", Capacity: Resources{CPU: 4, MemMB: 4096}})
	s := New(c, nil)
	mid := &Job{ID: "mid", Owner: "m", Priority: 9, Req: Resources{CPU: 2}, ArrayIndex: -1}
	low := &Job{ID: "low", Owner: "l", Priority: 1, Req: Resources{CPU: 2}, ArrayIndex: -1}
	huge := &Job{ID: "huge", Owner: "h", Priority: 9, Req: Resources{CPU: 4}, ArrayIndex: -1}
	logJobs(t, mid, low, huge)
	s.Submit(mid)
	s.Submit(low)
	logDecisions(t, s.RunCycle())

	s.Submit(huge)
	ds := s.RunCycle()
	logDecisions(t, ds)
	// 抢占 low 后仍只有 2 CPU 空闲（mid 同级不可抢），huge 等待且 low 必须原样恢复。
	if j, _ := s.Job("huge"); j.State != StatePending {
		t.Fatalf("huge should wait, got %s", j.State)
	}
	if j, _ := s.Job("low"); j.State != StateRunning {
		t.Fatalf("low should be restored after rollback, got %s", j.State)
	}
	n, _ := c.Node("n1")
	if n.Allocated.CPU != 4 {
		t.Fatalf("rollback should restore full allocation, got %+v", n.Allocated)
	}
	checkConsistency(t, s, c)
}

func TestConcurrentConsistency(t *testing.T) {
	c := NewCluster(
		&Node{ID: "n1", Capacity: Resources{CPU: 8, MemMB: 8192}},
		&Node{ID: "n2", Capacity: Resources{CPU: 8, MemMB: 8192}},
	)
	s := New(c, nil)

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(7 + w))) // 每个 goroutine 独立随机源
			for i := 0; i < 10; i++ {
				j := &Job{
					ID:       fmt.Sprintf("w%d-j%d", w, i),
					Owner:    fmt.Sprintf("owner-%d", w%2),
					Priority: rng.Intn(5),
					Req:      Resources{CPU: 1 + rng.Intn(2)},
					ArrayIndex: -1,
				}
				_ = s.Submit(j)
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			s.RunCycle()
		}
	}()
	wg.Wait()

	// 完成所有运行中的作业，反复收敛到静止状态。
	for round := 0; round < 20; round++ {
		s.RunCycle()
		s.mu.Lock()
		var running []string
		for id, j := range s.jobs {
			if j.State == StateRunning {
				running = append(running, id)
			}
		}
		s.mu.Unlock()
		if len(running) == 0 && len(s.Pending()) == 0 {
			break
		}
		for _, id := range running {
			_ = s.Complete(id)
		}
	}
	checkConsistency(t, s, c)
	if p := s.Pending(); len(p) != 0 {
		t.Fatalf("no jobs should remain pending, got %v", p)
	}
	t.Logf("concurrent run converged; pending=0")
}

// TestReproducibleWithSeed 用固定随机种子生成负载，两次运行结论必须完全一致。
func TestReproducibleWithSeed(t *testing.T) {
	run := func() string {
		rng := rand.New(rand.NewSource(42))
		c := NewCluster(
			&Node{ID: "n1", Capacity: Resources{CPU: 4, MemMB: 4096}},
			&Node{ID: "n2", Capacity: Resources{CPU: 4, MemMB: 4096}},
			&Node{ID: "n3", Capacity: Resources{CPU: 2, MemMB: 2048}},
		)
		s := New(c, map[string]Resources{"alice": {CPU: 6, MemMB: 6144}})
		owners := []string{"alice", "bob"}
		for i := 0; i < 12; i++ {
			j := &Job{
				ID:         fmt.Sprintf("r-%02d", i),
				Owner:      owners[rng.Intn(2)],
				Priority:   rng.Intn(4),
				Req:        Resources{CPU: 1 + rng.Intn(3), MemMB: 1024 * (1 + rng.Intn(3))},
				MaxRetries: 1,
				ArrayIndex: -1,
			}
			_ = s.Submit(j)
		}
		out := ""
		for round := 0; round < 6; round++ {
			for _, d := range s.RunCycle() {
				out += fmt.Sprintf("%s:%s;", d.JobID, d.Action)
			}
			if round == 1 {
				ds, _ := s.FailNode("n1")
				for _, d := range ds {
					out += fmt.Sprintf("%s:%s;", d.JobID, d.Action)
				}
			}
			if round == 3 {
				_ = s.RecoverNode("n1")
			}
			// 完成一个运行中的作业（确定性选择 ID 最小者）。
			s.mu.Lock()
			best := ""
			for id, j := range s.jobs {
				if j.State == StateRunning && (best == "" || id < best) {
					best = id
				}
			}
			s.mu.Unlock()
			if best != "" {
				_ = s.Complete(best)
			}
		}
		// 汇总最终状态。
		s.mu.Lock()
		for i := 0; i < 12; i++ {
			id := fmt.Sprintf("r-%02d", i)
			out += fmt.Sprintf("%s=%s/%d;", id, s.jobs[id].State, s.jobs[id].Retries)
		}
		out += fmt.Sprintf("quota=%+v;", s.quota)
		s.mu.Unlock()
		return out
	}
	first, second := run(), run()
	t.Logf("run1: %s", first)
	t.Logf("run2: %s", second)
	if first != second {
		t.Fatalf("same seed must produce identical outcomes:\nrun1=%s\nrun2=%s", first, second)
	}
}
