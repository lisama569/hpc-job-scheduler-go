package scheduler

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// 计划内维护 / 排空行为的自动化测试。
// 所有用例复用 logJobs / logDecisions 打印作业输入与判定依据（go test -v 可见）。

func TestDrainMigratesJobToHealthyNode(t *testing.T) {
	c := twoNodeCluster()
	s := New(c, nil)
	j := &Job{ID: "j", Owner: "a", Req: Resources{CPU: 2}, ArrayIndex: -1}
	logJobs(t, j)
	s.Submit(j)
	logDecisions(t, s.RunCycle()) // first-fit：落在 n1

	jj, _ := s.Job("j")
	if jj.AssignedNode != "n1" {
		t.Fatalf("j should start on n1, got %s", jj.AssignedNode)
	}

	// 切入维护：不接收新调度，但作业先保留。
	if err := s.BeginMaintenance("n1"); err != nil {
		t.Fatalf("begin maintenance: %v", err)
	}
	if st, _ := s.NodeState("n1"); st != NodeDraining {
		t.Fatalf("n1 should be Draining, got %s", st)
	}
	if jj, _ := s.Job("j"); jj.State != StateRunning || jj.AssignedNode != "n1" {
		t.Fatalf("j must keep running on n1 after entering maintenance, got %s/%s", jj.State, jj.AssignedNode)
	}

	// 显式排空：平滑迁移到 n2，作业不中断、不重试、配额不漂移。
	ds, err := s.DrainNode("n1")
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	logDecisions(t, ds)
	if st, _ := s.NodeState("n1"); st != NodeDrained {
		t.Fatalf("n1 should be Drained, got %s", st)
	}
	if jj, _ := s.Job("j"); jj.State != StateRunning || jj.AssignedNode != "n2" {
		t.Fatalf("j should be migrated to n2 without retry, got %s/%s retries=%d",
			jj.State, jj.AssignedNode, jj.Retries)
	}
	if q := s.QuotaUsed("a"); q.CPU != 2 {
		t.Fatalf("quota must not drift on migration, got %+v", q)
	}
	if n1, _ := c.Node("n1"); !n1.Allocated.IsZero() {
		t.Fatalf("n1 must be empty after drain, holds %+v", n1.Allocated)
	}
	if n2, _ := c.Node("n2"); n2.Allocated.CPU != 2 {
		t.Fatalf("n2 should hold migrated job, got %+v", n2.Allocated)
	}
	checkConsistency(t, s, c)

	// 维护结束，n1 重新参与调度。
	if err := s.EndMaintenance("n1"); err != nil {
		t.Fatalf("end maintenance: %v", err)
	}
	if st, _ := s.NodeState("n1"); st != NodeOnline {
		t.Fatalf("n1 should be Online after maintenance, got %s", st)
	}
	checkConsistency(t, s, c)
}
func TestDrainRequeuesWhenNowhereToMigrate(t *testing.T) {
	c := twoNodeCluster()
	s := New(c, nil)
	victim := &Job{ID: "victim", Owner: "a", Priority: 5, Req: Resources{CPU: 2}, MaxRetries: 1, ArrayIndex: -1}
	blocker := &Job{ID: "blocker", Owner: "b", Priority: 1, Req: Resources{CPU: 4}, ArrayIndex: -1}
	logJobs(t, victim, blocker)
	s.Submit(victim)
	s.RunCycle() // victim -> n1
	s.Submit(blocker)
	s.RunCycle() // blocker -> n2（占满）
	t.Logf("pre-drain: victim on %s, blocker on %s (n2 full, migration must fail)",
		func() string { j, _ := s.Job("victim"); return j.AssignedNode }(),
		func() string { j, _ := s.Job("blocker"); return j.AssignedNode }())

	if err := s.BeginMaintenance("n1"); err != nil {
		t.Fatalf("begin maintenance: %v", err)
	}
	ds, err := s.DrainNode("n1")
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	logDecisions(t, ds)

	jj, _ := s.Job("victim")
	if jj.State != StatePending || jj.Retries != 1 || jj.AssignedNode != "" {
		t.Fatalf("victim should be requeued with retry 1, got %s retries=%d node=%s",
			jj.State, jj.Retries, jj.AssignedNode)
	}
	if got := s.Pending(); len(got) != 1 || got[0] != "victim" {
		t.Fatalf("pending should be [victim], got %v", got)
	}
	if q := s.QuotaUsed("a"); !q.IsZero() {
		t.Fatalf("victim quota released once, got %+v", q)
	}
	if n1, _ := c.Node("n1"); !n1.Allocated.IsZero() {
		t.Fatalf("n1 leaked %+v", n1.Allocated)
	}
	checkConsistency(t, s, c)

	// blocker 完成腾出 n2；victim 重新调度后，再次排空（此时重试已达上限）→ Failed。
	if err := s.Complete("blocker"); err != nil {
		t.Fatalf("complete blocker: %v", err)
	}
	s.RunCycle()
	jj, _ = s.Job("victim")
	if jj.State != StateRunning || jj.AssignedNode != "n2" {
		t.Fatalf("victim should resume on n2, got %s/%s", jj.State, jj.AssignedNode)
	}
	if err := s.BeginMaintenance("n2"); err != nil {
		t.Fatalf("begin maintenance n2: %v", err)
	}
	ds, _ = s.DrainNode("n2")
	logDecisions(t, ds)
	jj, _ = s.Job("victim")
	if jj.State != StateFailed {
		t.Fatalf("victim should be Failed after retries exhausted, got %s", jj.State)
	}
	checkConsistency(t, s, c)
}
func TestDrainGangMigratesTogether(t *testing.T) {
	c := NewCluster(
		&Node{ID: "n1", Capacity: Resources{CPU: 8, MemMB: 8192}},
		&Node{ID: "n2", Capacity: Resources{CPU: 8, MemMB: 8192}},
		&Node{ID: "n3", Capacity: Resources{CPU: 8, MemMB: 8192}},
	)
	s := New(c, nil)
	g1 := &Job{ID: "g1", Owner: "g", GroupID: "gang", Req: Resources{CPU: 3}, MaxRetries: 1, ArrayIndex: -1}
	g2 := &Job{ID: "g2", Owner: "g", GroupID: "gang", Req: Resources{CPU: 3}, MaxRetries: 1, ArrayIndex: -1}
	g3 := &Job{ID: "g3", Owner: "g", GroupID: "gang", Req: Resources{CPU: 3}, MaxRetries: 1, ArrayIndex: -1}
	logJobs(t, g1, g2, g3)
	s.Submit(g1)
	s.Submit(g2)
	s.Submit(g3)
	logDecisions(t, s.RunCycle()) // first-fit：g1,g2 -> n1，g3 -> n2（跨节点作业组）

	placement := map[string]string{}
	for _, id := range []string{"g1", "g2", "g3"} {
		j, _ := s.Job(id)
		placement[id] = j.AssignedNode
	}
	t.Logf("pre-drain placement: %v", placement)
	if placement["g1"] != "n1" || placement["g2"] != "n1" || placement["g3"] != "n2" {
		t.Fatalf("unexpected initial placement: %v", placement)
	}

	if err := s.BeginMaintenance("n1"); err != nil {
		t.Fatalf("begin maintenance: %v", err)
	}
	ds, err := s.DrainNode("n1")
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	logDecisions(t, ds)

	// 整组一起搬：三个成员都必须保持 Running、都不能落在 n1、都不消耗重试。
	for _, id := range []string{"g1", "g2", "g3"} {
		j, _ := s.Job(id)
		if j.State != StateRunning {
			t.Fatalf("gang member %s must stay Running after whole-group migration, got %s", id, j.State)
		}
		if j.AssignedNode == "n1" {
			t.Fatalf("gang member %s must not remain on drained n1", id)
		}
		if j.Retries != 0 {
			t.Fatalf("gang member %s should not spend retries on smooth migration, got %d", id, j.Retries)
		}
	}
	if q := s.QuotaUsed("g"); q.CPU != 9 {
		t.Fatalf("gang quota unchanged at 9 CPU, got %+v", q)
	}
	if n1, _ := c.Node("n1"); !n1.Allocated.IsZero() {
		t.Fatalf("drained n1 leaked %+v", n1.Allocated)
	}
	checkConsistency(t, s, c)
}
func TestDrainGangWaitsTogether(t *testing.T) {
	c := NewCluster(
		&Node{ID: "n1", Capacity: Resources{CPU: 8, MemMB: 8192}},
		&Node{ID: "n2", Capacity: Resources{CPU: 8, MemMB: 8192}},
	)
	s := New(c, nil)
	// 整组共需 10 CPU，必须跨两台节点；任一节点离线后单台 8 CPU 放不下整组。
	g1 := &Job{ID: "g1", Owner: "g", GroupID: "gang", Req: Resources{CPU: 5}, MaxRetries: 2, ArrayIndex: -1}
	g2 := &Job{ID: "g2", Owner: "g", GroupID: "gang", Req: Resources{CPU: 5}, MaxRetries: 2, ArrayIndex: -1}
	logJobs(t, g1, g2)
	s.Submit(g1)
	s.Submit(g2)
	logDecisions(t, s.RunCycle()) // g1 -> n1, g2 -> n2（跨节点作业组）

	if j, _ := s.Job("g1"); j.AssignedNode != "n1" {
		t.Fatalf("g1 expected on n1, got %s", j.AssignedNode)
	}
	if j, _ := s.Job("g2"); j.AssignedNode != "n2" {
		t.Fatalf("g2 expected on n2, got %s", j.AssignedNode)
	}

	if err := s.BeginMaintenance("n1"); err != nil {
		t.Fatalf("begin maintenance: %v", err)
	}
	ds, err := s.DrainNode("n1")
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	logDecisions(t, ds)

	// 整组一起等：不允许一半运行一半排队；两个成员都 Pending、重试结论一致。
	for _, id := range []string{"g1", "g2"} {
		j, _ := s.Job(id)
		if j.State != StatePending {
			t.Fatalf("gang member %s must wait with the whole group, got %s", id, j.State)
		}
		if j.AssignedNode != "" {
			t.Fatalf("gang member %s must not hold a node while waiting", id)
		}
		if j.Retries != 1 {
			t.Fatalf("gang member %s retry must be 1/2 uniformly, got %d", id, j.Retries)
		}
	}
	if q := s.QuotaUsed("g"); !q.IsZero() {
		t.Fatalf("group quota must be released once when requeued, got %+v", q)
	}
	n1, _ := c.Node("n1")
	if !n1.Allocated.IsZero() {
		t.Fatalf("drained n1 leaked %+v", n1.Allocated)
	}
	checkConsistency(t, s, c)

	// n1 仍在下线，单台 n2（8 CPU）放不下整组（10 CPU），继续等。
	logDecisions(t, s.RunCycle())
	for _, id := range []string{"g1", "g2"} {
		if j, _ := s.Job(id); j.State != StatePending {
			t.Fatalf("gang member %s still waits while n1 offline, got %s", id, j.State)
		}
	}

	// 维护结束 n1 回来，下一轮整组同时恢复运行。
	if err := s.EndMaintenance("n1"); err != nil {
		t.Fatalf("end maintenance: %v", err)
	}
	logDecisions(t, s.RunCycle())
	for _, id := range []string{"g1", "g2"} {
		if j, _ := s.Job(id); j.State != StateRunning {
			t.Fatalf("gang member %s should run together after n1 returns, got %s", id, j.State)
		}
	}
	if q := s.QuotaUsed("g"); q.CPU != 10 {
		t.Fatalf("group quota should be back to 10 CPU, got %+v", q)
	}
	checkConsistency(t, s, c)
}
func TestSubmitDuringMaintenanceRescheduleAfter(t *testing.T) {
	c := NewCluster(
		&Node{ID: "n1", Capacity: Resources{CPU: 8, MemMB: 8192}},
		&Node{ID: "n2", Capacity: Resources{CPU: 8, MemMB: 8192}},
	)
	limits := map[string]Resources{"alice": {CPU: 4}}
	s := New(c, limits)

	// n1 空节点切入维护：维护中的节点不算可用容量，只剩 n2(8 CPU)。
	if err := s.BeginMaintenance("n1"); err != nil {
		t.Fatalf("begin maintenance: %v", err)
	}
	runner := &Job{ID: "runner", Owner: "bob", Req: Resources{CPU: 8}, ArrayIndex: -1}
	a := &Job{ID: "a", Owner: "alice", Req: Resources{CPU: 3}, ArrayIndex: -1}
	b := &Job{ID: "b", Owner: "alice", Req: Resources{CPU: 3}, ArrayIndex: -1} // 受配额限制
	dep := &Job{ID: "dep", Owner: "bob", DependsOn: []string{"runner"}, Req: Resources{CPU: 1}, ArrayIndex: -1}
	big := &Job{ID: "big", Owner: "bob", Req: Resources{CPU: 6}, ArrayIndex: -1} // 单节点物理可容纳；维护窗口内无空位故等待，不应被拒
	logJobs(t, runner, a, b, dep, big)

	s.Submit(runner)
	s.Submit(a)
	s.Submit(b)
	s.Submit(dep)
	if err := s.Submit(big); err != nil {
		t.Fatalf("submit big: %v", err)
	}
	logDecisions(t, s.RunCycle())

	// runner 占满唯一在线节点 n2；a 无容量等待；b 还叠加配额等待；
	// dep 依赖未满足等待；big 不被永久拒绝（维护是暂时的）。
	if j, _ := s.Job("runner"); j.State != StateRunning || j.AssignedNode != "n2" {
		t.Fatalf("runner should run on the only online node n2, got %s/%s", j.State, j.AssignedNode)
	}
	for _, id := range []string{"a", "b", "dep", "big"} {
		if j, _ := s.Job(id); j.State != StatePending {
			t.Fatalf("%s should be Pending during maintenance, got %s", id, j.State)
		}
	}
	if j, _ := s.Job("big"); j.RejectWhy != RejectNone {
		t.Fatalf("big must not be permanently rejected for a temporary maintenance, got %s", j.RejectWhy)
	}
	if q := s.QuotaUsed("bob"); q.CPU != 8 {
		t.Fatalf("bob quota should be 8 (runner), got %+v", q)
	}

	// runner 完成：容量腾出但 n1 仍离线，alice 配额 4 → a 运行，b 因配额等待；
	// dep 依赖满足随后可运行；big(6) 受当前空位限制持续等待（不拒）。
	if err := s.Complete("runner"); err != nil {
		t.Fatalf("complete runner: %v", err)
	}
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("a"); j.State != StateRunning {
		t.Fatalf("a should run within quota, got %s", j.State)
	}
	if j, _ := s.Job("b"); j.State != StatePending {
		t.Fatalf("b should wait on alice quota (4), got %s", j.State)
	}
	if j, _ := s.Job("dep"); j.State != StateRunning {
		t.Fatalf("dep should run after dependency succeeded, got %s", j.State)
	}

	// n1 本来就空，排空后下线，再结束维护恢复。
	ds, err := s.DrainNode("n1")
	if err != nil {
		t.Fatalf("drain empty n1: %v", err)
	}
	logDecisions(t, ds)
	if st, _ := s.NodeState("n1"); st != NodeDrained {
		t.Fatalf("n1 should be Drained, got %s", st)
	}
	if err := s.EndMaintenance("n1"); err != nil {
		t.Fatalf("end maintenance: %v", err)
	}

	// a 完成释放配额，随后 b 可在恢复的 n1 上运行；big 仍超单节点容量而等待。
	if err := s.Complete("a"); err != nil {
		t.Fatalf("complete a: %v", err)
	}
	if err := s.Complete("dep"); err != nil {
		t.Fatalf("complete dep: %v", err)
	}
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("b"); j.State != StateRunning {
		t.Fatalf("b should be rescheduled after node return and quota release, got %s", j.State)
	}
	// 恢复的容量让维护窗口内一直等待的 big 也能被调度（证明未被永久拒绝）。
	if j, _ := s.Job("big"); j.State != StateRunning {
		t.Fatalf("big should run once capacity returns, got %s", j.State)
	}
	checkConsistency(t, s, c)
}
func TestDrainNoDoubleCount(t *testing.T) {
	c := NewCluster(
		&Node{ID: "n1", Capacity: Resources{CPU: 8, MemMB: 8192}},
		&Node{ID: "n2", Capacity: Resources{CPU: 8, MemMB: 8192}},
		&Node{ID: "n3", Capacity: Resources{CPU: 8, MemMB: 8192}},
	)
	s := New(c, nil)

	// 可平滑迁移的独立作业
	soloA := &Job{ID: "soloA", Owner: "alice", Req: Resources{CPU: 2}, ArrayIndex: -1}
	soloB := &Job{ID: "soloB", Owner: "bob", Req: Resources{CPU: 3}, ArrayIndex: -1}
	// 整组（成员同属 carol），排空时会被一起搬走
	ga := &Job{ID: "ga", Owner: "carol", GroupID: "grp", Req: Resources{CPU: 3}, MaxRetries: 2, ArrayIndex: -1}
	gb := &Job{ID: "gb", Owner: "carol", GroupID: "grp", Req: Resources{CPU: 3}, MaxRetries: 2, ArrayIndex: -1}
	logJobs(t, soloA, soloB, ga, gb)
	s.Submit(soloA)
	s.Submit(soloB)
	s.Submit(ga)
	s.Submit(gb)
	logDecisions(t, s.RunCycle())

	// 确认四者都落在 n1（first-fit 装箱：2+3+3+3=11 > 8，可能有一个溢出；
	// 用一个明确的前置占满作业不如直接按实际布局断言）。
	t.Logf("placement: soloA=%s soloB=%s ga=%s gb=%s",
		func() string { j, _ := s.Job("soloA"); return j.AssignedNode }(),
		func() string { j, _ := s.Job("soloB"); return j.AssignedNode }(),
		func() string { j, _ := s.Job("ga"); return j.AssignedNode }(),
		func() string { j, _ := s.Job("gb"); return j.AssignedNode }())

	// 记录排空前期望总量，排空后集群总占用必须保持不变（迁移）+
	// 被退回作业占用归零，且全集群无重复计数。
	usedBefore := Resources{CPU: 11}

	if err := s.BeginMaintenance("n1"); err != nil {
		t.Fatalf("begin maintenance: %v", err)
	}
	ds, err := s.DrainNode("n1")
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	logDecisions(t, ds)

	// 每个节点的占用必须与其上 Running 作业一致（checkConsistency 已覆盖），
	// 这里额外断言：n1 归零；所有作业的 AssignedNode 都不指向 n1。
	n1, _ := c.Node("n1")
	if !n1.Allocated.IsZero() {
		t.Fatalf("n1 must be fully released, got %+v", n1.Allocated)
	}
	for _, id := range []string{"soloA", "soloB", "ga", "gb"} {
		if j, _ := s.Job(id); j.State == StateRunning && j.AssignedNode == "n1" {
			t.Fatalf("running job %s still points at drained n1", id)
		}
	}
	// 配额恒等式：所有 owner 配额之和 == 所有 Running 作业需求之和 == 全部节点占用之和。
	checkConsistency(t, s, c)
	s.mu.Lock()
	var quotaSum, nodeSum Resources
	for _, q := range s.quota {
		quotaSum = quotaSum.Add(q)
	}
	s.mu.Unlock()
	for _, n := range c.Nodes() {
		nodeSum = nodeSum.Add(n.Allocated)
	}
	if quotaSum != nodeSum {
		t.Fatalf("quota total %+v != node allocation total %+v", quotaSum, nodeSum)
	}
	t.Logf("after drain: quota total=%+v, node total=%+v (before total in-use %+v)",
		quotaSum, nodeSum, usedBefore)

	// 再来一轮调度，反复确认迁移/退回后不存在超卖或重复计数。
	logDecisions(t, s.RunCycle())
	checkConsistency(t, s, c)
}
func TestMaintenanceFailDuringDrainRecoversToDrained(t *testing.T) {
	c := twoNodeCluster()
	s := New(c, nil)
	j := &Job{ID: "j", Owner: "a", Req: Resources{CPU: 2}, MaxRetries: 2, ArrayIndex: -1}
	logJobs(t, j)
	s.Submit(j)
	logDecisions(t, s.RunCycle()) // j -> n1

	// 切入维护后，节点在排空之前发生故障（计划外）。
	if err := s.BeginMaintenance("n1"); err != nil {
		t.Fatalf("begin maintenance: %v", err)
	}
	ds, err := s.FailNode("n1")
	if err != nil {
		t.Fatalf("fail node during maintenance: %v", err)
	}
	logDecisions(t, ds)
	if st, _ := s.NodeState("n1"); st != NodeFailed {
		t.Fatalf("n1 should be Failed, got %s", st)
	}

	// 故障期间不允许排空（必须先恢复）。
	if _, err := s.DrainNode("n1"); err == nil {
		t.Fatalf("drain on a failed node must be rejected")
	}

	// 恢复：维护意图保留 → 回到 Drained（不是 Online，避免提前接收调度）。
	if err := s.RecoverNode("n1"); err != nil {
		t.Fatalf("recover node: %v", err)
	}
	if st, _ := s.NodeState("n1"); st != NodeDrained {
		t.Fatalf("n1 should recover to Drained (maintenance intent kept), got %s", st)
	}
	// Drained 节点不能再排空，但可以结束维护回到在线。
	if _, err := s.DrainNode("n1"); err == nil {
		t.Fatalf("drain on an already drained node must be rejected")
	}
	if err := s.EndMaintenance("n1"); err != nil {
		t.Fatalf("end maintenance: %v", err)
	}
	if st, _ := s.NodeState("n1"); st != NodeOnline {
		t.Fatalf("n1 should be Online after ending maintenance, got %s", st)
	}

	// 故障时 j 已退回；节点回归后可重新调度。
	logDecisions(t, s.RunCycle())
	if jj, _ := s.Job("j"); jj.State != StateRunning {
		t.Fatalf("j should resume after node returns, got %s", jj.State)
	}
	checkConsistency(t, s, c)
}
func TestMaintenanceCancelAndEndTransitions(t *testing.T) {
	c := NewCluster(&Node{ID: "n1", Capacity: Resources{CPU: 4, MemMB: 4096}})
	s := New(c, nil)
	j := &Job{ID: "j", Owner: "a", Req: Resources{CPU: 2}, MaxRetries: 2, ArrayIndex: -1}
	logJobs(t, j)
	s.Submit(j)
	s.RunCycle() // j -> n1

	// 取消维护（Draining 阶段）：节点立即回到在线，原有作业保留、不中断。
	if err := s.BeginMaintenance("n1"); err != nil {
		t.Fatalf("begin maintenance: %v", err)
	}
	if err := s.CancelMaintenance("n1"); err != nil {
		t.Fatalf("cancel maintenance while draining: %v", err)
	}
	if st, _ := s.NodeState("n1"); st != NodeOnline {
		t.Fatalf("n1 should be Online after cancel, got %s", st)
	}
	if jj, _ := s.Job("j"); jj.State != StateRunning || jj.AssignedNode != "n1" {
		t.Fatalf("j keeps running through cancel, got %s/%s", jj.State, jj.AssignedNode)
	}

	// 排空后取消（Drained 阶段）：空节点回到在线。
	if err := s.BeginMaintenance("n1"); err != nil {
		t.Fatalf("second begin maintenance: %v", err)
	}
	logDecisions(t, func() []Decision {
		ds, err := s.DrainNode("n1") // 先把 j 迁走（无处可迁 → 退回）
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
		return ds
	}())
	if st, _ := s.NodeState("n1"); st != NodeDrained {
		t.Fatalf("n1 should be Drained, got %s", st)
	}
	if err := s.CancelMaintenance("n1"); err != nil {
		t.Fatalf("cancel maintenance while drained: %v", err)
	}
	if st, _ := s.NodeState("n1"); st != NodeOnline {
		t.Fatalf("n1 should be Online after cancel from drained, got %s", st)
	}
	logDecisions(t, s.RunCycle())
	if jj, _ := s.Job("j"); jj.State != StateRunning {
		t.Fatalf("j should run again after node back online, got %s", jj.State)
	}

	// 非法状态流转必须报错且无副作用。
	if err := s.EndMaintenance("n1"); err == nil {
		t.Fatalf("EndMaintenance on an Online node must be rejected")
	}
	if err := s.BeginMaintenance("ghost"); err == nil {
		t.Fatalf("operations on unknown node must return an error")
	}
	s.Complete("j")
	if _, err := s.FailNode("n1"); err != nil {
		t.Fatalf("fail node: %v", err)
	}
	if err := s.BeginMaintenance("n1"); err == nil {
		t.Fatalf("BeginMaintenance on a Failed node must be rejected")
	}
	if err := s.EndMaintenance("n1"); err == nil {
		t.Fatalf("EndMaintenance on a Failed node must be rejected")
	}
	checkConsistency(t, s, c)
}
func TestMaintenancePreemptionDoesNotTouchDrainingJobs(t *testing.T) {
	c := NewCluster(
		&Node{ID: "n1", Capacity: Resources{CPU: 4, MemMB: 4096}},
		&Node{ID: "n2", Capacity: Resources{CPU: 4, MemMB: 4096}},
	)
	s := New(c, nil)
	draining := &Job{ID: "draining", Owner: "a", Priority: 1, Req: Resources{CPU: 4}, MaxRetries: 2, ArrayIndex: -1}
	online := &Job{ID: "online", Owner: "a", Priority: 2, Req: Resources{CPU: 4}, MaxRetries: 2, ArrayIndex: -1}
	hi := &Job{ID: "hi", Owner: "h", Priority: 9, Req: Resources{CPU: 4}, ArrayIndex: -1}
	logJobs(t, draining, online, hi)
	s.Submit(draining)
	s.Submit(online)
	logDecisions(t, s.RunCycle()) // first-fit：draining -> n1, online -> n2

	// draining 所在节点进入维护（未排空）：其上作业不应成为抢占牺牲者。
	dj, _ := s.Job("draining")
	drainingNode := dj.AssignedNode
	oj, _ := s.Job("online")
	onlineNode := oj.AssignedNode
	if drainingNode == onlineNode {
		t.Fatalf("setup: two full-node jobs must land on different nodes")
	}
	if err := s.BeginMaintenance(drainingNode); err != nil {
		t.Fatalf("begin maintenance: %v", err)
	}
	s.Submit(hi)
	ds := s.RunCycle()
	logDecisions(t, ds)
	// hi(9) 只能抢占在线节点上严格更低优先级的 online(2)；
	// 维护中节点上的 draining(1) 虽优先级更低，但抢占它只在不可调度节点上腾空间，不允许。
	if j, _ := s.Job("draining"); j.State != StateRunning || j.AssignedNode != drainingNode {
		t.Fatalf("draining job must be untouched by preemption, got %s/%s", j.State, j.AssignedNode)
	}
	if j, _ := s.Job("online"); j.State != StatePending {
		t.Fatalf("online(2) should be preempted by hi(9) and requeued, got %s", j.State)
	}
	if j, _ := s.Job("hi"); j.State != StateRunning || j.AssignedNode != onlineNode {
		t.Fatalf("hi should run on the online node %s after preempting online, got %s/%s",
			onlineNode, j.State, j.AssignedNode)
	}
	checkConsistency(t, s, c)
}

// TestConcurrentMaintenanceConsistency 在并发提交、调度循环、节点维护/排空/
// 恢复的交错操作下，验证最终状态一致（配额/节点占用恒等式），配合 -race 使用。
func TestConcurrentMaintenanceConsistency(t *testing.T) {
	c := NewCluster(
		&Node{ID: "n1", Capacity: Resources{CPU: 8, MemMB: 8192}},
		&Node{ID: "n2", Capacity: Resources{CPU: 8, MemMB: 8192}},
	)
	s := New(c, nil)
	nodeIDs := []string{"n1", "n2"}

	var wg sync.WaitGroup
	// 提交者：持续提交作业。
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(100 + w)))
			for i := 0; i < 12; i++ {
				_ = s.Submit(&Job{
					ID:         fmt.Sprintf("w%d-j%d", w, i),
					Owner:      fmt.Sprintf("owner-%d", w%2),
					Priority:   rng.Intn(5),
					Req:        Resources{CPU: 1 + rng.Intn(3)},
					MaxRetries: 3,
					ArrayIndex: -1,
				})
			}
		}(w)
	}
	// 调度者：反复跑调度循环。
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 80; i++ {
			s.RunCycle()
		}
	}()
	// 运维者：对两个节点反复 维护→排空→结束维护（失败则忽略后继续）。
	wg.Add(1)
	go func() {
		defer wg.Done()
		rng := rand.New(rand.NewSource(777))
		for i := 0; i < 20; i++ {
			id := nodeIDs[rng.Intn(2)]
			if err := s.BeginMaintenance(id); err == nil {
				if _, err := s.DrainNode(id); err == nil {
					_ = s.EndMaintenance(id)
				} else {
					_ = s.CancelMaintenance(id)
				}
			}
		}
	}()
	wg.Wait()

	// 确保两节点最终在线，反复收敛到静止。
	for _, id := range nodeIDs {
		switch st, _ := s.NodeState(id); st {
		case NodeDraining, NodeDrained:
			_ = s.CancelMaintenance(id)
		case NodeFailed:
			_ = s.RecoverNode(id)
		}
	}
	for round := 0; round < 30; round++ {
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
	for _, id := range nodeIDs {
		if st, _ := s.NodeState(id); st != NodeOnline {
			t.Fatalf("%s should end Online, got %s", id, st)
		}
	}
	t.Logf("concurrent maintenance run converged; both nodes Online, pending=0")
}

// TestReproducibleWithSeedAndMaintenance 与既有可复现测试同思路：
// 固定随机种子生成负载，并在固定轮次交错维护/排空/结束维护/故障恢复，
// 完整运行两遍，断言判定序列与最终状态逐字节一致。
func TestReproducibleWithSeedAndMaintenance(t *testing.T) {
	run := func() string {
		rng := rand.New(rand.NewSource(2026))
		c := NewCluster(
			&Node{ID: "n1", Capacity: Resources{CPU: 4, MemMB: 4096}},
			&Node{ID: "n2", Capacity: Resources{CPU: 4, MemMB: 4096}},
			&Node{ID: "n3", Capacity: Resources{CPU: 2, MemMB: 2048}},
		)
		s := New(c, map[string]Resources{"alice": {CPU: 6, MemMB: 6144}})
		owners := []string{"alice", "bob"}
		for i := 0; i < 12; i++ {
			j := &Job{
				ID:         fmt.Sprintf("m-%02d", i),
				Owner:      owners[rng.Intn(2)],
				Priority:   rng.Intn(4),
				Req:        Resources{CPU: 1 + rng.Intn(3), MemMB: 1024 * (1 + rng.Intn(3))},
				MaxRetries: 2,
				ArrayIndex: -1,
			}
			_ = s.Submit(j)
		}
		out := ""
		for round := 0; round < 8; round++ {
			for _, d := range s.RunCycle() {
				out += fmt.Sprintf("%s:%s;", d.JobID, d.Action)
			}
			switch round {
			case 1:
				// n1 切入维护并排空。
				if err := s.BeginMaintenance("n1"); err == nil {
					if ds, err := s.DrainNode("n1"); err == nil {
						for _, d := range ds {
							out += fmt.Sprintf("%s:%s;", d.JobID, d.Action)
						}
					}
				}
			case 2:
				// 维护尚未结束：n2 突发故障（与维护状态并存）。
				if ds, err := s.FailNode("n2"); err == nil {
					for _, d := range ds {
						out += fmt.Sprintf("%s:%s;", d.JobID, d.Action)
					}
				}
			case 3:
				_ = s.RecoverNode("n2")
			case 4:
				// n1 维护结束，容量回来，等待作业重新调度。
				_ = s.EndMaintenance("n1")
			case 5:
				// 对 n3 做一次取消维护（切维护后立刻取消，节点恢复在线）。
				if err := s.BeginMaintenance("n3"); err == nil {
					_ = s.CancelMaintenance("n3")
				}
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
		s.mu.Lock()
		for i := 0; i < 12; i++ {
			id := fmt.Sprintf("m-%02d", i)
			out += fmt.Sprintf("%s=%s/%d;", id, s.jobs[id].State, s.jobs[id].Retries)
		}
		out += fmt.Sprintf("quota=%+v;", s.quota)
		for _, id := range []string{"n1", "n2", "n3"} {
			n, _ := c.Node(id)
			out += fmt.Sprintf("%s=%s/%+v;", id, n.State, n.Allocated)
		}
		s.mu.Unlock()
		return out
	}
	first, second := run(), run()
	t.Logf("run1: %s", first)
	t.Logf("run2: %s", second)
	if first != second {
		t.Fatalf("same seed with maintenance must produce identical outcomes:\nrun1=%s\nrun2=%s", first, second)
	}
}
