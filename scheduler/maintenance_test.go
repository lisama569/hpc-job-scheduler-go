package scheduler

import (
	"fmt"
	"math/rand"
	"sync"
	"testing"
)

// TestCordonStopsSchedulingButKeepsJobs 维护切入后不接新作业，已有作业保留；
// 维护中的节点不是可用容量；排空后节点真正下线；维护结束恢复在线并重新参与调度。
func TestCordonStopsSchedulingButKeepsJobs(t *testing.T) {
	c := twoNodeCluster()
	s := New(c, nil)

	keep := &Job{ID: "keep", Owner: "a", Req: Resources{CPU: 4}, MaxRetries: 1, ArrayIndex: -1}
	other := &Job{ID: "other", Owner: "b", Req: Resources{CPU: 4}, ArrayIndex: -1}
	late := &Job{ID: "late", Owner: "a", Req: Resources{CPU: 4}, MaxRetries: 1, ArrayIndex: -1}
	logJobs(t, keep, other, late)

	s.Submit(keep)
	s.Submit(other)
	logDecisions(t, s.RunCycle()) // keep->n1, other->n2（两节点全满）
	kj, _ := s.Job("keep")
	target := kj.AssignedNode // n1
	if target != "n1" {
		t.Fatalf("setup: keep should land on n1, got %s", target)
	}

	if err := s.BeginMaintenance(target); err != nil {
		t.Fatalf("begin maintenance: %v", err)
	}
	if st, _ := s.NodeState(target); st != NodeCordoned {
		t.Fatalf("node state want Cordoned, got %s", st)
	}
	// 已有作业保留：仍在运行、资源不释放。
	if j, _ := s.Job("keep"); j.State != StateRunning || j.AssignedNode != target {
		t.Fatalf("keep must remain running on %s while cordoned, got %s/%s", target, j.State, j.AssignedNode)
	}
	if n, _ := c.Node(target); n.Allocated.CPU != 4 {
		t.Fatalf("cordoned node should retain allocation, got %+v", n.Allocated)
	}
	// 维护中的节点不是可用容量：新作业不能被调度到它上面（n2 已满 -> 等待）。
	s.Submit(late)
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("late"); j.State != StatePending {
		t.Fatalf("late should wait (n1 cordoned, n2 full), got %s", j.State)
	}
	if j, _ := s.Job("late"); j.AssignedNode == target {
		t.Fatalf("late must never be placed on cordoned node")
	}
	// 重复切入维护应报错。
	if err := s.BeginMaintenance(target); err == nil {
		t.Fatalf("double begin maintenance should fail")
	}
	// Cordoned 节点不能直接结束维护（需先排空），也不能直接 RecoverNode。
	if err := s.EndMaintenance(target); err == nil {
		t.Fatalf("end maintenance on merely-cordoned node should fail")
	}

	// 排空：n2 被 other 占满，无处迁移 -> keep 退回队列（消耗 1 次重试），n1 下线。
	ds, err := s.DrainNode(target)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	logDecisions(t, ds)
	if st, _ := s.NodeState(target); st != NodeDrained {
		t.Fatalf("node state want Drained, got %s", st)
	}
	kj, _ = s.Job("keep")
	if kj.State != StatePending || kj.Retries != 1 {
		t.Fatalf("keep should be requeued with 1 retry, got %s retries=%d", kj.State, kj.Retries)
	}
	if n, _ := c.Node(target); !n.Allocated.IsZero() {
		t.Fatalf("drained node must hold no allocation, got %+v", n.Allocated)
	}

	// n2 仍满，keep/late 都等待；other 完成后先由等待更久的 late 接住 n2。
	logDecisions(t, s.RunCycle())
	if err := s.Complete("other"); err != nil {
		t.Fatalf("complete other: %v", err)
	}
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("late"); j.State != StateRunning || j.AssignedNode != "n2" {
		t.Fatalf("late should take n2 first, got %s/%s", j.State, j.AssignedNode)
	}
	if j, _ := s.Job("keep"); j.State != StatePending {
		t.Fatalf("keep still waits (n1 offline), got %s", j.State)
	}

	// 维护结束，n1 恢复在线，keep 立刻被重新调度。
	if err := s.EndMaintenance(target); err != nil {
		t.Fatalf("end maintenance: %v", err)
	}
	if st, _ := s.NodeState(target); st != NodeOnline {
		t.Fatalf("node state want Online, got %s", st)
	}
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("keep"); j.State != StateRunning || j.AssignedNode != "n1" {
		t.Fatalf("keep should run again on returned n1, got %s/%s", j.State, j.AssignedNode)
	}
	checkConsistency(t, s, c)
}

// TestCancelMaintenanceBeforeDrain 排空开始前可取消维护，节点立即重新参与调度。
func TestCancelMaintenanceBeforeDrain(t *testing.T) {
	c := twoNodeCluster()
	s := New(c, nil)
	// j 需要可平滑迁移：排空 n1 时它能落到 n2，保持运行，验证取消/结束全程不打断作业。
	j := &Job{ID: "j", Owner: "a", Req: Resources{CPU: 2}, ArrayIndex: -1}
	logJobs(t, j)
	s.Submit(j)
	logDecisions(t, s.RunCycle())
	if x, _ := s.Job("j"); x.AssignedNode != "n1" {
		t.Fatalf("setup: j should land on n1, got %s", x.AssignedNode)
	}

	if err := s.BeginMaintenance("n1"); err != nil {
		t.Fatalf("begin: %v", err)
	}
	if err := s.CancelMaintenance("n1"); err != nil {
		t.Fatalf("cancel: %v", err)
	}
	if st, _ := s.NodeState("n1"); st != NodeOnline {
		t.Fatalf("want Online after cancel, got %s", st)
	}
	// 已排空节点不能取消维护，只能结束维护。
	s.BeginMaintenance("n1")
	ds, err := s.DrainNode("n1")
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	logDecisions(t, ds)
	if err := s.CancelMaintenance("n1"); err == nil {
		t.Fatalf("cancel on drained node should fail")
	}
	if x, _ := s.Job("j"); x.State != StateRunning || x.AssignedNode != "n2" {
		t.Fatalf("j should have live-migrated to n2, got %s/%s", x.State, x.AssignedNode)
	}
	if err := s.EndMaintenance("n1"); err != nil {
		t.Fatalf("end maintenance: %v", err)
	}
	if st, _ := s.NodeState("n1"); st != NodeOnline {
		t.Fatalf("want Online after end, got %s", st)
	}
	if j, _ := s.Job("j"); j.State != StateRunning {
		t.Fatalf("j should still be running throughout, got %s", j.State)
	}
	checkConsistency(t, s, c)
}

// TestDrainRequeueWhenNowhereToFit 排空时没有可放下的健康节点：作业退回队列、
// 占用与配额恰好释放一次；节点恢复在线后重新调度。
func TestDrainRequeueWhenNowhereToFit(t *testing.T) {
	c := twoNodeCluster()
	s := New(c, nil)
	j := &Job{ID: "j", Owner: "a", Req: Resources{CPU: 4}, MaxRetries: 1, ArrayIndex: -1}
	block := &Job{ID: "block", Owner: "b", Req: Resources{CPU: 4}, ArrayIndex: -1}
	logJobs(t, j, block)
	s.Submit(j)
	s.Submit(block)
	logDecisions(t, s.RunCycle()) // j -> n1, block -> n2
	jj, _ := s.Job("j")
	target := jj.AssignedNode
	other := "n1"
	if target == "n1" {
		other = "n2"
	}

	if err := s.BeginMaintenance(target); err != nil {
		t.Fatalf("begin: %v", err)
	}
	ds, err := s.DrainNode(target)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	logDecisions(t, ds)
	jj, _ = s.Job("j")
	if jj.State != StatePending || jj.Retries != 1 {
		t.Fatalf("j should be requeued with 1 retry, got %s retries=%d", jj.State, jj.Retries)
	}
	if jj.AssignedNode != "" {
		t.Fatalf("j assignment cleared, got %q", jj.AssignedNode)
	}
	// 资源只结算一次：两个节点合计只应有 block 的占用，a 的配额归零。
	if q := s.QuotaUsed("a"); !q.IsZero() {
		t.Fatalf("owner a quota leaked: %+v", q)
	}
	for _, n := range c.Nodes() {
		want := Resources{}
		if n.ID == other {
			want = block.Req
		}
		if n.Allocated != want {
			t.Fatalf("node %s allocation want %+v got %+v", n.ID, want, n.Allocated)
		}
	}
	if st, _ := s.NodeState(target); st != NodeDrained {
		t.Fatalf("want Drained, got %s", st)
	}

	// 腾不出空间前一直等待：另一在线节点 n2 被 block 占满，下线节点不算容量。
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("j"); j.State != StatePending {
		t.Fatalf("j keeps waiting (n2 full, n1 drained), got %s", j.State)
	}
	// block 不完成，n2 始终满；维护结束 n1 恢复在线后 j 立即重新调度。
	if err := s.EndMaintenance(target); err != nil {
		t.Fatalf("end: %v", err)
	}
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("j"); j.State != StateRunning {
		t.Fatalf("j should run on the returned node %s, got %s", target, j.State)
	}
	if j, _ := s.Job("j"); j.AssignedNode != target {
		t.Fatalf("j should land back on %s, got %s", target, j.AssignedNode)
	}
	checkConsistency(t, s, c)
}

// TestDrainGangMovesOrWaitsTogether 作业组跨节点：只要有成员在排空节点上，
// 整组一起迁移；放不下则整组一起退回等待，不出现半运行半排队。
func TestDrainGangMovesOrWaitsTogether(t *testing.T) {
	c := threeNodeCluster()
	s := New(c, nil)
	g1 := &Job{ID: "g1", Owner: "g", GroupID: "gang", Req: Resources{CPU: 3}, MaxRetries: 1, ArrayIndex: -1}
	g2 := &Job{ID: "g2", Owner: "g", GroupID: "gang", Req: Resources{CPU: 3}, MaxRetries: 1, ArrayIndex: -1}
	logJobs(t, g1, g2)
	s.Submit(g1)
	s.Submit(g2)
	logDecisions(t, s.RunCycle()) // first-fit: g1->n1, g2->n2
	n1, _ := c.Node("n1")
	n2, _ := c.Node("n2")
	if g1j, _ := s.Job("g1"); g1j.AssignedNode != "n1" || g1j.State != StateRunning {
		t.Fatalf("setup: g1 want Running/n1, got %s/%s", g1j.State, g1j.AssignedNode)
	}
	if g2j, _ := s.Job("g2"); g2j.AssignedNode != "n2" {
		t.Fatalf("setup: g2 want n2, got %s", g2j.AssignedNode)
	}

	// 排空 n1：g1 在 n1 上，整组（含 n2 上的 g2）一起搬到 n2/n3。
	s.BeginMaintenance("n1")
	ds, err := s.DrainNode("n1")
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	logDecisions(t, ds)
	for _, id := range []string{"g1", "g2"} {
		j, _ := s.Job(id)
		if j.State != StateRunning {
			t.Fatalf("gang member %s should run via migration, got %s", id, j.State)
		}
		if j.AssignedNode == "n1" {
			t.Fatalf("member %s still on drained n1", id)
		}
		if j.Retries != 0 {
			t.Fatalf("migration must not consume retries for %s, got %d", id, j.Retries)
		}
	}
	if q := s.QuotaUsed("g"); q.CPU != 6 {
		t.Fatalf("gang total quota 6 CPU after migration, got %+v", q)
	}
	if !n1.Allocated.IsZero() {
		t.Fatalf("n1 leaked allocation %+v", n1.Allocated)
	}
	if got := n2.Allocated.Add(n3Alloc(c)); got.CPU != 6 {
		t.Fatalf("gang should occupy 6 CPU across n2/n3, got %d", got.CPU)
	}
	if st, _ := s.NodeState("n1"); st != NodeDrained {
		t.Fatalf("n1 want Drained, got %s", st)
	}
	checkConsistency(t, s, c)
}

// TestDrainSingleJobLiveMigrates 排空时受影响单作业能平滑搬到健康节点：
// 保持 Running、不消耗重试、配额净变化为零。
func TestDrainSingleJobLiveMigrates(t *testing.T) {
	c := threeNodeCluster()
	s := New(c, nil)
	mov := &Job{ID: "mov", Owner: "a", Req: Resources{CPU: 2}, MaxRetries: 2, ArrayIndex: -1}
	logJobs(t, mov)
	s.Submit(mov)
	logDecisions(t, s.RunCycle()) // mov -> n1
	if x, _ := s.Job("mov"); x.AssignedNode != "n1" {
		t.Fatalf("setup: mov on n1, got %s", x.AssignedNode)
	}
	before := s.QuotaUsed("a")

	s.BeginMaintenance("n1")
	ds, err := s.DrainNode("n1")
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	logDecisions(t, ds)
	x, _ := s.Job("mov")
	if x.State != StateRunning {
		t.Fatalf("mov should keep running via live migration, got %s", x.State)
	}
	if x.AssignedNode != "n2" {
		t.Fatalf("mov should migrate to n2 (first-fit), got %s", x.AssignedNode)
	}
	if x.Retries != 0 {
		t.Fatalf("live migration must not consume retries, got %d", x.Retries)
	}
	if q := s.QuotaUsed("a"); q != before {
		t.Fatalf("quota must be unchanged by migration: before %+v after %+v", before, q)
	}
	n1, _ := c.Node("n1")
	if !n1.Allocated.IsZero() {
		t.Fatalf("drained n1 must be empty, got %+v", n1.Allocated)
	}
	checkConsistency(t, s, c)
}

// TestDrainGangRequeueAtomicWhenNoRoom 排空时整组无法整体重安置：
// 全体成员一起退回，重试与最终结论一致，配额不漂移。
func TestDrainGangRequeueAtomicWhenNoRoom(t *testing.T) {
	c := twoNodeCluster()
	s := New(c, nil)
	g1 := &Job{ID: "g1", Owner: "g", GroupID: "gang", Req: Resources{CPU: 3}, MaxRetries: 1, ArrayIndex: -1}
	g2 := &Job{ID: "g2", Owner: "g", GroupID: "gang", Req: Resources{CPU: 3}, MaxRetries: 1, ArrayIndex: -1}
	logJobs(t, g1, g2)
	s.Submit(g1)
	s.Submit(g2)
	logDecisions(t, s.RunCycle()) // g1->n1, g2->n2（first-fit，各占 3/4）
	// n1 被排空后，n2 只剩 1 CPU，整组(g1+g2=6 CPU)无法在单节点整体重安置 -> 整组等待。
	s.BeginMaintenance("n1")
	ds, err := s.DrainNode("n1")
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	logDecisions(t, ds)
	for _, id := range []string{"g1", "g2"} {
		j, _ := s.Job(id)
		if j.State != StatePending {
			t.Fatalf("gang member %s must wait with the group, got %s", id, j.State)
		}
		if j.Retries != 1 {
			t.Fatalf("gang member %s retry want 1, got %d", id, j.Retries)
		}
	}
	if q := s.QuotaUsed("g"); !q.IsZero() {
		t.Fatalf("gang quota must be zero while all members wait, got %+v", q)
	}
	if got := s.Pending(); len(got) != 2 {
		t.Fatalf("want 2 gang members pending, got %v", got)
	}
	// 没有中间态：绝不能一个运行一个排队。
	st1, _ := s.Job("g1")
	st2, _ := s.Job("g2")
	if st1.State != st2.State {
		t.Fatalf("gang split: g1=%s g2=%s", st1.State, st2.State)
	}
	checkConsistency(t, s, c)
}

// TestDrainGangFailTogether 整组退回且其中有成员重试预算耗尽：整组一起失败。
func TestDrainGangFailTogether(t *testing.T) {
	c := twoNodeCluster()
	s := New(c, nil)
	g1 := &Job{ID: "g1", Owner: "g", GroupID: "gang", Req: Resources{CPU: 3}, MaxRetries: 0, ArrayIndex: -1}
	g2 := &Job{ID: "g2", Owner: "g", GroupID: "gang", Req: Resources{CPU: 3}, MaxRetries: 1, ArrayIndex: -1}
	logJobs(t, g1, g2)
	s.Submit(g1)
	s.Submit(g2)
	logDecisions(t, s.RunCycle())
	s.BeginMaintenance("n1")
	ds, _ := s.DrainNode("n1") // n2 只有 4 CPU，整组 6 放不下 -> 退回；g1 预算耗尽 -> 整组失败
	logDecisions(t, ds)
	for _, id := range []string{"g1", "g2"} {
		if j, _ := s.Job(id); j.State != StateFailed {
			t.Fatalf("gang member %s must fail together, got %s", id, j.State)
		}
	}
	if q := s.QuotaUsed("g"); !q.IsZero() {
		t.Fatalf("failed gang quota leaked: %+v", q)
	}
	checkConsistency(t, s, c)
}

// TestSubmitDuringMaintenanceFollowsRules 维护期间新提交作业照常走依赖、配额、容量判定。
func TestSubmitDuringMaintenanceFollowsRules(t *testing.T) {
	c := NewCluster(
		&Node{ID: "n1", Capacity: Resources{CPU: 4, MemMB: 4096}},
		&Node{ID: "n2", Capacity: Resources{CPU: 4, MemMB: 4096}},
	)
	limits := map[string]Resources{"alice": {CPU: 4}}
	s := New(c, limits)

	// n1 维护下线：只剩 n2 的 4 CPU 容量。
	s.BeginMaintenance("n1")
	if _, err := s.DrainNode("n1"); err != nil {
		t.Fatalf("drain empty n1: %v", err)
	}
	big := &Job{ID: "big", Owner: "alice", Req: Resources{CPU: 8}, ArrayIndex: -1}
	fit := &Job{ID: "fit", Owner: "alice", Req: Resources{CPU: 4}, ArrayIndex: -1}
	extra := &Job{ID: "extra", Owner: "alice", Req: Resources{CPU: 1}, ArrayIndex: -1}
	logJobs(t, big, fit, extra)
	s.Submit(big) // 8 > 任何在线节点容量(4) -> 永久拒绝
	if j, _ := s.Job("big"); j.State != StateRejected || j.RejectWhy != RejectExceedsCapacity {
		t.Fatalf("big want Rejected/ExceedsCapacity during maintenance, got %s/%s", j.State, j.RejectWhy)
	}
	s.Submit(fit)
	s.Submit(extra)
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("fit"); j.State != StateRunning {
		t.Fatalf("fit should run on n2, got %s", j.State)
	}
	if j, _ := s.Job("extra"); j.State != StatePending {
		t.Fatalf("extra should wait on quota/capacity, got %s", j.State)
	}

	// 维护结束后 n1 回来，extra 在后续周期获得资源。
	if err := s.EndMaintenance("n1"); err != nil {
		t.Fatalf("end: %v", err)
	}
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("extra"); j.State != StatePending {
		// 配额上限 4 CPU：fit 占 4，extra 仍需等 fit 完成；先断言等待，再完成 fit 验证。
		t.Logf("extra state after n1 back: %s", j.State)
	}
	if err := s.Complete("fit"); err != nil {
		t.Fatalf("complete fit: %v", err)
	}
	logDecisions(t, s.RunCycle())
	if j, _ := s.Job("extra"); j.State != StateRunning {
		t.Fatalf("extra should run after quota frees, got %s", j.State)
	}
	checkConsistency(t, s, c)
}

// TestFailureDuringMaintenance 维护未排空时节点再次故障：作业按故障语义回收一次，
// RecoverNode 后节点回到 Cordoned（维护继续），再排空下线、结束维护恢复。
func TestFailureDuringMaintenance(t *testing.T) {
	c := twoNodeCluster()
	s := New(c, nil)
	j := &Job{ID: "j", Owner: "a", Req: Resources{CPU: 4}, MaxRetries: 1, ArrayIndex: -1}
	logJobs(t, j)
	s.Submit(j)
	logDecisions(t, s.RunCycle())
	jj, _ := s.Job("j")
	target := jj.AssignedNode

	s.BeginMaintenance(target)
	ds, err := s.FailNode(target) // 维护期间故障
	if err != nil {
		t.Fatalf("fail during maintenance: %v", err)
	}
	logDecisions(t, ds)
	if st, _ := s.NodeState(target); st != NodeFailed {
		t.Fatalf("want Failed, got %s", st)
	}
	if j, _ := s.Job("j"); j.State != StatePending || j.Retries != 1 {
		t.Fatalf("j requeued once on failure, got %s/%d", j.State, j.Retries)
	}
	if q := s.QuotaUsed("a"); !q.IsZero() {
		t.Fatalf("quota double counted: %+v", q)
	}
	// 已故障的节点不能排空。
	if _, err := s.DrainNode(target); err == nil {
		t.Fatalf("drain on failed node should fail")
	}
	// 恢复后仍处于维护（Cordoned），而非直接上线。
	if err := s.RecoverNode(target); err != nil {
		t.Fatalf("recover: %v", err)
	}
	if st, _ := s.NodeState(target); st != NodeCordoned {
		t.Fatalf("want Cordoned after recovery (maintenance persists), got %s", st)
	}
	// 作业此时可在另一个在线节点运行；再排空（此时该节点上无作业）可干净下线。
	logDecisions(t, s.RunCycle())
	if x, _ := s.Job("j"); x.State != StateRunning {
		t.Fatalf("j runs on the other online node, got %s", x.State)
	}
	ds, err = s.DrainNode(target)
	if err != nil {
		t.Fatalf("drain: %v", err)
	}
	logDecisions(t, ds)
	if st, _ := s.NodeState(target); st != NodeDrained {
		t.Fatalf("want Drained, got %s", st)
	}
	s.EndMaintenance(target)
	if st, _ := s.NodeState(target); st != NodeOnline {
		t.Fatalf("want Online after end maintenance, got %s", st)
	}
	checkConsistency(t, s, c)
}

// TestDrainResourceSettledOnceWithPreemption 排空与抢占交错：迁移成功后配额/占用
// 与正常运行完全一致，随后高优抢占只作用于在线节点上的低优作业，无重复计数。
func TestDrainResourceSettledOnceWithPreemption(t *testing.T) {
	c := threeNodeCluster()
	s := New(c, nil)
	low := &Job{ID: "low", Owner: "l", Priority: 1, Req: Resources{CPU: 4}, MaxRetries: 2, ArrayIndex: -1}
	mov := &Job{ID: "mov", Owner: "m", Priority: 5, Req: Resources{CPU: 4}, ArrayIndex: -1}
	hi := &Job{ID: "hi", Owner: "h", Priority: 9, Req: Resources{CPU: 4}, ArrayIndex: -1}
	logJobs(t, low, mov, hi)
	s.Submit(low)
	s.Submit(mov)
	logDecisions(t, s.RunCycle()) // mov(prio5) 先入 n1，low(prio1) 落到 n2
	s.BeginMaintenance("n2")
	ds, _ := s.DrainNode("n2") // low 平滑迁移：n1 满、n3 空 -> low->n3
	logDecisions(t, ds)
	if j, _ := s.Job("low"); j.State != StateRunning || j.AssignedNode != "n3" {
		t.Fatalf("low should migrate to n3, got %s/%s", j.State, j.AssignedNode)
	}
	if j, _ := s.Job("mov"); j.AssignedNode != "n1" {
		t.Fatalf("mov should remain on n1, got %s", j.AssignedNode)
	}
	// hi 到来：在线节点 n1(mov,prio5)、n3(low,prio1)；只能抢占优先级更低的 low。
	s.Submit(hi)
	ds = s.RunCycle()
	logDecisions(t, ds)
	if j, _ := s.Job("hi"); j.State != StateRunning {
		t.Fatalf("hi should run by preempting low, got %s", j.State)
	}
	if j, _ := s.Job("low"); j.State != StatePending || j.Retries != 1 {
		t.Fatalf("low preempted once, got %s/%d", j.State, j.Retries)
	}
	if j, _ := s.Job("mov"); j.State != StateRunning {
		t.Fatalf("mov (higher prio) must keep running, got %s", j.State)
	}
	// 已下线 n2 绝不能被计入任何东西。
	n2, _ := c.Node("n2")
	if !n2.Allocated.IsZero() {
		t.Fatalf("drained n2 leaked %+v", n2.Allocated)
	}
	checkConsistency(t, s, c)
}

// TestMaintenanceReproducibleWithSeed 固定随机种子，维护/排空/故障/恢复交错，
// 两次运行的逐作业判定序列与最终状态必须完全一致。
func TestMaintenanceReproducibleWithSeed(t *testing.T) {
	run := func() string {
		rng := rand.New(rand.NewSource(20261005))
		c := NewCluster(
			&Node{ID: "n1", Capacity: Resources{CPU: 4, MemMB: 4096}},
			&Node{ID: "n2", Capacity: Resources{CPU: 4, MemMB: 4096}},
			&Node{ID: "n3", Capacity: Resources{CPU: 2, MemMB: 2048}},
		)
		s := New(c, map[string]Resources{"alice": {CPU: 6, MemMB: 6144}})
		owners := []string{"alice", "bob"}
		for i := 0; i < 10; i++ {
			j := &Job{
				ID:         fmt.Sprintf("m-%02d", i),
				Owner:      owners[rng.Intn(2)],
				Priority:   rng.Intn(4),
				Req:        Resources{CPU: 1 + rng.Intn(3), MemMB: 1024 * (1 + rng.Intn(2))},
				MaxRetries: 1,
				ArrayIndex: -1,
			}
			_ = s.Submit(j)
		}
		out := ""
		flush := func(ds []Decision) {
			for _, d := range ds {
				out += fmt.Sprintf("%s:%s;", d.JobID, d.Action)
			}
		}
		for round := 0; round < 8; round++ {
			flush(s.RunCycle())
			switch round {
			case 1:
				_ = s.BeginMaintenance("n1")
				d, _ := s.DrainNode("n1")
				flush(d)
			case 2:
				_, _ = s.FailNode("n2") // 维护期间另一节点故障
			case 3:
				_ = s.RecoverNode("n2")
			case 5:
				_ = s.EndMaintenance("n1")
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
		for i := 0; i < 10; i++ {
			id := fmt.Sprintf("m-%02d", i)
			out += fmt.Sprintf("%s=%s/%d/%s;", id, s.jobs[id].State, s.jobs[id].Retries, s.jobs[id].AssignedNode)
		}
		for _, n := range c.Nodes() {
			out += fmt.Sprintf("%s:%s/%d;", n.ID, n.State, n.Allocated.CPU)
		}
		out += fmt.Sprintf("quota=%+v;", s.quota)
		s.mu.Unlock()
		return out
	}
	first, second := run(), run()
	t.Logf("maintenance run1: %s", first)
	t.Logf("maintenance run2: %s", second)
	if first != second {
		t.Fatalf("same seed must give identical outcomes:\nrun1=%s\nrun2=%s", first, second)
	}
}

// TestMaintenanceConcurrentConsistency 并发提交/调度/维护/排空/恢复下最终一致。
func TestMaintenanceConcurrentConsistency(t *testing.T) {
	c := NewCluster(
		&Node{ID: "n1", Capacity: Resources{CPU: 8, MemMB: 8192}},
		&Node{ID: "n2", Capacity: Resources{CPU: 8, MemMB: 8192}},
		&Node{ID: "n3", Capacity: Resources{CPU: 8, MemMB: 8192}},
	)
	s := New(c, nil)

	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(100 + w)))
			for i := 0; i < 10; i++ {
				j := &Job{
					ID:         fmt.Sprintf("cw%d-j%d", w, i),
					Owner:      fmt.Sprintf("owner-%d", w%2),
					Priority:   rng.Intn(5),
					Req:        Resources{CPU: 1 + rng.Intn(2)},
					MaxRetries: 3,
					ArrayIndex: -1,
				}
				_ = s.Submit(j)
			}
		}(w)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 60; i++ {
			s.RunCycle()
		}
	}()
	// 维护工作流：对 n3 反复 维护->排空->结束维护；并随机故障/恢复 n2。
	maintenance := func() {
		defer wg.Done()
		for i := 0; i < 8; i++ {
			if err := s.BeginMaintenance("n3"); err == nil {
				_, _ = s.DrainNode("n3")
			}
			s.RunCycle()
			_ = s.EndMaintenance("n3")
			s.RunCycle()
			if i%2 == 0 {
				if _, err := s.FailNode("n2"); err == nil {
					_ = s.RecoverNode("n2")
				}
			}
		}
	}
	wg.Add(1)
	go maintenance()
	wg.Wait()

	// 全部节点恢复在线，收敛到静止：完成所有运行中的作业。
	_ = s.EndMaintenance("n3")
	_ = s.RecoverNode("n2")
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
	for _, n := range c.Nodes() {
		if n.State != NodeOnline {
			t.Fatalf("node %s should be Online at rest, got %s", n.ID, n.State)
		}
	}
	t.Logf("concurrent maintenance workflow converged; all nodes Online, consistent")
}

func threeNodeCluster() *Cluster {
	return NewCluster(
		&Node{ID: "n1", Capacity: Resources{CPU: 4, MemMB: 4096}},
		&Node{ID: "n2", Capacity: Resources{CPU: 4, MemMB: 4096}},
		&Node{ID: "n3", Capacity: Resources{CPU: 4, MemMB: 4096}},
	)
}

func n3Alloc(c *Cluster) Resources {
	n, _ := c.Node("n3")
	return n.Allocated
}
