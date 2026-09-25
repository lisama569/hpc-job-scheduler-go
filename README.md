# hpc-job-scheduler-go

单机可复现的集群作业调度系统：用本地模拟的节点与作业负载，演示资源分配、
组调度（gang scheduling）、作业数组、配额核算、优先级抢占与节点故障恢复，
并保证所有对外可见状态在并发下一致、调度结论可解释、结果可复现。

## 构建与验证

```bash
go build ./...        # 编译
go test ./...         # 运行全部测试
go test -race ./...   # 并发一致性（竞态检测）
go test -v -run TestGang ./scheduler/   # 查看单测日志中的作业输入与判定依据
```

每个测试都会通过 `t.Logf` 打印：作业输入（ID/归属方/优先级/资源需求/依赖/
组/数组下标/重试上限）与每一次调度判定（`Decision{JobID, Action, Reason}`），
`go test -v` 下可见完整判定依据。

## 调度模型

- **资源**：`Resources{CPU, MemMB, GPU}` 三维向量，逐维比较（`Fits`）。
- **节点**：`Cluster` 持有固定顺序的节点列表，分配采用"首个可放下"
  （first-fit）策略，遍历顺序固定，因此调度结果确定、可复现。
- **队列**：就绪队列按 **优先级降序、同优先级按入队序号升序（FIFO）** 排序，
  杜绝 map 遍历等不确定因素。
- **调度循环**：`RunCycle()` 一次性排空队列，按序处理每个作业：
  依赖未就绪 → 重新入队等待；配额不足 → 等待；资源不足 → 尝试抢占；
  均不可行 → 等待，绝不做部分调度。
- **作业状态机**：`Pending → Running → Succeeded`；
  异常路径 `→ Preempted/Pending`（重试中）、`→ Failed`（重试耗尽）、
  `→ Rejected`（提交即拒绝，永不可调度）。

### 依赖与可区分的拒绝原因

提交时校验，`RejectReason` 可区分：

| 原因 | 含义 |
|---|---|
| `ExceedsCapacity` | 需求超出任何健康单节点的全部容量，永远放不下 |
| `DependencyUnsatisfiable` | 依赖的作业不存在、已被拒绝或已失败 |
| `DependencyCycle` | 自依赖环 |

依赖链在运行时同样检查：依赖未全部 `Succeeded` 前作业等待；
依赖在运行期间失败/被拒时，后继作业以 `DependencyUnsatisfiable` 拒绝，
不会被部分调度。

## 组调度与作业数组

- **组调度**：同 `GroupID` 的作业整体获得资源或整体等待。
  实现为试分配 + 回滚：任一成员放不下去时，本轮已试分配的资源逐个释放，
  不留残留占用，配额也不发生任何变化（测试断言回滚前后 `totalFree` 不变）。
- **作业数组**：`SubmitArray(owner, prefix, count, tmpl)` 生成
  `prefix-0 … prefix-(n-1)` 个元素。元素之间互不影响（一个完成/失败/被拒
  不影响其余元素），但配额按 **Owner 独立核算**——数组元素与同名归属方的
  普通作业共享同一配额上限。

## 配额与抢占规则

- **配额**：仅 `Running` 作业计入归属方（Owner）配额；未配置上限的 Owner 不限。
  配额不足时作业等待（而非拒绝），因为配额会随作业完成而释放。
- **抢占**：高优先级作业资源不足时触发，规则确定：
  1. 候选牺牲者 = 优先级**严格更低**的运行中作业；
  2. 按 **(优先级升序, 作业 ID 升序)** 逐个抢占，直到高优作业能放下；
  3. 全部候选抢完仍放不下 → **整体回滚**，牺牲者原样恢复，视同资源不足；
  4. 组调度成员不参与抢占（整组等待），避免组语义被撕裂。
- **有界重试**：被抢占或遭遇节点故障的作业 `Retries++`，
  未超过 `MaxRetries` 则重新入队，否则置为 `Failed`，重试次数有界。

## 异常处理语义

- **节点故障**（`FailNode`）：节点标记为不健康（不再参与分配），
  其上运行中作业按 ID 升序逐个处理——资源**恰好释放一次**
  （节点占用与配额同时扣减），然后按有界重试重新入队或置为 `Failed`。
  不存在重复计数：配额只在"提交分配"与"释放"两条路径上成对变更。
- **节点恢复**（`RecoverNode`）：节点重新参与后续调度循环。
- **并发一致性**：调度器全部对外方法共用一把互斥锁，作业状态、节点占用、
  配额三类状态在同一临界区内变更；`TestConcurrentConsistency` 在
  `-race` 下验证并发提交/调度/完成后的最终一致性
  （配额 == 运行中作业需求之和，节点占用 == 落在其上的运行中作业之和）。

## 可复现性

- 节点遍历、队列排序、抢占选 victim、故障处理顺序均为确定规则；
- 测试负载用固定随机种子生成（如 `rand.New(rand.NewSource(42))`），
  `TestReproducibleWithSeed` 将同一场景完整运行两遍，
  断言逐作业的判定序列与最终状态逐字节一致。

## 测试覆盖

| 测试 | 场景 |
|---|---|
| `TestRejectExceedsCapacity` | 超出节点容量的拒绝原因可区分 |
| `TestDependencyRejectionAndChain` | 依赖缺失/自环拒绝，依赖链按序解锁 |
| `TestInsufficientResourcesWaitsNotPartial` | 资源不足等待而非部分调度 |
| `TestGangSchedulingAllOrNothing` | 组调度失败回滚无残留，整组同生共死 |
| `TestJobArrayQuotaPerOwner` | 数组元素独立、配额按归属方核算 |
| `TestNodeFailureRequeueNoDoubleCount` | 节点故障重入队、资源不重复计数、重试耗尽转 Failed |
| `TestPreemptionDeterministicAndBounded` | 抢占 victim 选择确定、重试有界 |
| `TestPreemptionRollbackWhenStillNotFitting` | 抢占后仍放不下时整体回滚 |
| `TestConcurrentConsistency` | 并发下对外可见状态一致（`-race`） |
| `TestReproducibleWithSeed` | 固定种子两次运行结论完全一致 |
