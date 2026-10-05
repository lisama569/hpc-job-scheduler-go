# hpc-job-scheduler-go

单机可复现的集群作业调度系统：用本地模拟的节点与作业负载，演示资源分配、
组调度（gang scheduling）、作业数组、配额核算、优先级抢占、节点故障恢复，
以及**计划内维护与安全排空**，并保证所有对外可见状态在并发下一致、
调度结论可解释、结果可复现。

## 构建与验证

```bash
go build ./...        # 编译
go test ./...         # 运行全部测试
go test -race ./...   # 并发一致性（竞态检测）
go test -v -run TestGang ./scheduler/        # 查看组调度判定日志
go test -v -run Maintenance ./scheduler/     # 查看维护/排空/并发维护判定日志
go test -v -run TestDrain ./scheduler/       # 查看排空迁移/退回判定日志
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

## 计划内维护与安全排空

节点下架检修走的是"维护"工作流，而不是直接故障。节点有四种运维状态
（`NodeState`）：

| 状态 | 含义 | 是否计为可用容量 |
|---|---|---|
| `Online` | 正常在线，参与调度 | 是 |
| `Cordoned` | 已切入维护、**尚未排空**：不接新作业，已有作业原样保留并可正常完成 | 否 |
| `Drained` | 排空完成、**真正下线**：无运行作业、无占用 | 否 |
| `Failed` | 节点故障 | 否 |

> 维护中的节点（`Cordoned`/`Drained`）与故障节点一样**不是可用容量**：
> `tryAllocate`/`totalFree`/`fitsAnyNode` 都只统计 `Online` 节点。
> 因此维护期间提交的、只有维护节点放得下的作业，会照常得到
> `ExceedsCapacity` 永久拒绝。

对应 API：

- `BeginMaintenance(node)`：`Online → Cordoned`。节点停止接收新调度，
  但**已有作业全部保留**（继续运行、可 `Complete`，占用与配额不动）。
- `DrainNode(node)`：仅对 `Cordoned` 节点有效。显式发起排空，把上面的活儿
  安全挪走；处理干净后节点转为 `Drained`（真正下线）。
- `CancelMaintenance(node)`：仅对 `Cordoned` 有效（排空开始前），
  立即回到 `Online`，保留的作业不受任何影响。
- `EndMaintenance(node)`：仅对 `Drained` 有效，维护结束，节点回到 `Online`
  并在后续 `RunCycle` 重新参与调度。
- 非法流转会返回错误（如对 `Cordoned` 直接 `EndMaintenance`、
  对 `Drained` 再 `DrainNode`），状态不会被悄悄改坏。

### 排空的搬迁单元与"只结算一次"

`DrainNode` 以**单个作业**或**整个作业组**为搬迁单元（`evictUnit`）：

1. 收集该节点上的运行中作业：无组作业各自成单元；有组作业则把该组
   **分布在任意在线节点上的全部运行成员**纳入同一单元——只要组里有成员
   在被排空节点上，就整组一起搬/一起等，不允许出现同组一半运行、一半排队。
2. 每个单元先**释放旧占用与配额（恰好一次）**，然后：
   - **平滑迁移**（维护专属）：在其他 `Online` 节点上对整组试分配，
     全部放下才提交（成员保持 `Running`、`Retries` 不变）；任一成员放不下
     就回滚试分配，整组退回队列。
   - **退回队列**：整组按有界重试处理——全体成员都还有重试预算则一起
     `Retries++` 重新入队；只要有一个成员预算耗尽，则**整组一起 Failed**，
     组内重试次数与最终结论保持一致。
3. 单元处理顺序按其最小成员 ID 升序、组内成员按 ID 升序，结论确定。

资源不重复计数的关键：

- **迁移成功**：旧位置释放一次（节点占用−、配额−），新位置提交一次
  （节点占用+、配额+），两者成对，**配额净变化为零、不漂移**，
  作业全程保持 `Running`、不消耗重试。
- **退回队列**：旧位置只释放一次，作业回到 `Pending`（不再计入配额），
  重新调度时才在新节点上重新计入一次，与故障回收路径完全相同，不会超卖。
- 排空期间该节点上已不存在运行作业，因此**维护期间节点再次故障**不会
  二次释放这些资源（状态为 `Drained` 时 `FailNode` 直接返回错误）。

### 与故障恢复的区别

| 维度 | 计划内维护（Cordon/Drain） | 节点故障（FailNode） |
|---|---|---|
| 触发 | 人工显式两步：先 cordon 再 drain | 突发，一步置 `Failed` |
| 已有作业 | cordon 后**保留运行**，drain 时优先**平滑迁移** | 立即回收，不做迁移，只能重排/失败 |
| 重试 | 成功迁移不消耗重试；只有退回队列才 `Retries++` | 一律按有界重试重排或失败 |
| 可取消 | 排空开始前可 `CancelMaintenance` | 只能 `RecoverNode` |
| 维护中再故障 | cordon 节点可故障；恢复后**回到 `Cordoned`**（维护意图保留），仍需排空 | — |

两条回收路径共用同一套"收集单元 → 释放一次 → 迁移/退回"的内部实现，
因此故障时作业组也会被**整组原子处理**（避免同组撕裂），只是故障不尝试
平滑迁移。

### 与作业组、配额、抢占、依赖的交互

- **作业组**：排空/故障都以整组为最小单元，成功整组迁移，失败整组等待，
  结论一致；组调度在初始调度时仍是"全部放下或整体等待"。
- **配额**：仅 `Running` 计入。迁移保持作业 Running，配额只在旧/新位置
  成对结算（净零）；退回队列则释放，重新调度再计入。任何时刻都满足
  "配额 == 各 owner 运行中作业需求之和"。
- **抢占**：抢占 victim 只从 **Online 节点**上、**非作业组**、优先级严格
  更低的运行中作业中按确定顺序选取——既不会去抢维护/故障节点上换不回
  可用容量的作业，也不会把一个作业组抢成半运行半排队。维护迁移与抢占
  交错时，迁移后的作业视同普通运行作业参与后续抢占判定。
- **依赖**：被迁移的作业保持 Running、被退回的作业回到 Pending，
  依赖语义不变；维护期间新提交的作业照常走依赖、配额、容量判定，
  放不下就按原有规则等待或被拒。
- **维护结束恢复**：`EndMaintenance` 后节点立刻是 `Online`，排队作业在
  下一个 `RunCycle` 按既有优先级/配额/容量规则重新填入，不需要特殊补偿。

## 可复现性

- 节点遍历、队列排序、抢占选 victim、故障/排空处理顺序均为确定规则；
  排空单元按最小成员 ID 升序、组内成员按 ID 升序，维护流转全部是显式的
  有限状态迁移，不含 map 遍历或随机选择；
- 测试负载用固定随机种子生成（如 `rand.New(rand.NewSource(42))` 与维护场景
  的 `rand.NewSource(20261005)`），`TestReproducibleWithSeed` 与
  `TestMaintenanceReproducibleWithSeed` 各将同一场景完整运行两遍，
  断言逐作业的判定序列、最终状态、节点状态/占用与配额逐字节一致。

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
| `TestCordonStopsSchedulingButKeepsJobs` | cordon 后不接新作业、已有作业保留；维护节点不是容量；排空下线、结束恢复 |
| `TestCancelMaintenanceBeforeDrain` | 排空开始前取消维护回到在线；Drained 只能结束不能取消 |
| `TestDrainSingleJobLiveMigrates` | 单作业平滑迁移：保持运行、不耗重试、配额净零 |
| `TestDrainRequeueWhenNowhereToFit` | 无处可搬时退回队列、资源只结算一次、恢复后重调度 |
| `TestDrainGangMovesOrWaitsTogether` | 作业组跨节点时整组迁移，不出现半运行半排队 |
| `TestDrainGangRequeueAtomicWhenNoRoom` | 整组放不下时一起等待、重试一致、配额不漂移 |
| `TestDrainGangFailTogether` | 组内有成员重试耗尽则整组一起失败 |
| `TestSubmitDuringMaintenanceFollowsRules` | 维护期间新作业照常走容量/配额判定，恢复后重新调度 |
| `TestFailureDuringMaintenance` | 维护未排空时再故障：回收一次，恢复后仍处维护，再排空/结束 |
| `TestDrainResourceSettledOnceWithPreemption` | 排空迁移与高优抢占交错，无重复计数、抢占不撕裂作业组 |
| `TestMaintenanceReproducibleWithSeed` | 固定种子下维护/排空/故障/恢复交错，两次结论一致 |
| `TestMaintenanceConcurrentConsistency` | 并发提交/调度/维护/排空/恢复下最终一致（`-race`） |
