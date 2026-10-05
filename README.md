# hpc-job-scheduler-go

单机可复现的集群作业调度系统：用本地模拟的节点与作业负载，演示资源分配、
组调度（gang scheduling）、作业数组、配额核算、优先级抢占、节点故障恢复，
以及**计划内维护与节点排空（maintenance / drain）**，
并保证所有对外可见状态在并发下一致、调度结论可解释、结果可复现。

## 构建与验证

```bash
go build ./...        # 编译
go test ./...         # 运行全部测试
go test -race ./...   # 并发一致性（竞态检测，含维护/排空交错场景）
go test -v -run TestGang ./scheduler/          # 组调度用例
go test -v -run 'Drain|Maintenance' ./scheduler/  # 维护与排空用例
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
- **节点状态机**：节点有四种运维状态，调度器只对 `Online` 节点做分配
  （维护中/已排空/故障节点都**不算可用容量**）：

  | 状态 | 接收新调度 | 容量计入 | 说明 |
  |---|---|---|---|
  | `Online` | 是 | 是 | 正常在线 |
  | `Draining` | 否 | 否 | 计划内维护中：不再接收新调度，**已有作业先保留**，等待显式排空 |
  | `Drained` | 否 | 否 | 排空完成、真正下线检修 |
  | `Failed` | 否 | 否 | 计划外故障 |

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

## 计划内维护与排空

计划内维护是**两阶段、可取消**的运维流程，区别于故障的"直接掉"：

1. `BeginMaintenance(node)`：`Online → Draining`。节点立即停止接收新调度，
   但**已有作业原样保留继续运行**；容量不计入可用资源。
2. `DrainNode(node)`：仅对 `Draining` 节点有效，显式排空：
   - 节点上的独立作业优先**平滑迁移**到其他 `Online` 节点（first-fit），
     作业保持 `Running`、不消耗重试、**配额不变**（源端释放一次、目标端占用一次）；
   - 放不下才退回队列：`Retries++`，未超 `MaxRetries` 则 `Pending` 重新参与调度，
     耗尽则 `Failed`；资源与配额对每个作业**恰好结算一次**。
   - **作业组整体处理**：只要组里有成员在受影响节点上，就取整组全部运行中成员，
     先把整组占用摘下、在其他在线节点从零试排，能同时落地才整体迁移
     （判定为 `migrated`，配额全程不变）；放不下则整组退回，
     全组成员**重试结论一致**（任一成员再退一次即耗尽则整组 `Failed`），
     绝不出现同组一半运行、一半排队的中间状态。
   - 排空不触发抢占（见下节）。排空干净后节点转为 `Drained`（真正下线）。
3. `EndMaintenance(node)`：`Drained → Online`，检修完成，下一轮调度重新可用。
4. `CancelMaintenance(node)`：`Draining` 或 `Drained → Online`，
   可在任意时刻取消维护；`Draining` 阶段取消时其上作业从未被打断。

判定动作新增：`migrated`（平滑迁移）、`requeued/failed`（排空退回）、
`node-drained`（节点排空完成下线）。

### 维护与故障恢复的区别

| | 计划内维护（`BeginMaintenance`/`DrainNode`） | 故障（`FailNode`） |
|---|---|---|
| 切换后已有作业 | 先保留，显式排空时才迁移/退回 | 立即全部退回 |
| 是否尝试平滑迁移 | 是，优先搬到其他健康节点保持运行 | 否，直接按有界重试退回 |
| 是否消耗重试 | 迁移不消耗；退回才 `Retries++` | 一定 `Retries++` |
| 恢复方式 | `EndMaintenance`/`CancelMaintenance` 主动回在线 | `RecoverNode` |
| 与容量判定 | 维护中节点容量暂不计入调度，但**仍计入"能否容纳"判定** | 故障节点完全排除 |

**维护期间节点再次故障**：`Draining/Drained` 节点可被 `FailNode` 打断为
`Failed`，系统保留"维护意图"标记；`RecoverNode` 时回到 `Drained`
（而非误判为普通 `Online` 提前接收调度），随后仍需 `EndMaintenance` 才上线。

### 维护与作业组、配额、抢占、依赖的交互

- **作业组**：如上，排空以整组为单位迁移或等待，回滚试排时占用恢复原位，
  不留残留、配额不漂移。
- **配额**：迁移路径上作业始终 `Running`，归属方配额**不发生任何变化**；
  退回路径与故障/完成共用同一条成对结算路径，配额只扣一次。
- **抢占**：高优先级作业的抢占牺牲者**只从 `Online` 节点上的运行中作业选取**；
  维护中节点上的作业即使优先级更低也不会被抢——抢占它们只在不可调度的
  节点上腾出空间，新作业仍无法落位。
- **依赖**：维护窗口内新提交的作业照常走依赖、配额、容量判定；
  依赖未满足则等待。`fitsAnyNode` 把暂时维护的节点容量计入"可容纳性"
  （维护是暂时的、结束后容量回来），因此不会因维护窗口把本可调度的作业
  永久拒绝为 `ExceedsCapacity`；故障节点则排除（可能持久不可用）。
- **唯一性**：任何资源回收（迁移、退回、抢占回滚、故障释放）都在同一把锁内
  对每个作业恰好生效一次，迁移/排空后的下一调度轮次不存在重复计数或超卖。

## 异常处理语义

- **节点故障**（`FailNode`）：节点标记为不健康（不再参与分配），
  其上运行中作业按 ID 升序逐个处理——资源**恰好释放一次**
  （节点占用与配额同时扣减），然后按有界重试重新入队或置为 `Failed`。
  不存在重复计数：配额只在"提交分配"与"释放"两条路径上成对变更。
- **节点恢复**（`RecoverNode`）：故障节点重新可用——普通故障恢复为 `Online`；
  维护流程中故障的节点因保留了维护意图，恢复为 `Drained`，
  需再调用 `EndMaintenance` 才回到在线调度。
- **并发一致性**：调度器全部对外方法共用一把互斥锁，作业状态、节点占用、
  配额三类状态在同一临界区内变更；`TestConcurrentConsistency` 在
  `-race` 下验证并发提交/调度/完成后的最终一致性
  （配额 == 运行中作业需求之和，节点占用 == 落在其上的运行中作业之和）。

## 可复现性

- 节点遍历、队列排序、抢占选 victim、故障处理顺序、排空时受影响作业的
  收集与整组试排顺序均为确定规则（作业按 ID 升序、作业组按 GroupID 升序）；
- 测试负载用固定随机种子生成（如 `rand.New(rand.NewSource(42))`），
  `TestReproducibleWithSeed` 与 `TestReproducibleWithSeedAndMaintenance`
  将同一场景（后者还交错维护/排空/故障恢复/取消维护）完整运行两遍，
  断言逐作业的判定序列与最终状态（含节点状态与各节点占用）逐字节一致。

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
| `TestDrainMigratesJobToHealthyNode` | 维护切入不打断作业、排空平滑迁移到健康节点、配额不漂移 |
| `TestDrainRequeuesWhenNowhereToMigrate` | 无处可迁退回队列、有界重试、重试耗尽转 Failed |
| `TestDrainGangMigratesTogether` | 跨节点作业组排空时整组一起迁移 |
| `TestDrainGangWaitsTogether` | 整组放不下时一起等待、节点回归后整组同时恢复 |
| `TestSubmitDuringMaintenanceRescheduleAfter` | 维护窗口提交走依赖/配额/容量判定，恢复后重新调度 |
| `TestDrainNoDoubleCount` | 迁移/退回后配额总额 == 节点占用总额，不重复计数 |
| `TestMaintenanceFailDuringDrainRecoversToDrained` | 维护中故障保留意图，恢复回 Drained，非法操作被拒 |
| `TestMaintenanceCancelAndEndTransitions` | 取消/结束维护的状态流转与非法流转报错无副作用 |
| `TestMaintenancePreemptionDoesNotTouchDrainingJobs` | 抢占牺牲者不来自维护中节点 |
| `TestConcurrentMaintenanceConsistency` | 并发提交/调度/维护排空下一致（`-race`） |
| `TestReproducibleWithSeedAndMaintenance` | 维护/故障交错场景固定种子两次运行结论完全一致 |
| `TestConcurrentConsistency` | 并发下对外可见状态一致（`-race`） |
| `TestReproducibleWithSeed` | 固定种子两次运行结论完全一致 |
