# hpc-job-scheduler-go

一个**可在单机复现**的集群作业调度系统：用本地模拟的节点与作业负载，实现资源约束调度、
组调度（gang scheduling）、作业数组、确定性优先级抢占与节点故障恢复，并保证
**资源不重复计数、状态不丢失、结论可复现且可解释**。

无任何外部依赖，仅用 Go 标准库；所有对外状态由单一互斥锁串行化，`go test -race` 干净。

```
go build ./...
go test ./... -race
go run ./cmd/hpc-sim -seed 20260925 -ticks 10
```

相同 `-seed` 的两次运行：生成的作业输入、抢占/故障判定、每个作业的最终结论逐字节一致。

## 目录结构

| 包 | 职责 |
|---|---|
| `internal/domain` | 核心模型：`Resource` 向量、`Node`、`Job`/`Task`、作业状态与可区分的拒绝原因 |
| `internal/cluster` | 节点池：容量/空闲核算、原子分配、故障置 DOWN 并归零用量、恢复 |
| `internal/quota` | 按归属方（owner）独立核算配额与当前用量 |
| `internal/queue` | 确定性优先级队列：优先级降序 → 入队时刻升序 → ID 升序 |
| `internal/scheduler` | 调度核心：准入、依赖、配额、组调度、数组、抢占、故障恢复、一致快照 |
| `internal/simulator` | 固定种子工作负载生成 + 脚本化时间线（完成/故障/恢复）驱动 |
| `cmd/hpc-sim` | 端到端可执行示例，打印作业输入、判定依据、最终结论与对账结果 |

## 调度模型

### 资源与作业
- 资源是三维可加向量 `(CPU, Memory, GPU)`；分配是逐节点做**最佳匹配（best-fit）**，
  剩余总量最少的节点优先，节点 ID 做最终次序裁决。
- 一个 `Job` 含一个或多个 `Task`。普通作业 1 个 task；组作业每个成员 1 个 task。
- 作业可声明 `Priority`（越大越优先）、资源需求与 `DependsOn`。
- 状态机：`PENDING → RUNNING → COMPLETED`；异常分支为 `REJECTED`（永久拒绝）与
  `FAILED`（有界重试耗尽）。

### 调度一趟（`RunPass`）做什么
1. 对**本趟开始时**的队列顺序做一次快照扫描，每个作业恰好评估一次，避免队列自改导致
   的漏判或同趟级联抢占（抢占受害者下一趟才重新参与，保证确定性）。
2. 对每个“调度单元”按以下判定阶梯（ladder）做决策，前一步不通过即返回：
   1. **依赖**：依赖 `FAILED/REJECTED/不存在` → `DEPENDENCY_FAILED`（拒绝）；
      依赖尚未 `COMPLETED` → `DEPENDENCY_NOT_MET`（等待）。
   2. **硬容量**：任一 task 在**所有在线节点**都放不下 → `EXCEEDS_NODE_CAPACITY`（永久拒绝）。
      这与“节点暂时没空位”严格区分。
   3. **硬配额**：单作业需求超过 owner 配额上限 → `QUOTA_EXCEEDED`（永久拒绝）；
      gang 同 owner 成员合计超过上限 → 以 `GROUP_CO_SCHEDULING` 永久拒绝该组。
   4. **试放置**：在不动真实状态的前提下模拟放置全部 task；空间不足为瞬时等待
      `INSUFFICIENT_RESOURCE`，配额“当前饱和”为可重试等待（区别于硬上限）。
   5. **抢占**：仅当能找到**严格更低优先级**的运行中受害者并在模拟中腾出足够资源时才执行。

### 组调度（gang）与数组
- 相同 `GroupID` 的成员构成一个不可分割单元：**要么整组全部获得资源，要么整组等待**，
  失败路径不残留任何占用（试放置是纯计算，提交只在全组可行后一次完成）。
- 抢占一个运行中的 gang 时，**整组一起撤离**；故障打到任一成员所在节点时，
  **该组所有运行成员一起撤离重排队**——永远不会出现“部分成员 RUNNING、部分 PENDING”。
- 数组（`ArrayID` + `Index`）的元素是**互不影响的独立作业**，可以单独被放置、抢占、
  故障撤离；它们唯一的共性是用量按 `owner` 归并，因此**配额按归属方独立核算**。

## 配额规则
- 每个 owner 一份 `(CPU, Memory, GPU)` 上限；未配置配额的 owner 视为不限。
- 判定分两层，拒绝原因可区分：
  - **硬上限**：作业（或同组同 owner 合计）自身超过配额 → 永久 `REJECTED`。
  - **瞬时饱和**：作业本身合法，但当前占用 + 本单元需求超过配额 → 保持 `PENDING`，
    待占用释放后下一趟自动可调度。
- 分配/释放与节点用量在同一临界区内成对更新；快照中
  `Σ 节点 Used == Σ RUNNING 作业需求 == Σ owner 用量`，CLI 末尾会做该对账。

## 抢占规则（确定性、可界定重试）
- 触发：高优先级单元直接放不下，且存在**严格更低优先级**的运行中“受害者组”能腾出资源。
- 受害者组：普通作业自身成一组；运行中的 gang 整组成一组。gang 的**有效优先级取成员
  最高值**——只要有一个成员不低于请求方，整组就不可抢占。
- 受害者组排序是全序：组有效优先级升序 → 组持有总资源升序 → 最小成员 ID 升序。
  因此同一局面永远选出同一批受害者。
- 受害者被释放后**重新入队**并消耗一次重试预算；预算耗尽（默认/CLI 为 2，单测默认 3）
  后转为 `FAILED / PREEMPTED`，实现“可被有界重试”。
- 节点故障撤离与抢占共用同一份有界重试预算（语义统一：被外部事件打断后可重试的次数）。

## 异常处理语义
- **节点故障 `FailNode`**：
  1. 集群先把节点置 `DOWN` 并把其用量**恰好归零一次**（`DOWN` 节点不参与放置，
     归零不会被误判为“全空闲”）；
  2. 直接受影响作业 + 同组所有运行成员构成撤离集合，健康节点上的 sibling task 走正常
     释放路径——因此**不会重复计数**；
  3. 每个撤离作业的放置索引与 task 绑定被清空，再原子重排队或 `FAILED`，**不丢状态**；
  4. 重排队原因记为 `NODE_UNAVAILABLE`，`RestoreNode` 后可重新调度到健康/恢复节点。
- **拒绝原因可区分**：`EXCEEDS_NODE_CAPACITY`（永久）、`INSUFFICIENT_RESOURCE`（瞬时）、
  `DEPENDENCY_NOT_MET` / `DEPENDENCY_FAILED`、`QUOTA_EXCEEDED`（硬上限或瞬时饱和，
  见作业 `Message`）、`GROUP_CO_SCHEDULING`、`NODE_UNAVAILABLE`、`PREEMPTED`。
- **一致性**：`Submit/RunPass/CompleteJob/FailNode/RestoreNode/Snapshot/Job` 全部由
  `scheduler.mu` 串行；`Snapshot` 返回深拷贝，读操作不会看到中间态。
- 分配是原子的：溢出时返回失败且不改用量；释放若导致负用量会直接 panic（内部不变量保护）。

## 可复现与验证方式
- **固定随机种子**：`simulator.New(seed, ...)` 用 `math/rand` 固定源生成全部作业
  （owner、优先级、资源、gang 规模、数组规模、依赖）。`TestDeterministicReproduction`
  构造两个独立世界（含相同故障脚本），断言规范化快照逐字节相同；
  `TestDifferentSeedsCanDiverge` 防止种子被硬编码。
- **可解释日志**：提交时打印作业完整输入（owner/优先级/组/数组/资源/依赖/task 数/重试），
  判定时打印 `evaluate ...` 与 `judgment: ...`（放置计划、拒绝原因、抢占受害者）。
  单测失败时自动输出完整 trace（见各测试的 `t.Cleanup`）。
- 测试覆盖：
  - 资源不足：`TestRejectExceedsNodeCapacity`、`TestInsufficientResourceIsTransient`、
    `TestQuotaHardCeilingAndTransient`；
  - 依赖：`TestDependencyReasons`（未满足 vs 依赖失败）；
  - 组调度：`TestGangAllOrNothingAndNoResidue`、`TestGangFailureAtomic`、
    `TestGangPreemptionAtomic`、`TestGangMixedPriorityNotPreemptable`；
  - 数组独立性：`TestArrayElementsAreIndependent`；
  - 抢占与有界重试：`TestPreemptionRules`；
  - 节点故障不重复计数：`TestNodeFailureNoDoubleCount`、`TestScriptedFailureReconciles`；
  - 并发一致：`TestConcurrentConsistency`（多 goroutine 提交/调度/快照后三方对账）；
  - cluster/quota/queue 各自有独立单测。

```
go test ./... -race -v        # 全量用例
go test ./internal/scheduler  # 调度核心（失败时打印输入与判定 trace）
```

## CLI 输出解读
`go run ./cmd/hpc-sim` 依次打印：每个作业的提交输入与判定依据（trace）、节点容量/用量、
按状态的计数、各 owner 的配额用量、逐作业最终结论（状态 + 原因 + 落点 + 说明），
最后做 `节点用量 == 运行中需求 == 配额账本` 的三方对账并输出 `OK`。
