# Region CPU：`unified-read` / `scheduler pool` 完整度分析与 patch 方案

本文只回答两个更聚焦的问题：

1. 如果只关心 `unified-read` 和 `scheduler pool`，当前 `region CPU` 收集是否完整？
2. 如果目标只是“提升 `scheduler pool` 的 region CPU 收集完整度”，可以怎么改？

为避免歧义，本文里的 `scheduler pool` 主要指：

- `sched-pool`
- `sched-high`
- `sched-pri`

不展开讨论：

- `store-writer`
- `apply`
- raftstore 线程
- “完整 write path CPU” 这种更大的口径

## 先给结论

### 结论 1：`unified-read` 当前已经相对完整

`unified-read` 的主要读请求，在提交到读池之前就已经创建了 `resource_tag`，并把实际执行 future 包在 `.in_resource_metering_tag(...)` 里。

典型入口：

- `src/storage/mod.rs:645`
- `src/storage/mod.rs:804`
- `src/storage/mod.rs:1033`
- `src/storage/mod.rs:1238`
- `src/storage/mod.rs:1448`
- `src/storage/mod.rs:1649`
- `src/storage/mod.rs:1818`
- `src/storage/mod.rs:2082`
- `src/storage/mod.rs:2240`
- `src/storage/mod.rs:2347`
- `src/storage/mod.rs:2881`
- `src/storage/mod.rs:3047`
- `src/storage/mod.rs:3136`
- `src/storage/mod.rs:3316`
- `src/coprocessor/endpoint.rs:581`
- `src/coprocessor/endpoint.rs:605`
- `src/coprocessor/endpoint.rs:875`
- `src/coprocessor/endpoint.rs:884`

因此，如果只看 `unified-read`，当前模型已经覆盖了绝大多数“请求业务逻辑本身”的 CPU。

它仍然不会和 `store_cpu_usages["unified-read"]` 完全相等，但主要差异通常来自：

- executor/runtime 自身调度开销；
- 少量不带 region tag 的任务；
- 采样窗口误差。

换句话说：

> `unified-read` 现在的问题更像“统计边界天然不可能 100% 精确”，而不是“有一大片业务 CPU 根本没包住”。

### 结论 2：`scheduler pool` 当前是结构性不完整

当前 txn scheduler 的 tag attach 时机偏晚。

代码里真正 attach tag 的地方在 `TxnScheduler::process()`：

- `src/storage/txn/scheduler.rs:1241`
- `src/storage/txn/scheduler.rs:1288`

但在此之前，命令已经在 scheduler worker 线程上跑过一段 CPU，包括：

- `schedule_command` 之后的调度逻辑：`src/storage/txn/scheduler.rs:564`
- `fail_fast_or_check_deadline` 里的 `precheck_write_with_ctx(...)`：`src/storage/txn/scheduler.rs:608`
- `execute()` 里的 `kv::snapshot(...)`：`src/storage/txn/scheduler.rs:719`

所以对 `sched-pool/sched-high/sched-pri` 来说，当前漏掉的不是零碎边角，而是明确存在的前半段 CPU。

### 结论 3：这个缺口能修，但占比多少必须实测

“scheduler 前半段 CPU 没被 region tag 收进去”这件事本身是能修的。

但“它占整个 `scheduler pool` 的多少”不能靠静态读代码得出，需要 workload 实测。

只能定性判断：

- `unified-read` 的缺口通常偏小；
- `scheduler pool` 的缺口可能不小，尤其是：
  - 写多；
  - 冲突多；
  - precheck/snapshot 重；
  - 普通 raw write 多。

## 为什么 `scheduler pool` 会漏

### 1) txn scheduler 在 `process()` 之前已经跑了一段 CPU

当前链路大致是：

```text
run_cmd
  -> schedule_command
  -> fail_fast_or_check_deadline (部分路径)
  -> execute
       -> snapshot
       -> process
            -> 这里才 attach resource tag
```

对应代码：

- `src/storage/txn/scheduler.rs:525`
- `src/storage/txn/scheduler.rs:564`
- `src/storage/txn/scheduler.rs:608`
- `src/storage/txn/scheduler.rs:719`
- `src/storage/txn/scheduler.rs:1236`
- `src/storage/txn/scheduler.rs:1241`
- `src/storage/txn/scheduler.rs:1288`

因此，下面这些 CPU 当前不在 region tag 覆盖里：

- precheck；
- 获取 snapshot；
- `process()` 之前的 scheduler 框架逻辑。

### 2) 普通 raw write 复用 scheduler pool，但当前没有 metering tag

当前 `raw_put/raw_batch_put/raw_delete/raw_batch_delete/raw_delete_range` 都会通过 `sched_raw_command(...)` 把 future 扔进 scheduler pool：

- `src/storage/mod.rs:1922`
- `src/storage/mod.rs:2414`
- `src/storage/mod.rs:2526`
- `src/storage/mod.rs:2591`
- `src/storage/mod.rs:2652`
- `src/storage/mod.rs:2700`

但 `sched_raw_command(...)` 当前只是直接 `spawn`，没有像读路径那样做 `.in_resource_metering_tag(...)`。

因此，如果你关心的是：

> `store_cpu_usages` 里 `sched-*` 线程的 CPU，能有多少被归到 region

那么普通 raw write 是一个明确漏项。

### 3) 当前 CPU recorder 是“按采样点当前 attached tag 记账”的

`CpuRecorder` 的逻辑是：

- 每次 tick 读取线程当前 `attached_tag`；
- 如果有 tag，就把自上次采样以来的 CPU delta 记到这个 tag 上。

代码：

- `components/resource_metering/src/recorder/sub_recorder/cpu.rs:27`

因此即使 attach 时机完全修正，按 region 的细分结果仍会受采样窗口影响，不会变成严格逐指令精确归因。

这也意味着：

> 只提升 `scheduler pool` 覆盖度是可行的，但不应期待修完后 `sum(region cpu in sched pool) == store_cpu_usages["sched-*"]`。

## 按线程池看当前状态

| 线程池 | 当前完整度 | 主要原因 | 是否建议现在动 |
|---|---|---|---|
| `unified-read` | 相对完整 | 请求 future 在进入读池前已显式打 tag | 暂不建议 |
| `sched-pool/sched-high/sched-pri` | 明显不完整 | tag attach 偏晚，且普通 raw write 未打 tag | 建议优先修 |

## patch 目标

本方案只追求一件事：

> **尽量把运行在 `scheduler pool` 上、且本来就能从 RPC `Context` 找到 `region_id` 的请求 CPU，更早、更完整地归到当前 region。**

### 明确不做的事

本方案**不**试图解决：

- `store-writer` / `apply` / raftstore 的下游 CPU 传播；
- PD 上报 `write CPU` 字段；
- 给 region CPU 新增“线程池维度”原生字段；
- `unified-read` 的 runtime 开销精确归因。

## 建议 patch 方案

### Patch A：把 txn scheduler 的 tag attach 从 `process()` 前移到 `execute()`

这是收益最大、改动也最聚焦的一步。

#### 当前问题

现在的 attach 在 `process()` 内：

- `src/storage/txn/scheduler.rs:1241`
- `src/storage/txn/scheduler.rs:1288`

因此 `execute()` 里的 `snapshot` 不在 tag 内：

- `src/storage/txn/scheduler.rs:719`

#### 建议改法

在 `TxnScheduler::execute()` 里，在构造 `execution` future 之前就创建：

```rust
let resource_tag = self.inner.resource_tag_factory.new_tag(task.cmd().ctx());
```

然后把整个 `execution` future 包进：

```rust
.in_resource_metering_tag(resource_tag)
```

同时删除 `process()` 内部这层包裹，避免 nested attachment。

这一点很重要，因为 `ResourceMeteringTag::attach()` 明确禁止 nested attachment：

- `components/resource_metering/src/lib.rs:76`

#### 预期收益

这一步能把以下 scheduler pool CPU 补进 region 归因：

- `snapshot`
- `process()` 之前的框架逻辑
- `process_read/process_write` 本身

它不会影响 `unified-read`，也不会改变 downstream write CPU 的覆盖边界。

### Patch B：给 `fail_fast_or_check_deadline()` 的 scheduler future 也打 tag

#### 当前问题

`fail_fast_or_check_deadline()` 会在 scheduler pool 上再 `spawn` 一个 future：

- `src/storage/txn/scheduler.rs:608`

其中的：

- `precheck_write_with_ctx(&ctx)`
- deadline 检查 future

当前也没包在 `.in_resource_metering_tag(...)` 里。

#### 建议改法

在 `fail_fast_or_check_deadline()` 里，基于 `cmd.ctx()` 创建 `resource_tag`，把 `execution` future 也包起来。

#### 预期收益

这部分收益比 Patch A 小，但它能把 scheduler pool 上的 precheck CPU 一并收进来，口径更完整。

### Patch C：给 `sched_raw_command()` 加上 metering tag

如果你的目标明确是“提升 scheduler pool 收集完整度”，这一步也应该做。

#### 当前问题

`sched_raw_command(...)` 当前签名只有：

- `metadata`
- `pri`
- `tag`
- `future`

见：

- `src/storage/mod.rs:1922`

它没有接收 `Context`，也没有创建 `resource_tag`。

#### 建议改法

把 `sched_raw_command(...)` 扩成接收 `&Context`（或直接接收已创建好的 `ResourceMeteringTag`），例如：

```rust
fn sched_raw_command<T>(
    &self,
    ctx: &Context,
    metadata: TaskMetadata<'_>,
    pri: CommandPri,
    tag: CommandKind,
    future: T,
) -> Result<()>
where
    T: Future<Output = ()> + Send + 'static,
```

内部创建：

```rust
let resource_tag = self.resource_tag_factory.new_tag(ctx);
```

然后用：

```rust
future.in_resource_metering_tag(resource_tag)
```

再送入 scheduler pool。

#### 建议覆盖的调用点

至少包括这些直接跑在 scheduler pool 上的 API：

- `src/storage/mod.rs:2414` `raw_put`
- `src/storage/mod.rs:2526` `raw_batch_put`
- `src/storage/mod.rs:2591` `raw_delete`
- `src/storage/mod.rs:2652` `raw_delete_range`
- `src/storage/mod.rs:2700` `raw_batch_delete`
- `src/storage/mod.rs:3165` `raw_compare_and_swap_atomic`
- `src/storage/mod.rs:3194` `raw_batch_put_atomic`
- `src/storage/mod.rs:3215` `raw_batch_delete_atomic`
- `src/storage/mod.rs:3384` `update_txn_status_cache`

#### 预期收益

这一步主要补的是：

- 普通 raw write 在 scheduler pool 上的 CPU；
- raw atomic 进入 txn scheduler 之前那一小段 scheduler CPU；
- `update_txn_status_cache` 这类复用 scheduler pool 的管理类路径。

### Patch D：不改 `unified-read`

这一步是刻意的“保持不动”。

理由很简单：

- 当前 `unified-read` 已经相对完整；
- 它和 `store` 侧的残余差异主要是 runtime/框架开销，不是简单移动 attach 时机就能补齐的；
- 如果只想提高 ROI，优先修 `scheduler pool` 更合适。

## 一个更具体的落地顺序

如果只做最小收益最大的版本，建议顺序是：

1. 先做 Patch A；
2. 再做 Patch B；
3. 最后做 Patch C。

原因：

- Patch A 直接补上 txn scheduler 最大的结构性缺口；
- Patch B 是同一条链路上的自然补充；
- Patch C 虽然也重要，但它涉及 `Storage` 侧多个 raw API 调用点，改动面略大。

## 改完后应该期待什么

改完后，更合理的预期是：

- `scheduler pool` 的 region CPU 覆盖率明显提升；
- txn 写请求在 scheduler pool 上的前半段 CPU 能更多进入 region 归因；
- 普通 raw write 在 scheduler pool 上不再完全漏掉；
- `unified-read` 基本不变；
- `sum(region cpu in scheduler pool)` 会更接近 `store_cpu_usages["sched-*"]`，但仍通常小于它。

仍然会漏掉的部分包括：

- scheduler runtime 自身开销；
- 不带 region tag 的后台任务；
- 下游 `store-writer` / `apply` CPU；
- 采样窗口误差。

## 如何量化“修了多少”

这件事不能靠静态代码判断，建议直接做 before/after workload 对比。

### 建议指标

在同一个时间窗内，对比：

1. `store_cpu_usages` 中：
   - `unified-read`
   - `sched-pool`
   - `sched-high`
   - `sched-pri`
2. region CPU 汇总值：
   - `sum(region_cpu where pool == unified-read)`
   - `sum(region_cpu where pool in sched-*)`

如果当前还没有把线程池名带进 tag，可临时把 pool 信息塞进 `extra_attachment` 做观测版实验。

### 建议 workload

至少分三类测：

1. 读多写少；
2. txn 写多、冲突高；
3. raw write 多。

### 预期现象

- baseline 下，`unified-read` 覆盖率应已较高；
- baseline 下，`scheduler pool` 覆盖率会明显偏低；
- 打上 Patch A/B/C 后，`scheduler pool` 覆盖率应明显改善；
- `unified-read` 不应有明显变化。

## 风险点

### 风险 1：忘了删 `process()` 里的旧 tag，导致 nested attachment

`ResourceMeteringTag::attach()` 已经对 nested attachment 做了保护：

- `components/resource_metering/src/lib.rs:76`

所以 Patch A 一定要和“移除 `process()` 内层包裹”一起做。

### 风险 2：deadline future 的少量 CPU 也会被记到 region

这是 Patch B 的自然结果，但这部分 CPU 本来就运行在 scheduler pool 且由该请求触发，通常是可接受的。

### 风险 3：raw atomic 路径会出现“前半段”和“真正 txn 处理段”分两次记账

这不是 double count。

原因是：

- 外层 `sched_raw_command` 只覆盖它自己在 scheduler pool 上运行的那一小段；
- 真正进入 txn scheduler 后，又是另一段实际 CPU。

它们是两段不同时间的真实执行，叠加是合理的。

## 一句话建议

如果你当前只关心 `unified-read` 和 `scheduler pool`：

- **先不要动 `unified-read`；**
- **优先把 txn scheduler 的 tag attach 前移到 `execute()`，再补 `fail_fast_or_check_deadline()` 和 `sched_raw_command()`。**

这是当前提升 `scheduler pool` region CPU 完整度、且改动边界最清晰的一套方案。
