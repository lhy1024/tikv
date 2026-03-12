# Region CPU 归因与 RPC 明细

本文基于当前仓库代码，说明：

1. 当前 TiKV 的 `region` 级 CPU 是如何采集和归因的；
2. TiKV 上报给 PD 的 `region` CPU 是否包含读写两侧；
3. 按具体 RPC 名称列出：当前实现里，该 RPC 是否会计入 `region CPU`。

## 先给结论

### 结论 1：当前 `region` 级 CPU 的“归因”关键是 `in_resource_metering_tag(...)`

- 更准确地说，CPU 的“采集”来自 `resource_metering` 对线程 CPU 的周期性采样；
- `in_resource_metering_tag(...)` 的作用，是把一段 future/stream poll 期间消耗的线程 CPU 绑定到某个 `ResourceMeteringTag`；
- `ResourceMeteringTag` 里带有 `store_id / region_id / peer_id`，其中 `region_id` 来自 RPC `Context`；
- PD worker 收到这些 `RawRecords` 后，再按 `tag.region_id` 聚合成 region 级 CPU。

关键代码：

- `components/resource_metering/src/lib.rs:223`
- `components/resource_metering/src/lib.rs:240`
- `components/resource_metering/src/lib.rs:296`
- `components/resource_metering/src/lib.rs:306`
- `components/resource_metering/src/recorder/sub_recorder/cpu.rs:27`
- `components/raftstore/src/store/worker/pd.rs:2052`
- `components/raftstore/src/store/worker/pd.rs:2286`

### 结论 2：TiKV 上报给 PD 的 `region` CPU 当前不是“读 CPU + 写 CPU”两套

- 当前代码里，PD 上报使用的是 `CpuStats.unified_read`；
- 无论是 `region heartbeat` 还是 `store heartbeat` 里的 hot peer，上报 CPU 时都只设置了 `unified_read`；
- 读写流量统计本身是都有的：`read_bytes/read_keys` 和 `written_bytes/written_keys` 都会上报；
- 但 CPU 不是分开上报读写两份，只有 `unified_read` 这一份；
- 另外，PD worker 注释已明确说明：当前 write path 的 CPU 只大致覆盖了 lock checking 等“写路径中的读负载”，并不代表完整写 CPU。

关键代码：

- `components/raftstore/src/store/worker/pd.rs:957`
- `components/raftstore/src/store/worker/pd.rs:994`
- `components/raftstore/src/store/worker/pd.rs:2046`
- `components/raftstore/src/store/worker/pd.rs:2444`
- `components/raftstore/src/store/worker/pd.rs:2491`

## 判断口径

下表里的“是否计入当前 region CPU”，按以下口径理解：

- `是`：该 RPC 当前实现会进入现有的 `resource metering -> region_id 聚合 -> PD/热点统计` 归因链；
- `部分`：只有该 RPC 的一部分 CPU 会进入当前归因链，或者是否计入取决于子路径/插件行为；
- `否`：当前实现没有进入这条 `region CPU` 归因链；
- `继承子命令`：该 RPC 只是容器，是否计入取决于其内部子命令；
- `未实现`：当前服务端直接 `unimplemented!()` 或返回 `UNIMPLEMENTED`。

## Top-level RPC 明细

| RPC 名称 | 主要下游路径 | 是否计入当前 region CPU | 说明 |
|---|---|---|---|
| `kv_get` | `future_get -> storage.get_entry` | 是 | 读请求显式创建 `resource_tag` 并在读池 future 上调用 `.in_resource_metering_tag(...)`。 |
| `kv_scan` | `future_scan -> storage.scan` | 是 | 同上，读路径按 `region_id` 归因。 |
| `kv_batch_get` | `future_batch_get -> storage.batch_get` | 是 | 同上。 |
| `kv_buffer_batch_get` | `future_buffer_batch_get -> storage.buffer_batch_get` | 是 | 同上。 |
| `kv_scan_lock` | `future_scan_lock -> storage.scan_lock` | 是 | 同上。 |
| `kv_prewrite` | `future_prewrite -> storage.sched_txn_command` | 部分 | 会进入 txn scheduler 的 metering 归因链，但当前 write path CPU 覆盖不完整。 |
| `kv_pessimistic_lock` | `future_acquire_pessimistic_lock -> storage.sched_txn_command` | 部分 | 同上。 |
| `kv_pessimistic_rollback` | `future_pessimistic_rollback -> storage.sched_txn_command` | 部分 | 同上。 |
| `kv_commit` | `future_commit -> storage.sched_txn_command` | 部分 | 同上。 |
| `kv_cleanup` | `future_cleanup -> storage.sched_txn_command` | 部分 | 同上。 |
| `kv_batch_rollback` | `future_batch_rollback -> storage.sched_txn_command` | 部分 | 同上。 |
| `kv_txn_heart_beat` | `future_txn_heart_beat -> storage.sched_txn_command` | 部分 | 同上。 |
| `kv_check_txn_status` | `future_check_txn_status -> storage.sched_txn_command` | 部分 | 该类命令可能带状态推进/回滚副作用，当前 write path CPU 仍只部分覆盖。 |
| `kv_check_secondary_locks` | `future_check_secondary_locks -> storage.sched_txn_command` | 是 | 经 txn scheduler，并更偏读路径。PD 侧 CPU 仍只上报 `unified_read`。 |
| `kv_resolve_lock` | `future_resolve_lock -> storage.sched_txn_command` | 部分 | 经 txn scheduler，但属于带写副作用路径。 |
| `kv_flush` | `future_flush -> storage.sched_txn_command` | 部分 | 经 txn scheduler，但属于写路径。 |
| `kv_delete_range` | `future_delete_range -> storage.delete_range` | 否 | 当前实现直接 `kv::write(...)`，未见 `.in_resource_metering_tag(...)`。 |
| `kv_prepare_flashback_to_version` | `future_prepare_flashback_to_version -> engine.start_flashback + storage.sched_txn_command` | 部分 | 中间的特殊 txn command 会进入 metering；`start_flashback` 这部分不走该链路。 |
| `kv_flashback_to_version` | `future_flashback_to_version -> storage.sched_txn_command + engine.end_flashback` | 部分 | txn command 部分进入 metering；`end_flashback` 不走该链路。 |
| `mvcc_get_by_key` | `future_mvcc_get_by_key -> storage.sched_txn_command` | 是 | 通过 txn scheduler 处理，归因到请求 `Context` 的 `region_id`。 |
| `mvcc_get_by_start_ts` | `future_mvcc_get_by_start_ts -> storage.sched_txn_command` | 是 | 同上。 |
| `raw_get` | `future_raw_get -> storage.raw_get` | 是 | raw 读路径显式使用 `.in_resource_metering_tag(...)`。 |
| `raw_batch_get` | `future_raw_batch_get -> storage.raw_batch_get` | 是 | 同上。 |
| `raw_scan` | `future_raw_scan -> storage.raw_scan` | 是 | 同上。 |
| `raw_batch_scan` | `future_raw_batch_scan -> storage.raw_batch_scan` | 是 | 同上。 |
| `raw_get_key_ttl` | `future_raw_get_key_ttl -> storage.raw_get_key_ttl` | 是 | 同上。 |
| `raw_checksum` | `future_raw_checksum -> storage.raw_checksum` | 是 | 同上。 |
| `raw_put` | `future_raw_put -> storage.raw_put` | 否 / 部分 | `for_cas = false` 时走普通 raw write，不计入；`for_cas = true` 时改走 atomic 路径，经 txn scheduler，仅部分覆盖写 CPU。 |
| `raw_batch_put` | `future_raw_batch_put -> storage.raw_batch_put` | 否 / 部分 | `for_cas = false` 不计入；`for_cas = true` 走 atomic 路径，部分计入。 |
| `raw_delete` | `future_raw_delete -> storage.raw_delete` | 否 / 部分 | `for_cas = false` 不计入；`for_cas = true` 走 atomic 路径，部分计入。 |
| `raw_batch_delete` | `future_raw_batch_delete -> storage.raw_batch_delete` | 否 / 部分 | `for_cas = false` 不计入；`for_cas = true` 走 atomic 路径，部分计入。 |
| `raw_delete_range` | `future_raw_delete_range -> storage.raw_delete_range` | 否 | 直接 `kv::write(...)`，未见 `.in_resource_metering_tag(...)`。 |
| `raw_compare_and_swap` | `future_raw_compare_and_swap -> storage.raw_compare_and_swap_atomic` | 部分 | 经 txn scheduler，会进入 metering；但属于写路径，当前 write CPU 只部分覆盖。 |
| `coprocessor` | `future_copr -> copr.parse_and_handle_unary_request` | 是 | cop unary 显式创建 `resource_tag`，并在 handler future 上调用 `.in_resource_metering_tag(...)`。 |
| `coprocessor_stream` | `copr.parse_and_handle_stream_request` | 是 | cop stream 同样显式打 tag。 |
| `raw_coprocessor` | `future_raw_coprocessor -> coprocessor_v2::Endpoint::handle_request` | 部分 | v2 endpoint 本身未见 `.in_resource_metering_tag(...)`；插件内部若调用 `storage.raw_get/raw_scan` 等读接口，会计入；插件自身 CPU 与普通 raw write 不会完整计入。 |
| `broadcast_txn_status` | `future_broadcast_txn_status -> storage.update_txn_status_cache` | 否 | 仅更新 txn status cache，走 `sched_raw_command`，未见 `.in_resource_metering_tag(...)`。 |
| `unsafe_destroy_range` | `gc_worker.unsafe_destroy_range` | 否 | GC worker 路径，不走当前 region CPU 归因链。 |
| `split_region` | `engine.raft_extension().split(...)` | 否 | raft extension 管理路径，不走 storage/coprocessor metering 链路。 |
| `raft` | `raft_extension` 收包 | 否 | Raft 通信路径，不是当前 external RPC resource metering 的 region CPU 口径。 |
| `batch_raft` | `raft_extension` 收包 | 否 | 同上。 |
| `snapshot` | `snap_scheduler.schedule(...)` | 否 | 快照传输路径，不走当前 region CPU 归因链。 |
| `tablet_snapshot` | `snap_scheduler.schedule(...)` | 否 | 同上。 |
| `check_leader` | `check_leader_scheduler.schedule(...)` | 否 | 控制/查询路径，不走 storage/coprocessor metering 链路。 |
| `get_store_safe_ts` | `check_leader_scheduler.schedule(...)` | 否 | 同上。 |
| `get_lock_wait_info` | `storage.dump_wait_for_entries` | 否 | 调试/观测接口，不走当前 region CPU 归因链。 |
| `get_health_feedback` | 本地拼响应 | 否 | 无存储执行路径。 |
| `batch_commands` | 容器 RPC | 继承子命令 | 是否计入取决于子命令；详见下节。 |
| `batch_coprocessor` | - | 未实现 | 当前 `unimplemented!()`。 |
| `dispatch_mpp_task` | - | 未实现 | 当前 `unimplemented!()`。 |
| `cancel_mpp_task` | - | 未实现 | 当前 `unimplemented!()`。 |
| `establish_mpp_connection` | - | 未实现 | 当前 `unimplemented!()`。 |
| `kv_import` | - | 未实现 | 当前 `unimplemented!()`。 |
| `kv_gc` | - | 未实现 | 当前返回 `UNIMPLEMENTED`。 |

## `batch_commands` 子命令明细

`batch_commands` 只是一个多路复用容器。每个子命令最终仍会落到对应的 `future_*` 或批处理实现上，因此是否计入 `region CPU` 继承自子命令本身。

| `batch_commands` 子命令 | 主要下游路径 | 是否计入当前 region CPU | 说明 |
|---|---|---|---|
| `Get` | `future_get` 或批量 `storage.batch_get_command` | 是 | 读路径，显式打 tag。批量合并时也仍走 metering。 |
| `RawGet` | `future_raw_get` 或批量 `storage.raw_batch_get_command` | 是 | raw 读路径，显式打 tag。 |
| `Coprocessor` | `future_copr` | 是 | cop unary 路径，显式打 tag。 |
| `Scan` | `future_scan` | 是 | 同 top-level `kv_scan`。 |
| `Prewrite` | `future_prewrite` | 部分 | 同 top-level `kv_prewrite`。 |
| `Commit` | `future_commit` | 部分 | 同 top-level `kv_commit`。 |
| `Cleanup` | `future_cleanup` | 部分 | 同 top-level `kv_cleanup`。 |
| `BatchGet` | `future_batch_get` | 是 | 同 top-level `kv_batch_get`。 |
| `BatchRollback` | `future_batch_rollback` | 部分 | 同 top-level `kv_batch_rollback`。 |
| `TxnHeartBeat` | `future_txn_heart_beat` | 部分 | 同 top-level `kv_txn_heart_beat`。 |
| `CheckTxnStatus` | `future_check_txn_status` | 部分 | 同 top-level `kv_check_txn_status`。 |
| `CheckSecondaryLocks` | `future_check_secondary_locks` | 是 | 同 top-level `kv_check_secondary_locks`。 |
| `ScanLock` | `future_scan_lock` | 是 | 同 top-level `kv_scan_lock`。 |
| `ResolveLock` | `future_resolve_lock` | 部分 | 同 top-level `kv_resolve_lock`。 |
| `DeleteRange` | `future_delete_range` | 否 | 同 top-level `kv_delete_range`。 |
| `PrepareFlashbackToVersion` | `future_prepare_flashback_to_version` | 部分 | 同 top-level `kv_prepare_flashback_to_version`。 |
| `FlashbackToVersion` | `future_flashback_to_version` | 部分 | 同 top-level `kv_flashback_to_version`。 |
| `BufferBatchGet` | `future_buffer_batch_get` | 是 | 同 top-level `kv_buffer_batch_get`。 |
| `Flush` | `future_flush` | 部分 | 同 top-level `kv_flush`。 |
| `RawBatchGet` | `future_raw_batch_get` | 是 | 同 top-level `raw_batch_get`。 |
| `RawPut` | `future_raw_put` | 否 / 部分 | 同 top-level `raw_put`。 |
| `RawBatchPut` | `future_raw_batch_put` | 否 / 部分 | 同 top-level `raw_batch_put`。 |
| `RawDelete` | `future_raw_delete` | 否 / 部分 | 同 top-level `raw_delete`。 |
| `RawBatchDelete` | `future_raw_batch_delete` | 否 / 部分 | 同 top-level `raw_batch_delete`。 |
| `RawScan` | `future_raw_scan` | 是 | 同 top-level `raw_scan`。 |
| `RawDeleteRange` | `future_raw_delete_range` | 否 | 同 top-level `raw_delete_range`。 |
| `RawBatchScan` | `future_raw_batch_scan` | 是 | 同 top-level `raw_batch_scan`。 |
| `PessimisticLock` | `future_acquire_pessimistic_lock` | 部分 | 同 top-level `kv_pessimistic_lock`。 |
| `PessimisticRollback` | `future_pessimistic_rollback` | 部分 | 同 top-level `kv_pessimistic_rollback`。 |
| `BroadcastTxnStatus` | `future_broadcast_txn_status` | 否 | 同 top-level `broadcast_txn_status`。 |
| `RawCoprocessor` | `future_raw_coprocessor` | 部分 | 同 top-level `raw_coprocessor`。 |
| `GetHealthFeedback` | 直接返回空响应，后续附加 health feedback | 否 | 无存储执行路径。 |
| `Empty` | `future_handle_empty` | 否 | 占位/空请求。 |
| `Import` | - | 未实现 | 当前 `unimplemented!()`。 |

## 关键依据汇总

### 1) resource metering 与 region 聚合

- `components/resource_metering/src/lib.rs:187`
- `components/resource_metering/src/lib.rs:240`
- `components/resource_metering/src/lib.rs:306`
- `components/resource_metering/src/recorder/sub_recorder/cpu.rs:32`
- `components/raftstore/src/store/worker/pd.rs:570`
- `components/raftstore/src/store/worker/pd.rs:2052`
- `components/raftstore/src/store/worker/pd.rs:2297`

### 2) PD 只上报 `unified_read`

- `components/raftstore/src/store/worker/pd.rs:993`
- `components/raftstore/src/store/worker/pd.rs:994`
- `components/raftstore/src/store/worker/pd.rs:2490`
- `components/raftstore/src/store/worker/pd.rs:2491`
- `components/raftstore/src/store/worker/pd.rs:2046`

### 3) txn scheduler 会统一打 metering tag

- `src/storage/txn/scheduler.rs:1241`
- `src/storage/txn/scheduler.rs:1270`
- `src/storage/txn/scheduler.rs:1288`

### 4) raw/coprocessor/storage 路径中显式打 tag 的典型位置

- `src/storage/mod.rs:645`
- `src/storage/mod.rs:804`
- `src/storage/mod.rs:2020`
- `src/storage/mod.rs:2082`
- `src/storage/mod.rs:2772`
- `src/storage/mod.rs:2881`
- `src/storage/mod.rs:3248`
- `src/storage/mod.rs:3316`
- `src/coprocessor/endpoint.rs:579`
- `src/coprocessor/endpoint.rs:605`
- `src/coprocessor/endpoint.rs:873`
- `src/coprocessor/endpoint.rs:884`

### 5) 当前不会进入该归因链的典型写路径

- `src/storage/mod.rs:1922`
- `src/storage/mod.rs:2387`
- `src/storage/mod.rs:2495`
- `src/storage/mod.rs:2573`
- `src/storage/mod.rs:2630`
- `src/storage/mod.rs:3355`

## 一句话总结

- 当前 region CPU 的归因关键机制确实是 `resource metering + in_resource_metering_tag(...)`；
- 但 TiKV 向 PD 上报的 region CPU 当前只有 `unified_read` 口径，不是读写两套 CPU；
- 因而看到 `written_bytes/written_keys` 不代表也有同等完整度的 write CPU；
- 对很多 txn 写请求和 atomic raw write，请理解为“部分进入当前 region CPU 模型”，而不是“完整反映该 RPC 的全部 CPU”。
