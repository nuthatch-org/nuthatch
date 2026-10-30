# Draft for feldera/feldera: building `dbsp` for `wasm32-unknown-unknown`

Not posted. The text below is what nuthatch would open against github.com/feldera/feldera, as one
issue and up to four PRs, once Chief says so. Every change here is commit `4813166` on
nuthatch-org/feldera's `nuthatch/wasm32-0.320.0` (and `1181ec2` on nuthatch-org/size-of), on the rev
`dbsp 0.320.0` was published from, which `embed/` builds; it runs natively and under wasm32 in Node.

## Issue: `dbsp` on `wasm32-unknown-unknown`, single-threaded

We (nuthatch, an EVM indexer that uses `dbsp` for incrementally maintained entities) run our
decoder, sealer and SQL engine in a browser tab already and would like the DBSP circuits there too.
`cargo check --target wasm32-unknown-unknown` on a crate depending on `dbsp = "=0.320.0"` fails
before reaching `dbsp` itself, and the runtime assumes threads once it compiles. This is what we
found, what we patched, and what we think is worth taking upstream. The patched crate builds and runs
on wasm32 in Node and answers the same relations as the native build (2 M rows with 500 k
retractions through a spine; our own entity circuit with a join, `aggregate_linear`, `Min`/`Max`
and an outer join over 1,000 windows against its batch oracle).

What stops the build, in dependency order:

1. `feldera-types` depends on `actix-web` unconditionally, used only in `error.rs` (the
   `ResponseError` bound on `DetailedError`) and `runtime_status.rs` (`Responder` and
   `ResponseError` impls, and `StatusCode` in `ExtendedRuntimeStatusError`). That pulls
   actix-server, tokio `net`, mio and socket2 into every consumer, and mio has a
   `compile_error!` for wasm.
2. `zip` with default features in `feldera-ir` and `dbsp` (both only write deflated entries),
   which builds `zstd-sys` from C.
3. `object_store` in `feldera-storage` with `aws`, `gcp`, `azure` and `http`, which bring reqwest,
   hyper and ring; `feldera-storage` only uses `object_store::path`.
4. tokio `rt-multi-thread` in `dbsp` and `feldera-storage`, a `compile_error!` on wasm since tokio
   1.3x.
5. `mimalloc-rust-sys` (C), `core_affinity`, `nix` and `fdlimit` as unconditional dependencies,
   none of which has a wasm build.
6. rkyv 0.7 with `size_64`: `Offset` is only implemented for `i64` under
   `target_pointer_width = "64"`, so the feature cannot be used on a 32-bit target at all
   (`ordered-float`'s `rkyv_64` has the same effect).
7. `feldera-size-of` imports `core::arch::wasm::v128`, which is the unstable `simd_wasm64` path;
   `core::arch::wasm32::v128` is stable.
8. Two `usize` literals wider than 32 bits (`spine_async.rs` `size_to_level`'s digit bands, and
   `trace.rs` comparing a `u64` separator against `Archived<usize>`), the multihost exchange's
   `tokio::net` types, and `std::os::fd` / `libc::pread` in `feldera-storage::fbuf`.

What stops it running:

9. `Runtime::run` spawns the merger tokio runtime (multi-thread), an `rss-monitor` aux thread and
   one std thread per worker; `init_circuit` is a client/worker handshake over bounded channels and
   a `Parker`. On a single thread that is a `panic!` at spawn, or a deadlock.
10. `Spine::new` requires a `Runtime`, so the no-runtime `RootCircuit::build` path cannot host an
    integral, join or aggregate. Spines merge on the tokio merger runtime; `Spine::insert` awaits
    `backpressure_wait` past 128 batches, which only a merger task relieves.
11. `std::time::Instant::now()` panics on this target, and dbsp reads it in `DBSPHandle`, the spine
    and the profiler. A tokio runtime built with the `time` feature on reads it at construction.
12. `feldera-samply` reads `clock_gettime`, `getpid` and `gettid`.

A manifest fault we found on the way, independent of wasm: `dbsp` uses `tokio::net`, `tokio::time`
and `tokio::select!` but only declares `macros`, `rt` and `rt-multi-thread`; the rest arrive by
feature unification through feldera-types' actix-web. Making actix optional breaks the native build
of `dbsp` until `net`, `time`, `io-util` and `sync` are named.

## PR 1: `feldera-types`, `actix-web` behind a feature

- `actix-web` optional; feature `actix = ["dep:actix-web"]`, in `default` so nothing changes for
  the pipeline manager and adapters.
- `DetailedError: StdError + Serialize`, with `+ ResponseError` only under the feature;
  `ErrorResponse::from_error` logs by status code only under the feature and is otherwise
  `from_error_nolog`.
- `ExtendedRuntimeStatusError` carries `status_code: u16` without the feature; the `Responder` and
  `ResponseError` impls are feature-gated. `coordination` compiles either way.
- `dbsp`'s tokio dependency names `net`, `time`, `io-util` and `sync` (the fault above).

34 lines in feldera-types, 4 in dbsp's manifest.

## PR 2: feature trims with no behaviour change

- `zip` with `default-features = false, features = ["deflate"]` in `feldera-ir` and `dbsp`.
- `object_store` with `default-features = false` in `feldera-storage`.
- `feldera-size-of`: `core::arch::wasm32::v128` on wasm32, `wasm64::v128` on wasm64.

Seven lines. Consumers who want the cloud object stores or zstd still get them by enabling the
features themselves, which cargo unifies.

## PR 3: the manifest and the std surface on wasm32

Target-conditional dependency blocks, so the native build is byte-for-byte what it is today:

- `mimalloc-rust-sys`, `core_affinity`, `fdlimit`, `nix` under `cfg(not(target_arch = "wasm32"))`
  (`nix` under `cfg(unix)`), with a 15-line `CoreId` shim so the pinning code compiles unchanged, a
  `ThreadCpuTime::now()` that answers zero off unix, and `init_fd_limit` a no-op.
- tokio split per target: `rt-multi-thread` (and the four named above) natively; `macros`, `rt`,
  `sync`, `io-util` on wasm32. The merger runtime is `new_current_thread` on wasm32 and does not
  call `enable_all()` there (the time driver reads `Instant` as it is built).
- rkyv and `ordered-float` split per target: `size_64` / `rkyv_64` natively, `size_32` / `rkyv_32`
  on wasm32. This changes the archived layout on wasm32 only; checkpoints are not portable across
  that line, which they were not going to be.
- `storage::backend::posixio_impl` and `storage::dirlock` under `cfg(unix)`, with a stub
  `LockedDirectory` whose constructors answer `StorageError::StorageDisabled`; `fbuf::read_exact_at`
  and its `AsRawFd` import under `cfg(unix)`; `feldera_storage::tokio` (the global multi-thread
  runtimes) and `dbsp::circuit::tokio`'s re-export under `cfg(not(target_arch = "wasm32"))`.
- The multihost exchange's `TcpStream`/`TcpListener` behind a 60-line `wasm_net` stub whose every
  call answers `ErrorKind::Unsupported`, and `tokio::time::sleep` a no-op there; the exchange is
  never configured on one thread.
- `Instant` through one `pub(crate) mod wasm_time` per crate (`web_time::Instant` on wasm32,
  `std::time::Instant` elsewhere), touching 13 import lines across `dbsp`, `feldera-storage` and
  `feldera-samply`. `web-time` 1.1 is MIT OR Apache-2.0.
- `feldera-samply`: `Timestamp::now()` answers 0 and `current_pid()` 0 off unix.
- `size_to_level` matches on `effective_len as u64`; the trace separator compares as `usize`.

About 250 lines, all `cfg`.

## PR 4: a `single-thread` feature for the runtime

Off by default; on wasm32 it is the only mode that can run. Two changes:

1. **Inline workers.** `Runtime::run` runs each worker's body on the calling thread (setting
   `WORKER_INDEX`, the thread type, the slab pool and `RUNTIME` there) and `init_circuit`'s worker
   loop becomes a `serve_one() -> Option<bool>` closure that handles one queued command and reports
   whether it did, found none, or has exited. Natively the closure runs inside the original
   `loop { ... parker.park() }`. Under the feature it is pushed to a thread-local list, and
   `DBSPHandle::broadcast_command` / `unicast_command` drive the list after sending a command, so
   the existing `Select` on the status channel finds the reply already there. The init channels are
   `bounded(1)` rather than `bounded(0)` so the handshake does not block; `RuntimeHandle.workers`
   holds `Option<JoinHandle>`; the RSS monitor is not started.
2. **Synchronous merges.** `MergeWorkers::run` loops `merge_step` on the calling thread until there
   is no work and clears the once-per-level bit, and `AsyncMerger::backpressure_wait` drains every
   level before it checks the batch count, so the `Notify` is never awaited. The memory-pressure
   spill path is untouched; with no storage configured it never fires.

About 120 lines. The `ShardedAccumulator` and its backpressure waiter are not on the one-worker
path (`dyn_shard_accumulate` takes `dyn_accumulate` when `num_workers() == 1`), and
`integrate_trace_retain_keys` / `complete_merges` have no callers on it, so the feature does not
touch them; a multi-worker layout under the feature should be refused at `Runtime::run`.

We would carry PR 3 and PR 4 as a `[patch]` until they land or are declined; PR 1 and PR 2 we think
stand on their own.
