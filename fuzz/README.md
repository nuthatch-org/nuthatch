# Fuzzing the decode path (nuthatch#290)

Decode is the one place untrusted input meets stored state (CLAUDE.md §4): an ABI is
attacker-supplied the moment a nest is pulled from a registry, and a log is whatever an RPC
endpoint hands back. These targets hunt **panics and unbounded allocation**, not wrong answers -
a `Result::Err` or an `Ok(None)` is always an acceptable outcome; a crash or an OOM abort is a
finding.

## Targets

- **`abi_json`** - raw fuzzer bytes interpreted directly as ABI JSON text. Finds malformed
  *shapes* (wrong JSON types, missing fields, truncated documents) that a structured generator
  would never emit. Seeded from `corpus/abi_json/`.
- **`abi_arbitrary`** - a small `Arbitrary`-derived generator that reliably reaches the shapes raw
  byte mutation rarely stumbles into: absurd tuple depth (iteratively nested, so the generator
  itself can't stack-overflow first), duplicate topic0 (repeated identical event signatures), and a
  `uint256[huge]` fixed-array param.
- **`decode_log`** - a fixed, curated set of ABIs (ordinary indexed/non-indexed params, an indexed
  `string` that arrives as a topic hash, a dynamic array + tuple, a `uint256[4000000000]` fixed
  array, a tuple nested 512 levels deep, and a non-indexed `uint64` for the COR-11 narrow-uint
  branch) decoded against a fuzzed `Log`: arbitrary topic count
  and length, truncated/garbage `data`, and corrupted hex. `topic0` is left fixed per fixture -
  it's resolved ahead of time by whoever built the registry, not something a byzantine RPC response
  or a malicious log actually controls.

Two more crates hold targets that link more than the decode path (#2743), each its own workspace and
built on the pinned stable with `-s none`, as CI's `fuzz-sql` and `fuzz-embed` jobs run them:

- **`fuzz/sql`**: `sql_confinement` (RFC-0073 FR8), `cid_car` (IPFS CAR bodies, CID text and
  `cid::verify`) and `subgraph_manifest` (`parse_manifest`).
- **`fuzz/embed`**: `header_chain` (the verified follow's header RLP and `verify_chain`) and
  `attach_ipc` (`attach`'s Arrow IPC stream, which CI does not run until #2811).

```sh
cargo fuzz run --fuzz-dir fuzz/sql -s none -O cid_car
cargo fuzz run --fuzz-dir fuzz/embed -s none -O header_chain
```

## Running

```sh
cargo +nightly fuzz run abi_json
cargo +nightly fuzz run abi_arbitrary
cargo +nightly fuzz run decode_log
```

Requires `cargo install cargo-fuzz` and a nightly toolchain (libFuzzer's sanitizer coverage
instrumentation isn't available on the pinned stable 1.95.0 - see the toolchain note in
`../Cargo.toml`). This is dev-only tooling: `fuzz/` is excluded from the published crate and never
sits in the runtime data path.

**No sanitizer/debug-assertions workaround needed as of nuthatch#581.** These targets used to
require `-O --sanitizer none`: nightly's trait solver ICEs computing a vtable slot for dbsp's
`StarJoinFuncTrait`/`DynClone` impl whenever the build carried debug-assertions *or* ASan on top of
SanitizerCoverage instrumentation - a real rustc bug, reproduced on both a 2026-06-22 and a
2026-08-13 nightly, nothing to do with nuthatch's own code. #581 extracted the decode path
(`registry.rs`/`rpc.rs`) into the standalone `nuthatch-decode` crate this crate now depends on;
`dbsp` is not in `nuthatch-decode`'s or `nuthatch-fuzz`'s dependency graph at all
(`cargo +nightly tree -p nuthatch-fuzz | grep -c dbsp` is `0`), so the ICE cannot be reached
regardless of flags. ASan and debug-assertions run by default again. `overflow-checks = true` (set
in this crate's `[profile.release]`) is unaffected either way and remains the property this target
actually needs - a `uint256[huge]`-shaped size computation must panic, not silently wrap.

CI runs all three under a bounded `-runs=300000 -max_total_time=180` smoke pass on every push/PR
(`fuzz-smoke` in `.github/workflows/ci.yml`) - not a real fuzzing campaign, a regression tripwire.
For an actual campaign, run locally or in a dedicated job with a much larger time budget and let
the corpus grow (`cargo +nightly fuzz run <target> -- -max_total_time=3600`).

## Verifying the harness actually reaches decode

An acceptance criterion phrased as "no crash found" passes trivially if the harness silently isn't
reaching the code it claims to fuzz - this project has been bitten by that shape of false-green
five times (CLAUDE.md's docs-go-stale lesson generalises). Before trusting a green fuzz run, prove
it reds on a known-bad build: reintroduce a fixed panic (e.g. revert
`decode/src/registry.rs:886`'s `u.saturating_to::<u64>()` guard, COR-11, to an unchecked
`.to::<u64>()`) and confirm
`cargo +nightly fuzz run decode_log` finds a crashing input against the seeded corpus, then revert.
That check isn't automated here - a permanently red "this must find a bug" target isn't buildable -
so re-run it by hand after any change to the fuzz harness itself.

**Status as of 2026-08-18: the live proof reds, and the harness is trusted for COR-11.** Two
things were in the way, and only one of them was the ICE.

1. The ICE is gone. #581/#607 extracted `nuthatch-decode`; `dbsp` is not in `nuthatch-fuzz`'s
   dependency graph at all, so the build carries ASan and debug-assertions again by default.

2. The fixture set could not reach the guard. None of this target's five ABIs declared a uint
   narrower than `uint256`, so `value_from_dynsol`'s `*bits <= 64` branch - the one line COR-11
   lives on - was structurally unreachable no matter how long libFuzzer ran. That is a coverage
   gap, not a mutation-budget problem, and it is what nuthatch#612 asked for: `fuzz__narrow`
   (`Narrow(uint64 id)`, non-indexed) is the fixture that closes it.

Proven live on 2026-08-18, both directions, on the post-#618 sprint branch with the fixture
applied:

- **Reverted guard reds.** With `u.saturating_to::<u64>()` at `decode/src/registry.rs:886` put
  back to an unchecked `u.to::<u64>()`, `decode_log` panicked after **16,484 executions** - about
  a second - with `Uint conversion error: Overflow(256, 17289301308300282624, ...)` at that exact
  line, and wrote the crashing input to `fuzz/artifacts/decode_log/`. Before this fixture the same
  reverted build survived 300,000 runs untouched, so the fixture is what made the branch findable
  rather than the fuzzer getting luckier.
- **Restored guard runs clean.** With the guard back, 300,000 runs finished in 14s at ~21k
  exec/s, 347 new corpus units, no crash.

Re-run that pair by hand after any change to this harness. It is the only thing standing between
"the fuzz job is green" and "the fuzz job would notice", and this project has been bitten by that
distinction five times.

**The unit-test half is proven too, and needs no nightly at all.** The COR-11 *oracle*
- that `value_from_dynsol` catches a dirty-high-bits uint and does not panic - is exercised by a
plain `cargo test` in `decode/src/registry.rs`
(`registry::tests::dirty_high_bits_on_a_sub64_uint_saturate_instead_of_panicking`). It runs a
crafted log (a `uint64` field with a 32-byte word of `0xff`) through the real
`DecodeRegistry::decode` entry point: with the guard in place it passes, with it reverted it
panics. That proves the guard is correct; the fuzz proof above is what proves libFuzzer can find
its way there unaided.
