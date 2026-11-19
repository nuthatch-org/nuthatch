# Correctness rules (full text)

Moved verbatim from `CLAUDE.md` on 2026-11-19 to keep the always-loaded standing brief short. Nothing was reworded or dropped. `CLAUDE.md` links here; the non-negotiables and the rules summarised there still bind.

## Correctness rules

- Decode: deterministic Rust, topic0-keyed, contract-ABI priority with generic fallback.
  ABI acquisition: Sourcify first, then Etherscan-class APIs. Cache ABIs locally.
- Never retroactively re-decode stored history when ABIs improve; version decodings.
- Golden/deterministic-simulation tests for every handler and view (Matchstick lineage):
  fixed block fixtures in, exact entity state out. AI-generated tests are welcome, but they
  must be deterministic and reviewed like any code.
- Property tests for reorg handling: random reorg depths against the hot store must always
  converge to the canonical chain state.
- Benchmarks are CI artifacts. Single-nest RSS, dense multi-nest RSS, entity point-read p50 and
  tip lag's after-seen p50 (fetch, decode and commit at the tip: 30 ms against 17-19 ms measured)
  fail the build on regression. Backfill events/sec is measured on every PR and tracked, not gated:
  the CI fixture cannot see a 4x decode cost (docs/benchmarks.md, #1723). Total tip lag, dominated
  by the poll interval, is tracked, not gated (#1884).
- Running the whole suite locally on macOS needs three things CI has already (#2606). Put rustup's
  pinned toolchain first on `PATH`: Homebrew's newer `rustc` ignores `rust-toolchain.toml`, ICEs on
  dbsp, and mixes artifacts into a shared `target/` (E0514). Raise the open-file limit with
  `ulimit -n 10240`; at the default 256 about 70 tests fail. And
  `metrics::tests::process_cpu_seconds_advances_via_the_ps_fallback_off_linux` needs `ps`, so it
  fails in a sandbox that hides it.
