---
name: nuthatch-builder
description: Build, configure, run, or debug a nuthatch indexer (a "nest", one or many in a runtime). Use when the user wants to index a smart contract's events into a local SQL database with nuthatch - scaffolding from an address, editing nuthatch.toml/semantic.toml, adding contracts, factories, compliance labels and flags, packaging/mounting nests, running several nests in one runtime, or troubleshooting a running indexer.
---

# Building with nuthatch

nuthatch turns a contract address into a local SQL database: one Rust binary, one command, no
Postgres/Docker, no third-party data API, no bill. This skill teaches you to *drive* the CLI. For
*querying a running nest's data* (what a table means, what's in it as of block N), use the nest's MCP
`schema`/`sql` tools instead - that's runtime knowledge; this is authoring knowledge.

**Never fight the non-negotiables** (the binary enforces them; working against them is always wrong):
one writer, one cursor per chain (a second chain = a second cursor, never a shared one), sealed Parquet segments are
immutable (reorgs only ever touch the hot store), and the `/sql` guards (timeout, row cap, concurrency)
are node self-protection, not obstacles to remove.

## The 90-second happy path

```sh
nuthatch init 0xA0b86991c6218b36c1D19D4a2e9Eb0cE3606eB48 --alias usdc   # USDC - chain auto-detected
nuthatch dev --backfill 300   # the last 300 blocks, follow the tip, serve an API on :8288
nuthatch sql "SELECT count(*), sum(value_dec) FROM usdc__transfer WHERE NOT value_overflow"
```

- `init` resolves the ABI (Sourcify → Etherscan), writes `nuthatch.toml`, `abis/`, `schema.json`,
  `semantic.toml`, `views/`, `llms.txt`, and a `.claude/skills/` scaffold. **Omit `--chain`** - it
  probes the known chains for the contract's bytecode. Pass several addresses to index them together.
  Without `--alias` the table prefix is the contract's name from the ABI (`fiat_token_v2_2__transfer`
  for USDC), so name it if you want `usdc__transfer`.
- `dev` shows a live backfill progress line, then "caught up to tip". It *is* the serve command.
  `--backfill 300` keeps this short: it indexes recent history only, so the sum is
  not an absolute balance. Without it `dev` backfills from deployment (block 6,082,465 for USDC, tens
  of millions of blocks on public endpoints), and `init` warns when the present ABI may not decode
  every older implementation era; read that warning before trusting full history.
- `nuthatch sql` with no query opens a REPL (`.tables`, `.schema <t>`, history).
- `sum(value_dec)` is the amounts that fit. A full-width word is NULL and is not a term, so
  `WHERE NOT value_overflow` is that same sum. Ids, nonces and hashes stay on the raw column.

## When to read what

- **[cli-reference.md](cli-reference.md)** - GENERATED from the binary. The authoritative list of every
  subcommand and flag. If a flag isn't here, it doesn't exist - never invent one.
- **[config-reference.md](config-reference.md)** - every `nuthatch.toml` / `semantic.toml` / `mounts.toml`
  key.
- **[config-as-code.md](config-as-code.md)** - the `nest.star` (Starlark) front-end, **REMOVED in 2.0**.
  Author nests in plain `nuthatch.toml`; a directory holding a `nest.star` is refused at load. Read
  this only to port a legacy `nest.star` you've inherited - don't write new ones.
- **[workflows.md](workflows.md)** - the recipes: init→dev→sql, add a contract, factories, publish a
  nest (bundle/load), run several nests in one runtime, wire an AI client.
- **[views.md](views.md)** - a nest's logic layer: authoring `views/*.sql` derivations, describing them
  in `semantic.toml`, and the reserved-word / big-int / hot∪cold footguns. Also `maintained.toml`,
  which answers an expensive view from a stored copy of its own evaluation (RFC-0062).
- **[entities.md](entities.md)** - `entities.toml`: a relation the indexer *maintains* as blocks
  arrive rather than recomputing per query (RFC-0041, 3.0.0). Read it with `views.md` - the two look
  almost identical in the file and are completely different at runtime.
- **[compliance.md](compliance.md)** - labels, flags, exposure and the signed
  audit pack (only relevant if the user asks for compliance features).
- **[troubleshooting.md](troubleshooting.md)** - symptom → `/metrics` series → remedy for tip lag, RPC
  failover, reorgs, guard rejections, and the RAM budget.

## Golden rules

1. **Read `cli-reference.md` before using a flag you're unsure of.** Hallucinated flags are the #1 way
   agents break nuthatch.
2. **One chain per cursor.** A second chain is a second cursor: a second `nuthatch dev`, or a second
   `[[chains]]` entry in a runtime's `mounts.toml` (one isolated cursor per chain). Never try to
   multiplex chains behind one cursor.
3. **Don't touch sealed data.** If a task seems to need mutating `segments/` or the sealed history, the
   approach is wrong - reorgs are handled by the hot store, not by rewriting Parquet.
4. **The guards are protection, not bugs.** A 503 from `/sql` means "too many concurrent queries"; a
   timeout means "add filters or a LIMIT" - not "raise the cap."
5. **Everything is local.** No telemetry unless the operator opts in (`nuthatch count`), no API token, no phone-home. AI features are BYO-key or
   local Ollama, and degrade gracefully offline.
