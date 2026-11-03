# AGENTS.md

For coding agents **using** nuthatch to index a contract or an account. Contributing to nuthatch
itself: read `CLAUDE.md`. Every `nuthatch` command here is checked against the binary by
`tests/doc_command_check.rs`.

## Install

```sh
curl -fsSL https://nuthatch-indexer.com/install.sh | sh
export PATH="$HOME/.local/bin:$PATH"
```

macOS Apple Silicon and Linux x86_64/aarch64. Elsewhere, build from source with Rust 1.95.0
([docs/install.md](docs/install.md)).

## Index a contract

```sh
nuthatch init 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 --alias weth
nuthatch dev --backfill 300
```

- `init` resolves the ABI, detects the chain and writes the nest into the current directory
  (`--dir` to choose). Omit `--chain` unless detection picks wrong.
- `dev` serves on `127.0.0.1:8288` (`--listen` to change). Without `--backfill` it reads from the
  deployment block.
- The bundled public RPCs are for trying it out. Use your own with `--rpc`, on `init` or `dev`.
- More contracts: `nuthatch init 0xA 0xB --alias a,b`, or `nuthatch add 0xC` in an existing nest.
- After hand-editing `nuthatch.toml`, run `nuthatch schema` to regenerate `schema.json` and `llms.txt`.

## Query

```sh
nuthatch sql "SELECT count(*) FROM weth__transfer"
nuthatch sql "SELECT * FROM weth__transfer LIMIT 5" --json
```

Tables are `{alias}__{event}`. `nuthatch sql` with no query opens a REPL; `.tables` lists them.

## MCP

```sh
nuthatch mcp --print-config
claude mcp add nuthatch -- nuthatch mcp --url http://127.0.0.1:8288
```

Stdio server bridging to a running `nuthatch dev`. Works offline against the local instance.

## Skill and llms.txt

- Every nest `init` creates holds an `llms.txt` and a `.claude/skills/` scaffold. Read them first
  when working inside a nest.
- The builder skill, for driving the CLI and config: `skills/nuthatch-builder/`. Install it from a clone of
  this repository with `cp -r skills/nuthatch-builder ~/.claude/skills/`; the binary does not carry it.
- Every flag: `skills/nuthatch-builder/cli-reference.md`, generated from the binary. Every config
  key: `skills/nuthatch-builder/config-reference.md`.

## Account history

A nest with `[address_history]` and no contracts serves watched accounts' history at an
Etherscan-shaped `/api`. Template for Base, BNB Chain and Gnosis:
[examples/paywalled-chains](examples/paywalled-chains/).

## When it misbehaves

- `nuthatch doctor --rpc <url>` probes an endpoint's `eth_getLogs` width, batch size and archive depth.
- `nuthatch watch` shows a running nest's tip, seal, memory and RPCs.
- `curl -s 127.0.0.1:8288/ready` says what the nest is doing and whether it is stalled.
