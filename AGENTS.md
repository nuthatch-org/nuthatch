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
mkdir weth-nest && cd weth-nest
nuthatch init 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 --alias weth
```

- `init` resolves the ABI, detects the chain and writes the nest into the current directory
  (`--dir` to choose). Omit `--chain` unless detection picks wrong. It runs without a prompt when
  stdin is not a terminal.
- The bundled public RPCs are for trying it out. Use your own with `--rpc`, on `init` or `dev`.
- More contracts: `nuthatch init 0xA 0xB --alias a,b`, or `nuthatch add 0xC` in an existing nest.
- After hand-editing `nuthatch.toml`, run `nuthatch schema` to regenerate `schema.json`, `llms.txt`
  and the nest's `AGENTS.md`.

## Run it headless

`nuthatch dev` indexes and serves until it is stopped, so running it in the foreground blocks the
shell, and querying straight after backgrounding it finds nothing answering yet. Start it in the
background with its output in a log, wait until `/ready` puts it fewer than ten blocks behind the
tip, then query. The log goes outside the nest: its identity counts every file in the directory.

```sh
nuthatch dev --backfill 300 > /tmp/nuthatch-dev.log 2>&1 &
for i in $(seq 150); do curl -sf 127.0.0.1:8288/ready | grep -Eq '"lag_blocks":[0-9],' && break; sleep 2; done
nuthatch sql "SELECT count(*) FROM weth__transfer" --json
```

- `dev` serves on `127.0.0.1:8288` (`--listen` to change). `--backfill 300` starts 300 blocks behind
  the tip, which has rows within seconds; without it `dev` reads from the deployment block, which
  for WETH is 21 million blocks of history.
- The loop gives up after five minutes. If it does, the log and `curl -s 127.0.0.1:8288/ready` say
  why; `/ready` answers 503 while the nest is stalled.
- Stop it with `pkill -f 'nuthatch dev'`.

## Query

```sh
nuthatch sql "SELECT count(*) FROM weth__transfer"
nuthatch sql "SELECT * FROM weth__transfer LIMIT 5" --json
```

Tables are `{alias}__{event}`. `nuthatch sql` with no query opens a REPL; `.tables` lists them.

## MCP

`nuthatch mcp` is a stdio server bridging to a running `nuthatch dev`, for the client to launch. It
works offline against the local instance. `nuthatch mcp --print-config --client <name>` prints one
client's config file on stdout and where it goes on stderr; `<name>` is `claude`, `cursor`, `codex`,
`vscode`, `windsurf` or `gemini`. Merge it into a file that already lists other servers rather than
overwriting it.

Claude Code and Codex take a command:

```sh
claude mcp add nuthatch -- nuthatch mcp --url http://127.0.0.1:8288
codex mcp add nuthatch -- nuthatch mcp --url http://127.0.0.1:8288
```

Cursor (`.cursor/mcp.json`), Windsurf (`~/.codeium/windsurf/mcp_config.json`), Gemini CLI
(`~/.gemini/settings.json`) and Claude Code's project `.mcp.json` share one shape:

```json
{
  "mcpServers": {
    "nuthatch": { "command": "nuthatch", "args": ["mcp", "--url", "http://127.0.0.1:8288"] }
  }
}
```

VS Code and Copilot (`.vscode/mcp.json`):

```json
{
  "servers": {
    "nuthatch": { "type": "stdio", "command": "nuthatch", "args": ["mcp", "--url", "http://127.0.0.1:8288"] }
  }
}
```

Codex's `~/.codex/config.toml`, if you would rather edit it than run `codex mcp add`:

```toml
[mcp_servers.nuthatch]
command = "nuthatch"
args = ["mcp", "--url", "http://127.0.0.1:8288"]
```

Without installing first, the npm launcher downloads the release binary, checks its SHA-256 and
runs it: `npx -y nuthatch-indexer mcp --url http://127.0.0.1:8288`, and the same for any other
command (`npx -y nuthatch-indexer init 0x...`).

`--print-config` writes the binary's absolute path in place of `nuthatch`, which works when the
client's `PATH` lacks `~/.local/bin`. A runtime hosting several nests serves nothing at its root:
point `--url` at one mount, `http://127.0.0.1:8288/<alias>` (or `/<tenant>/<alias>`). `GET /nests`
lists them.

A running nest also answers MCP over Streamable HTTP at `/mcp` (per mount, `/<alias>/mcp`), on by
default: `claude mcp add --transport http nuthatch http://127.0.0.1:8288/mcp`. It offers the query
tools only, each call goes through the same routes and limits as `/sql`, and off localhost the
gateway in front does the authentication. See [docs/operators.md](docs/operators.md).

## Skill, llms.txt and the nest's AGENTS.md

- Every nest `init` creates holds an `AGENTS.md`, an `llms.txt` and a `.claude/skills/` scaffold,
  naming its own tables. Read them first when working inside a nest. `init` and `schema` leave an
  `AGENTS.md` they did not write alone.
- The builder skill, for driving the CLI and config: `skills/nuthatch-builder/`. The binary does not
  carry it. Install it, with `nuthatch-subgraph-port` and `evm-indexing`, in one command:
  `npx skills add nuthatch-org/nuthatch`. In Claude Code, `/plugin marketplace add nuthatch-org/nuthatch`
  then `/plugin install nuthatch@nuthatch` adds the skills and the MCP server. From a clone:
  `cp -r skills/nuthatch-builder ~/.claude/skills/`.
- Cursor: the repository is a Cursor plugin (`.cursor-plugin/plugin.json`), and
  `rules/nuthatch.mdc` is a rule to copy into a project's `.cursor/rules/`.
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
