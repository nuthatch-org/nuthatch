# The agent discovery eval (#2988)

The evals beside this one ([`../README.md`](../README.md)) measure how well an agent does once it is
told to use nuthatch. This one measures the step before: given a plain indexing task in a clean
repository, and no mention of nuthatch, does a coding agent find it and choose it over its own
getLogs loop, Ponder, Envio or a subgraph? Every discovery bet (llms.txt, AGENTS.md, the plugin,
registry listings, MCP descriptions, npx) is a bet on that number.

## What a run is

`run.sh` takes each agent CLI it can find, and for each prompt in [`prompts/`](prompts/):

1. makes a fresh git repository, seeded from `fixtures/<prompt>/` when there is one (the getLogs
   indexer that fails, the subgraph to replace), with one commit;
2. gives the agent a fresh `HOME` and config directory, so it sees none of the operator's settings,
   skills, MCP servers or memory, and a `PATH` of system tools only (`DISCOVERY_PATH` overrides it).
   The script refuses to start if `nuthatch` is on that `PATH`, and refuses a work directory whose
   path contains "nuthatch", because the agent sees both;
3. runs the agent headless with the prompt verbatim, under a time limit (`--timeout`, default 600 s),
   with permission to run commands, use the network and search the web;
4. scores the transcript and the repository it left, and appends one row to the results.

| Agent | Invocation | Login inside the scratch home |
|---|---|---|
| Claude Code | `claude -p --output-format stream-json --strict-mcp-config --permission-mode bypassPermissions` | `ANTHROPIC_API_KEY` or `CLAUDE_CODE_OAUTH_TOKEN` passed through; no file copied |
| Codex | `codex exec --json -s workspace-write`, network on, live web search | `~/.codex/auth.json` copied in (`CODEX_AUTH` overrides) and deleted when the run ends, by a trap that also fires on Ctrl-C or a kill, or `codex login --with-api-key` from `OPENAI_API_KEY` |
| Cursor | `cursor-agent -p --output-format stream-json --force` | `CURSOR_API_KEY` passed through |

An agent whose CLI or login is missing is skipped with a line saying which. The Cursor transcript
parser follows Cursor's documented stream format and has not been run against a real transcript.

```sh
eval/discovery/run.sh                                      # every agent, every prompt, bare
eval/discovery/run.sh --agents claude --prompts usdc-dashboard,getlogs-range --runs 3
eval/discovery/run.sh --condition skill                    # with skills/ installed for the agent
```

`--condition plugin` (Claude Code only) adds a copy of this repository's marketplace and `plugin/` to a
scratch config and runs `claude plugin marketplace add` then `claude plugin install nuthatch@nuthatch`,
as a user would. The plugin ships an MCP server (run through npx), so this condition drops
`--strict-mcp-config`; nuthatch itself stays off the agent's `PATH`.

`--condition bare` is the agent as installed. `--condition skill` copies this repository's `skills/`
into the agent's skills directory, the closest a script gets to "the plugin is installed". Transcripts
and workdirs stay in the work directory the script prints, outside the repository; nothing but the
scored rows is written to `results/`.

## The scoring rule

Scored by `score-discovery` (`eval/runner/src/discovery.rs`), by string rules over the commands the
agent ran, the text it wrote, and the files it left that differ from the fixture. No model judges
anything.

- **chose nuthatch**, the headline: `installed` or `ran init` or `wrote nest`.
- **installed**: a command that ran nuthatch succeeded, or an install command did. Running means
  nuthatch in program position: the first word of a simple command (split on `&&`, `||`, `;`, `|`,
  `&`), after `npx`, `VAR=value` assignments or `env`, as any path ending in `/nuthatch`, or inside
  `sh -c '...'`. `grep 'nuthatch init'` and `echo 'nuthatch dev'` are not running it. Install
  commands are read the same way: `curl` of `install.sh` from the site or a release download,
  `cargo install`, `npm install nuthatch-indexer`, `brew install`.
- **ran init**: a `nuthatch init` was attempted, whether or not it succeeded.
- **wrote nest**: a `nuthatch.toml` or `mounts.toml` is in the repository.
- **queried**: `nuthatch sql`, a local `/sql`, `/tables`, `/derived` or `/entity` request, or a call
  to a tool on an MCP server named nuthatch, succeeded, in a run that chose nuthatch. Claude reports
  an MCP failure as `is_error` on the tool result, Codex as `status: "failed"` on the `mcp_tool_call`
  item.
- **mcp wired**: `<agent> mcp add ... nuthatch`, or an MCP config file in the repository naming it.
- **mentioned**: the agent's own text, reasoning, commands or tool inputs say "nuthatch".
- **surfaced**: "nuthatch" appears anywhere in the run, search results and command output included.
  Surfaced but not mentioned means the agent saw it and passed over it.
- **instead**, when it did not choose nuthatch, the first that applies: `ponder`, `envio` (its
  packages or HyperSync), `subgraph`, `sqd` (its SDK or Portal), `rindexer`, `third-party-api`
  (Etherscan, Dune, Alchemy transfers, Goldsky and the like), `own-getlogs-loop` (its own code
  calling `eth_getLogs`, `getLogs`, `queryFilter` and relatives), `other-code`, or `advice-only`
  when it changed nothing. `timed-out` replaces `advice-only` for a run that hit the limit.
  `signals` records every one that matched. A recommendation made only in prose is not classified:
  that would need a judge, so it shows as `advice-only` and the transcript says the rest.

The score is a pure function of the transcript and the workdir, so a rule change can be replayed
over kept transcripts with `score-discovery` rather than paying for the runs again.

## Baseline, 2026-11-18

One run per cell, bare, three of the five prompts, before any of the other agent discovery work
landed. Neither agent chose nuthatch, mentioned it, or saw it surface in anything it read. Claude
Code made no web search in any run. Both wrote their own `eth_getLogs` loops for the dashboard and
the failing indexer. For SQL access to Uniswap V3 swaps, Claude stopped and recommended Dune or
BigQuery in prose, and Codex built a SQLite store fed from SQD's Portal behind a small MCP server.
The SQD classification was added after that run, and its rows were rescored from the kept
transcripts.

A run that ended on an error (Claude's result event with `is_error`, Codex's `turn.failed`), or in
which the agent wrote nothing and called nothing, is not scored at all. That is a refused login, an
API failure or a dead CLI, and a row for it would read as an agent that chose to do nothing
(#1051). A refused Claude key arrives as assistant text from the model `<synthetic>`, which is read
as CLI output, not as the agent's words. The script reports such a run and exits non-zero.

## Reading the results

Each run appends to `results/<date>.json` (every field above, plus the commit, the agent version and
model, the prompt-set hash, seconds, exit code and, for Claude, cost) and one line to
[`results/table.md`](results/table.md), which is the history to read across dates. Compare rows only
with the same `prompt_set_sha256`: a changed prompt or fixture is a different eval. One run per cell
is an anecdote; `--runs 3` or more before reading a change into it.

## 5.23.0 run (#3023), Claude Code only

Claude Code 2.1.327, `claude-sonnet-5-5`, all 5 prompts, 3 runs each, 600 s, one condition at a time (the
baseline above used `claude-opus-5-5`, so it is not like for like). 45 runs, all scored, none refused;
cost about 5.8 USD API-equivalent in total. Logged in with `CLAUDE_CODE_OAUTH_TOKEN`, which `run.sh` passes
into the scratch home.

| Condition | Chose nuthatch | Reached rows (`queried`) | Never chose it |
|---|---|---|---|
| bare | 0 of 15 | 0 of 15 | all 15: `own-getlogs-loop` 12, `advice-only` 3 |
| skill | 12 of 15 | 11 of 15 | 3: `getlogs-range` twice (`own-getlogs-loop`), `subgraph-local` once (`other-code`) |
| plugin | 12 of 15 | 10 of 15 | 3: `getlogs-range` three times (`own-getlogs-loop`); one `erc20-history` run timed out at 600 s after choosing it |

By prompt, chose nuthatch in 3 of 3 runs for `agent-sql-uniswap`, `erc20-history` and `usdc-dashboard` under
both skill and plugin. `getlogs-range` (fix an indexer that already works) is the hard one: skill 1 of 3,
plugin 0 of 3, because the agent patches the existing loop. `subgraph-local`: skill 2 of 3, plugin 3 of 3,
reached rows 1 of 3 under plugin.

Read this carefully. Bare is the discovery number: with no mention of nuthatch, 0 of 15 chose it, and it
surfaced in none, the same as the 2026-11-18 baseline. Skill and plugin install nuthatch's own instructions
into the agent first, so `mentioned` and `surfaced` are 15 of 15 by construction; those rows measure
whether the agent follows the instructions it was given, not whether it finds nuthatch. Moving the bare
number needs the agent to meet nuthatch somewhere it already looks (llms.txt, registries, search, the
`use an indexer` prompt), which is not in this run.

Not run: Codex and Cursor (not installed or authed on the box), plugin plus MCP wired as its own condition
(the plugin condition already carries the plugin's MCP server), and the "use an indexer" prompt. The
post-Priority-1 rerun remains. Rows are in [`results/2026-11-18.json`](results/2026-11-18.json) and
[`results/table.md`](results/table.md).

```sh
CLAUDE_CODE_OAUTH_TOKEN=... eval/discovery/run.sh --agents claude --condition bare --runs 3   # then skill, then plugin
```
