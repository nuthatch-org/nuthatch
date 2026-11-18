# Errors and exit codes

Every `nuthatch` command exits 0 when it did its job. When it did not, the exit status says which
kind of failure it was, so a script can tell "no nest here" from "the nest is not running" from "bad
SQL" without reading the message.

| Exit | Code | What happened |
|---|---|---|
| 0 | | Success. |
| 1 | `error` | Anything not classified below. |
| 2 | `usage` | The command line was refused: an unknown flag, a missing argument. |
| 3 | `no_nest` | There is no nest in the directory: no `nuthatch.toml`, and for `sql` no store either. |
| 4 | `nest_not_running` | The command needs a running nest, and nothing answered at its `--url`. |
| 5 | `sql_invalid` | The query was refused as written: an unknown table or column, a syntax error. |
| 6 | `abi_not_found` | No ABI could be resolved for a contract. |
| 7 | `chain_not_detected` | The contract has no code on any chain probed or on the chain named, or that chain is unknown. |
| 8 | `rpc_refused` | An RPC endpoint could not be reached, or refused the request. |
| 9 | `rpc_range_too_wide` | An RPC endpoint refused an `eth_getLogs` range that could not be narrowed further. |
| 10 | `port_in_use` | Something already listens on the `--listen` address, so `dev --detach` would have reported another process's nest as its own. |
| 11 | `rows_timeout` | `dev --detach --wait-rows` found the nest serving but no row indexed within `--wait-timeout`. The nest is left running. |

The codes and statuses are stable: new ones may be added, and none is renumbered or renamed.

## With `--json`

A command run with `--json` writes its error to stderr as one JSON object, in place of the
`Error:` text, and exits with the same status:

```json
{"error": {"code": "nest_not_running", "exit": 4, "message": "querying http://127.0.0.1:8288 - is `nuthatch dev` running?: ...", "next": "run `nuthatch dev --detach` in the nest's directory, or pass --url to a running one"}}
```

`message` is the whole error on one line. `next` is the step to take: for `sql_invalid` it is the
query's own fix hint when there is one, such as the column the query probably meant.

Without `--json`, the text an error prints is what it always was; only the exit status says more.

```sh
nuthatch sql "SELECT count(*) FROM weth__transfer" --json
case $? in
  0) ;;
  4) echo "start the nest first" ;;
  5) echo "fix the query" ;;
esac
```

## Output a script can read

These commands print one JSON object on stdout with `--json`, and move the lines a person reads to
stderr:

- `nuthatch init 0xADDR --json` and `nuthatch add 0xADDR --json`: `nest_dir`, `name`, `chain`,
  `chain_id`, `contracts` (each with `address`, `alias`, `deployment_block`, `proxy_checked` and
  `proxy_warning`), `tables`, `files_written` and `next`, the commands to run next.
  `proxy_checked` is true only for a contract this run resolved and compared against its proxy
  history: a contract given `--abi`, one added earlier, or one whose proxy slots or blocks could
  not be read is false, and its `proxy_warning` null says nothing. The nest keeps no record of the
  check. `tables` is computed from the config and ABIs, as schema.json is, so a nest from
  `init --from` that carries no schema.json still lists them and is not given one.
- `nuthatch schema --json`: the same object, after the artifacts are regenerated.
- `nuthatch watch --once --json`: the running nest's own `/ready`, `/` and `/nests` answers, as
  `ready`, `summary` and `nests`, with each mount's `/ready` under `mounts`. Nothing answering is
  `nest_not_running`.
- `nuthatch sql "..." --json`: one row per line, as before.

```sh
nuthatch init 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 --json | jq .tables
```

## Running a nest in the background

`nuthatch dev --detach` starts `dev` with the same flags in the background and returns once its API
answers `/ready` with a first poll of the chain behind it; for a `mounts.toml` runtime, once every
mount listed at `/nests` has polled. If `dev` exits first, or is not serving
within two minutes, `--detach` fails with the end of its log, and with `dev`'s own exit status where
it had one. With `--json` it prints `pid`, `url`, `dir`, `log`, `stop`, `has_rows` and
`first_block_indexed`, read from `/ready` once when `--wait-rows` did not already wait on it (so a
restart over an existing store reports the rows it holds).

Serving is not the same as holding data: a nest that has polled once can still have no row, and
`nuthatch sql "SELECT count(*) ..."` straight after `--detach` then returns 0. Add `--wait-rows` to
also block until a row is indexed (any mount's, for a runtime), for at most `--wait-timeout` seconds
(default 300). On timeout it exits 11 (`rows_timeout`) and leaves the nest running. `/ready` and the
MCP `index_status` report `has_rows` and `first_block_indexed`. `has_rows` is true when the hot
store holds at least one entity row, or the segment catalogue names a sealed segment with more than
zero rows; it is not `sealed_through > 0`, since a seal can pass blocks that held no event. It is
read from the store and catalogue (one key read, plus a catalogue read only while none has been
found), so it stays cheap on a poll. `first_block_indexed` is the block of the smallest hot-store
key (`{block:012}-{log_index:06}`), the oldest row still hot; null before any row and once every row
is sealed away.

The log and the record of the process are kept in `$XDG_STATE_HOME/nuthatch/dev/` (else
`~/.local/state/nuthatch/dev/`), one directory per nest, and never in the nest itself: every file in
a nest counts toward its NID. The record names the executable that was started and when the
process began, and a pid counts as that `dev` only while it is the same process running that
executable, whatever the file is called. A pid that has exited and not been reaped, belongs to
another user, or began at another time (after a reboot, say) is treated as gone. `nuthatch stop`
sends the process SIGTERM and waits for it to finish; with no detached `dev` running for the
directory it fails with `nest_not_running`. Only a process that is the recorded one by its start
time but whose executable cannot be read is refused rather than signalled, by `stop` and by a
second `--detach`. When `dev` is not serving in time, `--detach` stops it the same way, with
SIGKILL only if it has not exited ten seconds after SIGTERM.

```sh
nuthatch dev --backfill 300 --detach && nuthatch sql "SELECT count(*) FROM weth__transfer" --json
nuthatch stop
```
