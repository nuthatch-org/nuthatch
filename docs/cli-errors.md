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

The codes and statuses are stable: new ones may be added, and none is renumbered or renamed.

## With `--json`

A command run with `--json` writes its error to stderr as one JSON object, in place of the
`Error:` text, and exits with the same status:

```json
{"error": {"code": "nest_not_running", "exit": 4, "message": "querying http://127.0.0.1:8288 - is `nuthatch dev` running?: ...", "next": "run `nuthatch dev` in the nest's directory, or pass --url to a running one"}}
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
