#!/usr/bin/env bash
# Serve fixture-rpc on <port> while <command> runs, then stop it: yatr waits on every process a task
# leaves behind, so a server left in the background would hold the task open.
# Usage: with-fixture-rpc.sh <port> <fixture-rpc args...> -- <command...>
set -euo pipefail
rpc="$(dirname "$0")/../tools/fixture-rpc/target/release/fixture-rpc"
port=$1; shift
args=()
while [ "$1" != "--" ]; do args+=("$1"); shift; done
shift
"$rpc" "${args[@]}" --port "$port" &
pid=$!
trap 'kill "$pid" 2>/dev/null || true' EXIT
for _ in $(seq 50); do curl -sf -X POST -d '{"id":1,"method":"eth_chainId"}' "http://127.0.0.1:$port/" && break; sleep 0.2; done
"$@"
