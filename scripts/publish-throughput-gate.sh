#!/usr/bin/env bash
# RFC-0052 S2's throughput gate: `bench backfill` with and without a mirror publishing to an S3 server (versitygw, #1492) behind
# a Toxiproxy bandwidth limit, on the deterministic chain `fixture-rpc footprint` serves. Four
# batches, alternated so drift on the box lands on both arms. Results:
# docs/bench/publish-throttled.md.
#
# Needs versitygw, toxiproxy-server, toxiproxy-cli, jq, and a curl with --aws-sigv4.
# Env: BIN (target/release/nuthatch), VERSITYGW (versitygw), RUNS (15), RATE_KB (1024), OUT (a temp dir)
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${BIN:-$ROOT/target/release/nuthatch}"
VERSITYGW="${VERSITYGW:-versitygw}"
RUNS="${RUNS:-15}"
RATE_KB="${RATE_KB:-1024}"
WORK="$(mktemp -d)"
OUT="${OUT:-$WORK/out}"
RPC_PORT=18545 MINIO_PORT=19000 PROXY_PORT=19001 TOXI_PORT=18474
KEY=gateadmin SECRET=gateadmin-secret BUCKET=mirror
mkdir -p "$OUT" "$WORK/s3-data" "$WORK/nest/abis"

pids=()
cleanup() { for p in "${pids[@]}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

RPC_BIN="$(bash "$ROOT/scripts/fixture-rpc.sh")"
"$RPC_BIN" footprint "$RPC_PORT" > "$OUT/rpc.log" 2>&1 & pids+=($!)
ROOT_ACCESS_KEY=$KEY ROOT_SECRET_KEY=$SECRET "$VERSITYGW" --port "127.0.0.1:$MINIO_PORT" \
  --health /health posix "$WORK/s3-data" > "$OUT/s3.log" 2>&1 & pids+=($!)
toxiproxy-server -host 127.0.0.1 -port "$TOXI_PORT" > "$OUT/toxiproxy.log" 2>&1 & pids+=($!)

wait_for() {
  for _ in $(seq 1 80); do eval "$1" >/dev/null 2>&1 && return 0; sleep 0.25; done
  echo "FAIL: never ready: $1" >&2
  exit 1
}
wait_for "curl -fsS -X POST -H 'content-type: application/json' -d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_chainId\",\"params\":[]}' 127.0.0.1:$RPC_PORT"
wait_for "curl -fsS 127.0.0.1:$MINIO_PORT/health"
wait_for "curl -fsS 127.0.0.1:$TOXI_PORT/version"

sign=(--aws-sigv4 "aws:amz:us-east-1:s3" --user "$KEY:$SECRET")
curl -fsS "${sign[@]}" -X PUT "http://127.0.0.1:$MINIO_PORT/$BUCKET" >/dev/null
export TOXIPROXY_URL="http://127.0.0.1:$TOXI_PORT"
toxiproxy-cli create --listen "127.0.0.1:$PROXY_PORT" --upstream "127.0.0.1:$MINIO_PORT" minio >/dev/null
toxiproxy-cli toxic add -t bandwidth -a rate="$RATE_KB" -u -n up minio >/dev/null
toxiproxy-cli toxic add -t bandwidth -a rate="$RATE_KB" -d -n down minio >/dev/null

# A number measured behind a throttle that does not bite measures nothing.
head -c 3000000 /dev/urandom > "$WORK/probe.bin"
t0=$(date +%s)
curl -fsS "${sign[@]}" -T "$WORK/probe.bin" "http://127.0.0.1:$PROXY_PORT/$BUCKET/probe.bin" >/dev/null
took=$(( $(date +%s) - t0 ))
echo "throttle probe: 3 MB PUT through the proxy took ${took} s" | tee "$OUT/throttle-probe.txt"
if [ "$took" -lt 2 ]; then
  echo "FAIL: the throttle is not limiting uploads" >&2
  exit 1
fi

cat > "$WORK/nest/nuthatch.toml" <<TOML
[nest]
name = "publish-gate"
chain = "mainnet"
chain_id = 1
rpc_urls = ["http://127.0.0.1:$RPC_PORT"]
schema_version = 1

[[contracts]]
alias = "usdc"
address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
abi = "abis/usdc.json"
TOML
cat > "$WORK/nest/abis/usdc.json" <<'JSON'
[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
  {"name":"from","type":"address","indexed":true},
  {"name":"to","type":"address","indexed":true},
  {"name":"value","type":"uint256","indexed":false}]}]
JSON

export AWS_ACCESS_KEY_ID=$KEY AWS_SECRET_ACCESS_KEY=$SECRET AWS_REGION=us-east-1 \
  AWS_ENDPOINT="http://127.0.0.1:$PROXY_PORT" AWS_ALLOW_HTTP=true
common=(bench backfill --dir "$WORK/nest" --from 1 --to 20000 --runs "$RUNS" --seal-direct --concurrency 4)
mirror=(--publish-target "s3://$BUCKET/gate")

"$BIN" "${common[@]}" --label "publish gate: no mirror (a)" --out "$OUT/baseline-a.json" > "$OUT/baseline-a.log" 2>&1
"$BIN" "${common[@]}" "${mirror[@]}" --label "publish gate: S3 at ${RATE_KB} KB/s (a)" --out "$OUT/publish-a.json" > "$OUT/publish-a.log" 2>&1
"$BIN" "${common[@]}" "${mirror[@]}" --label "publish gate: S3 at ${RATE_KB} KB/s (b)" --out "$OUT/publish-b.json" > "$OUT/publish-b.log" 2>&1
"$BIN" "${common[@]}" --label "publish gate: no mirror (b)" --out "$OUT/baseline-b.json" > "$OUT/baseline-b.log" 2>&1

for f in baseline-a publish-a publish-b baseline-b; do
  jq -r --arg f "$f" '"\($f): events=\(.events) ev/s=\(.events_per_sec) rss=\(.peak_rss_mb)MB published_bytes=\(.published_bytes // "-")"' "$OUT/$f.json"
done | tee "$OUT/summary.txt"
if grep -h "bytes published" "$OUT"/publish-*.log | grep -q " 0 bytes published"; then
  echo "FAIL: a publishing run uploaded nothing, so it measured nothing about publishing" >&2
  exit 1
fi
echo "reports in $OUT"
