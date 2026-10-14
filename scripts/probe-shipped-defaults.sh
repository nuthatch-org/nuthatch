#!/usr/bin/env bash
# Probe every shipped default in src/chains.rs. Invoked by live-endpoints.yml.
# Keys on `nuthatch doctor --json` (`max_window` / `archive`), not on report prose (#716).
# Wire-up: the workflow step is `bash scripts/probe-shipped-defaults.sh` once a token
# with `workflow` scope can land that one-line change.

set -uo pipefail
fail=0
MAX_ATTEMPTS=3

# #633: eth.drpc.org and arbitrum.drpc.org both failed a scheduled run and answered fine two
# days later - one bad minute at a provider read exactly like a shipped default that had
# genuinely died, because the gate below judged a single attempt. Retry absorbs the blip;
# the gate still has to fire for the case the blip was hiding.
#
# Runs <cmd>, tee'd to <out-file> same as before retries existed, up to MAX_ATTEMPTS times,
# stopping at the first attempt whose output matches <success-pattern>. Leaves RETRY_OK (1
# iff a match was found) and RETRY_ATTEMPTS (how many attempts that took, capped at
# MAX_ATTEMPTS) for the caller - the caller's own gate decides what a full-attempts failure
# means, this just supplies the pattern.
# #716: key on `doctor --json` (`max_window` / `archive`), not on the wording of the
# human report. A reword of that report used to condemn every shipped endpoint.
retry() {  # retry <jq-expr> <label> <out-file> <cmd...>
  local jq_expr="$1" label="$2" outfile="$3"
  shift 3
  RETRY_OK=0
  for RETRY_ATTEMPTS in $(seq 1 "$MAX_ATTEMPTS"); do
    if "$@" > "$outfile" 2>"$outfile.err"; then
      cat "$outfile.err" >&2 || true
      if jq -e "$jq_expr" "$outfile" >/dev/null 2>&1; then
        RETRY_OK=1
        return 0
      fi
    else
      cat "$outfile.err" >&2 || true
    fi
    if [ "$RETRY_ATTEMPTS" -lt "$MAX_ATTEMPTS" ]; then
      backoff=$((RETRY_ATTEMPTS * 15))
      echo "  [$label] attempt $RETRY_ATTEMPTS/$MAX_ATTEMPTS did not succeed, retrying in ${backoff}s"
      sleep "$backoff"
    fi
  done
}
probe() {  # probe <chain> <busy-contract> <url>
  echo "== $1 =="
  retry '.[0].max_window != null' "$3" /tmp/out \
    ./target/release/nuthatch doctor --json --rpc "$3" --address "$2"
  # Allowlist, not denylist: a doctor run that never produced JSON has max_window missing,
  # which is failure, same as a probe that ran and got null.
  if [ "$RETRY_OK" -eq 1 ]; then
    if [ "$RETRY_ATTEMPTS" -gt 1 ]; then
      echo "::warning::$3 needed $RETRY_ATTEMPTS/$MAX_ATTEMPTS attempts to serve a getLogs window - a provider having a moment, not counted as dead"
    fi
  else
    echo "::error::$3 cannot serve getLogs in $RETRY_ATTEMPTS/$MAX_ATTEMPTS attempts (max_window null or no JSON) - it cannot back a zero-setup nest"
    fail=1
  fi
}
# Contracts chosen for being busy on each chain. URLs are the shipped defaults in
# `src/chains.rs` - probing a host we already dropped is not a test of the product (#716).
for u in https://mainnet.gateway.tenderly.co https://rpc.mevblocker.io; do
  probe mainnet 0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48 "$u"
done
for u in https://arb1.arbitrum.io/rpc https://arb-pokt.nodies.app; do
  probe arbitrum-one 0xaf88d065e77c8cC2239327C5EDb3A432268e5831 "$u"
done
for u in https://mainnet.base.org https://base-pokt.nodies.app; do
  probe base 0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913 "$u"
done
# Polygon: probe each shipped endpoint for getLogs failure (same gate as above).
# Additionally confirm the archive endpoint is still archive: a non-archive URL listed first
# silently breaks backfill even when its getLogs window is wide. That is what broke on
# 2026-08-20 - polygon-bor-rpc.publicnode.com (non-archive) was first.
for u in https://polygon.drpc.org https://polygon.gateway.tenderly.co; do
  probe polygon 0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270 "$u"
done
# Archive-depth gate for the first (archive) endpoint. Allowlist, not denylist, for the same
# reason as the getLogs gate above: a doctor run that never printed an "archive depth" line -
# because it never ran, errored, or the endpoint refused and doctor reported UNKNOWN - must
# not read as healthy just because it also isn't the exact string "no". Require the line
# present, then require it read "yes" exactly. Same retry budget as the getLogs probes above,
# for the same reason: this is the same live-provider dependency, just a different question.
echo "== polygon archive check =="
retry '.[0].archive == true' "https://polygon.drpc.org (archive)" /tmp/polygon-archive-out \
  ./target/release/nuthatch doctor --json --rpc https://polygon.drpc.org \
  --address 0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270
if [ "$RETRY_OK" -eq 1 ]; then
  if [ "$RETRY_ATTEMPTS" -gt 1 ]; then
    echo "::warning::polygon.drpc.org needed $RETRY_ATTEMPTS/$MAX_ATTEMPTS attempts to confirm archive depth - a provider having a moment, not counted as dead"
  fi
else
  echo "::error::polygon.drpc.org is not archive (or produced no JSON) in $RETRY_ATTEMPTS/$MAX_ATTEMPTS attempts - it cannot be the primary endpoint for polygon"
  fail=1
fi
# Deliberately fails the job rather than warning. A dead default is a broken promise for
# every new user of that chain, and a warning nobody reads is how this one survived a month.
exit $fail
