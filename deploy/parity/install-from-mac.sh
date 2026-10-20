#!/usr/bin/env bash
# Installs the daily parity timer (#1713, #1718) on Helsinki, run from a workstation that can
# `ssh root@<host>`. Builds the comparison (tools/lodestar-parity) with the host's cargo, prompts once
# for the Graph gateway key (never echoed), proves a failure pages Discord with a deliberately broken
# comparison, then enables the timer.
#   deploy/parity/install-from-mac.sh [root@89.167.109.4]
set -euo pipefail
host=${1:-root@89.167.109.4}
repo=$(cd "$(dirname "$0")/../.." && pwd)
files=(scripts/lodestar-parity.sh deploy/parity/nuthatch-parity.sh deploy/parity/nuthatch-parity.service deploy/parity/nuthatch-parity.timer
  tools/lodestar-parity/Cargo.toml tools/lodestar-parity/Cargo.lock tools/lodestar-parity/src/main.rs)

for f in "${files[@]}"; do
  [ -f "$repo/$f" ] || { echo "missing $f; run from an up-to-date checkout" >&2; exit 1; }
  ssh "$host" "mkdir -p /tmp/nuthatch-parity-src/$(dirname "$f") && cat > /tmp/nuthatch-parity-src/$f" < "$repo/$f"
done

ssh "$host" 'set -e
command -v jq >/dev/null || { echo "jq missing on the host" >&2; exit 1; }
cargo=$(command -v cargo || echo /root/.cargo/bin/cargo)
[ -x "$cargo" ] || { echo "cargo missing on the host; it builds the comparison" >&2; exit 1; }
command -v curl >/dev/null || { echo "curl missing on the host" >&2; exit 1; }
test -s /etc/nuthatch/deadman_discord_webhook_url || { echo "no /etc/nuthatch/deadman_discord_webhook_url; install the dead-man check first" >&2; exit 1; }
s=/tmp/nuthatch-parity-src
"$cargo" build --release --locked --quiet --manifest-path $s/tools/lodestar-parity/Cargo.toml --target-dir $s/target
install -D -m 755 $s/scripts/lodestar-parity.sh /usr/local/lib/nuthatch-parity/lodestar-parity.sh
install -m 755 $s/target/release/lodestar-parity-compare /usr/local/lib/nuthatch-parity/lodestar-parity-compare
install -m 755 $s/deploy/parity/nuthatch-parity.sh /usr/local/bin/nuthatch-parity
install -m 644 $s/deploy/parity/nuthatch-parity.service $s/deploy/parity/nuthatch-parity.timer /etc/systemd/system/
rm -rf $s
install -m 600 /etc/nuthatch/deadman_discord_webhook_url /etc/nuthatch/parity_discord_webhook_url
systemctl daemon-reload
echo "installed"'

if ssh "$host" 'test -s /etc/nuthatch/parity.env'; then
  echo "keeping the existing /etc/nuthatch/parity.env"
else
  read -rsp "GRAPH_API_KEY: " key; echo
  [ -n "$key" ] || { echo "empty key" >&2; exit 1; }
  printf 'GRAPH_API_KEY=%s\n' "$key" | ssh "$host" 'umask 077; cat > /etc/nuthatch/parity.env'
  unset key
fi

echo "deliberate failure: expect exit 1 and one PARITY line in Discord"
ssh "$host" 'sed "s/^ALLOC_N=\$(nest_count lodestar_allocations created_at_block)\$/ALLOC_N=\$(( \$(nest_count lodestar_allocations created_at_block) + 1 ))/" \
  /usr/local/lib/nuthatch-parity/lodestar-parity.sh > /tmp/parity-broken.sh
grep -q "ALLOC_N=\$(( " /tmp/parity-broken.sh || { echo "the break did not apply; the script changed" >&2; exit 1; }
chmod 755 /tmp/parity-broken.sh
rc=0; PARITY_SCRIPT=/tmp/parity-broken.sh PARITY_COMPARE=/usr/local/lib/nuthatch-parity/lodestar-parity-compare \
  PARITY_MODES=sealed nuthatch-parity || rc=$?; echo "exit $rc"
rm -f /tmp/parity-broken.sh
[ "$rc" -ne 4 ] || { echo "the deliberate failure exited 4: the subgraph side did not answer, so check GRAPH_API_KEY in /etc/nuthatch/parity.env" >&2; exit 1; }
[ "$rc" -eq 1 ] || { echo "the deliberate failure exited $rc, not 1: the timer would not page on a real one" >&2; exit 1; }'

ssh "$host" 'systemctl start --no-block nuthatch-parity.service && systemctl enable --now nuthatch-parity.timer && systemctl list-timers nuthatch-parity.timer --no-pager | head -2'
echo "done; results later with: ssh $host 'column -t -s \"\$(printf \"\\t\")\" /var/log/nuthatch/parity/runs.tsv | tail -n 8'"
