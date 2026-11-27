#!/usr/bin/env bash
# Take down a nest made by new-nest.sh. Dry run by default; --apply does it; --purge-data also deletes the
# nest's data directory (otherwise it is kept, so a removal can be undone by re-running new-nest.sh).
#   remove-nest.sh [--apply] [--purge-data] <name>
# NEST_ROOT prefixes every host path (tests); with it set, systemctl is not called.
set -euo pipefail
apply=0 purge=0
while [ $# -gt 1 ]; do case $1 in --apply) apply=1;; --purge-data) purge=1;; *) echo "unknown flag $1" >&2; exit 2;; esac; shift; done
name=${1:?usage: remove-nest.sh [--apply] [--purge-data] <name>}
[[ $name =~ ^[a-z][a-z0-9-]{1,30}[a-z0-9]$ ]] || { echo "bad name" >&2; exit 2; }
case $name in
  arc-usdc|base-usdc|arc-testnet) echo "refusing: $name is one of the permanent demo nests (deploy/public-nests)" >&2; exit 2;;
  proxy|*-proxy|*-nest|xdc-rpc-ua) echo "refusing: $name would name a shared unit (nuthatch-public-$name*), not a nest made by new-nest.sh" >&2; exit 2;;
esac
ROOT=${NEST_ROOT:-}
unit=nuthatch-public-$name
# Act only on what new-nest.sh made: its unit header and its marked Caddy block. Checked before anything is touched.
mine=/etc/systemd/system/$unit.service cf=$ROOT/etc/caddy/Caddyfile
head -n 3 "$ROOT$mine" 2>/dev/null | grep -qF "Made by deploy/hosted-nest/new-nest.sh" \
  || { echo "refusing: $mine is missing or lacks the 'Made by deploy/hosted-nest/new-nest.sh' header" >&2; exit 2; }
{ [ -f "$cf" ] && grep -qx "# BEGIN nuthatch-public $name" "$cf" && grep -qx "# END nuthatch-public $name" "$cf"; } \
  || { echo "refusing: no marked '# BEGIN/END nuthatch-public $name' block in /etc/caddy/Caddyfile" >&2; exit 2; }
run() { if [ $apply = 1 ]; then echo "+ $*"; "$@"; else echo "would: $*"; fi; }
rmf() { if [ -e "$ROOT$1" ]; then run rm -f -- "$ROOT$1"; else echo "absent:  $1"; fi; }

if [ -z "$ROOT" ]; then run systemctl disable --now "$unit-proxy" "$unit" || true; fi
rmf /etc/systemd/system/$unit-proxy.service
rmf /etc/systemd/system/$unit.service
rmf /etc/nuthatch-public/nginx-$name.conf
rmf /etc/nuthatch-public/$name.env
if [ $apply = 1 ]; then
  cp -p "$cf" "$cf.bak-remove-nest-$(date +%Y%m%d%H%M%S)"
  sed -i "/^# BEGIN nuthatch-public $name\$/,/^# END nuthatch-public $name\$/d" "$cf"
  echo "removed Caddy block for $name (backup beside the Caddyfile)"
else echo "would: remove the '# BEGIN/END nuthatch-public $name' block from /etc/caddy/Caddyfile"; fi
if [ -z "$ROOT" ]; then run systemctl daemon-reload; fi
data=/var/lib/nuthatch-public/$name
if [ $purge = 1 ]; then [ -d "$ROOT$data" ] && run rm -rf -- "$ROOT$data" || echo "absent:  $data"
else echo "kept:    $data (add --purge-data to delete it)"; fi
cat <<EOT

Still to do by hand:
  [ ] sudo caddy validate --config /etc/caddy/Caddyfile && sudo systemctl reload caddy
  [ ] nuthatch-frontend: delete the /nests/$name rewrite from vercel.json and the entry from
      src/data/public-nests.ts (and showcase config if any), merge, vercel --prod
  [ ] tell the prospect
EOT
