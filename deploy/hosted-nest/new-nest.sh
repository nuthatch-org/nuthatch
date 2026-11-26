#!/usr/bin/env bash
# Stand up one more public read-only nest at https://nuthatch-indexer.com/nests/<name>.
# Generates what deploy/public-nests/ does by hand for each nest, from that directory's own files (the
# Base USDC set is the template, so a fix there carries into every new nest). Docs: docs/hosted-nests.md.
#
#   new-nest.sh [--apply] <name> <chain> <spec> <start-block> [rpc-env-file]
#
#   name          lowercase letters, digits and dashes: becomes /nests/<name>, the unit names, the nest dir
#   chain         a nuthatch chain name (base, arbitrum-one, arc-testnet, ...)
#   spec          0xAddr[,0xAddr...]  contracts: the nest is made with `nuthatch init`
#                 a directory or git URL of a finished nest (its own nuthatch.toml decides the contracts)
#   start-block   first block to index; with addresses one number per address or one for all. Use - for a
#                 finished nest, which carries its own.
#   rpc-env-file  a file with a line NUTHATCH_RPC=<url> (the keyed URL). Its value is copied into the nest's
#                 env file and never printed. Omit it to use the chain's keyless endpoints.
#
# Dry run by default: prints every file and the two lines for nuthatch-frontend, writes nothing.
# --apply writes the host files (run on the host, with sudo) and does not start or reload anything.
# Idempotent: a second run changes nothing; a file that differs is reported and not overwritten.
#
# Environment: NEST_ROOT (prefix for every host path, for tests), HOST_IP (the sslip.io address, default
# the Helsinki host), NUTHATCH_BIN (default /usr/local/bin/nuthatch-5.23.0), SERVICE_USER (nests-public),
# NEST_DEV_ARGS (extra `nuthatch dev` flags, default "--poll-interval 3s --seal-direct --concurrency 4"; add
# "--window N" for a provider that caps eth_getLogs results, as the Base USDC unit does with --window 1500),
# NEST_LOCAL_HOST (the tailnet address Prometheus scrapes /metrics through; adds --local-host, default none).
set -euo pipefail

apply=0
if [ "${1:-}" = "--apply" ]; then apply=1; shift; fi
[ $# -ge 4 ] && [ $# -le 5 ] || { sed -n '2,24p' "$0" | sed 's/^# \{0,1\}//'; exit 2; }
name=$1 chain=$2 spec=$3 start=$4 rpc_file=${5:-}

die() { echo "error: $*" >&2; exit 1; }
[[ $name =~ ^[a-z][a-z0-9-]{1,30}[a-z0-9]$ ]] || die "name must be lowercase letters, digits and dashes (3-32): $name"
[[ $chain =~ ^[a-z0-9-]+$ ]] || die "bad chain name: $chain"
[[ $start =~ ^(-|[0-9]+(,[0-9]+)*)$ ]] || die "start-block must be numbers (comma separated) or -: $start"

here=$(cd "$(dirname "$0")" && pwd)
tpl=$here/../public-nests
for f in nuthatch-public-base-usdc.service nuthatch-public-base-usdc-proxy.service nginx-base-usdc.conf; do
  [ -f "$tpl/$f" ] || die "template missing: $tpl/$f (needs the public-nests kit, PR #3057)"
done

ROOT=${NEST_ROOT:-}
HOST_IP=${HOST_IP:-89.167.109.4}
NUTHATCH_BIN=${NUTHATCH_BIN:-/usr/local/bin/nuthatch-5.23.0}
SVC_USER=${SERVICE_USER:-nests-public}
DEV_ARGS=${NEST_DEV_ARGS:---poll-interval 3s --seal-direct --concurrency 4}
LOCAL_HOST=${NEST_LOCAL_HOST:-}
local_host_line=
[ -z "$LOCAL_HOST" ] || { [[ $LOCAL_HOST =~ ^[A-Za-z0-9.:-]+$ ]] || die "bad NEST_LOCAL_HOST: $LOCAL_HOST"; local_host_line="    --local-host $LOCAL_HOST \\"$'\n'; }
under=${name//-/_}
public_host=$name.nuthatch-indexer.com          # only ever sent as a Host header; never needs DNS
site=$name.$HOST_IP.sslip.io
unit=nuthatch-public-$name
nestdir=/var/lib/nuthatch-public/$name
envfile=/etc/nuthatch-public/$name.env
nginxconf=/etc/nuthatch-public/nginx-$name.conf
caddyfile=/etc/caddy/Caddyfile

# ---- the nest's source: init from addresses, or an existing nest
if [[ $spec =~ ^0x[0-9a-fA-F]{40}(,0x[0-9a-fA-F]{40})*$ ]]; then
  mode=init
  [ "$start" != "-" ] || die "start-block is needed with addresses"
  nest_cmd="$NUTHATCH_BIN init ${spec//,/ } --chain $chain --dir $nestdir --start-block $start"
elif [[ $spec =~ ^(https?://|git@) ]]; then
  mode=git; nest_cmd="git clone -- $spec $nestdir"
elif [ -d "$spec" ]; then
  mode=dir; nest_cmd="cp -a $(cd "$spec" && pwd)/. $nestdir/"
else
  die "spec is not 0xAddr[,..], a git URL or an existing directory: $spec"
fi

# After the clone or copy of a nest source, before anything starts: no symbolic link anywhere but .git.
# Printed into the checklist for the operator, who runs it; it prints the links and aborts if any.
link_check="links=\$(sudo find $nestdir -path $nestdir/.git -prune -o -type l -print); [ -z \"\$links\" ] || { echo \"refusing: symbolic link(s) in the nest:\" \"\$links\" >&2; sudo rm -rf $nestdir; false; }"

# ---- ports: reuse this nest's own if its unit exists, else the next free pair (nest 81xx, proxy 84xx)
used=$( { grep -rhoE '127\.0\.0\.1:(81|84)[0-9]{2}' "$tpl" "$ROOT/etc/systemd/system" "$ROOT/etc/nuthatch-public" "$ROOT/etc/caddy" 2>/dev/null || true
          { ss -ltnH 2>/dev/null || true; } | grep -oE ':(81|84)[0-9]{2}\b' | tr -d : | sed 's/^/127.0.0.1:/'
        } | cut -d: -f2 | sort -un)
existing=$ROOT/etc/systemd/system/$unit.service
if [ -f "$existing" ] && grep -q -- '--listen 127.0.0.1:' "$existing"; then
  port=$(grep -oE -- '--listen 127\.0\.0\.1:[0-9]+' "$existing" | grep -oE '[0-9]+$')
  pport=$(grep -oE '127\.0\.0\.1:84[0-9]{2}' "$ROOT$nginxconf" 2>/dev/null | head -1 | cut -d: -f2 || true)
  : "${pport:=$((port + 360))}"
else
  port=8120
  while echo "$used" | grep -qxE "$port|$((port + 360))"; do port=$((port + 1)); done
  [ "$port" -lt 8200 ] || die "no free port in 8120-8199"
  pport=$((port + 360))
fi

# ---- rpc
rpc_line=""
if [ -n "$rpc_file" ]; then
  [ -r "$rpc_file" ] || die "cannot read $rpc_file"
  rpc_line=$(grep -m1 '^NUTHATCH_RPC=' "$rpc_file" || true)
  [ -n "$rpc_line" ] || die "$rpc_file has no NUTHATCH_RPC=<url> line"
fi

# ---- renderers
render_env() {
  cat <<EOT
# $envfile   (root:$SVC_USER, mode 0640). Made by deploy/hosted-nest/new-nest.sh.
# PUBLIC_HOST is the Host the nest answers to (--public-host); Caddy's \`header_up Host\` sends the same name.
PUBLIC_HOST=$public_host
EOT
  if [ -n "$rpc_line" ]; then echo "# Keyed RPC (replaces the keyless rpc_urls in nuthatch.toml). The only secret in this file."; echo "$rpc_line"
  else echo "# No NUTHATCH_RPC: the nest uses the chain's keyless rpc_urls from nuthatch.toml."; fi
  cat <<'EOT'
NUTHATCH_SQL_TIMEOUT_MS=10000
NUTHATCH_SQL_MAX_ROWS=5000
NUTHATCH_SQL_MAX_RESULT_BYTES=8388608
NUTHATCH_NO_COUNT=1
EOT
}
render_nest_unit() {
  cat <<EOT
# /etc/systemd/system/$unit.service  Made by deploy/hosted-nest/new-nest.sh; docs/hosted-nests.md.
# $name on $chain, public and read-only: --no-admin removes the admin routes, --public-host names the Host the
# nest answers to (#3002). The backfill flags are NEST_DEV_ARGS, not the Base USDC unit's: that unit also sets --window 1500
# (a GraphOps log cap) and --local-host (the tailnet scrape); see deploy/public-nests/nuthatch-public-base-usdc.service.
# Without --local-host a loopback nest refuses the Host Prometheus sends, so set NEST_LOCAL_HOST to be scraped.
[Unit]
Description=nuthatch public read-only nest: $name ($chain)
Documentation=https://github.com/nuthatch-org/nuthatch/blob/main/docs/hosted-nests.md
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=$SVC_USER
Group=$SVC_USER
EnvironmentFile=$envfile
WorkingDirectory=$nestdir
ExecStart=$NUTHATCH_BIN dev \\
    --dir $nestdir \\
    --listen 127.0.0.1:$port \\
    --no-admin \\
    --public-host \${PUBLIC_HOST} \\
${local_host_line}    $DEV_ARGS
Restart=on-failure
RestartSec=5
# The 2 GB per-cursor budget (CLAUDE.md non-negotiable 2).
MemoryMax=2G

EOT
  sed -n '/^# Sandbox/,$p' "$tpl/nuthatch-public-base-usdc.service"
}
render_proxy_unit() {
  sed -e "s/nuthatch-public-base-usdc/$unit/g" -e "s/nginx-base-usdc\.conf/nginx-$name.conf/g" \
      -e "s/Base USDC/$name/g" -e "s/8481/$pport/g" "$tpl/nuthatch-public-base-usdc-proxy.service"
}
render_nginx() {
  sed -e "s/8121/$port/g" -e "s/8481/$pport/g" -e "s#nuthatch-public-base-usdc#$unit#g" \
      -e "s/nuthatch_public_base_usdc/nuthatch_public_$under/g" -e "s/Base USDC/$name/g" \
      -e "s/nginx-base-usdc\.conf/nginx-$name.conf/g" "$tpl/nginx-base-usdc.conf"
}
render_caddy() {
  cat <<EOT
# BEGIN nuthatch-public $name
$site {
	request_body {
		max_size 64KB
	}
	@viavercel header_regexp vercel X-Vercel-Forwarded-For ^[0-9a-fA-F:.]{3,45}\$
	handle @viavercel {
		reverse_proxy 127.0.0.1:$pport {
			header_up Host $public_host
			header_up X-Forwarded-For {http.request.header.X-Vercel-Forwarded-For}
			transport http {
				dial_timeout 2s
				response_header_timeout 20s
			}
		}
	}
	handle {
		reverse_proxy 127.0.0.1:$pport {
			header_up Host $public_host
			transport http {
				dial_timeout 2s
				response_header_timeout 20s
			}
		}
	}
}
# END nuthatch-public $name
EOT
}

# ---- output: dry run prints, --apply writes (only what is missing; a differing file is reported, not touched)
changed=0 differs=0
emit() { # path mode owner-group; content on stdin
  local path=$1 mode=$2 grp=$3 dest=$ROOT$1 content; content=$(cat)
  if [ $apply = 0 ]; then
    echo; echo "===== $path (mode $mode)"
    printf '%s\n' "$content" | sed -E 's#^(NUTHATCH_RPC=).*#\1<hidden>#'; return
  fi
  if [ -f "$dest" ]; then
    if [ "$(cat "$dest")" = "$content" ]; then echo "unchanged  $path"; else echo "DIFFERS    $path (left as is; remove it or edit by hand)"; differs=1; fi
    return
  fi
  install -d "$(dirname "$dest")"
  ( umask 077; printf '%s\n' "$content" > "$dest" ); chmod "$mode" "$dest"
  if [ "$(id -u)" = 0 ] && getent group "$grp" >/dev/null 2>&1; then chgrp "$grp" "$dest"; fi
  echo "wrote      $path"; changed=1
}

echo "nest      $name  ($chain)   port $port, proxy $pport   source: $mode"
echo "mode      $([ $apply = 1 ] && echo APPLY || echo 'dry run (add --apply to write)')"

render_env        | emit "$envfile" 0640 "$SVC_USER"
render_nest_unit  | emit "/etc/systemd/system/$unit.service" 0644 root
render_proxy_unit | emit "/etc/systemd/system/$unit-proxy.service" 0644 root
render_nginx      | emit "$nginxconf" 0644 root

# Caddy: a marked block appended to the host Caddyfile (the one Caddyfile.path is the repo's copy of)
if [ $apply = 0 ]; then
  echo; echo "===== $caddyfile (append; also add to deploy/public-nests/Caddyfile.path)"; render_caddy
else
  if [ -f "$ROOT$caddyfile" ] && grep -qx "# BEGIN nuthatch-public $name" "$ROOT$caddyfile"; then
    existing_block=$(sed -n "/# BEGIN nuthatch-public $name\$/,/# END nuthatch-public $name\$/p" "$ROOT$caddyfile")
    if [ "$existing_block" = "$(render_caddy)" ]; then echo "unchanged  $caddyfile (block present)"; else echo "DIFFERS    $caddyfile (block for $name differs; left as is)"; differs=1; fi
  else
    [ -f "$ROOT$caddyfile" ] && cp -p "$ROOT$caddyfile" "$ROOT$caddyfile.bak-new-nest-$(date +%Y%m%d%H%M%S)"
    install -d "$(dirname "$ROOT$caddyfile")"; { echo; render_caddy; } >> "$ROOT$caddyfile"
    echo "appended   $caddyfile (backup beside it)"; changed=1
  fi
fi

# ---- the two lines for nuthatch-frontend, and the checklist
cat <<EOT

===== nuthatch-frontend: vercel.json, in "rewrites" next to /nests/arc-usdc
    {
      "source": "/nests/$name/:path*",
      "destination": "https://$site/:path*"
    },

===== nuthatch-frontend: src/data/public-nests.ts, at the end of publicNests (edit id/name/summary/for)
  {
    id: '$name',
    name: '$name',
    chain: '$chain',
    for: { label: 'TODO: who it is for', href: 'TODO' },
    summary: 'TODO: one plain sentence on what is indexed, from which block, and that it follows the tip.',
    endpoint: 'https://nuthatch-indexer.com/nests/$name',
    example:
      "curl -sG 'https://nuthatch-indexer.com/nests/$name/sql' --data-urlencode 'q=SELECT count(*) FROM <table>'",
  },

===== checklist (docs/hosted-nests.md has the detail)
  [ ] 1. nest dir:   sudo install -d -o $SVC_USER -g $SVC_USER -m 0700 /var/lib/nuthatch-public   # once; skip if it exists
                     sudo -u $SVC_USER -H $nest_cmd
                     (then: sudo chmod 0700 $nestdir)
                     then refuse a symbolic link anywhere in the nest (a nest's files must be regular files):
                     $link_check
  [ ] 2. review the files above$([ $apply = 1 ] && echo ' (now on disk)'), then:  sudo systemd-analyze verify /etc/systemd/system/$unit*.service
  [ ] 3. sudo caddy validate --config $caddyfile && sudo systemctl reload caddy
  [ ] 4. sudo systemctl daemon-reload && sudo systemctl enable --now $unit; wait for lag: curl -s 127.0.0.1:$port/ready | grep -o '"lag_blocks":[0-9]*'
  [ ] 5. sudo systemctl enable --now $unit-proxy; curl -s -H 'Host: $public_host' 127.0.0.1:$pport/ready
  [ ] 6. frontend: add the two entries above, merge, \`vercel --prod\`; copy showcase/configs/_template.json to <name>.json
  [ ] 7. check:  deploy/public-nests/check-proxy.sh https://nuthatch-indexer.com/nests/$name
                 showcase/check-examples.sh https://nuthatch-indexer.com/nests/$name
  [ ] 8. send:   https://nuthatch-indexer.com/showcase/?nest=$name
  Remove later:  deploy/hosted-nest/remove-nest.sh $name   (dry run first)
EOT
[ $differs = 0 ] || { echo "note: some files differ from what this script would write; review them." >&2; exit 3; }
