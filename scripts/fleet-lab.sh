#!/usr/bin/env bash
# Stand up a throwaway nuthatch verification lab on Hetzner Cloud, run docs/verification.md against it,
# and destroy it. The same path an operator would take: it installs the **published release artifacts**,
# not a local build, so what gets verified is what people actually download.
#
#   export HCLOUD_TOKEN=...            # a Hetzner Cloud project API token (read+write)
#   export HCLOUD_SSH_KEY="my-key"     # the *name* of an SSH key already in that project
#
#   ./scripts/fleet-lab.sh up single    # one box: levels 0-4 + the compose fleet on one host
#   ./scripts/fleet-lab.sh up multi     # three boxes + a private network
#
#   ./scripts/fleet-lab.sh verify       # `multi` genuinely distributes: control plane + store + FE on
#                                       # one box, writers on their own, talking over the private net
#   ./scripts/fleet-lab.sh partition    # cut a writer off the control-plane API, assert it keeps indexing
#   ./scripts/fleet-lab.sh skew         # push a writer clock forward, assert the lease does not move
#   ./scripts/fleet-lab.sh pull         # delete a writer's nest, assert it pulls one from the registry
#   ./scripts/fleet-lab.sh down         # destroy everything this script created
#
# Until 2026-07-31 `multi` provisioned three boxes and then ran the whole compose fleet on the first,
# leaving the other two idle - so `partition` and `skew` were firewalling and clock-skewing machines
# that ran nothing, and passed. Fixed; the warning that used to live here is gone because the defect is.
#
# ## This spends money
#
# Hetzner bills **hourly**, which is what makes this cheap: a single box is ~€0.01/hr and three are
# ~€0.03/hr, so a day of verification is small change. It is only cheap if you destroy it, so `down`
# is a first-class verb, every resource is tagged, and `up` prints the running cost before creating
# anything.
#
# ## Why cloud-init rather than a pile of ssh commands
#
# The box configures itself from a declarative file you can read before it runs. That is the difference
# between a lab someone else can trust and a sequence of steps they have to take on faith - and this
# script exists partly so GraphOps can read it rather than be told what we did.

set -euo pipefail

API="https://api.hetzner.cloud/v1"
LABEL="nuthatch-lab"                  # every resource carries this; `down` deletes by it
TYPE_SINGLE="${TYPE_SINGLE:-cx33}"    # 4 vCPU / 8 GB - holds Postgres + control + 2 writers + 2 FEs
TYPE_WRITER="${TYPE_WRITER:-cx33}"    # a writer wants headroom above the 2 GB cursor budget
TYPE_SMALL="${TYPE_SMALL:-cx23}"      # 2 vCPU / 4 GB - control plane + Postgres + an FE
# Names checked against the live API: the current shared-vCPU x86 line is cx23/cx33, not cx22/cx32.
# `up` prices from the API rather than from a constant, so a renamed type surfaces as a clear failure
# instead of a wrong number in a comment.
LOCATION="${LOCATION:-hel1}"          # Helsinki, same as the existing production box
IMAGE="${IMAGE:-ubuntu-24.04}"
VERSION="${VERSION:-}"                # release to install; empty = latest

# x86 on purpose. Hetzner's ARM (CAX) boxes are cheaper per GB, and we publish **no ARM Linux binary** -
# a CAX box would mean building from source, where duckdb's unity translation units are 1-2 GB each and
# a 4 GB box OOMs unless you cap parallelism. Not a trap worth walking into for a few euros.

need() { command -v "$1" >/dev/null || { echo "need $1"; exit 1; }; }
need curl; need jq; need ssh

: "${HCLOUD_TOKEN:?set HCLOUD_TOKEN to a Hetzner Cloud API token}"

# `curl -f` makes a 4xx exit non-zero but **discards the response body**, which is where Hetzner puts
# the reason. A malformed request therefore surfaced as `curl: (56)` followed by a JSON decode
# traceback from whatever tried to parse the empty output - which is how a 422 on the `networks` field
# went undiagnosed. Keep the body, print it, and fail.
# Every ssh into a lab box goes through here.
#
# Lab boxes are ephemeral and **Hetzner recycles addresses**, so an IP you used an hour ago comes back
# attached to a different host key. With the default policy that is a hard `Host key verification
# failed`, which reads exactly like a provisioning failure and cost an afternoon on 2026-07-31. These
# are throwaway machines whose fingerprints are meaningless, so they get their own throwaway
# known-hosts file rather than polluting - and colliding with - the operator's real one.
SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o LogLevel=ERROR -o ConnectTimeout=15)
# `-n` is not optional: without it ssh reads from stdin, and an ssh inside a `while read` loop
# swallows the remaining input - so a loop over three hosts silently visits one. That is exactly how
# the second writer node "failed to start" for three runs while the first worked perfectly.
lab_ssh() { ssh -n "${SSH_OPTS[@]}" "$@"; }

hc() { # hc METHOD PATH [json]
  local m="$1" p="$2" body="${3:-}" out code
  if [ -n "$body" ]; then
    out=$(curl -sS -w '\n%{http_code}' -X "$m" "$API$p" -H "Authorization: Bearer $HCLOUD_TOKEN" \
      -H 'Content-Type: application/json' -d "$body")
  else
    out=$(curl -sS -w '\n%{http_code}' -X "$m" "$API$p" -H "Authorization: Bearer $HCLOUD_TOKEN")
  fi
  code="${out##*$'\n'}"; out="${out%$'\n'*}"
  case "$code" in
    2*) printf '%s' "$out" ;;
    *)  echo "hetzner API $m $p -> HTTP $code" >&2
        jq -r '.error | "  \(.code // "?"): \(.message // "?")",
          ((.details // {}).fields // [] | .[] | "  field \(.name): \(.messages)")' \
          <<<"$out" >&2 2>/dev/null || echo "  $out" >&2
        return 1 ;;
  esac
}
jq_() { jq -er --arg p "$1" 'getpath($p | split(".") | map(tonumber? // .)) // ""'; }

price_of() { # rough hourly, from the API so it is never a stale number in a comment
  hc GET "/server_types?name=$1" | jq -er '.server_types[0].prices[0].price_hourly.gross // empty' \
    | awk '{ printf "%.4f\n", $1 }'
}

cloud_init() { # cloud_init <role>
  local role="$1"
  cat <<YAML
#cloud-config
package_update: true
packages: [docker.io, docker-compose-v2, git, curl, jq]
runcmd:
  # The published artifact, not a build. If this fails, the release is broken and that is the finding.
  - |
    set -eux
    V="${VERSION}"
    if [ -z "\$V" ]; then
      V=\$(curl -fsSL https://api.github.com/repos/nuthatch-org/nuthatch/releases/latest | jq -r .tag_name)
    fi
    cd /opt
    # Two artifacts per release: the default (embedded) one and a -scaled one carrying worker/control.
    for a in nuthatch nuthatch-scaled; do
      curl -fsSLO "https://github.com/nuthatch-org/nuthatch/releases/download/\$V/\${a}-x86_64-unknown-linux-gnu.tar.gz"
      curl -fsSLO "https://github.com/nuthatch-org/nuthatch/releases/download/\$V/\${a}-x86_64-unknown-linux-gnu.tar.gz.sha256"
      sha256sum -c "\${a}-x86_64-unknown-linux-gnu.tar.gz.sha256"
      tar xzf "\${a}-x86_64-unknown-linux-gnu.tar.gz"
      mv nuthatch "/usr/local/bin/\${a}"
    done
    ln -sf /usr/local/bin/nuthatch /usr/local/bin/nuthatch-embedded
    git clone --depth 1 https://github.com/nuthatch-org/nuthatch /opt/nuthatch-src
    echo "$role" > /opt/nuthatch-role
    touch /opt/lab-ready
YAML
}

create_server() { # create_server <name> <type> <role> [network-id]
  local name="$1" type="$2" role="$3" net="${4:-}"
  local LOCATION="${LOCATION}"
  local ci; ci=$(cloud_init "$role" | jq -Rs .)
  local netpart=""; [ -n "$net" ] && netpart=",\"networks\":[$net]"
  hc POST /servers "{
    \"name\":\"$name\",\"server_type\":\"$type\",\"image\":\"$IMAGE\",\"location\":\"$LOCATION\",
    \"ssh_keys\":[\"${HCLOUD_SSH_KEY:?set HCLOUD_SSH_KEY to an SSH key name in the project}\"],
    \"labels\":{\"$LABEL\":\"true\",\"role\":\"$role\"},
    \"user_data\":$ci$netpart}" | jq_ server.public_net.ipv4.ip
}

# Private (10.44.x.x) address of a box, by role. The public IP is how *we* reach it; the private one
# is how the boxes reach each other, and it is what a writer must be given for the store.
private_ip() { # private_ip <role> [index]
  hc GET "/servers?label_selector=$LABEL" | jq -r --arg role "$1" --argjson idx "${2:-0}" \
    '[.servers[] | select(.labels.role == $role)] | sort_by(.name) | .[$idx].private_net // []
     | if length > 0 then .[0].ip else "" end'
}

ips_by_role() { hc GET "/servers?label_selector=$LABEL" \
  | jq -r '.servers[] | "\(.labels.role // "?") \(.public_net.ipv4.ip) \(.name)"'; }

wait_ready() { # wait_ready <ip>
  echo "  waiting for cloud-init on $1 (installs docker + the release; a few minutes)"
  for _ in $(seq 1 90); do
    if lab_ssh "root@$1" \
         'test -f /opt/lab-ready' 2>/dev/null; then echo "  ready: $1"; return 0; fi
    sleep 10
  done
  echo "  TIMED OUT on $1 - check: ssh root@$1 'cloud-init status; tail -40 /var/log/cloud-init-output.log'"
  return 1
}

cmd_up() {
  local shape="${1:-single}"
  local hourly
  case "$shape" in
    single) hourly=$(price_of "$TYPE_SINGLE") ;;
    multi)  hourly=$(awk -v w="$(price_of "$TYPE_WRITER")" -v s="$(price_of "$TYPE_SMALL")" \
              'BEGIN { if (w == "" || s == "") exit 1; printf "%.4f", 2 * w + s }') ;;
    *) echo "shape must be 'single' or 'multi'"; exit 1 ;;
  esac
  cat <<TXT

  shape:    $shape
  location: $LOCATION
  cost:     ~€$hourly/hour  (~€$(awk -v h="$hourly" 'BEGIN { printf "%.2f", h * 24 }')/day)

  Hetzner bills hourly, so this is only cheap if you destroy it:
      ./scripts/fleet-lab.sh down

TXT
  read -r -p "  create? [y/N] " ok; [ "$ok" = y ] || { echo "  nothing created"; exit 0; }

  if [ "$shape" = single ]; then
    echo "creating one box…"
    # Hetzner returns `resource_unavailable: error during placement` when a region has no capacity for
    # a type - and it can do so *after* accepting the request and handing out an IP, so the server
    # appears to exist and then vanishes. That happened on the first run of this script. Falling back
    # across type and location beats a lab that dies on someone else's capacity planning.
    local ip=""
    for loc in "$LOCATION" fsn1 nbg1 hel1; do
      for t in "$TYPE_SINGLE" "$TYPE_SMALL"; do
        ip=$(LOCATION="$loc" create_server "$LABEL-single" "$t" all 2>/dev/null) || ip=""
        if [ -n "$ip" ]; then echo "  $t in $loc -> $ip"; break 2; fi
        echo "  no capacity: $t in $loc"
      done
    done
    [ -z "$ip" ] && { echo "  no capacity anywhere for these types - try TYPE_SINGLE=cpx31"; exit 1; }
    wait_ready "$ip"
  else
    echo "creating a private network…"
    # Reuse an existing lab network rather than failing with a 409. A previous run that could not place
    # its boxes leaves the network behind (it is free, and `down` only runs if someone runs it), and a
    # hard failure there sends the operator hunting for a resource that costs nothing and is safe to
    # share. Idempotent `up` beats a tidy-first rule nobody remembers.
    local net; net=$(hc GET "/networks?label_selector=$LABEL" 2>/dev/null \
      | jq -r '(.networks // [])[0].id // ""' 2>/dev/null || true)
    if [ -n "$net" ]; then
      echo "  reusing network $net"
    else
    net=$(hc POST /networks \
      "{\"name\":\"$LABEL-net\",\"ip_range\":\"10.44.0.0/16\",\"subnets\":[{\"type\":\"cloud\",\"network_zone\":\"eu-central\",\"ip_range\":\"10.44.1.0/24\"}],\"labels\":{\"$LABEL\":\"true\"}}" \
      | jq_ network.id)
      echo "  network $net"
    fi
    echo "creating three boxes…"
    # A bare network **id**, not `{"network": id}`. Hetzner's POST /servers takes `networks` as an
    # array of ids; an array of objects is a 422, which is what this passed for as long as the `multi`
    # shape went unrun. That is the whole reason it went unnoticed - `single` never touches this path.
    #
    # **And the same capacity fallback `single` has.** `resource_unavailable: error during placement`
    # is transient and regional: on 2026-07-31 a `cx33` in hel1 succeeded from one call and 412'd from
    # the next, minutes apart. Without a retry the whole lab dies on someone else's capacity planning -
    # and worse, dies *partway*, leaving boxes billing (see the cleanup trap below).
    #
    # Every box must land in the **same network zone** as the private network (eu-central), so the
    # fallback walks locations within that zone only. Falling back to a smaller type is fine here: the
    # cross-machine cases test lease and reconcile *semantics*, not throughput.
    local made=""
    make_box() { # make_box <name> <role> <preferred-type>
      local name="$1" role="$2" want="$3" ip=""
      for loc in "$LOCATION" fsn1 nbg1 hel1; do
        for t in "$want" "$TYPE_SMALL"; do
          ip=$(LOCATION="$loc" create_server "$name" "$t" "$role" "$net" 2>/dev/null) || ip=""
          if [ -n "$ip" ]; then echo "  $name: $t in $loc -> $ip"; made="$made $ip"; return 0; fi
        done
      done
      echo "  $name: no capacity for $want or $TYPE_SMALL anywhere in eu-central" >&2
      return 1
    }
    # A half-built lab is worse than none: it bills, and `hosts` shows a fleet that cannot be verified.
    # Tear down whatever was created if any box cannot be placed.
    if ! ( make_box "$LABEL-cp" control "$TYPE_SMALL" \
        && make_box "$LABEL-writer1" writer "$TYPE_WRITER" \
        && make_box "$LABEL-writer2" writer "$TYPE_WRITER" ); then
      echo
      echo "could not place all three boxes - removing what was created so nothing bills." >&2
      yes y | cmd_down >/dev/null 2>&1 || true
      exit 1
    fi
    ips_by_role | while read -r _ ip _; do wait_ready "$ip"; done
  fi
  echo; echo "hosts:"; ips_by_role | sed 's/^/  /'
  echo; echo "next: ./scripts/fleet-lab.sh verify"
}

cmd_verify() {
  local target; target=$(ips_by_role | head -1 | awk '{print $2}')
  [ -z "$target" ] && { echo "no lab found - run 'up' first"; exit 1; }
  local n_writers; n_writers=$(ips_by_role | awk '$1=="writer"' | wc -l | tr -d ' ')

  echo "== levels 0-4 on $target =="
  # `verify.sh` comes from the checkout on the box, so the lab tests the released binary against the
  # runbook at the version that shipped it - not against whatever is on this laptop.
  lab_ssh "root@$target" 'cd /opt/nuthatch-src && NUTHATCH=/usr/local/bin/nuthatch ./scripts/verify.sh 0 2 || true'
  echo

  if [ "$n_writers" -gt 0 ]; then
    cmd_verify_distributed
  else
    cmd_verify_single "$target"
  fi
}

# Write the lab nest onto a box. Shared by both shapes so the two topologies index the same thing.
lab_nest() { # lab_nest <ip> [extra-docker-setup]
  lab_ssh "root@$1" 'set -eu
    cd /opt/nuthatch-src
    mkdir -p nest/abis
    printf "[nest]\nname=\"lab\"\nchain=\"arbitrum-one\"\nchain_id=42161\nrpc_urls=[\"https://arb1.arbitrum.io/rpc\"]\n\n[[contracts]]\nalias=\"usdc\"\naddress=\"0xaf88d065e77c8cC2239327C5EDb3A432268e5831\"\nabi=\"abis/usdc.json\"\n" > nest/nuthatch.toml
    printf "[{\"type\":\"event\",\"name\":\"Transfer\",\"inputs\":[{\"name\":\"from\",\"type\":\"address\",\"indexed\":true},{\"name\":\"to\",\"type\":\"address\",\"indexed\":true},{\"name\":\"value\",\"type\":\"uint256\",\"indexed\":false}],\"anonymous\":false}]" > nest/abis/usdc.json
    chown -R 10001:10001 nest'
}

# **The genuinely distributed run.** Control plane, store and FE tier on one box; workers on their own
# machines, reaching the store over the private network.
#
# This is what `multi` always claimed and never did: before 2026-07-31 `verify` brought the entire
# compose fleet up on the first host and left the writer boxes empty, so `partition` and `skew` were
# firewalling and clock-skewing machines that ran nothing.
cmd_verify_distributed() {
  local cp; cp=$(ips_by_role | awk '$1=="control"{print $2}' | head -1)
  local cp_priv; cp_priv=$(private_ip control)
  [ -z "$cp" ] || [ -z "$cp_priv" ] && { echo "need a control box with a private address"; exit 1; }
  echo "== distributed fleet: control plane on $cp ($cp_priv), workers on their own boxes =="

  lab_nest "$cp"
  # The control box runs the store, the control plane and the FE tier - and **no writers**. A writer
  # here would be indistinguishable from a remote one in the registry, which is exactly the confusion
  # this shape exists to remove.
  #
  # PG_BIND/CONTROL_BIND put both surfaces on the private address. They default to 127.0.0.1 in the
  # compose file; this is a closed 10.44.0.0/16 network, and the credentials are dev defaults.
  lab_ssh "root@$cp" "set -eux
    cd /opt/nuthatch-src
    mkdir -p .img && cp /usr/local/bin/nuthatch-scaled .img/nuthatch && cp Dockerfile .img/
    docker build -q -t nuthatch-scaled:lab .img
    PG_BIND=$cp_priv CONTROL_BIND=$cp_priv NUTHATCH_IMAGE=nuthatch-scaled:lab \
      docker compose -f docker-compose.scaled.yml --profile fleet up -d postgres control fe --scale fe=2
    for i in \$(seq 1 30); do curl -fsS -m3 $cp_priv:8290/workers -H 'authorization: Bearer dev-token-change-me' >/dev/null 2>&1 && break; sleep 2; done
    docker compose -f docker-compose.scaled.yml ps -a --format '{{.Service}} {{.State}}'"

  local i=0
  ips_by_role | awk '$1=="writer"{print $2}' | while read -r w; do
    echo "-- writer node $w -> store at $cp_priv --"
    lab_nest "$w"
    # `docker-compose.writer-node.yml` carries the writer and nothing else - no `depends_on: postgres`,
    # so no second empty database appears beside it.
    #
    # CONTROL_HOST is **exported, not prefixed onto one command**: compose interpolates the whole file
    # for *every* subcommand, so even a bare `ps` needs it, and the `:?` default makes its absence a
    # hard error. Prefixing only the `up` made the following `ps` fail, which under `set -e` aborted
    # this loop before the second writer started and before level 5 ran.
    #
    # **Keep prose out of the quoted script below.** These comments live here, outside the double
    # quotes, because backticks inside a double-quoted string are command substitution - an earlier
    # version of this note put `ps` in backticks inside the remote script, which executed the *local*
    # ps and spliced its output into the commands sent to the box.
    lab_ssh "root@$w" "set -eux
      cd /opt/nuthatch-src
      mkdir -p .img && cp /usr/local/bin/nuthatch-scaled .img/nuthatch && cp Dockerfile .img/
      docker build -q -t nuthatch-scaled:lab .img
      export CONTROL_HOST=$cp_priv NUTHATCH_IMAGE=nuthatch-scaled:lab
      docker compose -f docker-compose.writer-node.yml up -d
      docker compose -f docker-compose.writer-node.yml ps --format '{{.Service}} {{.State}}'"
    i=$((i+1))
  done

  # Workers heartbeat on a timer; checking immediately is how 5.1b failed on 2026-07-31 and passed on
  # a re-run. Wait for the registry to actually show them before asserting anything about it.
  echo "-- waiting for workers to register --"
  for _ in $(seq 1 30); do
    # The control plane binds to the **private** address (CONTROL_BIND), so `localhost` refuses -
    # which made this read 0 forever while the registry was in fact populated.
    local n; n=$(lab_ssh "root@$cp" "curl -fsS -m3 http://$cp_priv:8290/workers -H 'authorization: Bearer dev-token-change-me' 2>/dev/null" \
      | jq -e '(.workers // []) | length' 2>/dev/null || echo 0)
    echo "   registered: $n"
    [ "${n:-0}" -ge 1 ] && break
    sleep 5
  done

  # **Declare a nest before asserting that anything indexes.** 5.11 asks whether a held cursor moves
  # `last_block`; a cursor with no declared nest has nothing to index, so the check failed for want of
  # a precondition it never established. Measured 2026-08-02: `desired_nest` was empty at 5.11 time
  # because the level-5 checks that declare a nest clean up after themselves.
  echo "-- declaring a nest so there is something to index --"
  lab_ssh "root@$cp" "curl -s -X POST http://$cp_priv:8290/nests \
    -H 'authorization: Bearer dev-token-change-me' -H 'content-type: application/json' \
    -d '{\"name\":\"lab\",\"chain\":\"arbitrum-one\",\"estimated_rss_mb\":512}' >/dev/null" || true

  echo
  echo "== level 5 against the distributed fleet =="
  # `HOT_STORE_PSQL` is what lets **5.11** run at all - the check that asserts a held cursor actually
  # indexes. Without it 5.11 skips, which is how a runbook can report a healthy fleet while the writer
  # pool writes nothing (issue #250). Postgres lives in compose on this box, so the psql command is a
  # `compose exec`.
  lab_ssh "root@$cp" 'cd /opt/nuthatch-src \
    && export HOT_STORE_PSQL="docker compose -f docker-compose.scaled.yml exec -T postgres psql -U nuthatch" \
    && NUTHATCH=/usr/local/bin/nuthatch-scaled ./scripts/verify.sh 5'
}

# The original one-box run: everything in one compose project. Still the right shape for `single`.
cmd_verify_single() {
  local target="$1"
  echo "== the compose fleet, then level 5 =="
  lab_nest "$target"
  lab_ssh "root@$target" 'set -eux
    cd /opt/nuthatch-src
    mkdir -p .img && cp /usr/local/bin/nuthatch-scaled .img/nuthatch && cp Dockerfile .img/
    docker build -q -t nuthatch-scaled:lab .img
    NUTHATCH_IMAGE=nuthatch-scaled:lab docker compose -f docker-compose.scaled.yml --profile fleet up -d --scale writer=2 --scale fe=2
    for i in $(seq 1 30); do curl -fsS -m3 localhost:8290/health >/dev/null 2>&1 && break; sleep 2; done
    sleep 20   # workers heartbeat on a timer; 5.1b fails if asserted too early
    docker compose -f docker-compose.scaled.yml --profile fleet ps -a --format "{{.Service}} {{.State}}"
    NUTHATCH=/usr/local/bin/nuthatch-scaled ./scripts/verify.sh 5'
}

# Both of these used to *print* what to expect and leave the operator to squint at logs. That made
# them unfalsifiable: run it, see no explosion, tick the box - in the one document whose entire job is
# to be falsifiable. They now assert and exit non-zero, so "verified" means a machine checked it.
#
# Writing the assertions surfaced two defects in the tests themselves, which is exactly what an
# unasserted test hides:
#
#   1. `partition` blocked the whole control-plane HOST. In the `multi` shape Postgres runs on that
#      same box, so it cut the writer off from its **hot store** as well - and a writer that cannot
#      reach its store cannot index at all. The stated expectation ("the cursor it holds STILL
#      INDEXING") was therefore impossible to satisfy. It now blocks **only the control-plane API
#      port**, leaving 5432 reachable, which is the invariant we actually care about: the control
#      plane schedules, the store serves, and losing the former must not stop ingestion.
#
#   2. Both drafts polled the writer's `/metrics`. `worker` serves no HTTP at all and publishes no
#      port. Progress is instead read from the **shared Postgres**, which is a better signal anyway:
#      it proves the write landed in the store rather than that a process incremented a counter.
CONTROL_PORT="${CONTROL_PORT:-8290}"

# Run a query on the control box's Postgres. The lab driver reaches it directly, so this keeps working
# while a *writer* is partitioned from it - which is what lets us observe the writer during the cut.
psql_cp() { # psql_cp <control-ip> <sql>
  # `cd` first: compose resolves its file relative to the working directory, and ssh lands in $HOME.
  # Without this the query failed silently - stderr is discarded here, so an empty result was
  # indistinguishable from "nothing to report", and `set -e` turned the precondition check into an
  # exit with no output at all. Diagnosing that cost more than the fix.
  lab_ssh "root@$1" \
    "cd /opt/nuthatch-src && docker compose -f docker-compose.scaled.yml exec -T postgres psql -U nuthatch -tAc \"$2\"" \
    2>/dev/null | tr -d '\r'
}

# The nest schema, discovered rather than assumed - `PgStore` creates one schema per nest.
# The schema that actually holds a **cursor** - the one with lease/progress rows.
#
# `PgStore` creates a schema per nest *and* per cursor, so several carry a `meta` table: a nest's own
# schema records things like `block_timestamps`, while the cursor's records the lease and `last_block`.
# Picking the first `meta` alphabetically found `nest_lab` and reported "no lease to observe" while a
# lease was sitting in `nest_cursor_arbitrum_one`. Prefer a schema that has lease or progress rows,
# and fall back to any `meta` so the error message stays about the missing data rather than the query.
nest_schema() {
  local s
  s=$(psql_cp "$1" "select table_schema from information_schema.tables t where table_name='meta' and exists (select 1 from pg_catalog.pg_tables where schemaname=t.table_schema and tablename='meta') order by (table_schema like 'nest_cursor%') desc, table_schema limit 5" | head -5)
  local pick
  for pick in $s; do
    if [ -n "$(psql_cp "$1" "select 1 from \"$pick\".meta where key in ('lease_owner','last_block') limit 1")" ]; then
      echo "$pick"; return 0
    fi
  done
  echo "$s" | head -1
}

meta_value() { # meta_value <control-ip> <schema> <key>
  psql_cp "$1" "select value from \\\"$2\\\".meta where key='$3'" | head -1
}

cmd_partition() {
  local w; w=$(ips_by_role | awk '$1=="writer"{print $2}' | head -1)
  local cp; cp=$(ips_by_role | awk '$1=="control"{print $2}' | head -1)
  [ -z "$w" ] || [ -z "$cp" ] && { echo "needs the 'multi' shape"; exit 1; }
  local sch; sch=$(nest_schema "$cp")
  [ -z "$sch" ] && { echo "FAIL: no nest schema in Postgres - is the fleet indexing? run 'verify' first"; exit 1; }

  local before; before=$(meta_value "$cp" "$sch" last_block)
  [ -z "$before" ] && { echo "FAIL: no last_block in $sch.meta - nothing has indexed yet"; exit 1; }
  echo "last_block=$before; cutting writer $w off the control-plane API ($cp:$CONTROL_PORT) for 90s"
  echo "  (Postgres on :5432 stays reachable - this is a control-plane outage, not a store outage)"

  lab_ssh "root@$w" "iptables -I OUTPUT -d $cp -p tcp --dport $CONTROL_PORT -j DROP"
  # Heal on any exit, including Ctrl-C: a lab box left firewalled is a confusing bill later.
  trap "lab_ssh root@$w 'iptables -D OUTPUT -d $cp -p tcp --dport $CONTROL_PORT -j DROP' 2>/dev/null || true" EXIT
  sleep 90
  local during; during=$(meta_value "$cp" "$sch" last_block)
  lab_ssh "root@$w" "iptables -D OUTPUT -d $cp -p tcp --dport $CONTROL_PORT -j DROP"; trap - EXIT
  echo "healed. last_block during the cut: ${during:-none}"

  if [ -z "$during" ] || [ "$during" -le "$before" ]; then
    echo "FAIL: last_block did not advance during the partition ($before -> ${during:-none})."
    echo "      Losing the control plane must not stop the cursor. That split is the design:"
    echo "      the control plane schedules, the writer indexes."
    exit 1
  fi
  echo "PASS: indexed through the partition ($before -> $during) with the control plane unreachable"

  sleep 30
  local after; after=$(meta_value "$cp" "$sch" last_block)
  if [ -z "$after" ] || [ "$after" -lt "$during" ]; then
    echo "FAIL: did not resume cleanly after healing (during=$during after=${after:-none})"
    exit 1
  fi
  echo "PASS: resumed after healing ($during -> $after)"
}

# **A worker runs a nest it has never seen** (RFC-0019, issue #250).
#
# Every other check here starts from a nest the lab already placed on each box, which quietly assumes
# away the hardest part of a distributed fleet: an operator declares a nest *centrally*, and the
# machine that ends up holding the cursor may have nothing on disk. That was true of nuthatch until
# now - `desired_nest` recorded a `version` and a `bundle_hash` that nothing on the worker side read.
#
# So this deletes the nest from the writer box entirely, publishes it to a registry only the control
# box serves, pins it, and asserts the writer indexes anyway. Deleting is the point: a check that
# leaves the nest in place would pass whether or not the pull works.
cmd_pull() {
  local w; w=$(ips_by_role | awk '$1=="writer"{print $2}' | head -1)
  local cp; cp=$(ips_by_role | awk '$1=="control"{print $2}' | head -1)
  local cp_priv; cp_priv=$(private_ip control)
  { [ -z "$w" ] || [ -z "$cp" ] || [ -z "$cp_priv" ]; } && { echo "needs the 'multi' shape"; exit 1; }

  # A local S3 server (versitygw; MinIO was withdrawn upstream, #1492) rather than a real S3: the
  # registry has to be reachable from another machine (a local
  # directory would make this a single-box test and prove nothing), and a self-contained one keeps
  # long-lived cloud credentials out of a lab that gets destroyed and rebuilt.
  local key=labuser sec=labpassword123
  local s3env="AWS_ACCESS_KEY_ID=$key AWS_SECRET_ACCESS_KEY=$sec AWS_ENDPOINT=http://$cp_priv:9000 AWS_REGION=us-east-1 AWS_ALLOW_HTTP=true"
  echo "== registry pull: publishing to versitygw on $cp_priv:9000, writer $w starts with no nest =="

  lab_ssh "root@$cp" "set -eu
    docker rm -f s3 >/dev/null 2>&1 || true
    docker run -d --name s3 -p $cp_priv:9000:9000 -v /data \
      -e ROOT_ACCESS_KEY=$key -e ROOT_SECRET_KEY=$sec \
      versity/versitygw@sha256:30292fc2eeacc67a36993b01f7a7a5e3361a19cced0e80c1d71cfa2a4b0a2499 \
      --port :9000 --health /health posix /data >/dev/null
    for i in \$(seq 1 30); do curl -fsS -m2 http://$cp_priv:9000/health >/dev/null 2>&1 && break; sleep 2; done
    curl -fsS --aws-sigv4 aws:amz:us-east-1:s3 --user $key:$sec -X PUT http://$cp_priv:9000/nests >/dev/null"

  # Publish from the control box, which is the only machine that has the nest.
  local hash
  hash=$(lab_ssh "root@$cp" "set -eu
    cd /opt/nuthatch-src
    /usr/local/bin/nuthatch-scaled nest bundle nest --out /tmp/lab.bundle >/dev/null
    env $s3env /usr/local/bin/nuthatch-scaled nest publish /tmp/lab.bundle \
      --registry s3://nests/registry --as lab@1.0.0" | awk '/^ *hash:/{print $2; exit}' | tr -d '\r')
  [ -z "$hash" ] && { echo "FAIL: nothing published - no hash in the publish output"; exit 1; }
  echo "published lab@1.0.0 -> $hash"

  # Pin it fleet-wide. Without this the worker would resolve through the registry's mutable index;
  # with it, the fetch is by content address and the index is never consulted.
  lab_ssh "root@$cp" "curl -fsS -X PUT $cp_priv:$CONTROL_PORT/nests/lab/pin \
    -H 'authorization: Bearer dev-token-change-me' -H 'content-type: application/json' \
    -d '{\"version\":\"1.0.0\",\"bundle_hash\":\"$hash\"}'" >/dev/null \
    || { echo "FAIL: could not pin lab@1.0.0 - is the control plane up? run 'verify' first"; exit 1; }

  local sch; sch=$(nest_schema "$cp")
  local before; before=$(meta_value "$cp" "$sch" last_block)
  echo "last_block before: ${before:-none}"

  # **Take the nest away.** Everything below has to come from the registry.
  lab_ssh "root@$w" "set -eu
    cd /opt/nuthatch-src
    docker compose -f docker-compose.writer-node.yml down >/dev/null 2>&1 || true
    rm -rf nest && mkdir -p nest && chown -R 10001:10001 nest
    CONTROL_HOST=$cp_priv REGISTRY=s3://nests/registry NUTHATCH_IMAGE=nuthatch-scaled:lab \
      docker compose -f docker-compose.writer-node.yml up -d >/dev/null
    docker compose -f docker-compose.writer-node.yml ps --format '{{.Service}} {{.State}}'"

  echo "waiting 120s for the writer to pull and index..."
  sleep 120

  local logs
  logs=$(lab_ssh "root@$w" "cd /opt/nuthatch-src && REGISTRY=s3://nests/registry CONTROL_HOST=$cp_priv \
    docker compose -f docker-compose.writer-node.yml logs --no-color --tail 400" 2>/dev/null)
  echo "$logs" | grep -q "pulling from the registry" \
    || { echo "FAIL: the writer never tried to pull. Recent log:"; echo "$logs" | tail -20; exit 1; }
  echo "PASS: the writer pulled a nest it did not have"

  # A pull that lands but never indexes is the same silence #250 was: assert the store moved.
  local after; after=$(meta_value "$cp" "$sch" last_block)
  if [ -z "$after" ] || { [ -n "$before" ] && [ "$after" -le "$before" ]; }; then
    echo "FAIL: last_block did not advance after the pull (${before:-none} -> ${after:-none})."
    echo "      Locating a nest is only half of it - the cursor has to actually write."
    echo "$logs" | tail -20
    exit 1
  fi
  echo "PASS: indexed from a pulled nest (${before:-none} -> $after)"

  # The cache is content-addressed, which is what makes a re-pin re-pull rather than silently reuse.
  lab_ssh "root@$w" "docker compose -f docker-compose.writer-node.yml exec -T writer ls /var/lib/nuthatch/nests/lab 2>/dev/null" \
    | tr -d '\r' | grep -q "$hash" \
    && echo "PASS: cached at its content address (/var/lib/nuthatch/nests/lab/$hash)" \
    || echo "NOTE: could not list the cache directory - the pull and the indexing both passed"
}

cmd_skew() {
  local w; w=$(ips_by_role | awk '$1=="writer"{print $2}' | head -1)
  local cp; cp=$(ips_by_role | awk '$1=="control"{print $2}' | head -1)
  [ -z "$w" ] || [ -z "$cp" ] && { echo "needs the 'multi' shape"; exit 1; }
  local sch; sch=$(nest_schema "$cp")
  [ -z "$sch" ] && { echo "FAIL: no nest schema in Postgres - run 'verify' first"; exit 1; }

  # A lease is two rows in the per-nest `meta` table (`lease_owner` / `lease_expires_at`, see
  # `store.rs`); `expires_at` is unix seconds **on the store's clock**, which is the authority this
  # test exists to prove. (An earlier draft queried a `cursor_lease` table that does not exist - it
  # would have returned empty forever and "passed" by never contradicting anything.)
  local owner_before; owner_before=$(meta_value "$cp" "$sch" lease_owner)
  local exp_before;   exp_before=$(meta_value "$cp" "$sch" lease_expires_at)
  [ -z "$owner_before" ] && { echo "FAIL: no lease to observe in $sch.meta"; exit 1; }
  echo "lease before: owner=$owner_before expires_at=$exp_before"

  echo "pushing writer $w's clock 10 minutes forward"
  lab_ssh "root@$w" 'timedatectl set-ntp false && date -s "+10 minutes" && date'
  trap "lab_ssh root@$w 'timedatectl set-ntp true' 2>/dev/null || true" EXIT
  sleep 60
  local owner_after; owner_after=$(meta_value "$cp" "$sch" lease_owner)
  local exp_after;   exp_after=$(meta_value "$cp" "$sch" lease_expires_at)
  lab_ssh "root@$w" 'timedatectl set-ntp true'; trap - EXIT
  echo "lease after:  owner=${owner_after:-none} expires_at=${exp_after:-none}"

  if [ "$owner_before" != "$owner_after" ]; then
    echo "FAIL: the lease changed hands ($owner_before -> ${owner_after:-none}) because a worker's"
    echo "      clock moved. Expiry is evaluated on the database clock; a worker with a wrong clock"
    echo "      must neither gain nor lose a lease it should not have."
    exit 1
  fi
  # A renewing owner pushes expiry forward on the DB clock - by seconds, never by the 600 it skewed.
  local delta=$(( ${exp_after:-0} - ${exp_before:-0} ))
  if [ "$delta" -ge 300 ]; then
    echo "FAIL: expiry jumped ${delta}s - the worker's skewed clock leaked into the lease deadline"
    exit 1
  fi
  echo "PASS: lease held by $owner_after throughout; expiry moved ${delta}s on the DB clock, not 600s"
}

cmd_down() {
  echo "destroying everything labelled $LABEL:"
  ips_by_role | sed 's/^/  /' || true
  read -r -p "  destroy? [y/N] " ok; [ "$ok" = y ] || { echo "  kept"; exit 0; }
  hc GET "/servers?label_selector=$LABEL" | jq -r '.servers[].id' | while read -r id; do
    [ -n "$id" ] && { hc DELETE "/servers/$id" >/dev/null && echo "  server $id gone"; }
  done
  # Networks refuse deletion while attached, so they go last.
  sleep 5
  hc GET "/networks?label_selector=$LABEL" | jq -r '.networks[].id' | while read -r id; do
    [ -n "$id" ] && { hc DELETE "/networks/$id" >/dev/null && echo "  network $id gone"; }
  done
  echo "done - billing stops now."
}

case "${1:-}" in
  up)        shift; cmd_up "${1:-single}" ;;
  verify)    cmd_verify ;;
  partition) cmd_partition ;;
  skew)      cmd_skew ;;
  pull)      cmd_pull ;;
  down)      cmd_down ;;
  hosts)     ips_by_role ;;
  *) sed -n '2,30p' "$0" | sed 's/^# \{0,1\}//' ;;
esac
