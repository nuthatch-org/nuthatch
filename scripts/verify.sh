#!/usr/bin/env bash
# The executable half of docs/verification.md.
#
# Every check here mirrors a numbered step in that document, asserts a concrete result, and says what a
# failure means. Run it against a local build, a box over SSH, or a compose fleet.
#
#   ./scripts/verify.sh 0 1 2      # artifact, single nest, correctness
#   ./scripts/verify.sh 4          # the guards
#   ./scripts/verify.sh 5          # scaled mode (needs docker compose)
#   ./scripts/verify.sh all
#
# This is an acceptance runbook for a running binary, nest or fleet. It is not a git hook and not a
# pre-push gate: it needs something to point at, and level 5 needs docker. `yatr ci` is the local gate.
#
# Configuration, all optional:
#   NUTHATCH        path to the binary            (default: nuthatch on PATH)
#   NUTHATCH_URL    a running nest's API          (default: http://127.0.0.1:8288)
#   CONTROL_URL     the control plane             (default: http://127.0.0.1:8290)
#   CONTROL_TOKEN   control-plane bearer token    (required for level 5; same value the fleet uses)
#   RPC             an RPC endpoint for level 1   (default: the chain's built-in)
#   NEST_DIR        an existing nest to test       (default: a temp one is scaffolded)
#
# **A skip is not a pass.** Steps whose prerequisites are absent are reported as SKIP and counted
# separately, because a green run that silently skipped the interesting half is worse than a red one.
# `--strict` turns any skip into a failure, which is what CI should use.

set -uo pipefail

NUTHATCH="${NUTHATCH:-nuthatch}"
NUTHATCH_URL="${NUTHATCH_URL:-http://127.0.0.1:8288}"
CONTROL_URL="${CONTROL_URL:-http://127.0.0.1:8290}"
# No default. The compose stack no longer ships one, so a default here could only ever be wrong -
# it would send a token the control plane does not know and report the result as a 401, which reads
# as a broken fleet rather than an unset variable.
#
# Unset is not an error *here*, though: only Level 5 talks to the control plane, and making this a
# hard `:?` aborted the script before a single check ran - so verifying an embedded nest, which is
# what almost everyone runs this for, failed on a variable it never needed. Level 5 requires it at
# the point of use instead.
CONTROL_TOKEN="${CONTROL_TOKEN:-}"
COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.scaled.yml}"

STRICT=0
LEVELS=()
for arg in "$@"; do
  case "$arg" in
    --strict) STRICT=1 ;;
    all) LEVELS=(0 1 2 3 4 5) ;;
    *) LEVELS+=("$arg") ;;
  esac
done
[ ${#LEVELS[@]} -eq 0 ] && LEVELS=(0 1 2 3 4 5)

PASS=0; FAIL=0; SKIP=0
declare -a RESULTS=()

# check <step-id> <claim> <command...>
# The command's exit status is the verdict. Output is captured and only shown on failure, so a passing
# run stays readable and a failing one gives you everything.
check() {
  local id="$1" claim="$2"; shift 2
  local out rc
  out=$("$@" 2>&1); rc=$?
  if [ $rc -eq 0 ]; then
    PASS=$((PASS+1)); RESULTS+=("PASS|$id|$claim")
    printf '  \033[32m✓\033[0m %-6s %s\n' "$id" "$claim"
  else
    FAIL=$((FAIL+1)); RESULTS+=("FAIL|$id|$claim")
    printf '  \033[31m✗\033[0m %-6s %s\n' "$id" "$claim"
    printf '        %s\n' "${out:0:600}" | sed 's/^/        /'
  fi
}

skip() {
  SKIP=$((SKIP+1)); RESULTS+=("SKIP|$1|$2 — $3")
  printf '  \033[33m·\033[0m %-6s %s \033[33m(skipped: %s)\033[0m\n' "$1" "$2" "$3"
}

# Context for the checks that follow - not a verdict, so it scores nothing.
note() { printf '  \033[2m%s\033[0m\n' "$1"; }

have() { command -v "$1" >/dev/null 2>&1; }
api()  { curl -fsS -m 15 "$@"; }
ctl()  { curl -fsS -m 15 -H "authorization: Bearer $CONTROL_TOKEN" "$@"; }
# JSON field extraction without a jq dependency - python3 is present everywhere we run.
jget() { python3 -c 'import sys,json;d=json.load(sys.stdin)
for k in sys.argv[1].split("."):
    d = d[int(k)] if k.isdigit() else d[k]
print(d)' "$1"; }

hr() { printf '\n\033[1m%s\033[0m\n' "$1"; }

# `check` runs each assertion via `bash -c`, which is a *subshell* and does not inherit shell
# functions. Without exporting these, every check that used `api`/`ctl`/`jget` failed with
# "command not found" and read as a product failure - which is exactly how a harness starts lying.
# Found by running level 5 on a real box; level 0 does not use them, so it passed locally.
export -f api ctl jget have
export CONTROL_TOKEN NUTHATCH_URL CONTROL_URL

# ---------------------------------------------------------------------------------------------------
level0() {
  hr "Level 0 — the artifact is what it claims"

  check 0.1 "the binary runs and reports a version" \
    bash -c "$NUTHATCH --version | grep -qE '^nuthatch [0-9]+\.[0-9]+\.[0-9]+'"

  # 0.2 is in the document and was never emitted here, which is worse than skipping it: a check that
  # is never printed cannot fail, is not counted, and `--strict` cannot see the hole - the one failure
  # mode this harness exists to make impossible. It needs the published tarball, which is only lying
  # around if that is how you got the binary, so the honest outcome without one is a SKIP that names
  # what was missing. Both the checksum file *and* the tarball it covers have to be there: a `.sha256`
  # whose tarball was tidied away after unpacking would otherwise read as transport corruption.
  local bin_path bin_dir sums tarball
  bin_path=$(command -v "$NUTHATCH" 2>/dev/null || printf '%s' "$NUTHATCH")
  bin_dir=$(cd "$(dirname "$bin_path")" 2>/dev/null && pwd)
  shopt -s nullglob; sums=("$bin_dir"/*.tar.gz.sha256); shopt -u nullglob
  # `${sums[0]}` unsubscripted is an unbound variable under `set -u` when the glob matched nothing,
  # which is the *common* case - a locally built binary - and it aborted level 0 outright.
  tarball="${sums[0]:-}"; tarball="${tarball%.sha256}"
  if [ ${#sums[@]} -eq 0 ]; then
    skip 0.2 "the published tarball matches its checksum" \
      "no *.tar.gz.sha256 beside $bin_path - this binary did not come from an unpacked release"
  elif [ ! -f "$tarball" ]; then
    skip 0.2 "the published tarball matches its checksum" \
      "$(basename "${sums[0]}") is here but $(basename "$tarball") is not"
  elif have sha256sum; then
    check 0.2 "the published tarball matches its checksum" \
      bash -c "cd '$bin_dir' && sha256sum -c '$(basename "${sums[0]}")'"
  elif have shasum; then
    check 0.2 "the published tarball matches its checksum" \
      bash -c "cd '$bin_dir' && shasum -a 256 -c '$(basename "${sums[0]}")'"
  else
    skip 0.2 "the published tarball matches its checksum" "neither sha256sum nor shasum is on PATH"
  fi

  # The glibc floor. A failure here is a distro older than 2.35, not a nuthatch bug.
  check 0.3 "it starts on this libc (no GLIBC_… not found)" \
    bash -c "$NUTHATCH --version 2>&1 | grep -qv 'GLIBC_'"

  # Non-negotiable 1: the default artifact carries no database driver. The subcommand is *listed* on
  # purpose - a command that vanishes with build flags is harder to diagnose than one that explains
  # itself - so the assertion is on the refusal, not on absence from --help.
  # Only the *embedded* build makes this claim, and only it can be checked: a scaled build fails
  # differently (it tries to connect and complains about the URL), which is correct behaviour for a
  # different artifact rather than a failure of this one. So a scaled build SKIPs rather than FAILs -
  # and the skip names which artifact it saw, so a mis-set NUTHATCH is obvious.
  local worker_out
  worker_out=$($NUTHATCH worker --control-db x --hot-store y --chains z 2>&1)
  if grep -q "postgres-store" <<<"$worker_out"; then
    check 0.4 "embedded build refuses scaled roles by name" true
  else
    skip 0.4 "embedded build refuses scaled roles" "this is the scaled artifact"
  fi
}

# ---------------------------------------------------------------------------------------------------
level1() {
  hr "Level 1 — a single nest, end to end"

  if ! api "$NUTHATCH_URL/health" >/dev/null 2>&1; then
    skip 1.3 "the API answers" "nothing serving at $NUTHATCH_URL - start \`nuthatch dev\` first"
    skip 1.4 "responses carry provenance" "no API"
    return
  fi

  check 1.3a "/health returns ok" \
    bash -c "[ \"\$(api $NUTHATCH_URL/health)\" = ok ]"

  # A solo `dev` serves the nest API at the root; a runtime hosting many nests serves each nest under
  # `/<name>/`. Resolve which shape we are pointed at, so Level 1 exercises the nest API either way
  # rather than reporting a runtime's prefixed routes as a missing one - a false failure in a document
  # whose whole point is that a reported failure means something.
  local base="$NUTHATCH_URL"
  if ! api "$NUTHATCH_URL/tables" >/dev/null 2>&1; then
    local prefix
    prefix=$(api "$NUTHATCH_URL/nests" 2>/dev/null \
      | python3 -c 'import sys,json;print(json.load(sys.stdin)["nests"][0]["base_path"])' 2>/dev/null)
    if [ -n "$prefix" ]; then
      base="$NUTHATCH_URL$prefix"
      note "a runtime hosting many nests - Level 1 runs against $prefix"
    fi
  fi

  check 1.3b "/tables reports at least one table" \
    bash -c "[ \"\$(api $base/tables | jget count)\" -ge 1 ]"

  # A mount may answer only its declared queries (RFC-0034), in which case a refused `/sql` is the
  # configured behaviour and not a defect. Detect that rather than scoring it as a failure.
  local free_form
  free_form=$(api "$base/queries" 2>/dev/null \
    | python3 -c 'import sys,json;print(json.load(sys.stdin).get("free_form"))' 2>/dev/null)

  # The step that separates "it ran" from "it is correct" is comparing against an independent source,
  # which this script cannot do for you. It asserts the weaker machine-checkable claim and says so.
  local table
  table=$(api "$base/tables" | python3 -c 'import sys,json;print(json.load(sys.stdin)["tables"][0]["table"])' 2>/dev/null)
  if [ "$free_form" = "False" ]; then
    skip 1.3c "a decoded table is queryable" "this mount answers only its declared queries (sql != open)"
    skip 1.4 "responses carry provenance" "no free-form /sql on this mount"
    return
  elif [ -n "$table" ]; then
    check 1.3c "a decoded table is queryable via /sql" \
      bash -c "api --get $base/sql --data-urlencode 'q=select count(*) c from \"$table\"' >/dev/null"
  else
    skip 1.3c "a decoded table is queryable" "no tables in the schema"
  fi

  # Provenance is what makes an answer attributable to a decode version and a chain position.
  check 1.4 "responses carry provenance (as_of, registry_hash, sealed_through)" \
    bash -c "api --get $base/sql --data-urlencode 'q=select 1' | grep -q registry_hash"
}

# ---------------------------------------------------------------------------------------------------
level2() {
  hr "Level 2 — correctness under adverse conditions"

  # 2.1 needs a controlled restart of a process this script does not own. Rather than pretend, it is
  # skipped with the exact procedure - a scripted half-measure here would assert nothing useful.
  skip 2.1 "restart leaves no gaps or duplicates" \
    "manual: record a count for a fixed block range, SIGTERM, restart, re-count - see verification.md"

  if [ -n "${NEST_DIR:-}" ] && [ -d "${NEST_DIR:-}" ]; then
    check 2.2 "nuthatch check passes (invariants + committed parity fixtures)" \
      bash -c "$NUTHATCH check --dir '$NEST_DIR'"
  else
    skip 2.2 "nuthatch check passes" "set NEST_DIR to a scaffolded nest"
  fi

  skip 2.3 "a reorg converges to canonical state" \
    "needs a live reorg; covered by property tests in CI (e2e_reorg)"
}

# ---------------------------------------------------------------------------------------------------
level3() {
  hr "Level 3 — a runtime hosting many nests"

  if ! api "$NUTHATCH_URL/nests" >/dev/null 2>&1; then
    skip 3.1 "the roster lists mounted nests" "not a multi-nest runtime at $NUTHATCH_URL"
    skip 3.4 "mount and unmount without a restart" "not a multi-nest runtime"
    return
  fi

  check 3.1 "/nests lists mounted nests with chain and footprint" \
    bash -c "api $NUTHATCH_URL/nests | grep -q chain"

  # The live runtime. Mount/unmount is admin-gated, so a 401/404 means the admin surface is off rather
  # than the feature being broken - distinguished so the result is not misread.
  if api "$NUTHATCH_URL/_admin/nests" >/dev/null 2>&1; then
    check 3.4 "the lifecycle routes are served (live mount/unmount)" true
  else
    skip 3.4 "mount and unmount without a restart" "admin surface off (--no-admin or no token)"
  fi

  skip 3.5 "one bad nest is quarantined without harming co-tenants" \
    "manual: break one nest's authored view deliberately - see verification.md"
}

# ---------------------------------------------------------------------------------------------------
level4() {
  hr "Level 4 — the guards (a missing refusal is the finding)"

  if ! api "$NUTHATCH_URL/health" >/dev/null 2>&1; then
    for s in 4.1a 4.1b 4.2; do skip "$s" "guard" "nothing serving at $NUTHATCH_URL"; done
    return
  fi

  # Each of these must FAIL as a request. `! api …` is the assertion: a 2xx here is the finding.
  check 4.1a "DDL on /sql is refused" \
    bash -c "! api --get $NUTHATCH_URL/sql --data-urlencode 'q=CREATE TABLE zzz(a int)' >/dev/null 2>&1"

  # The one that was a real arbitrary-file-write before 0.6.2.
  check 4.1b ";-stacked statements are refused (COPY TO was a file write)" \
    bash -c "! api --get $NUTHATCH_URL/sql --data-urlencode \"q=SELECT 1; COPY (SELECT 1) TO '/tmp/nh-verify-leak'\" >/dev/null 2>&1"

  check 4.2 "filesystem reads via /sql are refused" \
    bash -c "! api --get $NUTHATCH_URL/sql --data-urlencode \"q=SELECT * FROM read_csv('/etc/passwd')\" >/dev/null 2>&1"

  # Belt and braces: even if the request somehow succeeded, the file must not exist.
  check 4.1c "no file was written by the stacked-statement attempt" \
    bash -c "[ ! -e /tmp/nh-verify-leak ]"
}

# ---------------------------------------------------------------------------------------------------
level5() {
  hr "Level 5 — scaled mode (a fleet)"

  if ! have docker || ! docker info >/dev/null 2>&1; then
    skip 5.x "the whole level" "docker is not available"
    return
  fi
  # Checked here rather than at the top: this is the only level that needs it, and an unset token
  # must not stop the other levels from running.
  if [ -z "$CONTROL_TOKEN" ]; then
    skip 5.x "the whole level" "set CONTROL_TOKEN to the same value the fleet was started with"
    return
  fi
  if ! ctl "$CONTROL_URL/health" >/dev/null 2>&1; then
    skip 5.x "the whole level" "no control plane at $CONTROL_URL - bring the fleet up first"
    return
  fi

  check 5.1a "the control plane answers /health" \
    bash -c "[ \"\$(api $CONTROL_URL/health)\" = ok ]"

  # Two workers is the minimum that makes 5.3 meaningful; one still proves registration.
  local workers
  workers=$(ctl "$CONTROL_URL/workers" | jget count 2>/dev/null || echo 0)
  check 5.1b "workers have registered with the control plane" \
    bash -c "[ '$workers' -ge 1 ]"

  local nest="verify-$$"
  check 5.2a "declaring a nest is accepted as desired state" \
    bash -c "ctl -X POST $CONTROL_URL/nests -H 'content-type: application/json' \
      -d '{\"name\":\"$nest\",\"chain\":\"verify-chain-$$\",\"estimated_rss_mb\":90}' | grep -q declared"

  check 5.2b "the plan assigns or explains - never silently omits" \
    bash -c "ctl $CONTROL_URL/plan | grep -qE 'assign|unplaceable'"

  # 5.10: under-scheduling must be loud. A cursor nobody can host must be reported with a reason that
  # distinguishes "add a worker" from "adding workers will not help".
  local huge="verify-huge-$$"
  ctl -X POST "$CONTROL_URL/nests" -H 'content-type: application/json' \
    -d "{\"name\":\"$huge\",\"chain\":\"verify-huge-chain-$$\",\"estimated_rss_mb\":9999999}" >/dev/null 2>&1
  # The claim is "under-scheduling is loud, with a reason you can act on" - not one specific reason.
  # With workers present an oversized cursor is `toolargeforanyworker`; with none it is `noworkers`,
  # which is equally actionable and equally loud. Asserting only the former made this fail whenever the
  # registry happened to be empty, which is a timing artefact rather than a finding.
  check 5.10 "an unplaceable cursor is reported with an actionable reason" \
    bash -c "ctl $CONTROL_URL/plan | grep -qE 'toolargeforanyworker|noroomrightnow|noworkers'"
  if [ "${workers:-0}" -ge 1 ]; then
    check 5.10b "with workers present, 'too large for anyone' is distinguished from 'no room'" \
      bash -c "ctl $CONTROL_URL/plan | grep -q toolargeforanyworker"
  else
    skip 5.10b "'too large' is distinguished from 'no room'" "no workers registered yet"
  fi

  # 5.11: **the writer pool actually writes.**
  #
  # Every other level-5 check tests the control plane - registration, planning, leases, fencing,
  # pinning, secrets - and for a long time all of them passed while `worker::run` contained no
  # indexing code at all (issue #250). "10/10, zero skipped" was true and meant less than it read: a
  # cursor was assigned and owned correctly, and owning one caused nothing to happen.
  #
  # So this asserts the product rather than the machinery around it: a worker holding a cursor moves
  # `last_block` in the shared store. It is the one check that could not have passed before the fix,
  # and the one whose absence let the gap survive a release.
  #
  # Generous timeout: a cold nest backfills before its first checkpoint, and a slow public endpoint is
  # not a product failure. A *skip* here is worth more than a flaky pass - the point is to be honest
  # about whether indexing was observed, not to go green.
  if [ -n "${HOT_STORE_PSQL:-}" ]; then
    check 5.11 "a held cursor actually indexes - last_block advances in the shared store" \
      bash -c '
        deadline=$(( $(date +%s) + 180 ))
        first=""
        while [ "$(date +%s)" -lt "$deadline" ]; do
          v=$($HOT_STORE_PSQL -tAc "select value from information_schema.tables t
                join lateral (select value from meta_all where key='"'"'last_block'"'"') m on true
                limit 1" 2>/dev/null | head -1)
          # Fall back to scanning cursor schemas directly - schema names vary by chain.
          [ -z "$v" ] && v=$($HOT_STORE_PSQL -tAc "select m.value from pg_tables t
                cross join lateral (select value from meta where key='"'"'last_block'"'"') m
                where t.schemaname like '"'"'cursor_%'"'"' limit 1" 2>/dev/null | head -1)
          if [ -n "$v" ]; then
            [ -z "$first" ] && first="$v"
            [ "$v" -gt "$first" ] 2>/dev/null && exit 0
          fi
          sleep 5
        done
        exit 1'
  else
    skip 5.11 "a held cursor actually indexes" "set HOT_STORE_PSQL to a psql command against the shared store"
  fi

  # 5.7: one pinned answer per endpoint, fleet-wide.
  check 5.7a "an unpinned endpoint is not servable" \
    bash -c "[ \"\$(ctl $CONTROL_URL/nests/$nest/resolve | jget servable)\" = False ]"
  check 5.7b "pinning makes it servable" \
    bash -c "ctl -X PUT $CONTROL_URL/nests/$nest/pin -H 'content-type: application/json' \
      -d '{\"version\":\"1.0.0\",\"bundle_hash\":\"abababababababababababababababababababababababababababababababab\"}' >/dev/null && \
      [ \"\$(ctl $CONTROL_URL/nests/$nest/resolve | jget servable)\" = True ]"

  # 5.8: write-only secrets. The value must never come back, and must not reach disk.
  local canary="NH-VERIFY-CANARY-$$"
  ctl -X PUT "$CONTROL_URL/nests/$nest/secrets" -H 'content-type: application/json' \
    -d "{\"key\":\"rpc_url\",\"value\":\"$canary\"}" >/dev/null 2>&1
  check 5.8a "secret keys are listed, values are not" \
    bash -c "ctl $CONTROL_URL/nests/$nest/secrets | grep -q rpc_url && \
             ! ctl $CONTROL_URL/nests/$nest/secrets | grep -q '$canary'"
  if [ -d "${NEST_DIR:-nest}" ]; then
    check 5.8b "the secret appears in no file on disk" \
      bash -c "! grep -rq '$canary' '${NEST_DIR:-nest}' 2>/dev/null"
  else
    skip 5.8b "the secret appears in no file on disk" "no nest directory to search"
  fi

  # 5.3: the invariant everything rests on. Read from the workers' own logs, because the control plane
  # records intent and the *lease* records ownership - asking the control plane would prove nothing.
  local acquired
  acquired=$(docker compose -f "$COMPOSE_FILE" logs writer 2>/dev/null | grep -c "acquired cursors")
  if [ "${acquired:-0}" -gt 0 ]; then
    check 5.3 "a declared cursor is acquired by exactly one worker" \
      bash -c "[ \$(docker compose -f '$COMPOSE_FILE' logs writer 2>/dev/null | \
        grep 'acquired cursors' | grep -oE 'chains=\[[^]]*\]' | sort | uniq -c | \
        awk '\$1 > 1 {print \"dup\"}' | wc -l) -eq 0 ]"
  else
    skip 5.3 "exactly one worker acquires a cursor" \
      "no acquisitions logged yet - declare a nest on a chain a worker hosts, then re-run"
  fi

  # Tidy up after ourselves; a verification run must not leave desired state behind.
  ctl -X DELETE "$CONTROL_URL/nests/$nest" >/dev/null 2>&1
  ctl -X DELETE "$CONTROL_URL/nests/$huge" >/dev/null 2>&1
}

# ---------------------------------------------------------------------------------------------------
printf '\033[1mnuthatch verification\033[0m — %s\n' "$($NUTHATCH --version 2>/dev/null || echo 'binary not found')"
printf 'levels: %s   strict: %s\n' "${LEVELS[*]}" "$([ $STRICT -eq 1 ] && echo yes || echo no)"

for l in "${LEVELS[@]}"; do
  case "$l" in
    0) level0 ;; 1) level1 ;; 2) level2 ;; 3) level3 ;; 4) level4 ;; 5) level5 ;;
    *) printf '\033[33munknown level: %s\033[0m\n' "$l" ;;
  esac
done

hr "Summary"
printf '  %d passed, %d failed, %d skipped\n' "$PASS" "$FAIL" "$SKIP"

if [ "$FAIL" -gt 0 ]; then
  printf '\n\033[31mFailures:\033[0m\n'
  for r in "${RESULTS[@]}"; do
    [ "${r%%|*}" = FAIL ] && printf '  %s\n' "$(echo "$r" | cut -d'|' -f2-)"
  done
  printf '\nThese are findings worth reporting - see the Reporting section of docs/verification.md.\n'
  exit 1
fi

if [ "$SKIP" -gt 0 ] && [ "$STRICT" -eq 1 ]; then
  printf '\n\033[31m--strict: a skip is not a pass.\033[0m Skipped:\n'
  for r in "${RESULTS[@]}"; do
    [ "${r%%|*}" = SKIP ] && printf '  %s\n' "$(echo "$r" | cut -d'|' -f2-)"
  done
  exit 1
fi

printf '\n\033[32mAll executed checks passed.\033[0m'
[ "$SKIP" -gt 0 ] && printf ' %d were skipped - a skip is not a pass.' "$SKIP"
printf '\n'
