#!/usr/bin/env bash
# Compare cargo-mutants survivors against the checked-in baseline (#768).
#
# Exit 1 on a survivor that is not in `.github/mutants-baseline.toml`. Exit 0 otherwise, and say which
# baseline entries no longer survive - a stale exemption is worth removing, and #769's allow-list check
# caught exactly that shape the day after it was built.
#
# The baseline is matched on (file, mutation-text) rather than line number, because a line number moves
# every time somebody adds a comment above it and a gate that fires on unrelated edits is a gate people
# learn to route around.
#
# **A missing or truncated run is a failure, not a pass (#841).** This script used to read an absent
# `missed.txt` as an empty survivor list and print "No new survivors", so a `cargo mutants` that never
# ran, died, or was killed at the job timeout reported a clean bill of health. Every nightly run
# between 2026-08-23 and 2026-08-25 was cancelled at `timeout-minutes: 300`, and this script is the
# reason nobody found out from the job itself. "I could not tell" must never render as "nothing found".
#
# **A timeout is neither caught nor missed, and used to be reported as neither (#853).** cargo-mutants
# writes survivors to `missed.txt` and timed-out mutants to `timeout.txt`; this script read only the
# first, so a mutant could escape the gate by being slow. On the 2026-08-24 run that was 3 of 39 - and
# all three turned out to be genuine: each sends a loop in the adaptive window controller into a spin,
# so a named test never terminates. They are now their own category, and an unbaselined one fails.
#
# Usage:
#     mutants-check.sh [--file src/foo.rs]
#
# `--file` scopes both the survivors and the baseline entries to one source file, so a matrix job that
# mutates only `src/seal.rs` does not report every `src/chunker.rs` baseline entry as newly stale.
set -uo pipefail

OUT=mutants.out
MISSED=$OUT/missed.txt
TIMEOUT=$OUT/timeout.txt
OUTCOMES=$OUT/outcomes.json
BASELINE=.github/mutants-baseline.toml

file=""
while [ $# -gt 0 ]; do
  case "$1" in
    --file) [ $# -ge 2 ] || { echo "usage: $0 [--file src/foo.rs]" >&2; exit 2; }; file=$2; shift 2 ;;
    --file=*) file=${1#--file=}; shift ;;
    *) echo "usage: $0 [--file src/foo.rs]" >&2; exit 2 ;;
  esac
done

refuse() {
  echo "FAIL: $1"
  echo
  echo "This is a failure and not a pass. An absent or truncated mutation run means the"
  echo "coverage question was not answered - it does not mean the answer was yes."
  exit 1
}

# A truncated sweep - killed at the job timeout, or one that enumerated zero mutants because a
# `--file` path stopped matching after a rename - is indistinguishable from a clean one by
# `missed.txt` alone: both are empty.
[ -f "$OUTCOMES" ] || refuse "$OUTCOMES is absent - cargo-mutants did not get far enough to write it"
if ! err=$(jq -e 'type == "object"' "$OUTCOMES" 2>&1 >/dev/null); then
  refuse "$OUTCOMES is unreadable (${err:-not a JSON object}) - treating the run as untrustworthy"
fi
verdict=$(jq -r --arg f "$OUTCOMES" '
  def falsy: . == null or . == false or . == 0 or . == "" or . == [] or . == {};
  def pyrepr: if . == null then "None" elif . == false then "False" elif type == "string" then "'"'"'\(.)'"'"'" else tojson end;
  .total_mutants as $total
  | ((.outcomes // []) | length) as $recorded
  | if ($total | falsy) then
      "\($f) reports total_mutants=\($total | pyrepr). A sweep that enumerated no mutants proves nothing - check the --file paths still match the tree."
    elif .end_time == null then
      "\($f) has no end_time: cargo-mutants was killed before it finished (\($recorded) scenario(s) recorded of \($total) mutant(s)). The results are a prefix, not a verdict - raise the job timeout or narrow the scope."
    else "ok \($total)" end
' "$OUTCOMES")
case "$verdict" in
  "ok "*) echo "run: ${verdict#ok } mutant(s), run completed" ;;
  *) refuse "$verdict" ;;
esac

if [ ! -f "$MISSED" ]; then
  echo "FAIL: $MISSED is absent although the run completed - cargo-mutants always writes it."
  exit 1
fi

baseline=""; [ -f "$BASELINE" ] && baseline=$(cat "$BASELINE")
timeouts=""; [ -f "$TIMEOUT" ] && timeouts=$(cat "$TIMEOUT")

# The last line of jq's output is the exit status; everything before it is the report.
report=$(jq -r -n \
  --rawfile missed "$MISSED" \
  --arg baseline "$baseline" \
  --arg timeouts "$timeouts" \
  --arg file "$file" \
  --arg bl "$BASELINE" \
  --slurpfile outcomes "$OUTCOMES" '
  def strip: sub("^\\s+"; "") | sub("\\s+$"; "");
  # cargo-mutants writes `path:line:col: <mutation text>` per line.
  def survivors: [split("\n")[] | strip | select(. != "")
    | capture("^(?<f>[^:]+):\\d+:\\d+:\\s*(?<m>.+)$")? | [.f, (.m | strip)]];
  # (file, mutation) pairs. A deliberately small parser - the file is ours and its shape is fixed.
  def baseline_entries: [split("[[survivor]]")[1:][] | split("\n") as $l
    | [($l[] | capture("^file\\s*=\\s*\"(?<v>[^\"]+)\"")? | .v)][0] as $f
    | [($l[] | capture("^mutation\\s*=\\s*\"(?<v>[^\"]+)\"")? | .v)][0] as $m
    | select($f != null and $m != null) | [$f, $m]];
  def scoped: if $file != "" then map(select(.[0] == $file)) else . end;
  def holds($x): any(.[]; . == $x);

  ($baseline | baseline_entries | scoped) as $known
  | ($missed | survivors | scoped) as $found
  | ($timeouts | survivors | scoped) as $timed_out
  | [$found[] | select(. as $s | $known | holds($s) | not)] as $new
  # A timeout is judged against the same baseline: it is an outcome the suite failed to produce a
  # verdict for, which is the same thing a survivor is, arrived at more slowly.
  | [$timed_out[] | select(. as $s | $known | holds($s) | not)] as $new_timeouts
  # Only a mutant this run enumerated can be stale. A shard runs an eighth of a file, and judging
  # the rest of the file'"'"'s baseline from it told #1971 to remove four entries that still survive.
  | ([($outcomes[0].outcomes // [])[] | .scenario | select(type == "object")
      | .Mutant.name // ""] | join("\n") | survivors) as $tested
  | [$known[] | select(. as $k | ($tested | holds($k)) and (($found | holds($k)) | not)
      and (($timed_out | holds($k)) | not))] as $gone
  | (if $file != "" then ["scope: \($file)"] else [] end)
    + [$gone[] | "note: baseline entry no longer survives, remove it: \(.[0]): \(.[1])"]
    + if ($new_timeouts | length) > 0 then
        ["", "\($new_timeouts | length) mutation(s) TIMED OUT and are not in \($bl):"]
        + [$new_timeouts[] | "  \(.[0]): \(.[1])"]
        + ["",
           "A timeout is not a caught mutant. The test process did not finish, so the suite",
           "produced no verdict on this mutation at all - and a test that hangs rather than fails",
           "cannot say what went wrong, in CI or in production. Check the per-mutant log in",
           "mutants.out/log/ for a test reported as '"'"'has been running for over 60 seconds'"'"': that",
           "names the loop that stopped advancing. See #853.", "1"]
      elif ($new | length) == 0 then
        ["No new survivors. \($found | length) survivor(s), all in the baseline with a reason.", "0"]
      else
        ["", "\($new | length) mutation(s) survived that are not in \($bl):"]
        + [$new[] | "  \(.[0]): \(.[1])"]
        + ["",
           "A survivor means a test asserted something the code does not have to do to pass.",
           "Either the behaviour wants a test, or the survivor wants an entry in the baseline with a",
           "reason a reader can disagree with. Both are fine; silence is not.", "1"]
      end
  | .[]
') || { echo "FAIL: could not read the mutation output"; exit 1; }

printf '%s\n' "$report" | sed '$d'
exit "$(printf '%s\n' "$report" | tail -n 1)"
