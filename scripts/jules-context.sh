#!/usr/bin/env bash
# What Jules cannot get from a diff: maintainers' replies on the pull request, the base branch's
# bodies of the functions the diff calls or names (one hop), and the pull request head's lines
# around each hunk.
#
# Runs with secrets on pull_request_target, in the default-branch checkout. Everything from the pull
# request is data: identifiers are validated before they reach grep, nothing is executed or sourced,
# and nothing PR-controlled is echoed to the log, where a line could be read as a workflow command.
#
# usage: jules-context.sh --diff pr.diff --comments issue-comments.jsonl
#          --review-comments review-comments.jsonl [--root .]
#          [--replies-out pr.author_replies] [--callee-out pr.callee_context]
#          [--head-dir DIR --surround-out pr.surrounding]
set -euo pipefail

JULES_LOGIN='nuthatch-jules[bot]'
MARKER='<!-- pr-review:luna -->'
MAX_REPLIES_CHARS=20000
MAX_CALLEE_CHARS=40000
MAX_FN_LINES=80
MAX_DEFS_PER_NAME=3
MAX_SURROUND_CHARS=40000
SURROUND_LINES=30
ident_re='^[A-Za-z_][A-Za-z0-9_]*$'

usage() {
  echo "usage: $0 --diff FILE --comments FILE --review-comments FILE [--root DIR] [--replies-out FILE] [--callee-out FILE] [--head-dir DIR] [--surround-out FILE]" >&2
  exit 2
}

diff_file='' comments='' review_comments='' root='.'
replies_out='pr.author_replies' callee_out='pr.callee_context'
head_dir='' surround_out=/dev/null
while [ $# -gt 0 ]; do
  [ $# -ge 2 ] || usage
  case "$1" in
    --diff) diff_file=$2 ;;
    --comments) comments=$2 ;;
    --review-comments) review_comments=$2 ;;
    --root) root=$2 ;;
    --replies-out) replies_out=$2 ;;
    --callee-out) callee_out=$2 ;;
    --head-dir) head_dir=$2 ;;
    --surround-out) surround_out=$2 ;;
    *) usage ;;
  esac
  shift 2
done
[ -n "$diff_file" ] || usage

: > "$replies_out"
: > "$callee_out"
: > "$surround_out"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

# ── Replies ───────────────────────────────────────────────────────────────────────────────────────
[ -n "$comments" ] && [ -r "$comments" ] || comments=/dev/null
[ -n "$review_comments" ] && [ -r "$review_comments" ] || review_comments=/dev/null

# Every reply, marked before or after her latest review, not only those after it: a reply posted
# while a review is in flight predates that review's comment and was never shown to it (#1370).
jq -nj --slurpfile issue "$comments" --slurpfile review "$review_comments" \
  --arg login "$JULES_LOGIN" --arg marker "$MARKER" --argjson cap "$MAX_REPLIES_CHARS" '
  ([$issue[] | select(.user.login == $login and .user.type == "Bot"
                      and ((.body // "") | contains($marker))) | .created_at] | max) as $since
  | [ ($issue[] | . + {where: "comment"}),
      ($review[] | . + {where: "review comment on \(.path // "?"):\(.line // .original_line // 0)"}) ]
  | map(select(.author_association == "OWNER" or .author_association == "MEMBER"
               or .author_association == "COLLABORATOR")
        | select(.user.login != $login))
  | sort_by(.created_at, .id)
  | map((if $since == null then "no review of yours yet"
         elif .created_at > $since then "after your latest review"
         else "before your latest review" end) as $when
        | "--- \(.user.login) (\(.author_association)) at \(.created_at), \(.where) (\($when)) ---\n\(.body // "")\n")
  | reverse
  | reduce .[] as $e ({kept: [], used: 0, dropped: 0, full: false};
      if .full then .dropped += 1
      elif .used + ($e | length) <= $cap then .kept += [$e] | .used += ($e | length)
      elif .kept == [] then .kept += [$e[0:$cap] + "\n[jules-context: this reply was shortened]\n"] | .full = true
      else .dropped += 1 | .full = true end)
  | if .kept == [] then ""
    else (if .dropped > 0 then "[jules-context: \(.dropped) earlier replies elided to fit]\n\n" else "" end)
         + (.kept | reverse | join("\n"))
    end' > "$replies_out"

# ── Callees ───────────────────────────────────────────────────────────────────────────────────────
# Added lines of Rust files, and each Rust hunk's old range (to mark a callee the diff changes).
# `---`/`+++` are headers only between `diff --git` and the first `@@`.
awk -v added="$tmp/added" -v hunks="$tmp/hunks" '
  BEGIN { printf "" > added; printf "" > hunks }
  /^diff --git / { hdr = 1; rs = 0; old = ""; next }
  hdr && /^--- / { old = substr($0, 5); sub(/^a\//, "", old); next }
  hdr && /^\+\+\+ / { p = substr($0, 5); sub(/^b\//, "", p); rs = (p ~ /\.rs$/); next }
  /^@@ / {
    hdr = 0
    if (rs && old != "/dev/null") {
      split($2, r, ","); s = substr(r[1], 2); l = (2 in r) ? r[2] : 1
      printf "%s\t%d\t%d\n", old, s, (l > 0 ? l : 1) > hunks
    }
    next
  }
  !hdr && rs && /^\+/ { print substr($0, 2) > added }
' "$diff_file"
# `.method(` is almost always a std or foreign method, and a same-named local fn would be a wrong match.
sed -E 's/\.[[:space:]]*[A-Za-z_][A-Za-z0-9_]*[[:space:]]*\(/.(/g' "$tmp/added" > "$tmp/calls"

# Deliberately loose: a token carrying `$`, `;`, a quote or a backtick survives this and is rejected
# by the identifier check below, so validation, not tokenising, is what keeps grep safe.
tok='[^][:space:](){},.!&|+*/<>=#@^%~?[-]+'
{
  grep -oE "${tok}\\(" "$tmp/calls" | sed 's/($//' || [ $? -eq 1 ]
  grep -oE "value_parser[[:space:]]*=[[:space:]]*${tok}" "$tmp/added" \
    | sed -E 's/^value_parser[[:space:]]*=[[:space:]]*//' || [ $? -eq 1 ]
  grep -E '^[[:space:]]*//' "$tmp/added" | grep -oE '`[^`]+`' | sed -E 's/^`//; s/`$//; s/\(\)$//' \
    || [ $? -eq 1 ]
} > "$tmp/tokens"

# Closures bound with `let` count as defined: `let word = |n| ...` is not a call to some `fn word`.
grep -oE '(fn|let)[[:space:]]+[A-Za-z_][A-Za-z0-9_]*' "$tmp/added" | awk '{ print $2 }' | sort -u \
  > "$tmp/defined" || [ $? -eq 1 ]

rejected=0
: > "$tmp/pairs"
while IFS= read -r t; do
  t=${t#::}
  name='' qual='' ok=1
  while :; do
    case $t in
      *::*) seg=${t%%::*}; t=${t#*::}; [ -n "$t" ] || ok=0 ;;
      *) seg=$t; t='' ;;
    esac
    [[ $seg =~ $ident_re ]] || { ok=0; break; }
    qual=$name; name=$seg
    [ -n "$t" ] || break
  done
  if [ "$ok" -ne 1 ]; then rejected=$((rejected + 1)); continue; fi
  case $name in
    [A-Z]*|if|while|match|for|loop|return|fn|in|as|let|mut|ref|move|where|impl|self|super|crate) continue ;;
  esac
  if grep -qxF -- "$name" "$tmp/defined"; then continue; fi
  printf '%s\t%s\n' "$name" "$qual" >> "$tmp/pairs"
done < "$tmp/tokens"
LC_ALL=C sort -u "$tmp/pairs" -o "$tmp/pairs"

dirs=()
for d in src decode/src; do [ -d "$root/$d" ] && dirs+=("$d"); done

: > "$tmp/defs"
if [ -s "$tmp/pairs" ] && [ ${#dirs[@]} -gt 0 ]; then
  while IFS="$(printf '\t')" read -r name qual; do
    pat="^[[:space:]]*(pub(\\([a-z]+\\))?[[:space:]]+)?((const|async|unsafe)[[:space:]]+)*fn[[:space:]]+${name}([^A-Za-z0-9_]|\$)"
    (cd "$root" && grep -rnE --include='*.rs' -- "$pat" "${dirs[@]}") | cut -d: -f1,2 \
      | LC_ALL=C sort -t: -k1,1 -k2,2n > "$tmp/found" || [ $? -eq 1 ]
    [ -s "$tmp/found" ] || continue
    # A qualifier narrows: `freshness::f` to freshness.rs, `Type::f` to files that impl Type. A
    # qualifier matching nothing here is another crate's function, not a local one of that name.
    case $qual in
      ''|crate|self|super|Self) cp "$tmp/found" "$tmp/narrow" ;;
      [A-Z]*)
        (cd "$root" && grep -rlE --include='*.rs' -- \
          "^[[:space:]]*impl[^{;]*[[:space:]<]${qual}([[:space:]<{]|\$)" "${dirs[@]}") \
          > "$tmp/impls" || [ $? -eq 1 ]
        awk -F: 'FILENAME == ARGV[1] { ok[$0] = 1; next } ok[$1]' "$tmp/impls" "$tmp/found" > "$tmp/narrow" ;;
      *)
        awk -F: -v q="$qual" '{ n = split($1, p, "/"); if (p[n] == q ".rs" || index("/" $1, "/" q "/")) print }' \
          "$tmp/found" > "$tmp/narrow" ;;
    esac
    n="$(wc -l < "$tmp/narrow" | tr -d ' ')"
    [ "$n" -gt 0 ] || continue
    if [ "$n" -gt "$MAX_DEFS_PER_NAME" ]; then
      printf '%s\n' "$name" >> "$tmp/ambiguous"
      continue
    fi
    awk -v n="$name" '{ print $0 ":" n }' "$tmp/narrow" >> "$tmp/defs"
  done < "$tmp/pairs"
fi
LC_ALL=C sort -u -t: -k1,1 -k2,2n "$tmp/defs" -o "$tmp/defs"

used=0 shown=0
: > "$tmp/elided"
while IFS=: read -r file line name; do
  awk -v start="$line" -v max="$MAX_FN_LINES" '
    NR < start { next }
    NR == start {
      match($0, /^[[:space:]]*/); ind = substr($0, 1, RLENGTH); print; n = 1
      if ($0 ~ /\{/) { opened = 1; if ($0 ~ /\}[[:space:]]*$/) exit }
      else if ($0 ~ /;[[:space:]]*$/) exit
      next
    }
    {
      if (n >= max) { print ind "    [jules-context: cut at " max " lines]"; exit }
      print; n++
      if (!opened && $0 ~ /\{/) opened = 1
      if (!opened && $0 ~ /;[[:space:]]*$/) exit
      if (substr($0, 1, length(ind) + 1) == ind "}") exit
    }' "$root/$file" > "$tmp/body"
  end=$((line + $(wc -l < "$tmp/body") - 1))
  changed="$(awk -F'\t' -v f="$file" -v a="$line" -v b="$end" \
    '$1 == f && $2 <= b && $2 + $3 - 1 >= a { print "yes"; exit }' "$tmp/hunks")"
  note=''
  [ -z "$changed" ] || note=' (this diff changes it; the base version is shown)'
  block="$(printf -- '--- %s:%s fn %s%s ---\n' "$file" "$line" "$name" "$note"; cat "$tmp/body")"
  size=$(( ${#block} + 2 ))
  if [ $((used + size)) -gt "$MAX_CALLEE_CHARS" ]; then
    printf '%s\n' "$name" >> "$tmp/elided"
    continue
  fi
  printf '%s\n\n' "$block" >> "$callee_out"
  used=$((used + size)) shown=$((shown + 1))
done < "$tmp/defs"

if [ -s "$tmp/elided" ]; then
  printf '[jules-context: %s more functions elided to fit: %s]\n' \
    "$(wc -l < "$tmp/elided" | tr -d ' ')" "$(sort -u "$tmp/elided" | paste -sd, - | sed 's/,/, /g')" >> "$callee_out"
fi
if [ -s "$tmp/ambiguous" ] && [ -s "$callee_out" ]; then
  printf '[jules-context: not shown, more than %s definitions each: %s]\n' \
    "$MAX_DEFS_PER_NAME" "$(sort -u "$tmp/ambiguous" | paste -sd, - | sed 's/,/, /g')" >> "$callee_out"
fi

# ── Surrounding code ──────────────────────────────────────────────────────────────────────────────
# The pull request head's lines around each hunk, so a finding about the next function or the start
# of the same script can be checked rather than guessed. The workflow fetched each changed file as
# data into `$head_dir/<n>`, with `$head_dir/index.tsv` mapping n to its path; no PR path is used as
# a filename here.
if [ -n "$head_dir" ] && [ -r "$head_dir/index.tsv" ]; then
  awk -v pad="$SURROUND_LINES" '
    /^diff --git / { path = ""; next }
    /^\+\+\+ / { path = (substr($0, 5) == "/dev/null") ? "" : substr($0, 7); next }
    /^@@ / && path != "" {
      split($3, n, ","); s = substr(n[1], 2) + 0; l = (n[2] == "" ? 1 : n[2] + 0)
      a = s - pad; if (a < 1) a = 1
      printf "%s\t%d\t%d\n", path, a, s + l - 1 + pad
    }' "$diff_file" > "$tmp/windows"
  used=0 kept=0
  : > "$tmp/surround_elided"
  while IFS=$'\t' read -r n path; do
    [ -r "$head_dir/$n" ] || continue
    block="$(awk -F'\t' -v p="$path" '$1 == p { print $2 "\t" $3 }' "$tmp/windows" | sort -n | awk -F'\t' '
      { if (have && $1 <= e + 1) { if ($2 > e) e = $2 } else { if (have) print s "\t" e; s = $1; e = $2; have = 1 } }
      END { if (have) print s "\t" e }' | while IFS=$'\t' read -r a b; do
        printf '== %s:%d-%d (pull request head)\n' "$path" "$a" "$b"
        awk -v a="$a" -v b="$b" 'NR >= a && NR <= b { printf "%6d  %s\n", NR, $0 }' "$head_dir/$n"
        printf '\n'
      done)"
    [ -n "$block" ] || continue
    size=${#block}
    if [ $((used + size)) -gt "$MAX_SURROUND_CHARS" ]; then
      printf '%s\n' "$path" >> "$tmp/surround_elided"
      continue
    fi
    printf '%s\n' "$block" >> "$surround_out"
    used=$((used + size)) kept=$((kept + 1))
  done < "$head_dir/index.tsv"
  if [ -s "$tmp/surround_elided" ]; then
    printf '[jules-context: surrounding code not shown for %s more files, to fit: %s]\n' \
      "$(wc -l < "$tmp/surround_elided" | tr -d ' ')" "$(paste -sd, "$tmp/surround_elided" | sed 's/,/, /g')" >> "$surround_out"
  fi
fi

echo "jules-context: $(wc -c < "$replies_out" | tr -d ' ') chars of replies; $shown functions, $(wc -c < "$callee_out" | tr -d ' ') chars of callee context; $(wc -c < "$surround_out" | tr -d ' ') chars of surrounding code; $rejected tokens rejected" >&2
