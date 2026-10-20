#!/usr/bin/env bash
# Print `<chain>\t<rpc url>` for every endpoint `src/chains.rs` actually ships.
#
# `live-endpoints.yml` probes what this prints. It used to carry its own copy of the list, and a copy
# goes stale in both directions: on 2026-09-07 (#1196) the workflow was still probing three endpoints
# that had been removed from the registry weeks earlier, and probing none of the five that BSC,
# Optimism and Gnosis ship. One of those five had been unable to serve a backfill for at least
# nineteen days with nothing to notice.
#
# Parsing Rust with a regex is a small sin, and the alternative - a `--list-endpoints` flag - is
# product surface added for CI's benefit. The shape here is fixed by `Chain`'s definition, and the
# workflow fails loudly if a chain yields no endpoints, so a parser that breaks reds the job rather
# than quietly judging nothing.
set -euo pipefail

exec awk '
# A line comment, and not the `//` inside `https://`.
function strip_comment(s,    i) {
  for (i = 1; i < length(s); i++)
    if (substr(s, i, 2) == "//" && (i == 1 || substr(s, i - 1, 1) != ":")) return substr(s, 1, i - 1)
  return s
}
function fail(msg) { print msg > "/dev/stderr"; failed = 1; exit 1 }
function chain_end(    name, urls, n, lines, i, line, found) {
  if (!match(body, /name:[[:space:]]*"[^"]+"/)) fail("cannot read the Chain block at line " start " in src/chains.rs: no name. Teach this script that shape rather than letting the chain drop out of the probe list.")
  name = substr(body, RSTART, RLENGTH); sub(/^name:[[:space:]]*"/, "", name); sub(/"$/, "", name)
  # A `Chain` block this cannot read is a hard failure, never a skip. Skipping is the shape of bug
  # this exists to remove: the chain vanishes from the probe list *and* from the coverage gate.
  if (!match(body, /rpc_urls:[[:space:]]*&[[][^]]*[]]/)) fail("cannot read the Chain block at line " start " in src/chains.rs: no rpc_urls. Teach this script that shape rather than letting the chain drop out of the probe list.")
  urls = substr(body, RSTART, RLENGTH)
  n = split(urls, lines, "\n")
  found = 0
  for (i = 1; i <= n; i++) {
    line = strip_comment(lines[i])
    while (match(line, /"https:\/\/[^"]+"/)) {
      print name "\t" substr(line, RSTART + 1, RLENGTH - 2)
      found++
      line = substr(line, RSTART + RLENGTH)
    }
  }
  if (found == 0) fail(name " declares rpc_urls and this script read none of them")
  rows += found
}
BEGIN {
  # Prove the comment stripper leaves URLs alone, on every run.
  u = "        \"https://eth-pokt.nodies.app\","
  if (strip_comment(u) != u) fail("the comment stripper ate a URL")
  if (strip_comment("\"https://x.example\", // a spare") != "\"https://x.example\", ") fail("the comment stripper missed a comment")
  if (strip_comment("// removed: https://gone.example") != "") fail("the comment stripper missed a whole-line comment")
}
inblock && /^[}];/ { chain_end(); chains++; inblock = 0; next }
inblock { body = body "\n" $0; next }
match($0, /const[[:space:]]+[A-Za-z0-9_]+:[[:space:]]*Chain[[:space:]]*=[[:space:]]*Chain[[:space:]]*[{]/) {
  inblock = 1; start = NR; body = substr($0, RSTART + RLENGTH)
}
END {
  if (failed) exit 1
  if (chains == 0 || rows == 0) { print "no chains extracted from src/chains.rs" > "/dev/stderr"; exit 1 }
}
' src/chains.rs
