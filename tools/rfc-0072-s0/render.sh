#!/usr/bin/env bash
# The 28 reports as one self-contained HTML page with no network requests.
# Usage: render.sh <out.html> <intro.html> <title>:<pass-root> ...   (one title:root per nest)
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
out=$1 intro=$2
shift 2
{
  cat <<'HEAD'
<!doctype html>
<html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width, initial-scale=1">
<title>Chain weather S0</title>
<style>
:root { --bg: #fbfaf7; --ink: #1d1c1a; --muted: #5d5a54; --rule: #dedad2; --code: #efece5; --accent: #8a4b2a; }
@media (prefers-color-scheme: dark) { :root:not([data-theme="light"]) {
  --bg: #161514; --ink: #ebe7df; --muted: #a8a299; --rule: #3a3733; --code: #26241f; --accent: #e09a6f; } }
:root[data-theme="dark"] { --bg: #161514; --ink: #ebe7df; --muted: #a8a299; --rule: #3a3733; --code: #26241f; --accent: #e09a6f; }
body { background: var(--bg); color: var(--ink); font: 16px/1.55 -apple-system, "Segoe UI", system-ui, sans-serif;
  max-width: 52rem; margin: 0 auto; padding: 1.5rem 16px 4rem; }
h1 { font-size: 1.6rem; margin: 0 0 .5rem; } h2 { font-size: 1.3rem; margin: 2.5rem 0 .5rem; border-bottom: 1px solid var(--rule); padding-bottom: .3rem; }
h3 { font-size: 1.05rem; margin: 0 0 .2rem; color: var(--accent); }
article { border-top: 1px solid var(--rule); padding: 1rem 0; }
p.note { color: var(--muted); font-size: .85rem; margin: 0 0 .5rem; }
p.lead { margin: .2rem 0 .4rem; font-weight: 600; }
ul { margin: 0; padding-left: 1.2rem; } li { margin: .25rem 0; }
code { background: var(--code); padding: 0 .25rem; border-radius: 3px; font-size: .85em; overflow-wrap: anywhere; }
nav a, a { color: var(--accent); } nav { font-size: .9rem; } .intro p, .intro li { color: var(--ink); }
</style></head><body>
HEAD
  cat "$intro"
  for spec in "$@"; do
    title=${spec%%:*} root=${spec#*:}
    nest=$(jq -r .nest "$(ls "$root"/*/facts.json | head -1)")
    echo "<h2 id=\"$nest\">$title</h2><nav>"
    for f in "$root"/*/facts.json; do d=$(jq -r .day "$f"); echo "<a href=\"#$nest-$d\">$d</a>"; done
    echo "</nav>"
    jq -s -r -f "$here/render.jq" "$root"/*/facts.json
  done
  echo "</body></html>"
} >"$out"
