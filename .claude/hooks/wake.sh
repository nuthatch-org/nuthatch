#!/usr/bin/env bash
# Notifies the orchestrator when a Claude Code cloud session stops, fails or needs input.
# No-op unless GROK_WAKE_URL is set (only in the claude.ai cloud environment). Sends IDs only.
set -uo pipefail
[ -z "${GROK_WAKE_URL:-}" ] && exit 0
command -v jq >/dev/null || exit 0
in=$(cat)
sid=$(jq -r '.session_id // "?"' <<<"$in")
br=$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "?")
sha=$(git rev-parse --short HEAD 2>/dev/null || echo "?")
case "$br" in agent/*) ;; *) exit 0;; esac
curl -fsS --max-time 5 -X POST "$GROK_WAKE_URL" \
  -H "Authorization: Bearer ${GROK_WAKE_KEY:-}" -H 'Content-Type: application/json' \
  -d "$(jq -nc --arg e "$1" --arg s "$sid" --arg b "$br" --arg h "$sha" '{event:$e,session:$s,branch:$b,sha:$h}')" >/dev/null || true
exit 0
