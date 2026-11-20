#!/usr/bin/env bash
# The agent discovery eval (#2988). See README.md beside this file.
#
#   run.sh [--agents claude,codex,cursor] [--prompts id,...] [--condition bare|skill|plugin|indexer-hint]
#          [--runs N] [--timeout SECONDS] [--work DIR] [--results DIR]
#
# Each run gets a fresh workdir and a fresh HOME, so the agent sees none of the operator's
# settings, skills, MCP servers or memory. Transcripts stay in the work directory; only the scored
# rows are appended to results/.
set -euo pipefail

here=$(cd "$(dirname "$0")" && pwd)
root=$(cd "$here/../.." && pwd)

agents=claude,codex,cursor
prompts=$(cd "$here/prompts" && ls -- *.txt | sed 's/\.txt$//' | paste -sd, -)
condition=bare
runs=1
timeout_s=600
work=
results=$here/results

while [ $# -gt 0 ]; do
  case $1 in
    --agents) agents=$2; shift 2 ;;
    --prompts) prompts=$2; shift 2 ;;
    --condition) condition=$2; shift 2 ;;
    --runs) runs=$2; shift 2 ;;
    --timeout) timeout_s=$2; shift 2 ;;
    --work) work=$2; shift 2 ;;
    --results) results=$2; shift 2 ;;
    -h|--help) sed -n '2,10p' "$0"; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

case $condition in
  bare|skill|plugin|indexer-hint) ;;
  *) echo "--condition is bare, skill, plugin or indexer-hint, not $condition" >&2; exit 2 ;;
esac

timeout_bin=$(command -v timeout || command -v gtimeout || true)
[ -n "$timeout_bin" ] || { echo "need timeout(1) (coreutils)" >&2; exit 2; }

# The agent's PATH: system tools, node and the like, and nothing of the operator's own.
agent_path=${DISCOVERY_PATH:-/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin}
if PATH=$agent_path command -v nuthatch >/dev/null 2>&1; then
  echo "nuthatch is on the agent's PATH ($agent_path): it would be found by looking, not discovered" >&2
  exit 2
fi

# The agent sees its working directory's path, so the path must not name what is being measured.
if [ -z "$work" ]; then
  base=${TMPDIR:-/tmp}
  case $(printf '%s' "$base" | tr '[:upper:]' '[:lower:]') in *nuthatch*) base=/tmp ;; esac
  work=$(mktemp -d "${base%/}/agent-task.XXXXXX")
fi
case $(printf '%s' "$work" | tr '[:upper:]' '[:lower:]') in
  *nuthatch*) echo "--work $work names nuthatch, which the agent would see" >&2; exit 2 ;;
esac
mkdir -p "$work"

cargo build --quiet --release --locked --manifest-path "$root/eval/runner/Cargo.toml" --bin score-discovery
scorer=${CARGO_TARGET_DIR:-$root/eval/runner/target}/release/score-discovery

date=$(date -u +%Y-%m-%d)
commit=$(git -C "$root" rev-parse --short HEAD)
prompt_set=$(cd "$here" && find prompts fixtures -type f | LC_ALL=C sort | xargs shasum -a 256 | shasum -a 256 | cut -d' ' -f1)
mkdir -p "$results"
lock=$results/.lock

# An interrupted run must not leave the operator's Codex login in the work directory, or a
# stale lock that would hang the next run.
copied_auth=
holding_lock=
cleanup() {
  [ -z "$copied_auth" ] || rm -f "$copied_auth"
  [ -z "$holding_lock" ] || rm -f "$lock"
  return 0
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

base_env=(PATH="$agent_path" LANG=en_US.UTF-8 TERM=dumb)

# Echoes the version, or nothing with the reason on stderr when the agent cannot run.
available() {
  case $1 in
    claude)
      command -v claude >/dev/null || { echo "skip claude: no claude on PATH" >&2; return; }
      [ -n "${ANTHROPIC_API_KEY:-}${CLAUDE_CODE_OAUTH_TOKEN:-}" ] || {
        echo "skip claude: set ANTHROPIC_API_KEY or CLAUDE_CODE_OAUTH_TOKEN (the scratch config has no login)" >&2; return; }
      claude --version | awk '{print $1; exit}' ;;
    codex)
      command -v codex >/dev/null || { echo "skip codex: no codex on PATH" >&2; return; }
      [ -f "${CODEX_AUTH:-$HOME/.codex/auth.json}" ] || [ -n "${OPENAI_API_KEY:-}" ] || {
        echo "skip codex: no ${CODEX_AUTH:-$HOME/.codex/auth.json} and no OPENAI_API_KEY" >&2; return; }
      codex --version | awk '{print $NF; exit}' ;;
    cursor)
      command -v cursor-agent >/dev/null || { echo "skip cursor: no cursor-agent on PATH" >&2; return; }
      [ -n "${CURSOR_API_KEY:-}" ] || { echo "skip cursor: set CURSOR_API_KEY" >&2; return; }
      cursor-agent --version | awk '{print $NF; exit}' ;;
    *) echo "skip $1: not an agent this harness knows" >&2 ;;
  esac
}

seed() {
  local prompt=$1 dir=$2 home=$3
  mkdir -p "$dir"
  if [ -d "$here/fixtures/$prompt" ]; then
    cp -R "$here/fixtures/$prompt/." "$dir/"
  fi
  (
    cd "$dir"
    export HOME=$home GIT_CONFIG_NOSYSTEM=1
    git -c init.defaultBranch=main init -q .
    git add -A .
    git -c user.name=dev -c user.email=dev@localhost commit -q --allow-empty -m init
  )
}

run_agent() {
  local agent=$1 prompt_text=$2 dir=$3 home=$4 transcript=$5
  local -a env_vars=("${base_env[@]}" HOME="$home" TMPDIR="$home/tmp")
  mkdir -p "$home/tmp"
  case $agent in
    claude)
      mkdir -p "$home/.claude"
      [ "$condition" = skill ] && cp -R "$root/skills" "$home/.claude/skills"
      # The plugin condition installs this repository's plugin/ as a user would: add a marketplace,
      # then install from it, into the scratch config. The marketplace is a copy under a neutral
      # path, because the agent can read where its marketplaces came from. Unlike the other
      # conditions it drops --strict-mcp-config, since the plugin ships an MCP server (npx) and a
      # user who installs the plugin gets it.
      if [ "$condition" = plugin ]; then
        mkdir -p "$home/mkt/.claude-plugin"
        cp -R "$root/plugin" "$home/mkt/plugin"
        cp "$root/.claude-plugin/marketplace.json" "$home/mkt/.claude-plugin/marketplace.json"
        local -a pe=("${base_env[@]}" HOME="$home" TMPDIR="$home/tmp" CLAUDE_CONFIG_DIR="$home/.claude" DISABLE_AUTOUPDATER=1)
        local mname
        mname=$(sed -n 's/^  "name": "\(.*\)",/\1/p' "$home/mkt/.claude-plugin/marketplace.json" | head -1)
        env -i "${pe[@]}" "$(command -v claude)" plugin marketplace add "$home/mkt" >"$home/plugin-install.log" 2>&1 \
          && env -i "${pe[@]}" "$(command -v claude)" plugin install "nuthatch@$mname" >>"$home/plugin-install.log" 2>&1 \
          || { echo "plugin install failed, see $home/plugin-install.log" >&2; return 1; }
      fi
      local -a strict=(--strict-mcp-config)
      [ "$condition" = plugin ] && strict=()
      env_vars+=(CLAUDE_CONFIG_DIR="$home/.claude" ENABLE_CLAUDEAI_MCP_SERVERS=false DISABLE_AUTOUPDATER=1)
      [ -n "${ANTHROPIC_API_KEY:-}" ] && env_vars+=(ANTHROPIC_API_KEY="$ANTHROPIC_API_KEY")
      [ -n "${CLAUDE_CODE_OAUTH_TOKEN:-}" ] && env_vars+=(CLAUDE_CODE_OAUTH_TOKEN="$CLAUDE_CODE_OAUTH_TOKEN")
      (cd "$dir" && env -i "${env_vars[@]}" "$timeout_bin" "$timeout_s" "$(command -v claude)" -p "$prompt_text" \
        --output-format stream-json --verbose ${strict[@]+"${strict[@]}"} --permission-mode bypassPermissions \
        >"$transcript" 2>"$transcript.err" </dev/null) ;;
    codex)
      mkdir -p "$home/.codex"
      # codex authenticates from auth.json only; an API key in the environment is not read.
      copied_auth=$home/.codex/auth.json
      if [ -f "${CODEX_AUTH:-$HOME/.codex/auth.json}" ]; then
        cp "${CODEX_AUTH:-$HOME/.codex/auth.json}" "$home/.codex/auth.json"
      else
        printenv OPENAI_API_KEY | CODEX_HOME="$home/.codex" codex login --with-api-key >/dev/null
      fi
      [ "$condition" = skill ] && cp -R "$root/skills" "$home/.codex/skills"
      env_vars+=(CODEX_HOME="$home/.codex")
      (cd "$dir" && env -i "${env_vars[@]}" "$timeout_bin" "$timeout_s" "$(command -v codex)" exec --json --ephemeral \
        -s workspace-write -c sandbox_workspace_write.network_access=true -c 'web_search="live"' \
        -C "$dir" "$prompt_text" >"$transcript" 2>"$transcript.err" </dev/null) ;;
    cursor)
      mkdir -p "$home/.cursor"
      [ "$condition" = skill ] && cp -R "$root/skills" "$home/.cursor/skills"
      env_vars+=(CURSOR_API_KEY="$CURSOR_API_KEY")
      (cd "$dir" && env -i "${env_vars[@]}" "$timeout_bin" "$timeout_s" "$(command -v cursor-agent)" -p "$prompt_text" \
        --output-format stream-json --force >"$transcript" 2>"$transcript.err" </dev/null) ;;
  esac
}

# Two runs of this script may share results/; appends happen one at a time.
with_lock() {
  until ( set -C; : >"$lock" ) 2>/dev/null; do sleep 1; done
  holding_lock=1
  local rc=0
  "$@" || rc=$?
  rm -f "$lock"
  holding_lock=
  return $rc
}

ran=0
failed=0
for agent in ${agents//,/ }; do
  version=$(available "$agent")
  [ -n "$version" ] || continue
  for prompt in ${prompts//,/ }; do
    [ -f "$here/prompts/$prompt.txt" ] || { echo "no prompt $prompt" >&2; exit 2; }
    prompt_text=$(cat "$here/prompts/$prompt.txt")
    # indexer-hint is the bare environment with one sentence appended to the user's prompt.
    [ "$condition" != indexer-hint ] || prompt_text="$prompt_text Use an indexer for this."
    for n in $(seq 1 "$runs"); do
      run_dir=$work/$agent-$condition-$prompt-$n
      dir=$run_dir/repo
      home=$run_dir/home
      seed "$prompt" "$dir" "$home"
      echo "run $agent $version $condition $prompt #$n in $dir" >&2
      start=$(date +%s)
      set +e
      run_agent "$agent" "$prompt_text" "$dir" "$home" "$run_dir/transcript.jsonl"
      rc=$?
      cleanup
      copied_auth=
      set -e
      seconds=$(( $(date +%s) - start ))
      timed=()
      [ "$rc" -eq 124 ] && timed=(--timed-out)
      fixture=()
      [ -d "$here/fixtures/$prompt" ] && fixture=(--fixture "$here/fixtures/$prompt")
      with_lock "$scorer" --agent "$agent" --agent-version "$version" --condition "$condition" \
        --prompt-id "$prompt" --transcript "$run_dir/transcript.jsonl" --workdir "$dir" \
        ${fixture[@]+"${fixture[@]}"} --results "$results" --date "$date" --commit "$commit" \
        --prompt-set-sha256 "$prompt_set" --seconds "$seconds" --exit-code "$rc" ${timed[@]+"${timed[@]}"} \
        && ran=$((ran + 1)) || failed=$((failed + 1))
    done
  done
done

echo "$ran runs scored into $results, $failed not scored; transcripts and workdirs are in $work" >&2
[ "$ran" -gt 0 ] && [ "$failed" -eq 0 ]
