#!/usr/bin/env bash
# Build plugin-directory/, the edition of the Claude plugin that Anthropic's plugin directory takes
# from an organisation that is not Verified (#3013). That review refuses a local stdio MCP server and
# any file that is not Markdown, text, JSON, YAML or a PNG/JPEG/GIF/WebP/ICO image, so this edition:
#   - has no local MCP server; mcpServers points at the public read-only demo nest;
#   - holds the skills of plugin/skills, with source fixtures (.ts, .graphql) rewritten as .md files
#     that show the source in a fenced block;
#   - names a package manager, cargo or a verified release download instead of `curl | sh`.
# plugin/ (Claude Code marketplace, Cursor) is not touched.
#
#   scripts/build-directory-plugin.sh          # regenerate plugin-directory/
#   scripts/build-directory-plugin.sh --check  # fail if plugin-directory/ differs or has a bad file type
set -euo pipefail
cd "$(dirname "$0")/.."

out=plugin-directory
check=0
[ "${1:-}" = "--check" ] && { check=1; out=$(mktemp -d)/plugin-directory; }

release=$(sed -n 's/^version = "\([^"]*\)".*/\1/p' Cargo.toml | head -1)
[ -n "$release" ] || { echo "no version in Cargo.toml" >&2; exit 2; }
# A -dev version is published nowhere: npm has no such package, so an `npx` pin or a listing version
# naming it would fail for a user. Until the release is cut, the edition names the last published
# release, which server.json records (the same source scripts/version-check.sh uses).
case "$release" in
  *-dev)
    release=$(sed -n 's/^  "version": "\([^"]*\)".*/\1/p' server.json | head -1)
    [ -n "$release" ] || { echo "no version in server.json" >&2; exit 2; } ;;
esac
# The listing's version is the release's, but never below listing_floor. The directory refused the
# 5.24.0 edition on 2026-11-27 and refuses the same version string again, so this edition (with the
# fixes for that refusal) is listed as 5.24.1 while Cargo.toml still says 5.24.0. Once a release passes
# the floor its own version is used again. If a listing version is refused, raise the floor past it.
listing_floor=5.24.1
# Python, not GNU sort -V: this script already needs python3, and stock macOS sort has no -V.
version=$(python3 -I -c '
import re, sys
def key(v):
    out = []
    for p in re.split(r"([^0-9]+)", v):
        if not p:
            continue
        out.append((0, int(p)) if p.isdigit() else (1, p))
    return out
a, b = sys.argv[1], sys.argv[2]
print(a if key(a) >= key(b) else b)
' "$release" "$listing_floor")

rm -rf "$out"
mkdir -p "$out/.claude-plugin"
cp -r plugin/skills "$out/skills"
cp plugin/.claude-plugin/icon.png "$out/.claude-plugin/icon.png"
# The directory wants a licence in the plugin folder; the core is MIT OR Apache-2.0, as the repo root.
{ printf 'nuthatch is licensed under either of\n\n'
  printf '  * Apache License, Version 2.0 (below, and LICENSE-APACHE in the repository)\n'
  printf '  * MIT License (below, and LICENSE-MIT in the repository)\n\n'
  printf 'at your option. SPDX-License-Identifier: MIT OR Apache-2.0\n\n'
  printf -- '-----------------------------------------------------------------------------\n\n'
  cat LICENSE-MIT
  printf '\n-----------------------------------------------------------------------------\n\n'
  cat LICENSE-APACHE; } > "$out/LICENSE"

# Source files the directory refuses become Markdown that shows them.
while IFS= read -r f; do
  lang=${f##*.}
  [ "$lang" = ts ] && lang=typescript
  { printf 'Source file `%s`, shown as Markdown because the plugin directory takes no source files.\n\n' "${f#"$out"/skills/}"
    printf '```%s\n' "$lang"; cat "$f"; printf '```\n'; } > "$f.md"
  rm "$f"
done < <(find "$out/skills" -type f \( -name '*.ts' -o -name '*.graphql' \) | LC_ALL=C sort)

# No download-and-execute: the install paths in docs/install.md instead.
python3 -I - "$out/skills/evm-indexing/SKILL.md" <<'PY'
import sys
p = sys.argv[1]
s = open(p).read()
old = ("To install instead of using `npx`: `curl -fsSL https://nuthatch-indexer.com/install.sh | sh`, then\n"
       "`export PATH=\"$HOME/.local/bin:$PATH\"`\n"
       "and drop the `npx -y nuthatch-indexer` prefix.\n")
new = ("To install instead of using `npx`: download the release archive for your platform from\n"
       "https://github.com/nuthatch-org/nuthatch/releases, check it against the `.sha256` file beside it\n"
       "(and with `gh attestation verify nuthatch-<target>.tar.gz --repo nuthatch-org/nuthatch`, as https://github.com/nuthatch-org/nuthatch/blob/main/docs/install.md describes), unpack it and put\n"
       "`nuthatch` on your `PATH`. Or build it with `rustup toolchain install 1.95.0` and\n"
       "`cargo +1.95.0 install --git https://github.com/nuthatch-org/nuthatch nuthatch`. Then drop the\n"
       "`npx -y nuthatch-indexer` prefix.\n")
assert old in s, "install line not found in evm-indexing/SKILL.md"
open(p, "w").write(s.replace(old, new))
PY

# A package launcher runs one exact version, the release this edition was built from, not whatever
# npm serves that day. Python, not sed -i: BSD sed requires a suffix after -i.
python3 -I - "$out/skills" "$release" <<'PY'
import os, re, sys
root, release = sys.argv[1], sys.argv[2]
pat = re.compile(r"npx -y nuthatch-indexer([^@A-Za-z0-9_-])")
repl = rf"npx -y nuthatch-indexer@{release}\1"
for dirpath, _, files in os.walk(root):
    for name in files:
        path = os.path.join(dirpath, name)
        try:
            text = open(path, encoding="utf-8").read()
        except (OSError, UnicodeDecodeError):
            continue
        new, n = pat.subn(repl, text)
        if n:
            open(path, "w", encoding="utf-8").write(new)
PY

cat > "$out/.claude-plugin/plugin.json" <<JSON
{
  "name": "nuthatch",
  "displayName": "nuthatch",
  "version": "$version",
  "description": "Skills for indexing an EVM contract's events into a local SQL database with nuthatch, a self-hosted indexer, and for porting a subgraph to it. The bundled MCP server is a public read-only demo nest on Arc testnet; run nuthatch locally to index your own contracts.",
  "author": {
    "name": "nuthatch",
    "url": "https://www.nuthatch-indexer.com"
  },
  "homepage": "https://www.nuthatch-indexer.com",
  "repository": "https://github.com/nuthatch-org/nuthatch",
  "icon": "./.claude-plugin/icon.png",
  "documentationUrl": "https://nuthatch-indexer.com/docs",
  "supportUrl": "https://github.com/nuthatch-org/nuthatch/issues",
  "privacyPolicyUrl": "https://nuthatch-indexer.com/privacy",
  "license": "MIT OR Apache-2.0",
  "keywords": ["evm", "ethereum", "indexer", "sql", "mcp", "subgraph", "nuthatch"],
  "mcpServers": {
    "nuthatch-demo": {
      "type": "http",
      "url": "https://arc.nuthatch-indexer.com/mcp"
    }
  }
}
JSON

# The README is the listing's description, and the directory's scan compares what the plugin does with
# what it says: name everything the plugin runs, sends or fetches.
cat > "$out/README.md" <<MD
# nuthatch (plugin directory edition)

[nuthatch](https://www.nuthatch-indexer.com) is a self-hosted, open-source indexer for EVM
blockchains. It reads a smart contract's public onchain events (logs), decodes them with the
contract's ABI, and keeps them in a local SQL database on your machine that you can query. It is
read-only: it never holds keys, signs or sends transactions, or moves funds.

This plugin gives Claude two things:

- **Skills** that teach Claude to use nuthatch: \`evm-indexing\` (index a contract and query it),
  \`nuthatch-builder\` (the CLI, config and SQL views of a nuthatch project) and
  \`nuthatch-subgraph-port\` (move a subgraph to nuthatch).
- **An MCP server, \`nuthatch-demo\`**, which is a **public, read-only demo**: one nest indexing USDC
  on Arc testnet, at https://arc.nuthatch-indexer.com/mcp. Its tools (\`status\`, \`schema\`,
  \`tables\`, \`sql\`, \`balance\`, \`top_balances\` and others) only read that public demo data. It is
  not your data, and it cannot index anything new.

## How to use it

Add the plugin, then ask Claude in plain words. With the demo server:

- "Using nuthatch-demo, how many USDC transfers has the Arc testnet demo indexed, and up to which block?"
- "Who are the ten largest USDC holders on the nuthatch demo nest?"
- "Show me the five most recent USDC transfers on the demo, with sender, receiver and amount."

With the skills, to index your own contract on your own machine:

- "Use nuthatch to index the USDC contract on Ethereum mainnet locally and count its transfers."
- "Port my subgraph to nuthatch and check the entities still answer."

For your own contracts, the skills show how to install nuthatch (a pinned \`npx\` package, a
verified release download, or \`cargo install\`) and run it locally; then point Claude at your local
\`nuthatch mcp\` instead of the demo. Documentation: https://nuthatch-indexer.com/docs

## Data and external services

**The demo MCP server** is the one remote service this plugin connects to, and we run it:

- **https://arc.nuthatch-indexer.com/mcp**, named in \`.claude-plugin/plugin.json\`. Claude sends it
  each MCP call, the tool name and its arguments (including any SQL), over HTTPS, with ordinary HTTP
  headers. Our proxy access log keeps the caller's IP address and the request line, not the POST body.
  Details: https://nuthatch-indexer.com/privacy

**When a skill runs nuthatch on your machine**, at your request, these are what it fetches, and
from where. None of it is sent to us:

- \`npx -y nuthatch-indexer@$release\` downloads that npm package from the npm registry, and on first
  run the matching nuthatch release binary from https://github.com/nuthatch-org/nuthatch/releases,
  checked against the checksums in the package.
- \`nuthatch init\` fetches the contract's ABI from Sourcify (https://sourcify.dev), or from Etherscan
  only if you set \`ETHERSCAN_API_KEY\`. Porting a subgraph fetches its files from IPFS.
- Indexing reads blocks and logs from the chain's JSON-RPC endpoint: a public keyless one by default,
  written into the nest's config, or your own.
- Your indexed data stays in a local directory. \`nuthatch mcp\` on your machine talks to that local
  nest and sends nothing to us. The binary sends us nothing unless you opt in to its anonymous head
  count, which is off until you say yes.

The plugin has no hooks, no scripts and no local MCP server. The Claude Code marketplace edition in
\`plugin/\` of the repository runs a local MCP server instead of the demo.

## Support and license

Questions and bugs: https://github.com/nuthatch-org/nuthatch/issues. Licensed MIT OR Apache-2.0; see
\`LICENSE\`. This folder is generated by \`scripts/build-directory-plugin.sh\` from \`plugin/\`. Do not
edit it by hand.
MD

if [ "$check" = 1 ]; then
  diff -r "$out" plugin-directory || { echo "plugin-directory/ is stale: run scripts/build-directory-plugin.sh and commit plugin-directory/ (also needed after a Cargo.toml version bump; docs/releases/README.md)" >&2; exit 1; }
  dir=plugin-directory
else
  dir=$out
fi

# LICENSE is the one name without an extension: the directory asks for that file, and it is plain text.
bad=$(find "$dir" -type f ! \( -path "$dir/LICENSE" -o -iname '*.md' -o -iname '*.txt' -o -iname '*.json' -o -iname '*.yaml' -o -iname '*.yml' \
  -o -iname '*.png' -o -iname '*.jpg' -o -iname '*.jpeg' -o -iname '*.gif' -o -iname '*.webp' -o -iname '*.ico' \))
[ -z "$bad" ] || { echo "file types the plugin directory refuses:" >&2; echo "$bad" >&2; exit 1; }
# Piping a download into a shell, or running its substitution: `| sh`, `| sudo bash`, `| /bin/sh`, `sh -c "$(curl`, `<(curl`, `eval "$(curl`, `curl | tee f | sh`.
# A download written to a file and run on a later line is not caught: this is a tripwire, review still reads the diff.
# The shell name ends at a word boundary, so `| shasum` is fine.
dl='(curl|wget)'
shell='(/[a-z/]*/)?(ba|z|da|k)?sh([^[:alnum:]_.-]|$)'
gate="$dl[^|]*(\\|[^|]*)*\\|[[:space:]]*(sudo[[:space:]]+(-[A-Za-z-]+[[:space:]]+)*)?$shell"
gate="$gate|(ba|z|da|k)?sh[[:space:]]+-c[[:space:]]+[\"']?\\\$\\([[:space:]]*$dl|<\\([[:space:]]*$dl|eval[[:space:]]+[\"']?\\\$\\([[:space:]]*$dl"
if grep -rEn "$gate" "$dir" >&2; then
  echo "download-and-execute pattern in plugin-directory/" >&2; exit 1
fi
echo "plugin-directory ok (version $version, release $release)"
