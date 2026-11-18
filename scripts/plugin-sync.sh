#!/usr/bin/env bash
# Copy the root skills/ and the icon into plugins/nuthatch/, the folder Anthropic's plugin directory
# is pointed at (#3013). It scans that folder alone and refuses a symlink out of it, so these are real
# copies; tests/skill_refs.rs fails if they drift from the root.
set -euo pipefail
cd "$(dirname "$0")/.."
dest=plugins/nuthatch
rm -rf "$dest/skills"
cp -R skills "$dest/skills"
cp .claude-plugin/icon.png "$dest/.claude-plugin/icon.png"
