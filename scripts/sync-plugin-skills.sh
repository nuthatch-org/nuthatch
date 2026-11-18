#!/usr/bin/env bash
# The Claude plugin in plugin/ must be self-contained (Anthropic's directory takes one folder), so
# plugin/skills/ is a copy of skills/, which the binary and the other consumers read in place.
# tests/skill_refs.rs fails when the two differ; run this to bring the copy back in line.
set -euo pipefail
cd "$(dirname "$0")/.."
rm -rf plugin/skills
cp -r skills plugin/skills
