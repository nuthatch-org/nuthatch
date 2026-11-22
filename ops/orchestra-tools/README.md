# Orchestra token-diet tools

Cut how many bytes Grok Bot has to read. Python 3 stdlib only; no install, run in place.

## squeeze

```
some-cmd 2>&1 | ~/orchestra/tools/squeeze
~/orchestra/tools/squeeze -- cargo build        # runs cmd, stderr merged, exit code preserved
SQUEEZE_MAX=80 ~/orchestra/tools/squeeze -- gh pr checks 3071
```

Strips ANSI, progress bars, spinners and `\r` redraws; collapses runs (3+) of near-identical lines
(digits/hashes normalised) to `line  [xN, last: ...]`; always keeps every line matching
error/warn/panic/fail/denied/fatal/exception/traceback/403/404 (never collapsed, never dropped, even past the cap)
plus 3 lines of context, the first 20 and last 40 lines; soft cap `SQUEEZE_MAX` (default 150) on content lines.
Redacts `ghp_/gho_/ghu_/ghs_/ghr_`, `github_pat_`, `sk-`, `gqk_`/`gqk-` keys, `Bearer ...`, `Authorization:`/`X-API-Key:` values.
Last line: `[squeeze] lines in=N out=M exit=C` (`exit=-` in stdin mode). Wrapper exits with the command's code (127 if not found).

## nh-delta

```
~/orchestra/tools/nh-delta                      # digest since last run, then advance state
~/orchestra/tools/nh-delta --dry                # same, do not advance state
~/orchestra/tools/nh-delta --since 2026-11-19T00:00:00Z --dry   # override window (ETag cache bypassed)
~/orchestra/tools/nh-delta --json
```

State: `~/.cache/nh-delta/state.json` (last run time, ETags, last-seen state/CI of open items; `NH_DELTA_STATE_DIR` overrides).
First run with no state looks back 24h. Uses `gh` with `GH_TOKEN` from the environment.

Sections: `nuthatch-org` repos (new/updated issues and PRs: opened, closed/merged, new comments, review state, CI flips;
per-repo conditional requests with `If-None-Match` so unchanged repos cost one 304);
bot-authored items (dependabot, github-actions, nuthatch-jules) collapsed to one `bots:` line;
outside repos (`commenter:cargopete` / `author:cargopete`, `-org:nuthatch-org`; only new comments from others, bots excluded);
notifications. Each unavailable source (403/422) becomes a single `note:` line. Output is capped at `NH_DELTA_MAX_LINES` (60);
empty: `nothing new since <time>`.
Other env: `NH_DELTA_ORG` (nuthatch-org), `NH_DELTA_USER` (cargopete).
Notes: comments by cargopete himself are omitted; a large backlog costs ~3 API calls per open/changed item (about 70 s for 60 items).

## Tests

```
cd ~/orchestra/tools/tests && python3 -m unittest -v test_squeeze test_nh_delta
python3 make_fixtures.py   # regenerate the synthetic fixture logs
```
nh-delta tests run offline against a fake `gh` (`tests/fake_gh.py`).

## RTK evaluation (rtk-ai/rtk v1.2.1)
Not used as the engine. Apache-2.0 CLI but it links a BUSL-1.1 engine; installs fine without root (musl tarball), yet it is
per-command wrappers (`rtk cargo/gh/git/npm/log`), has no journalctl filter, no stdin-compress-with-guaranteed-error-keeping
or secret redaction, and nags about hooks. `squeeze` is a generic stdin/wrapper compressor with those guarantees.
