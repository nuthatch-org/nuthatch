# ThinkPad unattended mode (#3034)

Everything installs into `$HOME` of `grokbot` only. No secrets live in this folder.

## Setup
    ops/thinkpad/setup.sh          # idempotent; re-run after edits
    tp-status

Installs: `~/.config/systemd/user/{tp-worker@.service,tp-health.service,tp-health.timer}`,
`~/bin/{tp-worker-run,tp-health-check,gh-preflight,tp-status}`, `~/.config/tp-health/units`
(expected units, one per line), `~/orchestra/workers/`, and placeholder comment lines for
`GRAPHOPS_TOKEN`, `CLAUDE_WAKE_URL`, `CLAUDE_WAKE_KEY` in `~/.config/nh/env`.

## Workers
`tp-worker@<name>.service` runs `~/orchestra/workers/<name>.sh` (executable) under systemd with
`Restart=on-failure`, sourcing `~/.config/nh/env` and `PATH=$HOME/.npm-global/bin:$HOME/.cargo/bin:$PATH`.
`tp-worker@test` is a built-in idle worker proving supervision. Add a worker: create the script,
`systemctl --user enable --now tp-worker@<name>`, add the unit to `~/.config/tp-health/units`.
The orchestrator's ad hoc `ssh thinkpad` + tmux + `claude -p` flow is untouched (paths unchanged);
no tmux server unit is installed, deliberately, because restarting one would kill live sessions.

## Health check (every 5 min)
Checks tailscaled, disk >15% free, expected units, `graphops auth status --check`. State in
`~/.local/state/tp-health/` (`failures`, `last`, `history`). It POSTs JSON to `$CLAUDE_WAKE_URL`
(`Authorization: Bearer $CLAUDE_WAKE_KEY`; adjust if the webhook expects another header) **only when the
failure set changes** (including recovery). Failure tokens are stable (`tailscaled`, `disk-low`,
`unit:<name>`, `graphops-auth`); the free-disk percentage goes only in the POST body (`disk_free_pct`).
- `failures` is the set last announced. It advances **only after a successful POST**, so a failed POST
  is retried on the next run.
- If `CLAUDE_WAKE_URL` is unset, nothing is sent and `failures` does not advance. The current failure set
  is therefore sent on the first run after the URL is set.
- `last` is a snapshot of the most recent run for `tp-status`; nothing uses it for dedupe.

Test without touching services: `TP_HEALTH_FAKE_FAIL=tailscaled tp-health-check`.

## GraphOps
`GRAPHOPS_TOKEN` in the env file is used if set; otherwise the stored interactive sign-in. The service
account needs read-only: usage read and credits read; no API-key management, no write scopes.

## GitHub token for outreach
Fine-grained `GH_TOKEN` needs Issues: read and write on public repos and Contents: write on
`nuthatch-org/gtm`. `gh-preflight` makes read-only calls only. Fine-grained tokens do not expose their
grants. The check it prints as "push permission" reads the repo `permissions.push` flag for the token's
identity; that is a repo role, not an Issues grant. It is a necessary signal only, and Issues write is
**not verifiable** here (a definitive test needs a write, which it refuses to do).

## Rollback
`ops/thinkpad/setup.sh --uninstall` stops and disables the timer and every `tp-worker@` instance (loaded ones, and those listed in `~/.config/tp-health/units`), then removes the units and scripts (env file and state kept).
`loginctl disable-linger grokbot` (root) reverts linger.

## Needs root (Pete)
`sudo loginctl enable-linger grokbot`. Without it, units start at login, not at boot, so the
"after a reboot" acceptance item is NOT met yet.
