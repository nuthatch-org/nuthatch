#!/bin/bash
# Idempotent. Installs into $HOME only. Usage: ops/thinkpad/setup.sh [--uninstall]
set -eu
here=$(cd "$(dirname "$0")" && pwd)
U=~/.config/systemd/user
if [ "${1:-}" = --uninstall ]; then
  systemctl --user disable --now tp-health.timer 'tp-worker@test.service' 2>/dev/null || true
  rm -f "$U"/tp-worker@.service "$U"/tp-health.service "$U"/tp-health.timer \
        ~/bin/tp-worker-run ~/bin/tp-health-check ~/bin/gh-preflight ~/bin/tp-status
  systemctl --user daemon-reload; echo "removed (state in ~/.local/state/tp-health kept)"; exit 0
fi
mkdir -p "$U" ~/bin ~/orchestra/workers ~/.config/tp-health ~/.local/state/tp-health
install -m644 "$here"/units/* "$U"/
for f in tp-worker-run tp-health-check gh-preflight tp-status; do install -m755 "$here/bin-$f" ~/bin/$f; done
[ -f ~/.config/tp-health/units ] || printf '# expected units, one per line\ntp-worker@test.service\n' > ~/.config/tp-health/units
touch ~/.config/nh/env; chmod 600 ~/.config/nh/env
for k in GRAPHOPS_TOKEN CLAUDE_WAKE_URL CLAUDE_WAKE_KEY; do
  grep -q "^[# ]*export $k=" ~/.config/nh/env || echo "# export $k=" >> ~/.config/nh/env
done
systemctl --user daemon-reload
systemctl --user enable --now tp-health.timer tp-worker@test.service
loginctl enable-linger "$USER" 2>/dev/null || echo "NOTE: enable-linger refused; ask root: sudo loginctl enable-linger $USER"
echo "installed; run tp-status"
