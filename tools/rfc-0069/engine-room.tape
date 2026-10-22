# RFC-0069 S5: the engine room, recorded live. A fresh copy of `defi-pulse-nest` follows mainnet over
# two public RPCs with `--poll-interval 12s` (the default is 300 s), after a 20,000-block seal-direct
# backfill; `nuthatch watch` attaches at its defaults while `traffic.sh` sends it a few queries a
# second. Nothing on screen is synthetic: every figure is a body the nest served as it was drawn.
#
# Run from an empty directory holding a copy of defi-pulse-nest at `./pulse` (`cp -cR`) and this
# directory's `traffic.sh`, with the build to record first on PATH and port 8393 free:
#   vhs path/to/tools/rfc-0069/engine-room.tape
# It writes engine-room.gif and engine-room.mp4; engine-room.txt, the text of each frame, which
# `layout-gate.sh` compares between two recordings; and engine-room.jsonl, every body the watcher read,
# which is where each figure on screen came from.
Output engine-room.gif
Output engine-room.mp4
Output engine-room.txt
Set Shell "bash"
Set FontSize 15
Set Width 1560
Set Height 900
Set Padding 24
Set Framerate 10
Set Theme { "name": "foundry", "background": "#14110d", "foreground": "#e8dfcd", "black": "#3a3227", "red": "#c1503c", "green": "#4d8266", "yellow": "#c9a227", "blue": "#5b91a8", "magenta": "#a4633a", "cyan": "#5f9e7d", "white": "#a2937a", "brightBlack": "#4d4231", "brightRed": "#e86951", "brightGreen": "#5f9e7d", "brightYellow": "#f0d891", "brightBlue": "#7cbcd6", "brightMagenta": "#cd7c48", "brightCyan": "#7fbb9b", "brightWhite": "#e8dfcd", "cursor": "#f0d891", "selection": "#4d4231" }
Env COLORTERM "truecolor"
Hide
Type "nuthatch dev --dir pulse --backfill 20000 --seal-direct --concurrency 2 --poll-interval 12s --listen 127.0.0.1:8393 --rpc https://ethereum-rpc.publicnode.com --rpc https://eth.drpc.org > dev.log 2>&1 &" Enter
Type "until curl -fs 127.0.0.1:8393/health > /dev/null; do sleep 0.2; done; (bash traffic.sh http://127.0.0.1:8393 120 &) ; clear" Enter
Sleep 2s
Show
Type "nuthatch watch --url http://127.0.0.1:8393 --record engine-room.jsonl" Enter
Sleep 90s
Hide
Type "q"
Type "kill %1; wait" Enter
