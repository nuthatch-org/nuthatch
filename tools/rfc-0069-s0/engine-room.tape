# RFC-0069 S0: the clip for Chief. `nuthatch watch` replaying capture.jsonl, 300 s of a live
# defi-pulse nest on mainnet (`dev --backfill 4000 --seal-direct --poll-interval 12s`, two public
# RPCs), at 5x. Run from the directory holding capture.jsonl, with the watch build first on PATH.
Output engine-room.gif
Output engine-room.mp4
Set Shell "bash"
Set FontSize 15
Set Width 1560
Set Height 480
Set Padding 24
Set Theme { "name": "foundry", "background": "#14110d", "foreground": "#e8dfcd", "black": "#3a3227", "red": "#c1503c", "green": "#4d8266", "yellow": "#c9a227", "blue": "#5b91a8", "magenta": "#a4633a", "cyan": "#5f9e7d", "white": "#a2937a", "brightBlack": "#4d4231", "brightRed": "#e86951", "brightGreen": "#5f9e7d", "brightYellow": "#f0d891", "brightBlue": "#7cbcd6", "brightMagenta": "#cd7c48", "brightCyan": "#7fbb9b", "brightWhite": "#e8dfcd", "cursor": "#f0d891", "selection": "#4d4231" }
Env COLORTERM "truecolor"
Hide
Type "clear; nuthatch watch --replay capture.jsonl --speed 5" Enter
Sleep 300ms
Show
Sleep 61s
Hide
Type "q"
