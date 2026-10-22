#!/usr/bin/env bash
#
# RFC-0069 S5 gate: two recordings of engine-room.tape, each against its own fresh nest, end on the
# same screen layout. Takes the last text frame vhs wrote for each (engine-room.txt), keeps the panel
# borders, the panel titles and the row labels, blanks every reading, and compares.
#
# usage: layout-gate.sh <first engine-room.txt> <second engine-room.txt>
set -euo pipefail

layout() {
  # The frames are separated by a line of box-drawing dashes alone; the last frame is the screen as the
  # tape left it. Labels and titles are the capitals right after a border; readings are not.
  awk '/^─+$/ { if (buf != "") last = buf; buf = ""; next } { buf = buf $0 "\n" } END { if (buf != "") last = buf; printf "%s", last }' "$1" |
    perl -CSD -Mutf8 -pe '
      my @keep;
      while (/(?<=[│┌] )([A-Z][A-Z ]*[A-Z]|[A-Z])(?=\s{2,}|\s[─]|$)/g) { push @keep, [$-[1], $1] }
      my $line = $_;
      $line =~ s/[^┌┐└┘│─\n]/ /g;
      substr($line, $_->[0], length $_->[1]) = $_->[1] for @keep;
      $_ = $line;
    ' | sed -e 's/[[:space:]]*$//'
}

a="$(mktemp)"; b="$(mktemp)"
trap 'rm -f "$a" "$b"' EXIT
layout "$1" > "$a"
layout "$2" > "$b"
lines="$(grep -c . "$a" || true)"
labels="$(grep -oE '[A-Z]{3,}( [A-Z]{3,})?' "$a" | wc -l | tr -d ' ')"
if [ "$lines" -lt 20 ] || [ "$labels" -lt 15 ]; then
  echo "FAIL: the last frame of $1 holds $lines lines and $labels labels; no engine room was drawn"
  exit 1
fi
if diff -u "$a" "$b"; then
  echo "PASS: the same layout, $lines lines and $labels labels"
else
  echo "FAIL: the layouts differ"
  exit 1
fi
