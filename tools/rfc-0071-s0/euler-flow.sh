#!/usr/bin/env bash
# RFC-0071 S0(a). Prints, for the six Euler attack transactions, every transfer row the spike nest
# holds (from `nuthatch sql`, so what S1 will see), then checks it log for log against the
# transactions' receipts: every Transfer log the receipts carry at an indexed address must be a row
# with the same from, to and exact value, and every row must be such a log.
#
#   euler-flow.sh <nuthatch binary> <kept data dir> <receipts dir>
set -euo pipefail
BIN=$1 DATA=$2 RCPT=$3
NEST=$(cd "$(dirname "$0")/euler-nest" && pwd)
TXS="'0xc310a0affe2169d1f6feec1c63dbc7f7c62a887fa48795d327d4d2da2d6b111d',
     '0x47ac3527d02e6b9631c77fad1cdee7bfa77a8a7bfd4880dccbda5146ace4088f',
     '0x71a908be0bef6174bccc3d493becdfd28395d78898e355d451cb52f7bac38617',
     '0x62bd3d31a7b75c098ccf28bc4d4af8c4a191b4b9e451fab4232258079e8b18c4',
     '0x465a6780145f1efe3ab52f94c006065575712d2003d83d85481f3d110ed131d9',
     '0x3097830e9921e4063d334acb82f6a79374f76f0b1a8f857e89b89bc58df1f311'"
ALIASES=$(grep '^alias' "$NEST/nuthatch.toml" | cut -d'"' -f2)
INDEXED=$(grep '^address' "$NEST/nuthatch.toml" | cut -d'"' -f2 | tr 'A-F' 'a-f')

# weth's ABI names its params src/dst/wad; every other table is from/to/value.
SQL=""
for a in $ALIASES; do
  if [ "$a" = weth ]; then cols='"src" AS f, "dst" AS t, wad AS v'; else cols='"from" AS f, "to" AS t, value AS v'; fi
  part="SELECT '$a' AS token, block_number, tx_hash, log_index, $cols FROM ${a}__transfer WHERE tx_hash IN ($TXS)"
  SQL="${SQL:+$SQL UNION ALL }$part"
done
"$BIN" sql --json --dir "$DATA" "SELECT * FROM ($SQL) ORDER BY block_number, log_index" \
  | jq -r '[.tx_hash, .log_index, .token, .f, .t, .v] | @tsv' > nest.tsv

# Receipt side: Transfer(address,address,uint256) logs, three topics, at an indexed address.
: > receipt.tsv
: > unindexed.tsv
for f in "$RCPT"/*.json; do
  jq -r '.result.logs[]
    | select(.topics[0] == "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef" and (.topics|length) == 3)
    | [.transactionHash, .logIndex, .address, ("0x" + .topics[1][26:]), ("0x" + .topics[2][26:]), .data] | @tsv' "$f" |
  while IFS=$'\t' read -r tx li addr from to data; do
    if echo "$INDEXED" | grep -qx "$addr"; then
      printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$tx" "$((li))" "$addr" "$from" "$to" "$(cast to-dec "$data")" >> receipt.tsv
    else
      printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$tx" "$((li))" "$addr" "$from" "$to" "$(cast to-dec "$data")" >> unindexed.tsv
    fi
  done
done

# Compare on (tx, log_index, from, to, value); the token is checked via the alias->address map.
map() { paste -d' ' <(grep '^alias' "$NEST/nuthatch.toml" | cut -d'"' -f2) <(echo "$INDEXED"); }
awk -F'\t' -v OFS='\t' 'NR==FNR{split($0,a," "); m[a[1]]=a[2]; next} {$3=m[$3]; print}' <(map) nest.tsv | sort > nest.cmp
sort receipt.tsv > receipt.cmp
echo "receipt Transfer logs at indexed addresses: $(wc -l < receipt.cmp | tr -d ' ')"
echo "nest transfer rows for the six txs:         $(wc -l < nest.cmp | tr -d ' ')"
echo "receipt Transfer logs at unindexed addresses: $(wc -l < unindexed.tsv | tr -d ' ')"
if diff receipt.cmp nest.cmp > /dev/null; then echo "IDENTICAL: every indexed Transfer log is a row, exact"; else echo "DIFFER:"; diff receipt.cmp nest.cmp | head -20; fi
