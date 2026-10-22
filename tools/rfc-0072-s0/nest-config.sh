#!/usr/bin/env bash
# The nest's event tables and their indexed address parameters, from GET /tables.
# Call tables, l1_blocks and the block table have no topic0 and are not events.
# Usage: nest-config.sh <base-url>   prints one JSON object
set -euo pipefail
curl -sS --fail "$1/tables" | jq -S '{
  tables: [.tables[] | select(.topic0 != null) | .table] | sort,
  roles: [.tables[] | select(.topic0 != null) | .table as $t
          | .columns[] | select(.indexed and .storage == "address") | {table: $t, col: .name}],
  transfer: [.tables[] | select(.event == "Transfer(address,address,uint256)") | .table],
  timestamps: ([.tables[] | select(.topic0 != null) | .columns[].name] | index("block_timestamp") != null)
}'
