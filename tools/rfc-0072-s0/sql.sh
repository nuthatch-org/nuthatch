#!/usr/bin/env bash
# One /sql call against a running nest. Usage: sql.sh <base-url> <sql>
set -euo pipefail
curl -sS --fail-with-body --get "$1/sql" --data-urlencode "q=$2"
