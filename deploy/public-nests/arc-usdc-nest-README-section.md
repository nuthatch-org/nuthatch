## Query it without installing anything

A live, read-only copy of this nest runs at **<PUBLIC_URL>**: Arc testnet, following the tip, no key, no
sign-up. It is the same nest as above on someone else's machine, so every query in this README works
against it. It is rate-limited per address (30 queries a minute) and each query is cut off at 10 seconds
and 5,000 rows.

```sh
# the five newest USDC transfers
curl -sG "<PUBLIC_URL>/sql" --data-urlencode \
  'q=SELECT block_number, tx_hash, "from", "to", value_dec FROM usdc__transfer ORDER BY block_number DESC, log_index DESC LIMIT 5'

# native USDC moved per hour, from the system emitter
curl -sG "<PUBLIC_URL>/sql" --data-urlencode \
  'q=SELECT hour, movements, mints, burns, value_raw / 1e18 AS usdc FROM movements_hourly ORDER BY hour DESC LIMIT 3'

# how USDC moves: through the ERC-20 interface, or natively
curl -sG "<PUBLIC_URL>/sql" --data-urlencode \
  "q=SELECT 'via ERC-20' AS path, count(*) AS movements FROM usdc__transfer WHERE value_dec > 0 UNION ALL SELECT 'native only', count(*) FROM native_only"
```

Every answer says how fresh it is: `provenance.as_of` is the block it was computed at, and `GET <PUBLIC_URL>/ready`
reports `lag_blocks` behind the Arc tip.

For an agent, the same nest speaks MCP over HTTP. With Claude Code:

```sh
claude mcp add --transport http arc-usdc <PUBLIC_URL>/mcp
```

or check what it offers first:

```sh
curl -s <PUBLIC_URL>/mcp -H 'content-type: application/json' -H 'accept: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

It serves reads only: `SELECT` and `WITH`, nothing that writes, and no admin routes.
