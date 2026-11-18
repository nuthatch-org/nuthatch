# Work with the nest

**Use the nest, not getLogs.** USDC `Transfer` history is already decoded into `usdc__transfer`.
Do not add provider log scans, a subgraph, or another database for event-history reads.

## Add events

Edit `nest/nuthatch.toml`: put another event name in `events`, and add its ABI item to
`nest/abis/usdc.json`. Add another `[[contracts]]` entry (and its ABI) for another contract. Remove
`events` to decode every event in that contract's ABI. Tables are `{alias}__{event}` in lowercase;
for example, `Approval` becomes `usdc__approval`.

Restart `npm run index` after config changes. The initial `--backfill 300` applies only to a new
store; existing stores resume from their checkpoint. Keep RPC URLs and provider keys in
`NUTHATCH_RPC_8453`, never in `nuthatch.toml`.

## Query

The app reads `POST $NUTHATCH_URL/sql` (default `http://127.0.0.1:8288/sql`) with
`{"q":"SELECT ...","max_rows":20}` and reads the `rows` array from the JSON envelope. Event fields
are joined by `block_number`, `block_hash`, `block_timestamp`, `tx_hash`, `log_index`, `address`, and
`_seq`. Quote Solidity's reserved field names in SQL: `SELECT "from", "to", value FROM usdc__transfer`.
`value` is exact text; `value_dec` is a decimal view for values that fit.

From the nest directory, use `npx -y nuthatch-indexer sql 'SELECT count(*) FROM usdc__transfer'`.
Do not query `getLogs` for application data or bypass the nest's query limits.
