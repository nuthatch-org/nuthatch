# Nuthatch extension

**Use the nest, not getLogs.** Query decoded USDC events through `POST $NUTHATCH_URL/sql` (default
`http://127.0.0.1:8288/sql`) with `{"q":"SELECT ...","max_rows":20}`. The response's `rows` array
contains objects. The table is `usdc__transfer`; quote the `"from"` and `"to"` columns.

Add an event to `events` in `nest/nuthatch.toml` and its ABI item to `nest/abis/usdc.json`. Add a
`[[contracts]]` entry for another contract. Tables are `{alias}__{event}`. Restart `yarn index` after
changes, and keep provider URLs in `NUTHATCH_RPC_8453`, not the nest config.
