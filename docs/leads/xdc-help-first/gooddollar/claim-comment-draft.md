# Claim comment for GoodDollar/data-team#40 (draft, do not post)

I'd like to take this. My plan: keep HyperSync for the other chains and add an optional way to get XDC
UBIScheme logs into `RawLogs` from a plain JSON-RPC endpoint (a small config for the Nuthatch indexer plus a loader
that uses the existing staging + MERGE path), checked against the contract's own daily claim ledger. It would be
opt-in and wouldn't touch `pipeline-v5`. I have a working version and will open a PR linking this issue.
