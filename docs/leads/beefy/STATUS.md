# beefy: BLOCKED (verdict: not feasible on a public RPC), nothing to file
Gap note, measured 2026-11-19:
- Public HyperEVM RPC (rpc.hyperliquid.xyz/evm) caps eth_getLogs at 1000 blocks ("query exceeds max block range 1000") and aborts slow queries at 5s.
- It also rate limits hard: 138 "rate limited" retries in the 8 minute probe. The nest's factory discovery used 160 block windows.
- Probe: 18,000 blocks (51,686,476..51,699,756 region, 75% of a 20k slice) took ~7.7 minutes, 0 events found.
- Beefy factories start at block 4,883,535; tip is ~51.7M, so ~46.8M blocks. At the measured rate that is roughly 12 days. Even at a clean 1000-block window it is ~47k calls on a rate-limited endpoint.
- So no honest side-by-side against their Envio output is possible without a dedicated HyperEVM archive RPC (paid, ruled out) or their own node.
- Nuthatch bug/gap noted: factory backfill is sequential ("--concurrency ignored until the step-5 filter flip"), which makes factory nests slow on RPC-only chains.
- Nest spec that ran: beefy/nest/nuthatch.toml (3 factories, 3 templates). Vault list: vaults_hyperevm.json. Repo clone untouched (no patch, no draft).
