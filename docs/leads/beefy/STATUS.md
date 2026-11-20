# beefy: BLOCKED (verdict: not feasible on a public RPC), nothing to file
Gap note, measured 2026-11-19:
- Public HyperEVM RPC (rpc.hyperliquid.xyz/evm) caps eth_getLogs at 1000 blocks ("query exceeds max block range 1000") and aborts slow queries at 5s.
- It also rate limits hard: 69 getLogs attempts failed with "rate limited" in the 8m48s probe (`probe.log`; each was followed by a backoff notice, so the log holds 138 matching lines). The nest's factory discovery used 160 block windows.
- Probe (`probe.log`, run 18:47:35Z to 18:56:23Z): the factory pass over blocks 51,684,717..51,704,653 (19,937 blocks) reached block 51,699,756 (75.4%) after about 7.7 minutes, i.e. 15,039 blocks, 0 events found.
- Beefy factories start at block 4,883,535; tip is ~51.7M, so 46,821,118 blocks to 51,704,653. At the measured rate (15,039 blocks / 7.7 min = about 1,953 blocks/min) that is about 23,970 minutes, roughly 16.6 days. This extrapolates one probe on a rate-limited endpoint; it is an estimate, not a run. Even at a clean 1000-block window it is ~47k calls on a rate-limited endpoint.
- So no honest side-by-side against their Envio output is possible without a dedicated HyperEVM archive RPC (paid, ruled out) or their own node.
- Nuthatch bug/gap noted: factory backfill is sequential ("--concurrency ignored until the step-5 filter flip"), which makes factory nests slow on RPC-only chains.
- Nest spec that ran: `nest-nuthatch.toml` in this folder (3 factories, 3 templates), with its ABIs in `abis/`, the vault list in `vaults_hyperevm.json` and the probe log in `probe.log`. Repo clone untouched (no patch, no draft).
