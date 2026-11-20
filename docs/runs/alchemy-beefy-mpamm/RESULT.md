alchemy-beefy-mpamm, 2026-11-20. Binary: main + PR #3076 (factory-parallel, still open) merged into agent/alchemy-beefy-mpamm, v5.23.0.
RPC: Alchemy only, through a local call-counting proxy (/tmp/abm-tools/proxy.py, localhost, key held in its env only). CU table: getLogs 75, blockNumber 10, getBlockByNumber 16, eth_call 26.

BEEFY (HyperEVM, 999)
- 200k tip slice: 4 getLogs, 394 CU (0 events, sparse). Extrapolated 234 slices ~ 92k CU. Far under 15M. Full-run actual: 36.6k CU.
- Full backfill ran: 4,883,535..51,794,187 in 5m14s, 487 getLogs, 40,177 events. 6 classic vaults, 29 reward pools, 29 CLM managers.
- Verification: 182 checks (totalSupply + top-2 holder balanceOf per discovered contract, at block 51,794,521): 182 pass, 0 fail.
- Draft: ~/orchestra/leads/beefy/DRAFT.md. Not compared with Envio output (not available).
- Nest: running, tmux session abm-beefy-full, dir ~/abm-run/beefy-full, API http://127.0.0.1:18204, poll 60s, lag 0. Follow cost ~9k CU/hour.

MPAMM (Monad, 143)
- 200k tip slice: 4 getLogs, 394 CU. Extrapolated 293 slices ~ 115k CU. Under 15M. Full-run actual: 45.4k CU.
- Full backfill ran: 65,199,911..123,946,714 in 3m00s, 602 getLogs, 25,199 events (3,772 Capricorn swaps).
- Verification vs capricorn.ts decode logic (independent getLogs): 3,772/3,772 rows identical, USD volume equal (389,882.29), 59/59 getReserves checks (+1 skipped). Metric v1 pools not compared.
- Draft: ~/orchestra/leads/mpamm/DRAFT.md.
- Nest: running, tmux session abm-mpamm-dense, dir ~/abm-run/mpamm-dense, API http://127.0.0.1:18203, poll 60s, lag 0. Follow cost ~8k CU/hour.

Both nests send RPC through the counting proxies on 127.0.0.1:18104 (beefy) and :18103 (mpamm); if a proxy dies the nest stalls until it is restarted with UPSTREAM set. Stats files ~/abm-run/*.stats.
Slice caveat: a --backfill slice on a factory nest cannot see children created before it, so the tip slices measure fixed per-call overhead, not density. The full runs are the real numbers.

Nuthatch bugs found: none. Notes: (1) the #3076 build ran both factory backfills 4-way as advertised, and Alchemy accepted 100k-block windows (487 and 602 getLogs over 47M and 59M blocks). (2) pkill -f in a tool shell matched its own command line and killed the shell (my mistake, same as the earlier lead's).
