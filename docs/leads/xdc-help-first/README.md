# XDC help-first leads: GoodDollar and Sablier

Local drafts only. Nothing here has been posted to an outside repo. The lead reviews and posts.

Shared XDC feasibility: Nuthatch indexes XDC mainnet through the public `https://rpc.xinfin.network` with no key.
`eth_getLogs` accepted the window sizes used (50,000 blocks, adapting down on dense ranges), one shallow reorg was
seen and handled, and both nests reached the tip with 0 blocks lag. Verdict: feasible.

| Lead | Verdict | Prepared patch (local, unpushed) |
|---|---|---|
| GoodDollar (nest gooddollar-xdc) | Feasible. 375 of 375 protocol days match the contract's daily ledger. BigQuery write not run. | `~/orchestra/leads/gooddollar/repo`, branch `nuthatch-xdc-ubischeme`, commit 3efb1d4 |
| Sablier (nest sablier-xdc) | Feasible as an event source (Lockup, Flow, Airdrops). Entities from `schema/` not built. | `~/orchestra/leads/sablier/repo`, branch `nuthatch-xdc`, commit a97edacc |

Each folder has `RESULT.md` (measured numbers, gaps), `PR-draft.md`, `agent-prompts.md`. GoodDollar also has
`claim-comment-draft.md` (CONTRIBUTING says claim the issue first).

Evidence committed here (small files only): `gooddollar/evidence/` (the nest config that ran, with the counting-proxy
URL replaced by a placeholder; `reconcile_nest.py`; `reconcile-full.json`, 375 days; `xdc-rpc-count.json`),
`sablier/evidence/` (`count_logs.py`, `count-lockup-v2.json`, `sablier-rpc-count.json`) and
`evidence-shared/rpcproxy.py` (the counting proxy). Not committed: raw run logs (large), the `nest/abis/` files, and the
patched clones under `~/orchestra/leads/*/repo`. The RPC count files were written by the proxy continuously and were last
saved on 2026-11-20 11:18Z, after the nests had been following the tip for hours, so they include following traffic and
are larger than the backfill-only totals quoted from the end of the backfill. Nothing here was independently rerun
for this PR; the figures are the author's measurements.

Nuthatch bugs and rough edges seen: factory nests backfill sequentially (12 min for Sablier); `--concurrency`
above 4 gave no speedup on one public endpoint when block-header calls dominate (GoodDollar); a cosmetic "tip could
not be read" notice on resume.
