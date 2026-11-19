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
`claim-comment-draft.md` (CONTRIBUTING says claim the issue first) and the nest config that ran.
Raw run logs and the RPC counts are in `~/orchestra/leads/{gooddollar,sablier,xdc-feasibility}`.

Nuthatch bugs and rough edges seen: factory nests backfill sequentially (12 min for Sablier); `--concurrency`
above 4 gave no speedup on one public endpoint when block-header calls dominate (GoodDollar); a cosmetic "tip could
not be read" notice on resume.
