# RFC-0072: Chain weather - a daily report a nest writes about itself

- Status: **Parked 2026-10-22 (Chief): stopped after S0.** Chief scratched it the day S0 reported;
  the engine room (RFC-0069) is the only programme running. Do not start S1 unasked. Accepted
  2026-10-21. S0 measured 2026-10-22 (§13): the answers are byte-identical across passes and
  machines and a day's set runs in under 10 s, but the full-history newcomer scan breaks the 256 MB
  rise on the GRT nest, so the 90-day fallback would have been taken.
- Author: Pete (cargopete)
- Date: 2026-10-21
- Depends on: RFC-0071 (nuthatch diff), for its row-to-sentence templates and its ranking, and on
  nothing else in it (§6). RFC-0010 Part B and RFC-0008 C5 (the delivery outbox the webhook reuses).
  RFC-0016 (`semantic.toml`, the descriptions a report quotes). RFC-0029 §6b (timestamp-free nests).
  RFC-0062 (a per-nest TOML that sits outside the data identity, which `weather.toml` copies).
- Blocks: nothing.
- Nature: new capability, opt-in per nest. §9 asks whether it belongs in the default binary.

## 1. What it is for

A nest knows a great deal about its contract that nobody asks it. The operator looks at it when
something breaks, and a user queries the three views they wrote. Neither notices that Tuesday had
four times the usual swaps, that a new address moved more than anyone else in the pool, or that the
contract went silent for nine hours.

Chain weather is a short report, one per UTC day, that a nest writes about its own data: how busy it
was against its usual, the busiest hour, the biggest mover, the largest single transfer, counterparties
seen for the first time, records broken and quiet spells. Every number in it is the answer to a fixed
SQL query over the nest's own tables. The sentences come from templates; an operator who wants nicer
prose can point it at a local model or their own API key, and the facts do not change.

It is a small thing, and meant to be. It gives a running nest a reason to be looked at on a quiet
day, and it shows off a property the product already has: a nest can answer questions about itself,
offline, and every answer can be re-derived.

## 2. What exists, read 2026-10-21 at `origin/main`

- **Timestamps are on by default.** `[nest] block_timestamps` defaults to `true`
  (`src/config.rs:543`), so every event table carries `block_timestamp` and a UTC day is a predicate.
  A nest that set it `false` (RFC-0029 §6b) has no wall clock at all (§5.6).
- **Token metadata is cached.** `metadata.json` holds `decimals`, `symbol` and `name` per token
  address (`src/metadata.rs`), fetched once by the `metadata` subcommand. A mover stated in token
  units needs `decimals` from there; without it the report says base units.
- **Addresses have names.** `labels/` holds content-addressed `{address, label}` snapshots that
  `/sql` already exposes as the `labels` view (`src/labels.rs`). `semantic.toml` describes tables and
  columns (`src/semantic.rs`). Both are presentation, which is how the report uses them.
- **A delivery engine exists.** Alerts and user webhooks push onto one durable outbox in the hot
  store, drained by one background worker, at-least-once, never blocking indexing
  (`src/alerts.rs`, `src/webhooks.rs`). `AlertFormat` already shapes a Discord body, because Discord
  refuses the raw JSON (`src/config.rs:237`).
- **There is no LLM client in the binary.** No source file mentions Ollama, Anthropic or OpenAI.
  RFC-0016's non-goals ruled out an LLM in core and in the data path, and allowed a BYO-key or Ollama
  authoring assist. The prose path here would be the first model call the binary makes.
- **There is no clock-driven scheduler.** `src/scheduler.rs` is cursor placement (RFC-0022), not a
  timer. §5.5 does not need one.
- **The analytical engine is budgeted.** Burrmill runs under `analytics.memory_limit` (512 MB default)
  and two threads (`src/analytics_budget.rs`); a report's queries run inside that, not beside it.

## 3. Goals

1. A daily report per nest, whose every number traces to a named query that re-runs to the same
   bytes.
2. A report that is final once written: built from sealed data only, so a reorg cannot change it.
3. Readable with no network at all, from templates.
4. Optional model-written prose, off by default, that cannot introduce a number the facts lack and is
   marked as model-written wherever it appears.
5. Four ways to read it: a file, an HTTP route, an MCP tool, and an opt-in webhook.
6. Costs nothing on a nest that has not turned it on.

## 4. Non-goals

- **Not an alerting system.** Alerts (RFC-0008 C5) fire per event and per block. Weather is a daily
  summary; it never pages anyone and has no thresholds an operator tunes for urgency.
- **Not a dashboard.** No charts, no admin tab. The Markdown is the presentation.
- **No ML.** Anomalies are ratios against trailing medians and record comparisons (§5.2).
- **No prices.** Values are in token units. A USD figure needs an oracle or offchain data, and
  RFC-0045 is the place for that, not here.
- **No cross-nest or cross-chain report.** One nest, one report.
- **No email transport in the binary.** No SMTP client. Email is a webhook into whatever bridge the
  operator already has.
- **The model chooses nothing.** It does not pick which facts matter, query the nest, or answer
  questions. It rewords a fixed set of facts.

## 5. Design

### 5.1 The window

A report covers one UTC day, `[D 00:00, D+1 00:00)` by `block_timestamp`. It is computed over the same
hot-and-sealed surface `/sql` reads, but only once the **sealed** watermark has passed the first block
at or after `D+1 00:00`. At that point every row of day D is past finality, so the report is final:
re-running its queries returns the same bytes for as long as the segments exist.

Baselines read the trailing 7 and 30 days before D, also sealed. The day's block range
`[first_block, last_block]` is recorded in the report and added to every query as a `block_number`
bound, so segment pruning works on the column the segments are ordered by.

An on-demand report for today so far is allowed, over hot and sealed, marked `provisional: true`. It
is never stored and never delivered.

### 5.2 The signals

Each signal is one query with a stable id. "Events" means rows across the nest's event tables; a
signal over transfers applies only where the nest has a Transfer-shaped table (the shape the built-in
`balances` view already detects). Ties break by the lower block, then log index, then address, so
every signal has exactly one answer.

| Id | Signal | Exact definition |
|----|--------|------------------|
| `activity` | Busy or quiet day | Per event table, `COUNT(*)` for D. Compared with the median of the daily counts over D-7..D-1 and D-30..D-1. |
| `unusual` | Unusual activity | A table, or an emitting address under a factory, whose count for D is at least 3x or at most a third of its 7-day median, where that median is at least 20. Below 20 it is not judged. |
| `busiest_hour` | Busiest hour | The UTC hour of D with the most events, all tables, with its count and its share of the day. |
| `quiet_spell` | Quiet spell | The longest gap between consecutive events in D, as start time, end time and blocks, including the gaps from midnight to the first event and from the last event to midnight. Reported only if it exceeds 4x the day's median gap. |
| `biggest_mover` | Biggest mover | The address with the largest absolute net change in D: `SUM(value_dec)` received minus sent, per token address. Stated in token units using `decimals` from `metadata.json`, else in base units and said so. |
| `largest_transfer` | Largest single transfer | `MAX(value_dec)` in D, with its tx hash, sender and receiver. |
| `new_counterparties` | Newly seen addresses | Addresses appearing as `from` or `to` in D with no appearance before D in this nest. The count, and the top three by volume in D. |
| `records` | Records broken | D's event count, transfer volume or distinct active addresses, where it exceeds every earlier day in the nest's history. |
| `first_of_kind` | First of its kind | An event table with rows in D and none before D. |

`new_counterparties` and `records` read the whole history before D. They are the only two signals
whose cost grows with the nest, and S0 measures them first (§7). If they fail its gate, they are
scoped to the trailing 90 days and the report says "first seen in 90 days" and "highest in 90 days",
which is less pleasing and still true.

Addresses are named from `labels` where a label exists. Tables are named from `semantic.toml`'s
description where the author improved it, else by their table name.

### 5.3 The facts and the report

Each run writes one JSON document, the facts, and one Markdown rendering of them:

```json
{
  "nest": "<nid>", "data_identity": "<hash>", "day": "2026-10-20",
  "blocks": [21034112, 21041287],
  "binary": "4.x.y", "weather_version": 1, "provisional": false,
  "facts": [
    { "id": "busiest_hour", "rank": 1, "values": { "hour": 14, "events": 1832, "share": "0.21" },
      "query": "SELECT ...", "query_sha256": "..." }
  ]
}
```

Two things a run knows are deliberately not in it. The sealed watermark at the time of computing moves
on without day D's rows changing, and nothing about prose belongs to the facts. Both go in a separate,
non-canonical `<day>.provenance.json` (watermark, wall-clock time of the run, which prose source the
stored Markdown used). `--verify` compares the facts document only.

Every number in the Markdown carries a reference to the fact it came from. Numbers are rendered once,
in the facts, as decimal strings with their unit, and the Markdown reuses those exact strings; nothing
downstream reformats them. `query` is the SQL as run, with its bounds inlined, so a reader can paste it
into `/sql` and get the same answer.

The facts document is deterministic: the same nest, day and binary give the same bytes. That is the
re-execution test, and S1's gate.

### 5.4 Prose: templates first, a model optionally

**Templates.** Each fact kind has one sentence template, and facts are ordered by a deterministic
score (§6). This is the report everyone gets, and it needs no network.

**A model, if the operator asks for one.** `weather.toml` may name a provider:

```toml
[prose]
provider = "ollama"            # or "anthropic", or "openai-compatible"
endpoint = "http://127.0.0.1:11434"
model = "llama3.1:8b"
key_env = "WEATHER_PROSE_KEY"  # read from the environment, never from the file
timeout_secs = 30
```

The fence around it:

1. **Off by default.** With no `[prose]` section, the binary makes no model call and contains no
   model call on any path. Nothing is downloaded, no model is bundled.
2. **What goes in.** The prompt is a fixed instruction plus the facts JSON and the labels and symbols
   those facts mention. No raw rows, no other addresses, no query access. Symbols and labels are
   quoted as data, since a token's `symbol()` is attacker-controlled text read from the chain.
3. **What comes out is checked.** Every numeral in the output (digits, with separators, decimals or
   a percent sign) must equal a rendered value in the facts. Number words are refused outright, and so
   are multipliers ("double", "tripled", "half"), so a ratio cannot be paraphrased into one the facts
   lack. Every `0x` string must be an address in the facts. URLs, Markdown links, HTML and Discord or
   Slack mentions are refused. Any failure discards the output and the template prose is used, with
   the reason logged once.
4. **One attempt.** One request per report, no retries, the timeout above. Offline, refused or slow,
   the result is the template, and the report is otherwise identical.
5. **Never stored, never served by the nest.** Non-negotiable 4 keeps LLM output out of the runtime
   data path, so the model's text is not written under `data/`, not part of the facts, and not
   returned by `/weather`, the MCP tool or any other route; those always carry the template
   rendering. Model prose exists only in transit: written at delivery time into a webhook body, or
   printed by the CLI when the operator asks for it, and then gone.
6. **Marked where it appears.** In a webhook body or the CLI output, the model's paragraph sits under a
   heading naming the provider and model and saying it was written from the facts that follow, and
   the facts follow it in full.

A BYO remote key sends the facts, including the addresses in them, to that provider. That is the
operator's choice, made by writing the section, and the docs say so in those words.

### 5.5 Configuration and schedule

`weather.toml` at the nest root turns the report on. Like `maintained.toml` (RFC-0062) it is outside
the data identity: adding it does not change the NID, and two operators of one nest may differ.

```toml
enabled = true
retain_days = 90

[[deliver]]
url = "https://discord.com/api/webhooks/..."
format = "discord"   # "raw", "discord" or "slack"
```

There is no clock. The ingestion side already advances the sealed watermark; when it crosses a day
boundary, the report for the closed day is queued and computed on the analytics connection pool. If
the runtime was down across several boundaries, the missed days are computed in order on start, up to
`retain_days`. A day is computed once; a stored report is not recomputed unless asked.

### 5.6 Storage, retention, timestamp-free nests

Reports live at `data/<nid>/weather/<day>.json` (the facts), `<day>.md` (the template rendering) and
`<day>.provenance.json` (§5.3). No model text is stored (§5.4). Retention deletes files older than
`retain_days`. A report is a few kilobytes, so 90 days is well under a megabyte.

Under a data directory keyed by NID, two mounts of one nest share the reports, which is right: the
facts are a function of the data. Two tenants configuring different models contend for nothing, since
model prose is produced per delivery.

A nest with `block_timestamps = false` has no UTC day. `weather.toml` on such a nest is refused at load,
by name, rather than approximated by block buckets that would read as days and are not.

### 5.7 Delivery

- **File**: §5.6.
- **HTTP**: `/weather` (the latest final report), `/weather/{day}`, and `/weather/today`
  (provisional). Markdown by default, JSON with `Accept: application/json`. A nest without
  `weather.toml` answers 404 with a body that names the file to add.
- **MCP**: one tool, `weather`, taking an optional day, returning the facts JSON and the Markdown.
- **Webhook**: a third producer on the existing outbox, so it inherits at-least-once delivery, the
  bounded outbox, signing and never blocking indexing. `discord` reuses `AlertFormat::Discord` and
  sends with `allowed_mentions` empty; `slack` sends `{"text": ...}`, which needs one new
  `AlertFormat` variant that alerts can use too. Nothing is sent to a URL the file does not name.
- **CLI**: a `weather` subcommand that prints a day's report, and with `--verify` recomputes the facts
  and compares them byte for byte with the stored file.

## 6. Reuse of RFC-0071

RFC-0071 (nuthatch diff, being written alongside this one) turns rows into templated sentences and
ranks them. Weather should use that and not grow a second set. This RFC depends only on its shape,
since 0071 is not written yet:

- a fact row is a kind, a subject, a map of named values and a provenance (the query);
- each kind has one sentence template, filled only from that row's values;
- ranking is a deterministic score per row, ties broken by a total order on the row's key.

Weather adds its kinds (§5.2) as templates in 0071's table. If 0071 lands in a different shape, S1
adapts to it, and if it has not landed when S1 starts, S1 waits for it rather than writing its own.

## 7. Cost

- **Nothing when off.** No `weather.toml`, no task, no route handler work, no memory. The dense
  multi-nest RSS benchmark must not move.
- **Bounded when on.** Ten-odd queries per day per nest, run one after another on one analytics
  connection, under its memory limit, each with a 60 s timeout. A query that times out drops its fact
  and the report says which, rather than retrying.
- **The expensive pair.** `new_counterparties` and `records` scan history. S0 measures them on a copy
  of the Lodestar nest; a projection of the address and timestamp columns over sealed Parquet is what
  is being bet on.
- **Inside the cursor.** The work comes out of the cursor's existing analytics reservation, not a new
  one, so the per-cursor 2 GB budget is unchanged by construction. S1's gate checks it anyway.

## 8. Slices

Each slice ends runnable and has a gate that can fail.

- **S0, the spike: are the reports worth reading, and cheap?** No binary change merged. The §5.2
  queries are written as plain `/sql` and run for 14 consecutive days against a copy of the Lodestar
  nest and one ERC-20 nest, rendered with throwaway templates. Gate: each query gives the same bytes
  twice; the whole set for one day completes in under 10 s with an RSS rise under 256 MB on the
  ThinkPad; and Chief reads the 14 reports and says at least half told him something he did not know.
  A no on the last condition stops the RFC here, which is the point of asking it first.
- **S1, facts, templates and reading surfaces.** `weather.toml`, the sealed-watermark trigger, the
  facts JSON, the Markdown, storage and retention, the routes, the MCP tool and the `weather`
  subcommand. Gate: `--verify` passes for every stored day of both S0 nests; a reorg property test shows
  a stored report never changes and a provisional one may; a test run with all outbound network
  refused produces identical reports; the dense multi-nest RSS benchmark is unchanged with no
  `weather.toml` present.
- **S2, webhook delivery.** The outbox producer, `discord` and `slack`. Gate: a down sink leaves
  indexing at full rate and the outbox bounded; a restart mid-delivery delivers once more, not zero
  times; a nest with no `[[deliver]]` makes no connection.
- **S3, model prose.** `[prose]`, Ollama and two BYO providers, the checker, the marking. Gate: a fixed
  corpus of bad outputs (a wrong number, a number word, "doubled", an unknown address, a link, an
  `@everyone`) is rejected in full; the facts JSON is byte-identical with prose on, off and failed; with
  Ollama stopped the report arrives on time with template prose; a real Ollama model on the ThinkPad
  passes the checker on at least 12 of the 14 S0 days, or the prompt is wrong and S3 does not ship.

## 9. Default binary or a feature

The honest form of RFC-0046 §1's test, applied in two parts:

- **The network parts** (the model call and the webhook). Delete them from the tree and a self-hoster
  loses nothing they need: the report is complete with templates and readable by file, route and MCP.
  Enable them and a nest that did not still runs exactly as before. Both pass, and both are calls the
  operator turns on by writing a URL, which is what non-negotiable 3 already permits.
- **The facts layer.** The test does not discriminate here. Removing weather loses a feature, as
  removing any feature does; it gates nothing and requires nothing to start.

Recommendation: **the default binary, opt-in per nest by `weather.toml`**, with the model path inside
it and off until `[prose]` is written. That is how `maintained.toml` shipped, and it keeps local AI
reachable for someone who installed with `curl | sh`, which an off-by-default cargo feature would not.
The alternative, if Chief prefers the line drawn at the model call: a `weather-prose` cargo feature,
off by default, so the default binary contains no model client at all. §11 asks.

## 10. Risks

- **True numbers, wrong claim.** The checker stops invented numbers, not a model saying "rose" of a
  count that fell. The facts sit under every model paragraph, the marking says which text to trust, and
  the template remains the default. This is the main reason the model is opt-in rather than a default.
- **Prompt injection from the chain.** Token symbols and names are whatever a deployer wrote. They go
  into the prompt as quoted data, and the output checker bounds what can come back: no links, no
  mentions, no numbers or addresses the facts lack.
- **Noise read as signal.** A ratio over a thin baseline is meaningless, hence the floor of 20 in
  `unusual` and the 4x floor in `quiet_spell`. S0 is where these thresholds earn their keep or change.
- **Cost creep.** History-wide signals grow with the nest. S0 measures, and the 90-day fallback is
  already specified.
- **Sealing lag.** A day's report waits for the day to seal. If that is hours on some chain, the report
  is late, not wrong. S1 records the lag per chain.

## 11. Open questions for Chief

1. Default binary with per-nest opt-in (recommended), or the model client behind an off-by-default
   `weather-prose` feature?
2. Settled in review: model prose is not stored or served by the nest, only produced at delivery
   (§5.4), because non-negotiable 4 forbids it in the runtime data path.
3. If S0's history scans are too slow, are "first seen in 90 days" and "highest in 90 days" acceptable,
   or should those two signals be dropped?
4. Is `slack` worth adding as an `AlertFormat` now, since alerts would get it too?
5. Should S1 wait for RFC-0071's templates, as §6 proposes, or may it start with its own and fold in
   later?
6. UTC days only, or a per-nest `utc_offset`? A Tokyo operator's Tuesday is not UTC's.

## 12. Alternatives considered

- **Model writes the whole report from rows.** Fails non-negotiable 4's spirit and the provenance goal:
  nothing in the output could be traced to a query.
- **An `entities.toml` entity per signal.** Incremental maintenance for a once-a-day summary costs
  memory all day to save seconds once. Request-time SQL over sealed data is the right tool.
- **A wall-clock schedule.** Needs a timer and a timezone setting, and would run before the day sealed.
  The sealed watermark is already the event that matters.
- **Shipping it as a skill or external script over `/sql`.** Possible today, and S0 is exactly that.
  It loses the MCP tool, the delivery and the `--verify` guarantee, which are what make it a nest's own
  report rather than someone's cron job.

## 13. S0, measured 2026-10-22

The harness is `tools/rfc-0072-s0/`, bash and jq against `nuthatch serve` 5.8.2 on copies of two nests.
`day.sh` writes one UTC day's §5.2 signals as plain `/sql` and keeps every answer; `facts.sh` merges
them into a facts document; `render.jq` is the throwaway template; `pass.sh` runs 14 days on a fresh
`serve` per day, so no answer is `/sql`'s memo and each day's RSS rise is its own; `compare.sh` checks
two passes byte for byte. `lodestar.config.json` and `grt.config.json` are each nest's tables and
address parameters as `nest-config.sh` read them, with Lodestar's allocation ids added by hand (5
below). No model is involved.

- **Lodestar's allocations nest** (`graph-allocations-nest`, the release-gate copy frozen
  2026-10-12): 2026-09-28 to 2026-10-11. 76 event tables are declared and 66 have rows. It has no
  Transfer table, so `biggest_mover` and `largest_transfer` do not apply.
- **The GRT supply nest** (`grt-supply-nest`, Arbitrum GRT `Transfer`, `BridgeMinted`,
  `BridgeBurned`): 2026-09-23 to 2026-10-06. It was built with `block_timestamps = false`, which §5.6
  says weather refuses. S0 ran it anyway, with its day and hour boundaries read once from Arbitrum
  (`grt-bounds.sh`, 428 first blocks by bisection), to have an ERC-20 at all; no other ERC-20 nest
  with timestamps and 90 days of history was to hand. Its records are 90-day only, since the full
  history has no clock to bucket it by, and its quiet-spell times are placed between hour boundaries.

| Gate | Result |
|---|---|
| Each query gives the same bytes twice | **Pass.** Every response body and every facts document identical on two MacBook passes: 728 files for Lodestar (350 statements over 14 days), 336 for GRT (154). The ThinkPad pass gives the same 1,064 files byte for byte |
| A day's set under 10 s on the ThinkPad | **Pass.** Lodestar 6.0 to 6.5 s with full-history signals (7.6 to 8.1 s with the 90-day variants run beside them), GRT 2.7 to 2.9 s (4.0 to 4.3 s) |
| RSS rise under 256 MB on the ThinkPad | **Fails on GRT with full history, passes on the 90-day fallback.** Lodestar 211 to 223 MiB. GRT 244 to 259 MiB, over 256 MiB on 4 of 14 days. With the full-history statements left out, 87 to 107 MiB (three GRT days, two Lodestar days, replayed by `stmt-rss.sh`) |
| Chief reads the reports, half told him something | **Open.** `rfc0072-chain-weather-s0.html`, 28 reports, 44 KB, no network requests |

| A day's set | Lodestar, MacBook | Lodestar, ThinkPad | GRT, MacBook | GRT, ThinkPad |
|---|---|---|---|---|
| Statements | 25 | 25 | 11 | 11 |
| Time, full-history signals | 4.2 to 5.6 s | 6.0 to 6.5 s | 2.4 to 4.0 s | 2.7 to 2.9 s |
| Time, 90-day signals only | | 4.3 to 4.6 s | | 2.4 to 2.5 s |
| RSS rise, full history | 169 to 226 MiB | 211 to 223 MiB | 202 to 256 MiB | 244 to 259 MiB |
| RSS rise, 90-day only | | 103 to 104 MiB | | 87 to 107 MiB |

The heaviest single statements: GRT's full-history newcomers, 1.55 s and a 153 MiB rise on its own
against 0.58 s and 68 MiB over 90 days; Lodestar's full-history distinct-address record, 1.23 s and
114 MiB against 0.48 s and 17 MiB.

What this changes:

1. **A signal over every event table is not one `/sql` statement.** Burrmill refuses more than 64
   `UNION`s in a statement, and `/sql` refuses a statement over 16 KiB. Lodestar has 66 tables with
   rows. S0 runs each event-row signal as one statement per chunk of 40 tables whose answers merge
   exactly (counts per table and day, min and max per day, distinct timestamps for gaps), puts a
   table's address parameters in one branch with `unnest`, and checks "seen before" against literal
   lists of 150 addresses. §5.3's `query`, one string a reader pastes into `/sql`, becomes a list of
   statements and a merge rule. S1 should keep that shape, with the merge in Rust, rather than run its
   own queries past `/sql`'s admission, so a reader can still re-run every statement.
2. **The obvious "new address" query runs out of memory.** A join of the day's addresses against 13
   million history rows failed in GRT's 512 MB analytics budget (`HashJoinInput`, can spill: false).
   A `GROUP BY address HAVING MIN(block_number) >= D's first block` gives the same set in 1.55 s.
3. **§11 Q3: take the 90-day fallback**, "first seen in 90 days" and "highest in 90 days", which is
   what the RFC recommended. Full history passes the time gate and fails the RSS one on GRT, and it
   is also the less useful signal: no full-history record fired on any of the 28 days, because
   Lodestar's highs are 28,131 events on 2024-12-06 and 10,306 active addresses on 2024-04-02, the
   L2 migration. Over 90 days, records fired three times (Lodestar 2026-10-09, GRT 2026-09-26 and
   2026-09-29). Newcomers over full history and over 90 days differ (Lodestar 0 to 4 against 0 to 12
   a day), and the report can say both only while full history is affordable.
4. **`quiet_spell` fired on all 28 days.** Four times the day's median gap is a few minutes on
   Lodestar and a few hundred blocks on GRT. One was checked against the chain: Lodestar's 3 h 58 min
   silence on 2026-10-10 (blocks 513,413,421 to 513,464,164) holds 234 logs from the nest's contracts
   by `eth_getLogs` on Arbitrum, all GRT `Transfer` and `Approval`, which this nest does not index,
   and none with one of its topics. It was the chain, not a hole in the nest. S1 should compare the
   day's longest gap with the previous days' longest gaps, not with its own median. Not measured.
5. **`new_counterparties` needs a definition for a nest without transfers.** S0 used every indexed
   address parameter, by name, and left out allocation ids, which are new on nearly every row and drown
   the rest (1,036 of 1,103 role and address pairs on the first try at 2026-10-11). That list was
   written by hand for S0; S1 needs an author hint in `semantic.toml` for a parameter that is an
   identifier, not a counterparty. With it, Lodestar named one to four new delegators, curators, payers or receivers on 13 of 14 days.
6. **`unusual` fired on 12 of 14 Lodestar days and never on GRT.** Lodestar's tables swing well past
   3x and a third of their 7-day medians most days; GRT is one large table and two with medians under
   20. Whether that is signal or noise is Chief's reading.
7. **Addresses need names.** GRT's report for 2026-09-23 names its largest net mover, in 2,149
   transfers, only as `0x00669a4c...ef03`, which Lodestar's nest calls `staking`. Neither nest has labels. S1
   should name addresses through `NestContext::address_text` as the diff does, and an ERC-20 nest will
   still not know its counterparties' names.
8. **Timestamp-free nests stay refused (§5.6).** S0 shows the alternative costs a day-boundary table
   read from the chain (25 first blocks a day for the hour), approximate times and no full-history
   records. Nothing in S1 should depend on it.

§11, as S0 took them: Q1 default binary with per-nest opt-in, the recommendation; Q2 settled; Q3 the
90-day fallback, above; Q4 `slack` added with S2, as recommended; Q5 S1 waits for RFC-0071's
templates, which have since landed (§17 of 0071); Q6 UTC days only.

Not verified: ranking (S0 orders facts by a fixed list); the reorg property of S1; any report on a
live nest, or one sealed under the chain-time age rule; `weather.toml`, routes and delivery, none of
which S0 builds; and the GRT nest's quiet-spell times, which are interpolated.
