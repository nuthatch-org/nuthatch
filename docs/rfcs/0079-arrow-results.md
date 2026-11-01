# RFC-0079: Arrow results - `/sql` in Arrow IPC, and Flight SQL for native clients

- Status: **Complete (2026-11-01): S0 to S3 built (§12 to §15); the flight build released and proven with ADBC (§16).** **Accepted (2026-10-30, Chief): "ok letsgoooo with the wave of rfcs".** Built in the order 0078, 0079, 0080, each slice on its own gate; an S0 that fails stops its RFC. Chief, the same day, on GraphOps's
  five suggestions: "lets do ALL that, 1-5 !!!!! but HA first", so this follows RFC-0078 in build order.
- Author: Pete (cargopete)
- Date: 2026-10-30
- Depends on: RFC-0073 (the hardened SQL surface, `POST /sql`, bearer tokens, the gate),
  RFC-0076 S3 (feed v2, the Arrow IPC frame and its budgeted decode), RFC-0061 (256-bit values stay
  decimal text), RFC-0077 (the engine's `nh_request`).
- Blocks: RFC-0080 (partial sync and federation), which needs a columnar wire both ways.
- Nature: new capability in the default binary for the Arrow IPC answer, which adds no dependency.
  Flight SQL behind a cargo feature, `flight`, off by default until S0 has measured what it costs.

## 1. Abstract

GraphOps, 2026-10-30: "you probably want to move from json sql interface to apache arrow flight". Today
every `/sql` answer is a JSON envelope (`core/src/answer.rs:396`): rows as objects, `columns` beside
them because objects lose order, `provenance` with `as_of`, `sealed_through` and `nid`. Burrmill
produces Arrow record batches and the node turns them into JSON; a client that wants columns, as every
DataFusion, DuckDB, Polars or pandas caller does, turns them back.

This RFC gives `/sql` a second answer format, an Arrow IPC stream, chosen by `Accept`, from the same
handler with the same guards. And it adds an Arrow Flight SQL endpoint for native clients (ADBC,
JDBC through the Flight SQL driver, `pyarrow.flight`, DBeaver), behind a feature, because a browser
cannot speak it and it brings a gRPC stack with it.

## 2. What exists, read 2026-10-30 at `ced48584`

- `/sql` is `GET` and `POST` (`src/serve.rs:565`), both into `run_sql_query_at`, which holds **every
  node guard**: the free-form switch, the length limit, the memo, the per-cursor gate and its permits,
  `max_rows`, the strict isolation of RFC-0073 FR8. `/q/{name}` declared queries go through the same
  function.
- `arrow` 59 with `ipc` is a direct dependency (`Cargo.toml:130`). Feed v2 already writes IPC streams
  (`src/feed_wire.rs:288`) and has the budgeted reader RFC-0076 §10 describes.
- Burrmill's arrow is pinned separately (`Cargo.toml:142`: "its arrow is not ours"). [VERIFY: whether
  batches cross from Burrmill's arrow to ours today, or whether JSON is produced inside Burrmill. If the
  versions differ, an IPC round trip between them is the conversion, which costs a copy.]
- 256-bit values are decimal text (RFC-0061) and their logical type lives in the mirror's
  `schema.json`, not in the column type.
- No gRPC dependency is in the tree. `arrow-flight` pulls `tonic`, `prost` and `h2`.

## 3. Goals

1. `/sql` and `/q/{name}` answer `Accept: application/vnd.apache.arrow.stream` with an IPC stream
   whose rows are byte-for-byte the same values as the JSON answer's, in the same order.
2. Provenance travels with the data: the same `as_of`, `sealed_through`, `nid`, `degraded`,
   `truncated` and `cached` facts, readable by any Arrow client without parsing a sidecar.
3. A Flight SQL client can connect, list the tables a route exposes, and run a read-only statement,
   through the same guards.
4. No guard is bypassed and none is reimplemented: both formats leave `run_sql_query_at` at the same
   point.

## 4. Non-goals

- Writes of any kind over Flight (`DoPut`, `CommandStatementUpdate`, ingest). The SQL surface is
  read-only and stays so.
- Prepared statements with parameters in v1. Declared queries (`/q/{name}`) are the parameterised
  form and are offered as Flight SQL "saved" statements in S2 only if a client needs it.
- gRPC-web for browsers. The browser path is IPC over plain HTTP (§5.1), which is what Arrow's own JS
  library reads.
- Authentication beyond what `/sql` has. Flight's bearer header carries the same RFC-0073 FR12 token.

## 5. Design

### 5.1 IPC over HTTP

`Accept: application/vnd.apache.arrow.stream` (or `?format=arrow` for a URL in a browser) returns
`200` with that content type and one IPC stream: the schema message, then the batches as Burrmill
produces them, streamed, so a large answer is not held whole. `max_rows` still truncates, and the
stream ends there.

**Provenance in the schema.** Arrow schemas carry `custom_metadata`, a string map every client
exposes. The answer's schema holds `nuthatch.provenance` as the same JSON object the envelope's
`provenance` is, plus `nuthatch.degraded_tables`, `nuthatch.tip_unavailable`, `nuthatch.cached`. One
fact cannot be known when the schema is sent: whether the answer was truncated, since it is streamed.
IPC record batches carry no metadata of their own, so it goes in an HTTP trailer, and in a response
header when the answer was not streamed. [Open question 1: trailers
are invisible to `fetch()` in browsers. The alternative is to buffer up to `max_rows` and set it as a
header, which costs the streaming.]

**Big integers.** A column RFC-0061 stores as decimal text stays `Utf8`, and its field metadata carries
`nuthatch.logical = "uint256"` (or `int256`), the name `schema.json` uses. A client that knows can
parse; one that does not sees the same strings the JSON answer had. Arrow's `Decimal256` holds 76
digits and a uint256 needs 78, so it cannot be the type.

**Errors** stay JSON. A refusal is decided before the first batch, and its envelope is the one every
client already handles; an error mid-stream (a segment read failing) ends the stream and sets the
`degraded` trailer, as it sets `degraded` today.

### 5.2 Flight SQL

With the `flight` feature, `nuthatch dev --flight 0.0.0.0:8815` (and the same key under `[serve]`)
starts a Flight SQL service beside the HTTP one. It implements:

| Flight SQL call | Answered by |
|---|---|
| `CommandGetCatalogs`, `CommandGetDbSchemas`, `CommandGetTables`, `CommandGetSqlInfo` | the route's table list, as `/sql`'s schema discovery serves it now; one Flight "catalog" per mounted route |
| `CommandStatementQuery` → `GetFlightInfo` → `DoGet` | `run_sql_query_at`, its batches streamed as `DoGet`'s |
| `Handshake` | nothing; the bearer token is checked per call as `/sql` checks it |

Everything else answers `Unimplemented`. The ticket is the statement and the route; nothing is held
server-side between `GetFlightInfo` and `DoGet` except what the memo already holds, so a
`GetFlightInfo` that is never followed costs nothing. `FlightInfo.total_records` is `-1`: the count is
not known before running.

Provenance cannot ride on the schema here as cleanly, because `GetFlightInfo` returns a schema before
the query has run. It goes in `FlightInfo.app_metadata` and is repeated in the `DoGet` stream's schema
metadata as in §5.1.

### 5.3 The engine

`nh_request` (RFC-0077 §3.2) returns status, content type and bytes, so an Arrow answer from the
module is a change to the router it already shares, not to the ABI. The tab's own Burrmill then reads
the bytes as IPC without a JSON parse. This is the half RFC-0080 needs.

## 6. Against the non-negotiables

The IPC answer adds no dependency and changes no default: a client that sends no `Accept` gets JSON
byte-for-byte as now. `flight` is off by default; the binary without it is the binary today, which is
RFC-0046 §1's deletion test. With it on, `tonic` and `prost` are MIT. The ≤2 GiB budget is unaffected
by format: both stream from the same batches, and the IPC path holds less than the JSON one.

## 7. Order of work

| Slice | What | Size |
|---|---|---|
| S0 | Measure. JSON against IPC on three answers (a Lodestar panel, a 100 k-row export, a point read): encode time, bytes, client decode time in pyarrow and in the browser. The binary-size and build-time cost of `arrow-flight` with `tonic`. The §2 [VERIFY] answered. Can stop S2, not S1. | three days |
| S1 | §5.1 on `/sql`, `/q/{name}`, `/explain`'s result sample; the engine router (§5.3); the parity test: every route and every fixture query, IPC rows equal JSON rows | one week |
| S2 | §5.2, the `flight` feature, ADBC and `pyarrow.flight` in CI against a fixture nest | one week |
| S3 | Docs, the builder skill, `nuthatch watch`'s SQL pane reads IPC | two days |

## 8. Testing

Parity is the gate: for every query in the SQL fixture suite and every route, the IPC answer decoded
to rows equals the JSON answer's rows, value for value and in order, with equal provenance. Each
guard's existing test runs once per format: a guard that refuses JSON and answers Arrow is the bug
this RFC is most likely to introduce. The budgeted reader from feed v2 is the model for the client
side in RFC-0080, so fuzzing extends to the answer stream.

## 9. Risks

- **Two formats drift.** Mitigated by one producer: the formats are two encoders over the same batch
  stream, and the parity gate runs on every PR.
- **Flight SQL's surface is large** and clients probe calls we do not implement. Each probe answered
  `Unimplemented` must not break the client; S2 tests the three clients named, not the spec.
- **gRPC beside HTTP is a second port** an operator has to know about and a gateway has to route.
  Off by default for that reason as much as for size.

## 10. Alternatives

- **Flight only, no IPC on `/sql`.** Browsers cannot reach it, which loses RFC-0080's client.
- **Parquet answers.** Good for downloads, bad for streaming: the footer comes last.
- **Arrow over WebSockets.** No advantage over a streamed HTTP body for a request-response query;
  RFC-0080's subscriptions are where a long-lived stream belongs.

## 11. Open questions

1. Truncation: an HTTP trailer (streamed, invisible to `fetch`) or a header after buffering to
   `max_rows` (visible, not streamed). Proposal: buffer when `max_rows` ≤ 10,000, stream with a trailer
   above it, and document both.
2. Whether `flight` should be on by default once S0 has the size figure.
3. Whether declared queries should be Flight SQL prepared statements, if a client asks.

## 12. S0 measured (2026-10-31)

On a MacBook, release build, against `main` at `b7d1a2f6`. Nothing here has run on a CI runner.

**§2's [VERIFY], answered.** Burrmill's arrow is nuthatch's: the lock holds one `arrow`, 59.3.0, and
a batch from `sql_for_each_metered` assigns to `arrow::record_batch::RecordBatch` without conversion
(checked by compiling it). The comment "its arrow is not ours" in `engine_burrmill.rs`, and the IPC
round trip in `query_arrow` that rests on it, are out of date. So the IPC answer is written in
nuthatch, inside `collect`'s per-batch closure, where the row cap, the byte cap and the cost meter
already run. Burrmill's `sql_ipc` is not the path: it collects every batch before writing, with no
cap and no meter. And the temporal rewrite `collect` applies (#1852) is a fix to JSON text, so an
Arrow answer, which keeps `Date32` and `Timestamp` as types, needs none.

**JSON against IPC.** `engine_burrmill::tests::rfc_0079_s0_json_against_ipc` (ignored; run with
`--release -- --ignored --nocapture`): 100,000 transfer-shaped rows, each a block, a log index, a
42-character address, a 78-digit decimal as text and a 66-character hash, through the real session.
Each figure is the median of five; encode includes running the statement, so the difference between
the two is the encoding. Decode is in Rust (`serde_json` against Arrow's `StreamReader`), standing in
for a client: pyarrow was not installed, so the RFC's pyarrow and browser figures are not measured.

| Answer | Rows | JSON | IPC | JSON encode, decode | IPC encode, decode |
|---|---|---|---|---|---|
| A point read | 1 | 229 B | 1,736 B | 1.69 ms, 0.003 ms | 1.64 ms, 0.008 ms |
| A panel (`GROUP BY address`) | 97 | 7,955 B | 8,200 B | 1.71 ms, 0.021 ms | 1.71 ms, 0.006 ms |
| An export | 100,000 | 22.8 MB | 21.2 MB | 41.8 ms, 36.8 ms | 8.0 ms, 2.3 ms |

Arrow pays off on large answers and only there. An export encodes about 5 times faster and decodes
about 16 times faster. A point read's answer is seven times larger in IPC, because the schema travels
with it, and its time is the statement's either way. The bytes barely move, because a nest's columns
are mostly text; Arrow saves size on numeric columns, and these rows have two. So IPC is opt-in by
`Accept`, as §5.1 has it, and JSON stays the default.

**Flight SQL's cost.** Two throwaway programs over arrow 59 and tokio, release and stripped: one
writes an IPC stream (1.46 MB), the other serves a minimal Flight SQL service (2.35 MB), so
`arrow-flight` and `tonic` add about 0.9 MB. Against nuthatch's own tree they bring eight crates it does
not already have (`arrow-flight`, `tonic`, `tonic-prost`, `prost`, `prost-derive`, `prost-types`,
`tokio-stream`, `hyper-timeout`); hyper and h2 are already in it. The 5.16.0 release archives are 41 to
47 MB. That does not stop S2. Whether `flight` should be on by default, open question 2, is now a
question of a second port rather than of size.

**S0 stops nothing.** S1 (IPC on `/sql`, in `collect`) and S2 (Flight SQL behind `flight`) go ahead.

## 13. S1 built (2026-10-31): `/sql`

`GET` and `POST /sql` answer as an Arrow IPC stream when `Accept` names
`application/vnd.apache.arrow.stream`, or `format=arrow` is in the query string or the body.
Otherwise JSON, as before, byte for byte.

- **Where it is written.** `Session::collect_ipc`, beside `collect`, in the same per-batch closure, so
  the row cap, the byte cap and the cost meter apply unchanged; `QueryGuard::arrow` chooses it. The cap
  is exact: at most `max_rows` rows are written and `truncated` says another existed, where JSON
  collects one row more and trims. The byte cap counts the stream's own bytes.
- **An empty answer** still opens with its schema, from the plan: `Engine::result_schema`, made public
  in burrmill#124 for this.
- **The envelope.** Departure from §5.1: provenance does not ride in the schema's metadata. It is read
  after the statement runs (#932), and the schema is the first thing written. The answer is bounded by
  the byte cap and so is buffered, so the envelope, the JSON one less its rows, travels as the
  `x-nuthatch-answer` header, ASCII-escaped. That settles open question 1: a header, visible to
  `fetch()`, rather than a trailer. The body is reserved against `sql_response_bytes` as JSON's is.
- **Unchanged.** A refusal is decided before any batch and stays JSON. An Arrow request does not read
  or write the memo, which holds JSON answers.

**Tested.** `an_arrow_answer_is_the_json_answer_row_for_row`: four statements over sealed and hot rows
(every row, an aggregate, an empty answer, a capped one), each Arrow answer decoded through the JSON
encoder equal to the JSON answer's rows, columns in order and envelope. `format=arrow` and `POST`
answer it; a refusal is JSON. Every target, core, clippy and fmt pass.

**`/q/{name}`, built.** A declared query answers in Arrow by `Accept` alone: its query string carries
the query's own parameters, and one may well be called `format`. `named_scan` passes the choice to
the guard on the executing run only; the quote a paid query is offered plans without answering, so
it is unchanged. `a_declared_query_answers_in_arrow_as_it_does_in_json` holds it over sealed and hot
rows.

**`/explain`, nothing to do.** §7's S1 row named "`/explain`'s result sample"; there is none.
`/explain` plans the statement and answers with the plan and its columns, and materialises no row
(`Want::Plan`), so it has nothing for Arrow to carry. S1 is complete with `/sql` and `/q/{name}`.

## 14. S2 built (2026-10-31): Flight SQL behind `flight`

`cargo build --features flight` gives `dev --flight <addr>`, a Flight SQL service beside the HTTP API,
on a runtime and on a single nest alike. Without the feature the binary is the binary before: no
flag in `dev --help`, the CLI reference unchanged (`tests/skill_refs.rs`), nothing new linked.

**One door, not two.** `DoGet` answers a statement by sending it through the same composed router as
`POST /<route>/sql`, asking for Arrow, with the caller's `authorization` passed on. Admission, the
caps, strict isolation, the cost meter and the response-bytes accounting are `/sql`'s own, and so is
every refusal, which comes back as a gRPC status carrying `/sql`'s JSON error. The envelope rides as
the `x-nuthatch-answer` response header, as on HTTP. A router layer `bind_and_serve` adds was checked:
it adds CORS and a shutdown marker, neither of which `/sql` relies on.

**Departures from §5.2.**

1. *The route is a request header*, `x-nuthatch-route`, and absent means `/sql` at the root. A Flight
   SQL statement has no catalogue field, so §5.2's "one catalog per route" would have needed
   `GetCatalogs` and a client that sets one; a header works with every client that can set metadata.
2. *`GetFlightInfo` runs nothing and its schema is empty.* The ticket carries the route and the
   statement, so nothing is held between the two calls, as §5.2 asked. Filling the schema would mean
   running the statement there and keeping its answer until `DoGet`; the stream's own schema is the
   answer's.
3. *`GetCatalogs`, `GetDbSchemas`, `GetTables` and `GetSqlInfo` answer `Unimplemented`* in this slice.
   Statements are what a client needs to query; the catalogue calls are for a browsing client such as
   DBeaver, and follow.

**Tested.** `a_flight_sql_client_gets_the_rows_sql_answers` (with `--features flight`): arrow-flight's
own `FlightSqlServiceClient` against an in-process service over the serve fixture, sealed and hot
rows. Its batches, encoded as `/sql` encodes rows, equal `/sql`'s JSON rows; a statement naming a
missing column is refused with that column named. Every target with and without `flight`, clippy on
the default build, on `flight` and on the nightly's features, fmt, and `cargo deny --all-features`
(bans, licences) pass. Not run: ADBC, JDBC, DBeaver and `pyarrow.flight` against it, which §8 names.

## 15. S3 built (2026-11-01): docs and the builder skill

- **Docs.** `docs/operators.md` says how to ask for an Arrow answer (`Accept`, or `format=arrow` on
  `/sql` only), what comes back (the IPC body, the envelope less `rows` in `x-nuthatch-answer`), the
  parity guarantee and where the types differ from JSON, the caps and the memo, and Flight SQL as
  built: the build flag, `x-nuthatch-route`, refusals as gRPC statuses, the empty `GetFlightInfo`
  schema, and the catalogue calls not yet answered. `README.md` has a line on it. The `llms.txt`
  `init` scaffolds is unchanged: it is in the package NID, and `the_fixture_nests_keep_their_identities`
  pins the NIDs of the scaffolded fixtures, so a line there would move every pin. That is a decision
  about the scaffold, not a doc fix, and is left for one.
- **The builder skill.** `workflows.md` has an "Export rows as Arrow" recipe: when to ask for it
  (exports, dataframes) and when not (point reads), where the envelope is, `truncated` before
  trusting an export, and that a release binary has no `--flight`.
- **`nuthatch watch`'s SQL pane: nothing to build.** It has none. RFC-0069 §4 took `/sql` out of the
  watcher on purpose, because the old client's `count(*)` per refresh spent the nest's query budget on
  watching it, and §17, its S2 record, settles it as decision 8: "`/sql` is never read". A pane
  reading Arrow would add the `/sql` reader that RFC decided against, a change a user would see, so it
  is not built here.

**What stays unrun.** §8's clients, ADBC, JDBC, DBeaver and `pyarrow.flight`, have not been run
against `--flight` (§14). `pyarrow.flight` and ADBC's Python bindings cannot go into CI under the
org's no-Python rule; a non-Python ADBC driver manager, or DBeaver by hand, is what would close it,
and DBeaver needs the catalogue calls first.

**Complete.** S0 to S3 are built. Flight SQL's catalogue calls follow when a browsing client asks.

## 16. Release and client tested (2026-11-01)

GraphOps asked to move from the JSON interface to Flight. Two things stood between that and a user:
no release carried `--flight`, and no client but our own crate had spoken to it.

- **Released.** The release matrix has a fourth variant, `flight`, built `--features flight` for the
  default binary's three targets: `nuthatch-flight-aarch64-apple-darwin.tar.gz`,
  `nuthatch-flight-x86_64-unknown-linux-gnu.tar.gz` and `nuthatch-flight-aarch64-unknown-linux-gnu.tar.gz`,
  each with its `.sha256`, attested on the runner that built it and verified in `publish` with the
  rest. The default binary is unchanged and `flight` stays off by default; this is an extra artefact,
  as `graph` and `scaled` are. The container job publishes `:<version>-flight`, never `latest`, smoke
  tested for the flag before it is pushed: the operator who asked deploys containers, and the image is
  the default one with a different binary, so it costs a build and a push.
- **A dry run that cannot release.** `workflow_dispatch` takes `binaries: true` to build every tarball
  in the matrix and keep it as a workflow artifact. Every step that attaches, attests, pushes or
  undrafts is gated on a tag push, by its own `if:` or its job's, and
  `release_provenance::a_dispatch_never_releases` fails if one is not. Dry run: run 41320217634.
- **The catalogue calls.** `GetCatalogs`, `GetDbSchemas`, `GetTables` (with each table's schema when
  asked), `GetTableTypes` and `GetSqlInfo` are answered, which departure 3 of §14 left out. They are
  statements through the same door: `information_schema.tables` on the route, and a table's schema is
  its own `SELECT * … LIMIT 0`, so they list what `/sql` on that route answers and a bounded mount
  refuses them as it refuses SQL. The filter patterns are matched before a schema is asked for, so a
  client asking for one table costs one statement, not one per table. About 300 lines in `src/flight.rs`.
- **Refusals.** `401` now answers `Unauthenticated` and `404` `NotFound`, where both were
  `PermissionDenied` and `InvalidArgument`; `docs/operators.md` has the table.
- **The client.** Apache Arrow ADBC's Flight SQL driver, v1.12.0, in Go (`tools/flight-client`). It is
  the driver ADBC's C, Python, R and Java bindings load, so it is what a pandas, Polars or DuckDB user
  reaching for ADBC actually runs, and it shares no code with arrow-rs. JDBC was the alternative: it is
  what DBeaver uses, but it needs a JVM in CI for a driver fewer of the people asking would run.
  `tests/e2e_flight_clients.rs` runs the real binary with `--flight` over a runtime of two mounts of
  the recorded tab-parity contract, differing only in a view, and puts both clients to it:
  arrow-flight's own every time (the features test leg), ADBC in the `flight-clients` CI job. Each:
  1. runs `SELECT block_number, log_index, tx_hash, "from", "to", value, value_dec, value_overflow`
     over 2,259 rows and compares it with `POST /a/sql`'s JSON, value for value;
  2. reads `which_nest` on each route by `x-nuthatch-route` and gets that mount's answer;
  3. sends no route to the runtime and gets `NotFound`, and a missing column and gets
     `InvalidArgument` whose message is `/sql`'s JSON error naming it;
  4. lists the route's tables: ADBC's `GetObjects` at full depth, `GetTableSchema` and
     `GetTableTypes`; arrow-flight's `GetTables` with a pattern and the schema.
  Auth pass-through is in `flight::tests`: nuthatch checks no credential on `/sql`, so a router that
  demands one stands in for the gateway, and the bearer token reaches it while its `401` comes back as
  `Unauthenticated` with its JSON. Each check was mutated and goes red: the route ignored, `404` mapped to `InvalidArgument`, the
  schema left out of `GetTables`, the token dropped, `401` unmapped; the ADBC client on its own
  catches the first against the binary. A `__hot_` filter copied from the REPL survived its mutation
  because `/sql` never lists such a table, and was removed.

**What stays unrun.** JDBC, DBeaver and `pyarrow.flight`. A JDBC URL takes the route header as a
parameter, and DBeaver now has the calls it lists tables with, but neither has been run here.
