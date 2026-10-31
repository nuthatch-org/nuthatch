# RFC-0079: Arrow results - `/sql` in Arrow IPC, and Flight SQL for native clients

- Status: **Accepted (2026-10-30, Chief): "ok letsgoooo with the wave of rfcs".** Built in the order 0078, 0079, 0080, each slice on its own gate; an S0 that fails stops its RFC. Chief, the same day, on GraphOps's
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
