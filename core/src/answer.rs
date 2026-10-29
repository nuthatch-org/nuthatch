//! RFC-0075 §7: the query routes' answers without a web framework. The node's router and a tab's
//! worker build each response here, so one request gets the same status and bytes from both.

use crate::entity_row::{Row, Scalar};
use crate::entity_view::Applied;
use crate::views::BalanceView;
use serde_json::value::RawValue;
use serde_json::{json, Value};

pub const CONTENT_TYPE: &str = "application/json";

/// The per-result Rust-side byte ceiling for the guarded `/sql` surface (64 MiB). Comfortably above
/// any legitimate 50k-row result, far below the per-cursor RAM budget - the backstop against a
/// wide-cell `SELECT` inflating the materialised buffer past the budget. Part of `collect`'s
/// contract whenever a row cap is given.
pub const SQL_MAX_RESULT_BYTES: usize = 64 * 1024 * 1024;

/// A cheap lower-bound byte estimate of a materialised cell - dominated by string payloads, which is
/// exactly the wide-cell attack vector. Numbers/bools/null count a small fixed cost.
pub fn value_bytes(v: &Value) -> usize {
    match v {
        Value::String(s) => s.len(),
        Value::Array(a) => 8 + a.iter().map(value_bytes).sum::<usize>(),
        Value::Object(o) => {
            8 + o
                .iter()
                .map(|(k, x)| k.len() + value_bytes(x))
                .sum::<usize>()
        }
        _ => 8,
    }
}

/// What an offchain read is reproducible by (RFC-0045 §6).
pub const SNAPSHOT_REPRODUCIBLE: &str = "snapshot";

/// A route's answer: a status and a JSON body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Answer {
    pub status: u16,
    pub body: Vec<u8>,
}

impl Answer {
    pub fn json(status: u16, body: &Value) -> Answer {
        Answer {
            status,
            body: serde_json::to_vec(body).expect("a Value serialises"),
        }
    }

    pub fn ok(body: &Value) -> Answer {
        Answer::json(200, body)
    }
}

/// The page size the listing routes take: 100 unless asked, never more than 1,000.
pub fn listing_limit(limit: Option<usize>) -> usize {
    limit.unwrap_or(100).min(1000)
}

/// SEC-11: an internal error's chain can name absolute paths, so the caller gets none of it.
pub fn internal_error(msg: &str) -> Answer {
    tracing::warn!("internal error serving request: {msg}");
    Answer::json(500, &json!({ "error": "internal error" }))
}

pub fn not_found(id: &str) -> Answer {
    Answer::json(404, &json!({ "error": "not found", "id": id }))
}

/// An entity id, `{block}-{log_index}`.
pub fn parse_id(id: &str) -> Option<(u64, u64)> {
    let (b, l) = id.split_once('-')?;
    Some((b.parse().ok()?, l.parse().ok()?))
}

/// `/entity/{id}` from a hot row's stored text.
pub fn entity_hot(raw: &str) -> Answer {
    match serde_json::from_str::<Value>(raw) {
        Ok(v) => Answer::ok(&v),
        Err(e) => internal_error(&format!("{e:#}")),
    }
}

/// `/entity/{id}` from the sealed segments, after a hot miss.
pub fn entity_sealed(id: &str, found: Option<Value>) -> Answer {
    match found {
        Some(v) => Answer::ok(&v),
        None => not_found(id),
    }
}

/// `/entities`: the newest hot rows, from their stored text.
pub fn entities(rows: anyhow::Result<Vec<String>>) -> Answer {
    let items = rows.and_then(|rows| {
        rows.iter()
            .map(|r| {
                serde_json::from_str::<Value>(r)
                    .map_err(|e| anyhow::anyhow!(e).context("unparseable hot row"))
            })
            .collect::<anyhow::Result<Vec<Value>>>()
    });
    match items {
        Ok(items) => Answer::ok(&json!({ "count": items.len(), "items": items })),
        Err(e) => internal_error(&format!("{e:#}")),
    }
}

/// `/balances`: `top` is the view's first `limit` holders. Balances are decimal strings, since a
/// JSON number cannot carry an i128 and an f64 reader would corrupt a large one.
pub fn balances(view: &BalanceView, top: Vec<(String, i128)>) -> Answer {
    let items: Vec<Value> = top
        .into_iter()
        .map(|(address, balance)| json!({ "address": address, "balance": balance.to_string() }))
        .collect();
    // COR-8 (#814): a transfer over 38 digits is dropped, both legs, and the count says so; zero is
    // reported rather than omitted, so its absence means an old build rather than a clean nest.
    Answer::ok(&json!({
        "holders": view.holders(),
        "count": items.len(),
        "items": items,
        "dropped_over_i128": view.dropped_over_i128(),
    }))
}

/// `/balance/{address}`.
pub fn balance(view: &BalanceView, address: &str) -> Answer {
    let address = address.to_ascii_lowercase();
    match view.balance(&address) {
        Some(b) => Answer::ok(&json!({
            "address": address,
            "balance": b.to_string(),
            "dropped_over_i128": view.dropped_over_i128(),
        })),
        None => Answer::json(404, &json!({ "error": "no balance", "address": address })),
    }
}

/// What `/derived` needs to know of one entity besides its lane.
pub struct EntityFacts<'a> {
    pub name: &'a str,
    pub unavailable: Option<&'a str>,
    pub fault: Option<String>,
    /// Whether it reads an offchain table, which makes its answers reproducible by snapshot only.
    pub reads_offchain: bool,
}

/// `v` labelled reproducible by snapshot where the entity reads an offchain table, and naming the
/// snapshots its rows were applied through (#1437). The label follows from what it reads, so it is
/// there before the first snapshot lands; both are absent, not null, for an entity that reads none.
pub fn with_offchain(mut v: Value, reads_offchain: bool, applied: &Applied) -> Value {
    if let Some(obj) = v.as_object_mut() {
        if reads_offchain {
            obj.insert("reproducibility".into(), json!(SNAPSHOT_REPRODUCIBLE));
        }
        if let Some(offchain) = applied.offchain_json() {
            obj.insert("offchain".into(), offchain);
        }
    }
    v
}

/// One entity in `/derived`'s index.
pub fn derived_index_item(e: &EntityFacts, rows: usize, applied: &Applied, head: u64) -> Value {
    with_offchain(
        json!({
            "name": e.name,
            "rows": rows,
            "incremental": true,
            "applied_through": applied.through,
            "current": applied.through >= head,
            "available": e.unavailable.is_none() && e.fault.is_none(),
        }),
        e.reads_offchain,
        applied,
    )
}

/// One entity in `/ready`'s `entities`, and whether it makes the nest unready. `progress` is when its
/// watermark last moved, and `wedged` is the caller's verdict on that.
pub fn entity_readiness(
    e: &EntityFacts,
    rows: usize,
    applied: &Applied,
    (head, progress, now): (u64, u64, u64),
    wedged: bool,
) -> (Value, bool) {
    let behind = applied.through < head;
    // Unavailable is unready too: an entity holding no answer must not be reachable behind a 200.
    let unready = e.fault.is_some() || wedged || e.unavailable.is_some();
    let item = with_offchain(
        json!({
            "name": e.name,
            "applied_through": applied.through,
            "current": !behind,
            "catching_up": behind && e.fault.is_none() && !wedged && e.unavailable.is_none(),
            "rows": rows,
            "faulted": e.fault.is_some(),
            "fault": e.fault,
            "wedged": wedged,
            "unavailable": e.unavailable,
            "seconds_since_progress": (progress != 0).then(|| now.saturating_sub(progress)),
        }),
        e.reads_offchain,
        applied,
    );
    (item, unready)
}

/// `/derived`.
pub fn derived_index(head: u64, items: Vec<Value>) -> Answer {
    Answer::ok(&json!({ "count": items.len(), "head": head, "entities": items }))
}

/// Why an entity cannot answer, decided once for the keyed and listing routes (RFC-0041 §5.1). A
/// faulted or unavailable one holds no answer: a 503. One catching up is served with `current: false`
/// and its own watermark, never the nest's head.
pub fn derived_refusal(e: &EntityFacts) -> Option<Answer> {
    if let Some(why) = e.unavailable {
        return Some(Answer::json(
            503,
            &json!({ "error": "entity unavailable", "entity": e.name, "reason": why }),
        ));
    }
    if let Some(why) = &e.fault {
        return Some(Answer::json(
            503,
            &json!({ "error": "entity faulted", "entity": e.name, "reason": why }),
        ));
    }
    None
}

pub fn no_such_entity(name: &str, known: &[&str]) -> Answer {
    Answer::json(
        404,
        &json!({ "error": "no such entity", "entity": name, "entities": known }),
    )
}

/// Which entity answered, how far it has folded, and that the answer came from maintained state.
pub fn derived_provenance(
    nid: Option<&str>,
    e: &EntityFacts,
    head: u64,
    applied: &Applied,
) -> Value {
    with_offchain(
        json!({
            "nid": nid,
            "entity": e.name,
            "incremental": true,
            "from_maintained_state": true,
            "applied_through": applied.through,
            "dataset_head": head,
            "current": applied.through >= head,
        }),
        e.reads_offchain,
        applied,
    )
}

/// `/derived/{entity}`: `rows` is the relation's size, `first` its first page.
pub fn derived_all(name: &str, rows: usize, first: &[(Row, Row)], provenance: Value) -> Answer {
    let items: Vec<Value> = first
        .iter()
        .map(|(k, v)| {
            json!({
                "key": k.0.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
                "row": v.0.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
            })
        })
        .collect();
    Answer::ok(&json!({
        "entity": name,
        "rows": rows,
        "returned": items.len(),
        "items": items,
        "provenance": provenance,
    }))
}

/// A `/derived/{entity}/{key}` path segment as the readings of each key part. A composite key's
/// parts are joined by the unit separator, which no address or decimal contains.
pub fn key_readings(key: &str) -> Vec<Vec<Scalar>> {
    key.split('\u{1f}').map(key_part_readings).collect()
}

/// The integer or bool a part spells, the text itself, and a `0x` text lowercased, as addresses and
/// hashes are stored (#2205). At most two per part.
fn key_part_readings(part: &str) -> Vec<Scalar> {
    let mut out = Vec::with_capacity(2);
    if let Ok(n) = part.parse::<i128>() {
        out.push(Scalar::Int(n));
    } else if let Ok(b) = part.parse::<bool>() {
        out.push(Scalar::Bool(b));
    }
    out.push(Scalar::Str(part.to_string()));
    let lower = part.to_ascii_lowercase();
    if lower.starts_with("0x") && lower != part {
        out.push(Scalar::Str(lower));
    }
    out
}

/// `/derived/{entity}/{key}`.
pub fn derived_key(name: &str, key: &str, found: Option<Row>, provenance: Value) -> Answer {
    match found {
        Some(row) => Answer::ok(&json!({
            "key": key,
            "row": row.0.iter().map(|c| c.to_string()).collect::<Vec<_>>(),
            "provenance": provenance,
        })),
        None => Answer::json(
            404,
            &json!({
                "error": "no such key",
                "entity": name,
                "key": key,
                "provenance": provenance,
            }),
        ),
    }
}

/// The body `POST /sql` and `POST /explain` take.
#[derive(Debug, serde::Deserialize)]
pub struct SqlRequest {
    pub q: String,
    /// The row cap for this request, clamped by the route; absent is the nest's cap.
    #[serde(default)]
    pub max_rows: Option<usize>,
}

/// A POST body as a [`SqlRequest`], or the 400 refusing it in the route's own envelope.
pub fn sql_request(body: &[u8], explain: bool) -> Result<SqlRequest, Answer> {
    serde_json::from_slice(body).map_err(|e| {
        let error = format!("the body must be {{\"q\": \"...\", \"max_rows\": n}}: {e}");
        let body = if explain {
            json!({ "valid": false, "error": error })
        } else {
            json!({ "error": error })
        };
        Answer::json(400, &body)
    })
}

pub fn sql_too_long(len: usize, max: usize) -> Answer {
    Answer::json(
        400,
        &json!({ "error": format!("query too long: {len} bytes (max {max})") }),
    )
}

pub fn explain_too_long() -> Answer {
    Answer::json(400, &json!({ "valid": false, "error": "query too long" }))
}

/// An answer's facts besides its rows, for `/sql`'s envelope.
pub struct SqlFacts<'a> {
    pub count: usize,
    pub truncated: bool,
    pub degraded_tables: &'a std::collections::BTreeSet<String>,
    pub tip_unavailable: bool,
    pub columns: &'a [String],
    pub cached: bool,
    pub as_of: Option<u64>,
    pub sealed_through: u64,
    pub registry_hash: Option<&'a str>,
    pub nid: Option<&'a str>,
    /// The maintained relations that answered, or null where that is not known.
    pub entities: Value,
    /// Whether the answer read offchain data, directly or through an entity.
    pub reproducible_by_snapshot: bool,
    /// The offchain snapshots it read, where it read any.
    pub offchain: Option<Value>,
}

/// One maintained relation in `/sql`'s `provenance.entities`: `through` is the watermark captured with
/// the rows (#932), not a fresh read.
pub fn sql_entity_item(e: &EntityFacts, through: u64, head: u64, applied: &Applied) -> Value {
    with_offchain(
        json!({
            "entity": e.name,
            "incremental": true,
            "applied_through": through,
            "current": through >= head,
        }),
        e.reads_offchain,
        applied,
    )
}

/// `/sql`'s envelope with `rows` in place; [`sql_bytes`] writes serialized rows into it instead.
pub fn sql_envelope(f: &SqlFacts, rows: Value) -> Value {
    let mut response = json!({
        "count": f.count,
        "truncated": f.truncated,
        // Cold data was incomplete (#435): a listed segment could not be read and its table was
        // reduced. Table names only; segment paths never reach this untrusted surface.
        "degraded": !f.degraded_tables.is_empty(),
        "degraded_tables": f.degraded_tables,
        // The hot-tip scan itself failed (#472), which is a different cause from a cold reduction.
        "tip_unavailable": f.tip_unavailable,
        // The query's column order (#1609), which the row objects do not keep.
        "columns": f.columns,
        "rows": rows,
        "cached": f.cached,
        "provenance": {
            "as_of": f.as_of,
            "sealed_through": f.sealed_through,
            "source": if f.tip_unavailable { "sealed" } else { "hot+sealed" },
            "registry_hash": f.registry_hash,
            "nid": f.nid,
            "entities": f.entities,
        },
    });
    if let Some(provenance) = response["provenance"].as_object_mut() {
        if f.reproducible_by_snapshot {
            provenance.insert("reproducibility".into(), json!(SNAPSHOT_REPRODUCIBLE));
        }
        if let Some(read) = &f.offchain {
            provenance.insert("offchain".into(), read.clone());
        }
    }
    response
}

struct WithRows<'a>(&'a Value, &'a RawValue);

impl serde::Serialize for WithRows<'_> {
    fn serialize<S: serde::Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let Some(fields) = self.0.as_object() else {
            return self.0.serialize(ser);
        };
        let mut map = ser.serialize_map(Some(fields.len()))?;
        for (k, v) in fields {
            if k == "rows" {
                map.serialize_entry(k, self.1)?;
            } else {
                map.serialize_entry(k, v)?;
            }
        }
        map.end()
    }
}

/// An envelope's bytes, with `rows` taken from `serialized` where the rows are already JSON text.
pub fn sql_bytes(envelope: &Value, serialized: Option<&RawValue>) -> serde_json::Result<Vec<u8>> {
    match serialized {
        None => serde_json::to_vec(envelope),
        Some(rows) => serde_json::to_vec(&WithRows(envelope, rows)),
    }
}

pub fn explain_valid(tip_unavailable: bool) -> Answer {
    Answer::ok(&json!({
        "valid": true,
        "note": "query binds; run it with the sql tool",
        "tip_unavailable": tip_unavailable,
    }))
}

pub fn explain_invalid(msg: &str) -> Answer {
    Answer::json(400, &json!({ "valid": false, "error": msg }))
}

/// How the cursor follows, as `/ready` reports it (RFC-0040 §4).
pub struct Freshness {
    pub mode: &'static str,
    pub poll_interval_secs: u64,
    pub poll_interval_source: &'static str,
}

/// Everything `/ready` reports, gathered by whoever holds the cursor.
pub struct Ready {
    pub version: String,
    pub stalled: bool,
    pub wedged: bool,
    pub initial_failure: bool,
    pub cursorless: bool,
    pub tip: u64,
    pub last: u64,
    pub lag: u64,
    pub sealed: u64,
    pub freshness: Freshness,
    pub last_poll: u64,
    pub age: Option<u64>,
    pub now: u64,
    pub seal_direct_active: bool,
    pub seal_direct_origin: u64,
    pub seal_direct_completed: u64,
    pub seal_direct_fetched: u64,
    pub fetch_window: u64,
    pub seal_direct_target: u64,
    pub seal_direct_concurrency: Option<(u64, u64)>,
    pub ipfs_gave_up_documents: Option<u64>,
    pub ipfs_window_deadline: Option<u64>,
    pub seal_stalled: bool,
    pub tip_seal_stalled: bool,
    pub seal_faulted: bool,
    pub last_seal_progress: u64,
    pub entities: Value,
    pub entities_stalled: bool,
    /// `(file, view, error)` for each authored view that failed to define.
    pub views_failed: Vec<(String, String, String)>,
    pub folds: Option<Value>,
    pub undecoded: Value,
    pub seal_fault: Option<(u64, String)>,
    pub maintained: Option<Value>,
}

/// `/ready`: 503 while stalled.
pub fn ready(r: &Ready) -> Answer {
    // Null, not 0, where there is no tip or lag yet: 0 reads as "exactly at tip" (#1020, #2342).
    let unpolled = r.cursorless || r.last_poll == 0;
    let mut body = json!({
        "version": r.version,
        "ready": !r.stalled,
        "stalled": r.stalled,
        "wedged": r.wedged,
        "initial_poll_failed": r.initial_failure,
        "tip": if unpolled { Value::Null } else { r.tip.into() },
        "last_block": r.last,
        "lag_blocks": if unpolled { Value::Null } else { r.lag.into() },
        "cursorless": r.cursorless,
        "freshness": {
            "mode": r.freshness.mode,
            "poll_interval_secs": r.freshness.poll_interval_secs,
            "poll_interval_source": r.freshness.poll_interval_source,
        },
        "sealed_through": r.sealed,
        // How far the seal trails the cursor (#1199): a fact, not a verdict, and null before
        // anything has sealed, since the whole history is not lag.
        "seal_lag_blocks": if r.cursorless || r.sealed == 0 {
            Value::Null
        } else {
            r.last.saturating_sub(r.sealed).into()
        },
        "last_poll_unixtime": r.last_poll,
        "seconds_since_poll": r.age,
        "seal_direct_active": r.seal_direct_active,
        "seal_direct_origin": r.seal_direct_origin,
        "seal_direct_completed": r.seal_direct_completed,
        // How far fetching has run ahead of what a restart resumes from (#1169).
        "seal_direct_fetched": r.seal_direct_fetched,
        // The cursor's current getLogs span (#1170).
        "fetch_window_blocks": r.fetch_window,
        "seal_direct_target": r.seal_direct_target,
        // One `--rpc` runs the pass a window at a time (#1399); null when no pass recorded it.
        "seal_direct_concurrency": r.seal_direct_concurrency.map(|(requested, effective)| json!({
            "requested": requested,
            "effective": effective,
            "capped_by": (effective < requested).then_some("single_rpc_endpoint"),
        })),
        // Documents given up on are absent from sealed history (#1410), listed at `/ipfs/gave-up`.
        "ipfs_gave_up_documents": r.ipfs_gave_up_documents,
        "seal_direct_ipfs_window_deadline_secs": if r.cursorless {
            Value::Null
        } else {
            r.ipfs_window_deadline.into()
        },
        "seal_direct_stalled": r.seal_stalled,
        "tip_seal_stalled": r.tip_seal_stalled,
        "seal_faulted": r.seal_faulted,
        "seconds_since_seal_progress": (r.last_seal_progress != 0)
            .then(|| r.now.saturating_sub(r.last_seal_progress)),
        "entities": r.entities,
        "entities_stalled": r.entities_stalled,
        // One broken view must not take a serving nest out of rotation (#1653).
        "views_failed": r.views_failed
            .iter()
            .map(|(file, view, error)| json!({ "file": file, "view": view, "error": error }))
            .collect::<Vec<_>>(),
    });
    if let Some(folds) = &r.folds {
        body["folds"] = folds.clone();
    }
    body["undecoded"] = r.undecoded.clone();
    if let Some((since, reason)) = &r.seal_fault {
        body["seal_fault"] = json!({ "reason": reason, "since_unixtime": since });
    }
    if let Some(maintained) = &r.maintained {
        body["maintained"] = maintained.clone();
    }
    Answer::json(if r.stalled { 503 } else { 200 }, &body)
}
