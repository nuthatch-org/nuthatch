//! RFC-0075 S5: the node's query routes answered in the tab. Each body is built by
//! `nuthatch_core::answer`, the module the node's router calls, over the tab's store, views,
//! entities and Burrmill, so one request gets the node's status and bytes.

use crate::cellar::Cellar;
use crate::derived::NestFiles;
use crate::query::{referenced_tables, TabQuery};
use crate::tab::{unix_now, TabNest};
use nuthatch_core::answer::{self, Answer};
use nuthatch_core::store::SEALED_THROUGH_KEY;
use serde_json::{json, Value};
use std::sync::atomic::Ordering::Relaxed;

// The node's, from `src/serve.rs` and `src/ipfs_resolve.rs`.
const SQL_MAX_QUERY_LEN: usize = 16 * 1024;
const SQL_MAX_ROWS: usize = 50_000;
const IPFS_WINDOW_DEADLINE_SECS: u64 = 300;
const IPFS_GAVE_UP_PREFIX: &str = "ipfs_gave_up:";

/// The nest a request is answered over, and what the page says of it.
pub struct Nest<'a> {
    pub nest: &'a TabNest,
    pub cellar: &'a Cellar,
    pub files: &'a NestFiles,
    pub nid: Option<&'a str>,
    pub version: &'a str,
    /// The page's poll interval, where it set one; the node's `--poll-interval`.
    pub poll_interval_secs: Option<u64>,
    /// The getLogs span the cursor asks for.
    pub log_window: u64,
}

/// `method` and `target` (a path with its query) as the node's router would answer them; a path it
/// does not route is a 404 with no body, as axum gives one.
pub async fn answer(n: &Nest<'_>, method: &str, target: &str, body: &[u8]) -> Answer {
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let params: Vec<(String, String)> = form_urlencoded::parse(query.as_bytes())
        .into_owned()
        .collect();
    let param = |k: &str| params.iter().find(|(n, _)| n == k).map(|(_, v)| v.clone());
    let number = |k: &str| -> Result<Option<usize>, Answer> {
        param(k)
            .map(|v| v.parse().map_err(|e| rejected(&format!("{k}: {e}"))))
            .transpose()
    };
    let path = path
        .strip_suffix('/')
        .filter(|p| !p.is_empty())
        .unwrap_or(path);
    let segs: Vec<String> = path
        .trim_start_matches('/')
        .split('/')
        .map(|s| {
            percent_encoding::percent_decode_str(s)
                .decode_utf8_lossy()
                .into_owned()
        })
        .collect();
    let segs: Vec<&str> = segs.iter().map(String::as_str).collect();
    let store = &n.nest.store;
    let routed = async {
        Ok(match (method, segs.as_slice()) {
            ("GET", ["ready"]) => ready(n),
            ("GET", ["sql"]) | ("GET", ["explain"]) => {
                let q = param("q").ok_or_else(|| rejected("missing field `q`"))?;
                let max_rows = number("max_rows")?;
                route_sql(n, segs[0] == "explain", answer::SqlRequest { q, max_rows }).await
            }
            ("POST", ["sql"]) | ("POST", ["explain"]) => {
                let explain = segs[0] == "explain";
                route_sql(n, explain, answer::sql_request(body, explain)?).await
            }
            ("GET", ["entity", id]) => entity(n, id).await,
            ("GET", ["entities"]) => {
                answer::entities(store.recent(answer::listing_limit(number("limit")?)))
            }
            ("GET", ["balances"]) => {
                let top = n.nest.balances.top(answer::listing_limit(number("limit")?));
                answer::balances(&n.nest.balances, top)
            }
            ("GET", ["balance", address]) => answer::balance(&n.nest.balances, address),
            ("GET", ["derived"]) => {
                let head = dataset_head(n);
                let items = n
                    .nest
                    .entities
                    .iter()
                    .map(|e| {
                        let (rows, applied) = e.lane().len_and_watermark();
                        answer::derived_index_item(&e.facts(), rows, &applied, head)
                    })
                    .collect();
                answer::derived_index(head, items)
            }
            ("GET", ["derived", name]) => derived(n, name, None, number("limit")?),
            ("GET", ["derived", name, key]) => derived(n, name, Some(key), None),
            _ => Answer {
                status: 404,
                body: Vec::new(),
            },
        })
    };
    routed.await.unwrap_or_else(|refused: Answer| refused)
}

/// axum's refusal of a query string its extractor cannot read: plain text, not JSON.
fn rejected(why: &str) -> Answer {
    Answer {
        status: 400,
        body: format!("Failed to deserialize query string: {why}").into_bytes(),
    }
}

/// The content type the node sends with `a`: JSON, except an extractor's plain-text refusal, and
/// none with an unrouted path's empty body.
pub fn content_type(a: &Answer) -> &'static str {
    if a.body.is_empty() {
        ""
    } else if !a.body.starts_with(b"{") {
        "text/plain; charset=utf-8"
    } else {
        answer::CONTENT_TYPE
    }
}

fn dataset_head(n: &Nest) -> u64 {
    n.nest.last_block().ok().flatten().unwrap_or(0)
}

fn sealed_through(n: &Nest) -> u64 {
    n.nest
        .store
        .get_meta(SEALED_THROUGH_KEY)
        .ok()
        .flatten()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0)
}

fn derived(n: &Nest, name: &str, key: Option<&str>, limit: Option<usize>) -> Answer {
    let Some(entity) = n.nest.entities.iter().find(|e| e.facts().name == name) else {
        let known: Vec<&str> = n.nest.entities.iter().map(|e| e.facts().name).collect();
        return answer::no_such_entity(name, &known);
    };
    let facts = entity.facts();
    if let Some(refused) = answer::derived_refusal(&facts) {
        return refused;
    }
    let head = dataset_head(n);
    match key {
        Some(key) => {
            let (found, applied) = entity.lane().get_with_watermark(&answer::key_readings(key));
            let provenance = answer::derived_provenance(n.nid, &facts, head, &applied);
            answer::derived_key(name, key, found, provenance)
        }
        None => {
            let (rows, first, applied) = entity
                .lane()
                .head_rows_with_watermark(answer::listing_limit(limit));
            let provenance = answer::derived_provenance(n.nid, &facts, head, &applied);
            answer::derived_all(name, rows, &first, provenance)
        }
    }
}

async fn bind(n: &Nest<'_>) -> anyhow::Result<TabQuery> {
    TabQuery::bind(
        &n.nest.store,
        n.cellar,
        &n.nest.registry().schema(),
        &n.nest.entities,
        n.files,
        n.nest.budget,
    )
    .await
}

/// An engine error with the node's hint appended (RFC-0016 §3). There are no paths to scrub.
fn engine_error(e: &burrmill::BurrmillError, sql: &str, n: &Nest) -> String {
    let raw = format!("{e}");
    match crate::sql_errors::enrich(&raw, sql, &n.nest.registry().schema()) {
        Some(hint) => format!("{raw}\n\nhint: {hint}"),
        None => raw,
    }
}

/// One `/sql` at a time: a statement yields at each batch, and without this a second could start
/// and hold its own `SQL_MAX_RESULT_BYTES` beside the first's.
static SQL_ONE_AT_A_TIME: futures::lock::Mutex<()> = futures::lock::Mutex::new(());

async fn route_sql(n: &Nest<'_>, explain: bool, q: answer::SqlRequest) -> Answer {
    let _one = SQL_ONE_AT_A_TIME.lock().await;
    if q.q.len() > SQL_MAX_QUERY_LEN {
        return if explain {
            answer::explain_too_long()
        } else {
            answer::sql_too_long(q.q.len(), SQL_MAX_QUERY_LEN)
        };
    }
    let refuse = |msg: String| {
        if explain {
            answer::explain_invalid(&msg)
        } else {
            Answer::json(400, &json!({ "error": msg }))
        }
    };
    let referenced = match referenced_tables(&q.q, n.files) {
        None => None,
        Some(Ok(tables)) => Some(tables),
        Some(Err(e)) => return refuse(engine_error(&e, &q.q, n)),
    };
    let engine = match bind(n).await {
        Ok(engine) => engine,
        Err(e) => return answer::internal_error(&format!("{e:#}")),
    };
    if explain {
        return match engine.plan(&q.q) {
            Ok(()) => answer::explain_valid(false),
            Err(e) => refuse(engine_error(&e, &q.q, n)),
        };
    }
    let cap = q.max_rows.unwrap_or(SQL_MAX_ROWS).clamp(1, SQL_MAX_ROWS);
    let (rows, columns, truncated) = match engine.collect(&q.q, cap).await {
        Ok(out) => out,
        Err(e) => return refuse(engine_error(&e, &q.q, n)),
    };
    let head = dataset_head(n);
    let entities = match &referenced {
        None => Value::Null,
        Some(names) => {
            let used: Vec<Value> = n
                .nest
                .entities
                .iter()
                .filter(|e| names.contains(&e.name_lower()))
                .map(|e| {
                    let (_, applied) = e.lane().len_and_watermark();
                    answer::sql_entity_item(&e.facts(), applied.through, head, &applied)
                })
                .collect();
            if used.is_empty() {
                Value::Null
            } else {
                Value::Array(used)
            }
        }
    };
    let registry_hash = format!("0x{}", hex::encode(n.nest.registry().hash()));
    let envelope = answer::sql_envelope(
        &answer::SqlFacts {
            count: rows.len(),
            truncated,
            degraded_tables: &Default::default(),
            tip_unavailable: false,
            columns: &columns,
            cached: false,
            as_of: n.nest.last_block().ok().flatten(),
            sealed_through: sealed_through(n),
            registry_hash: Some(&registry_hash),
            nid: n.nid,
            entities,
            reproducible_by_snapshot: false,
            offchain: None,
        },
        Value::Array(rows),
    );
    match answer::sql_bytes(&envelope, None) {
        Ok(body) => Answer { status: 200, body },
        Err(e) => Answer::json(
            500,
            &json!({ "error": format!("serializing an answer: {e}") }),
        ),
    }
}

/// A hot hit, else the sealed segments: probe each table for the id, then read the row from the one
/// that has it, in the shape a single-table query gives, as the node's `read_sealed` does.
async fn entity(n: &Nest<'_>, id: &str) -> Answer {
    match n.nest.store.get_entity(id) {
        Ok(Some(raw)) => return answer::entity_hot(&raw),
        Ok(None) => {}
        Err(e) => return answer::internal_error(&format!("{e:#}")),
    }
    let Some((block, log_index)) = answer::parse_id(id) else {
        return answer::not_found(id);
    };
    let tables: Vec<String> = n.cellar.manifest().tables.keys().cloned().collect();
    if tables.is_empty() {
        return answer::not_found(id);
    }
    let engine = match bind(n).await {
        Ok(engine) => engine,
        Err(e) => return answer::internal_error(&format!("{e:#}")),
    };
    let probe: Vec<String> = tables
        .iter()
        .map(|t| {
            format!(
                "SELECT '{}' AS t FROM \"{t}\" WHERE block_number = {block} AND log_index = {log_index}",
                t.replace('\'', "''")
            )
        })
        .collect();
    let found = match engine
        .collect(
            &format!("{} ORDER BY t LIMIT 1", probe.join(" UNION ALL ")),
            1,
        )
        .await
    {
        Ok((rows, _, _)) => rows,
        Err(e) => return answer::internal_error(&format!("{e}")),
    };
    let Some(table) = found.first().and_then(|r| r["t"].as_str()) else {
        return answer::not_found(id);
    };
    let sql = format!(
        "SELECT * FROM \"{table}\" WHERE block_number = {block} AND log_index = {log_index} LIMIT 1"
    );
    match engine.collect(&sql, 1).await {
        Ok((rows, _, _)) => answer::entity_sealed(id, rows.into_iter().next()),
        Err(e) => answer::internal_error(&format!("{e}")),
    }
}

fn ready(n: &Nest) -> Answer {
    let store = &n.nest.store;
    let cursor = &n.nest.cursor;
    let now = unix_now();
    let tip = cursor.tip.load(Relaxed);
    let last = dataset_head(n);
    let last_poll = cursor.last_poll.load(Relaxed);
    let head = last;
    let mut entities_stalled = false;
    let entities: Vec<Value> = n
        .nest
        .entities
        .iter()
        .map(|e| {
            let (rows, applied) = e.lane().len_and_watermark();
            // Folded inline in the window that carried them, so an entity cannot wedge behind the
            // cursor here.
            let (item, unready) = answer::entity_readiness(
                &e.facts(),
                rows,
                &applied,
                (head, e.lane().last_progress(), now),
                false,
            );
            entities_stalled |= unready;
            item
        })
        .collect();
    let (poll_interval_secs, source) = match n.poll_interval_secs {
        Some(secs) => (secs, "flag"),
        None => (0, "block_time"),
    };
    let undecoded = cursor
        .undecoded
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .summary();
    answer::ready(&answer::Ready {
        version: n.version.to_string(),
        stalled: entities_stalled,
        wedged: false,
        initial_failure: false,
        cursorless: false,
        tip,
        last,
        lag: tip.saturating_sub(last),
        sealed: sealed_through(n),
        freshness: answer::Freshness {
            mode: "tip",
            poll_interval_secs,
            poll_interval_source: source,
        },
        last_poll,
        age: (last_poll != 0).then(|| now.saturating_sub(last_poll)),
        now,
        // A tab has no seal-direct pass: it seals from its store as the tip path does.
        seal_direct_active: false,
        seal_direct_origin: 0,
        seal_direct_completed: 0,
        seal_direct_fetched: 0,
        fetch_window: n.log_window,
        seal_direct_target: 0,
        seal_direct_concurrency: None,
        ipfs_gave_up_documents: store.count_meta_with_prefix(IPFS_GAVE_UP_PREFIX).ok(),
        ipfs_window_deadline: Some(IPFS_WINDOW_DEADLINE_SECS),
        seal_stalled: false,
        tip_seal_stalled: false,
        seal_faulted: false,
        last_seal_progress: cursor.last_seal_progress.load(Relaxed),
        entities: Value::Array(entities),
        entities_stalled,
        views_failed: Vec::new(),
        folds: None,
        undecoded,
        seal_fault: None,
        maintained: None,
    })
}
