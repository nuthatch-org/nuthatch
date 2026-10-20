//! The comparison half of `scripts/lodestar-parity.sh` (#1076): the Lodestar nest against the Graph
//! Network subgraph at one pinned block. The script reads the nest's `/ready`, pins the block and
//! counts the views; this reads both sides at that pin and decides.
//!
//! Configured by the script through the environment: `GRAPH_API_KEY`, `BLOCK`, `PARITY_MODE`,
//! `NETWORK_SG`, `GATEWAY`, `NEST`, `ALLOC_N`, `EPOCH_N`, `ESCROW_N`, `EPOCH_PARITY_FROM` and
//! `EPOCH_SUMMARY`, the file the gated window and the known differences are written to.
//!
//! Exit status: 0 agrees, 1 disagrees or could not compare, 3 head mode could not be measured,
//! 4 the subgraph side did not answer (#1818).

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::process::ExitCode;
use std::time::Duration;

use serde_json::{json, Map, Value};

/// How a run stops early: the status, and what to print to stderr on the way out.
struct Stop {
    code: u8,
    msg: Option<String>,
}

type R<T> = Result<T, Stop>;

fn die(msg: impl Into<String>) -> Stop {
    Stop {
        code: 1,
        msg: Some(msg.into()),
    }
}

fn env(k: &str) -> R<String> {
    std::env::var(k).map_err(|_| die(format!("{k} is not set")))
}

/// What Python's `str()` printed for these values, which the output lines and comparisons rest on.
fn py_str(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Null => "None".into(),
        Value::Bool(true) => "True".into(),
        Value::Bool(false) => "False".into(),
        other => other.to_string(),
    }
}

fn truthy(v: Option<&Value>) -> bool {
    match v {
        None | Some(Value::Null) | Some(Value::Bool(false)) => false,
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(a)) => !a.is_empty(),
        Some(Value::Object(o)) => !o.is_empty(),
        Some(Value::Number(n)) => n.as_f64() != Some(0.0),
        Some(Value::Bool(true)) => true,
    }
}

fn parse_int(s: &str) -> Option<i128> {
    let t = s.trim();
    t.strip_prefix('+').unwrap_or(t).parse().ok()
}

/// An integer from a JSON number or a decimal string, exactly.
fn int_of(v: &Value) -> Option<i128> {
    match v {
        Value::String(s) => parse_int(s),
        Value::Number(n) => {
            let s = n.to_string();
            parse_int(&s).or_else(|| s.parse::<f64>().ok().map(|f| f.trunc() as i128))
        }
        Value::Bool(b) => Some(*b as i128),
        _ => None,
    }
}

fn int(v: &Value, what: &str) -> R<i128> {
    int_of(v).ok_or_else(|| die(format!("{what} is not an integer: {}", py_str(v))))
}

fn field<'a>(row: &'a Value, k: &str) -> R<&'a Value> {
    row.get(k)
        .ok_or_else(|| die(format!("a row has no `{k}`: {row}")))
}

fn ekey(e: &str) -> i128 {
    parse_int(e).unwrap_or(i128::MIN)
}

/// `application/x-www-form-urlencoded`, as `urllib.parse.urlencode` wrote it.
fn form_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

fn head_chars(s: &str, n: usize) -> String {
    s.chars().take(n).collect()
}

struct Ctx {
    key: String,
    block: i128,
    head_mode: bool,
    url: String,
    nest: String,
    failed: bool,
}

impl Ctx {
    fn not_measured(&self, why: &str) -> Stop {
        eprintln!("NOT MEASURED at head {}: {why}", self.block);
        Stop { code: 3, msg: None }
    }

    /// A disagreement already printed outranks the gateway failing afterwards.
    fn rig_fault(&self, why: &str) -> Stop {
        let why = why.replace(&format!("/api/{}/", self.key), "/api/<GRAPH_API_KEY>/");
        eprintln!("NOT RUN {why}");
        Stop {
            code: if self.failed { 1 } else { 4 },
            msg: None,
        }
    }

    fn gql(&self, query: &str) -> R<Map<String, Value>> {
        let payload = json!({ "query": query }).to_string();
        // file:// answers carry no status; the tests use it as a gateway.
        let body = if let Some(path) = self.url.strip_prefix("file://") {
            std::fs::read_to_string(path)
                .map_err(|e| self.rig_fault(&format!("subgraph did not answer: {e}: {path}")))?
        } else {
            match ureq::post(&self.url)
                .set("Content-Type", "application/json")
                .set("User-Agent", "nuthatch-lodestar-parity")
                .timeout(Duration::from_secs(60))
                .send_string(&payload)
            {
                Ok(r) => {
                    let status = r.status();
                    let body = r
                        .into_string()
                        .map_err(|e| self.rig_fault(&format!("subgraph did not answer: {e}")))?;
                    if status != 200 {
                        return Err(self.rig_fault(&format!(
                            "subgraph HTTP {status}: {}",
                            head_chars(&body, 300)
                        )));
                    }
                    body
                }
                Err(ureq::Error::Status(code, r)) => {
                    let mut bytes = Vec::new();
                    let _ = std::io::Read::read_to_end(
                        &mut std::io::Read::take(r.into_reader(), 300),
                        &mut bytes,
                    );
                    return Err(self.rig_fault(&format!(
                        "subgraph HTTP {code}: {}",
                        String::from_utf8_lossy(&bytes)
                    )));
                }
                Err(e) => return Err(self.rig_fault(&format!("subgraph did not answer: {e}"))),
            }
        };
        let d = match serde_json::from_str::<Value>(&body) {
            Ok(Value::Object(d)) => d,
            _ => {
                return Err(self.rig_fault(&format!(
                    "subgraph returned a malformed answer: {}",
                    head_chars(&body, 300)
                )))
            }
        };
        if truthy(d.get("errors")) {
            let errors = d["errors"].to_string();
            // The gateway may answer `_meta` from one indexer and a pinned query from another behind it.
            if self.head_mode && errors.contains("indexed up to block") {
                return Err(self.not_measured(&format!(
                    "an indexer behind the gateway is short of the pin: {errors}"
                )));
            }
            return Err(self.rig_fault(&format!("subgraph graphql error: {errors}")));
        }
        match d.get("data") {
            Some(Value::Object(data)) if !data.is_empty() => Ok(data.clone()),
            _ => Err(self.rig_fault(&format!(
                "subgraph returned no data: {}",
                head_chars(&body, 300)
            ))),
        }
    }

    fn nest_sql(&self, q: &str) -> R<Vec<Value>> {
        let url = format!("{}/sql?q={}", self.nest, form_encode(q));
        let body = ureq::get(&url)
            .set("User-Agent", "nuthatch-lodestar-parity")
            .timeout(Duration::from_secs(30))
            .call()
            .map_err(|e| die(format!("nest /sql failed: {e}")))?
            .into_string()
            .map_err(|e| die(format!("nest /sql failed: {e}")))?;
        let d: Value = serde_json::from_str(&body)
            .map_err(|e| die(format!("nest /sql answered malformed JSON ({e}) for: {q}")))?;
        if truthy(d.get("error")) {
            return Err(die(format!("nest sql error: {}", py_str(&d["error"]))));
        }
        let rows = match d.get("rows") {
            Some(Value::Array(rows)) => rows.clone(),
            Some(Value::Null) | None => Vec::new(),
            Some(other) => return Err(die(format!("nest /sql rows is not a list: {other}"))),
        };
        // `/sql` caps a response (50,000 rows when this was written) and flags it. A capped read is a
        // partial population, and comparing a partial population is how an absent comparison reads as
        // agreement. This is not hypothetical: the escrow join in #1114 was first attempted against a
        // silently capped 50,000 of 70,417 rows and produced a confident, entirely fictional answer.
        if truthy(d.get("truncated")) {
            return Err(die(format!(
                "nest /sql truncated the response ({} rows) for: {q}",
                rows.len()
            )));
        }
        Ok(rows)
    }

    fn rows_of(&self, data: &Map<String, Value>, entity: &str) -> R<Vec<Value>> {
        match data.get(entity) {
            Some(Value::Array(rows)) => Ok(rows.clone()),
            Some(Value::Null) | None => Ok(Vec::new()),
            Some(other) => Err(die(format!("subgraph {entity} is not a list: {other}"))),
        }
    }

    fn page_ids(&self, entity: &str, extra_where: &str) -> R<Vec<Value>> {
        let mut out = Vec::new();
        let mut last = String::new();
        loop {
            let mut parts = vec![
                "first: 1000".to_string(),
                "orderBy: id".into(),
                "orderDirection: asc".into(),
                format!("block: {{ number: {} }}", self.block),
            ];
            let mut cond = Vec::new();
            if !extra_where.trim().is_empty() {
                cond.push(extra_where.trim().to_string());
            }
            if !last.is_empty() {
                cond.push(format!("id_gt: \"{last}\""));
            }
            if !cond.is_empty() {
                parts.push(format!("where: {{ {} }}", cond.join(", ")));
            }
            let data = self.gql(&format!("{{ {entity}({}) {{ id }} }}", parts.join(" ")))?;
            let rows = self.rows_of(&data, entity)?;
            for r in &rows {
                out.push(field(r, "id")?.clone());
            }
            if rows.len() < 1000 {
                return Ok(out);
            }
            last = py_str(field(&rows[rows.len() - 1], "id")?);
        }
    }
}

// The nest indexes Horizon events only, so each epoch field crosses into comparability at its own,
// measured epoch.
//
// Each boundary below is **measured**, not chosen: it is the lowest epoch above which the field's
// class property holds - no disagreement at all for a gate, and every disagreement an adjacent
// equal-and-opposite pair for a drift field.
//
// The two fee fields were `drift` and became `gate` on 2026-09-03, once #1116's exact boundaries
// were deployed; `signalled_tokens` followed on 2026-10-04 (graph-allocations-nest#29). From the
// nest's `l1_blocks` re-index (#1882) every epoch is placed by its own L1 block: measured against the
// subgraph on 2026-10-05, signal and curator fees agree on every epoch the network has had, query
// fees from 290, and the reward trio stays at 1195. A nest from before the re-index starts at 1104
// with an observed boundary there, so these constants need it.
//
// Two classes:
//   gate   directly comparable. **Any** disagreement above the boundary fails the run, except an
//          exact boundary shift in the two fee fields (#1819, below).
//   drift  comparable, but subject to the observed-boundary problem in #1116: value filed one epoch
//          out. A disagreement above the boundary is tolerated **only if it is half of an adjacent
//          equal-and-opposite pair**. An unpaired one is a new defect and fails. That is the whole
//          difference between a classification and an excuse.
#[derive(Clone, Copy, PartialEq)]
enum Class {
    Gate,
    Drift,
}

/// (nest column, subgraph field, comparable-from epoch, class)
const EPOCH_FIELDS: [(&str, &str, i128, Class); 6] = [
    ("total_rewards", "totalRewards", 1195, Class::Gate),
    (
        "total_indexer_rewards",
        "totalIndexerRewards",
        1195,
        Class::Gate,
    ),
    (
        "total_delegator_rewards",
        "totalDelegatorRewards",
        1195,
        Class::Gate,
    ),
    ("signalled_tokens", "signalledTokens", 1, Class::Gate),
    (
        "query_fees_collected",
        "queryFeesCollected",
        290,
        Class::Gate,
    ),
    ("curator_query_fees", "curatorQueryFees", 1, Class::Gate),
];

// Fees carry no epoch and are bucketed by block. Where epoch_boundaries has no exact start an epoch
// starts at its first observed event, so a collection between the predecessor's last observation and
// that start lands one epoch early in the nest. That is excused only when exact: a run of adjacent
// disagreeing epochs nets to zero and the amount crossing each boundary is a block-aligned tail of
// the collections in that window, at the same block for both fields (#1819). Anything else is a DIFF.
const FEE_EVENT_INDEX: [(&str, usize); 2] =
    [("query_fees_collected", 1), ("curator_query_fees", 2)];

// The subgraph's redeems come from GraphTallyCollector's `PaymentCollected`, one log after the escrow
// event; handleEscrowCollected records none (graph-network-subgraph src/mappings/paymentsEscrow.ts:76,
// graphTallyCollector.ts:47). A collection by any other collector, an EOA collecting from itself
// (#1114) or from another payer (#1983), is therefore nest-only and KNOWN-DIFF, recorded as a *rule*
// rather than a list of ids: a nest-only row collected by GraphTallyCollector fails.
// Arbitrum One, from @graphprotocol/address-book horizon/addresses.json, which the subgraph builds from.
const GRAPH_TALLY_COLLECTOR: &str = "0x8f69f5c07477ac46fbc491b1e6d91e2bb0111a9e";

type Row = Map<String, Value>;
/// Signed nest-minus-subgraph per epoch, in the order the epochs were given.
type Deltas = Vec<(String, i128)>;

fn delta_of(d: &Deltas, e: &str) -> Option<i128> {
    d.iter().find(|(k, _)| k == e).map(|(_, v)| *v)
}

struct Epochs {
    nest: HashMap<String, Row>,
    sg: HashMap<String, Row>,
    open: String,
    top_closed: String,
}

impl Epochs {
    /// Signed, because the sign is the diagnosis: a pair that cancels is value in the wrong bucket, a
    /// residue that does not is value nobody has.
    fn deltas(&self, ids: &[String], nest_col: &str, sg_col: &str) -> R<Deltas> {
        let mut out = Vec::new();
        for e in ids {
            let a = py_str(
                self.nest[e]
                    .get(nest_col)
                    .ok_or_else(|| die(format!("nest epoch {e} has no {nest_col}")))?,
            );
            let b = py_str(
                self.sg[e]
                    .get(sg_col)
                    .ok_or_else(|| die(format!("subgraph epoch {e} has no {sg_col}")))?,
            );
            if a != b {
                let (x, y) = (parse_int(&a), parse_int(&b));
                let (Some(x), Some(y)) = (x, y) else {
                    return Err(die(format!(
                        "epoch {e}: {nest_col} {a} or {sg_col} {b} is not an integer"
                    )));
                };
                out.push((e.clone(), x - y));
            }
        }
        Ok(out)
    }

    /// Is the top closed epoch's residue the other half of a pair that ends in the open epoch?
    ///
    /// Exempting `top_closed` unconditionally was wrong (#1123 review): a genuine ingestion loss at the
    /// newest closed epoch would have been waved through as an expected edge artefact. The open epoch
    /// is excluded from the *comparison* because its totals still move, but its delta at this pin is a
    /// perfectly good witness for whether the residue cancels.
    fn cancels_into_open_epoch(&self, nest_col: &str, sg_col: &str, top_delta: i128) -> bool {
        // **Adjacency first.** If the overlap is missing the real next closed epoch, `top_closed` is
        // older than it looks and could be cancelled by a non-adjacent open epoch.
        if ekey(&self.open) != ekey(&self.top_closed) + 1 {
            println!(
                "    epoch {} is not adjacent to the open epoch {}, so its residue cannot be an edge \
                 artefact - the overlap is missing an epoch between them",
                self.top_closed, self.open
            );
            return false;
        }
        let Some(sg) = self.sg.get(&self.open) else {
            return false;
        };
        let a = self.nest[&self.open].get(nest_col).and_then(int_of);
        let b = sg.get(sg_col).and_then(int_of);
        matches!((a, b), (Some(a), Some(b)) if a - b == -top_delta)
    }

    /// Is every disagreement in `ids` half of an adjacent equal-and-opposite pair? This is the
    /// property that defines a `drift` field's boundary.
    fn pairing_holds(&self, ids: &[String], nest_col: &str, sg_col: &str) -> R<bool> {
        let bad = self.deltas(ids, nest_col, sg_col)?;
        let mut sorted: Vec<&(String, i128)> = bad.iter().collect();
        sorted.sort_by_key(|(e, _)| ekey(e));
        let mut seen = BTreeSet::new();
        for (e, d) in sorted {
            if seen.contains(e) {
                continue;
            }
            let nxt = (ekey(e) + 1).to_string();
            if delta_of(&bad, &nxt) == Some(-d) {
                seen.insert(e.clone());
                seen.insert(nxt);
            } else if *e == self.top_closed && self.cancels_into_open_epoch(nest_col, sg_col, *d) {
                seen.insert(e.clone());
            } else {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

type Window = (Option<i128>, Vec<(i128, i128, i128)>);

/// For each boundary, named by its later epoch: that epoch's nest start, and the collections in the
/// unobserved window below it as (block, net, curators).
fn fee_windows(ctx: &Ctx, boundaries: &[String]) -> R<HashMap<String, Window>> {
    let ids: BTreeSet<i128> = boundaries
        .iter()
        .flat_map(|b| [ekey(b), ekey(b) - 1])
        .collect();
    let mut eb: HashMap<String, Value> = HashMap::new();
    for r in ctx.nest_sql(&format!(
        "SELECT CAST(epoch AS VARCHAR) AS epoch, start_block, last_seen, boundary_source \
         FROM epoch_boundaries WHERE epoch IN ({})",
        ids.iter()
            .map(|i| i.to_string())
            .collect::<Vec<_>>()
            .join(",")
    ))? {
        eb.insert(py_str(field(&r, "epoch")?), r);
    }
    let mut out = HashMap::new();
    for b in boundaries {
        let (prev, cur) = (eb.get(&(ekey(b) - 1).to_string()), eb.get(b));
        let (Some(prev), Some(cur)) = (prev, cur) else {
            out.insert(b.clone(), (None, Vec::new()));
            continue;
        };
        let hi = int(field(cur, "start_block")?, "start_block")?;
        if cur.get("boundary_source") != Some(&Value::String("observed".into())) {
            out.insert(b.clone(), (Some(hi), Vec::new()));
            continue;
        }
        let lo = match prev.get("last_seen") {
            Some(v) if !v.is_null() => v,
            _ => field(prev, "start_block")?,
        };
        let lo = int(lo, "last_seen")?;
        // The same per-event net as lodestar_epochs' `fees`, which taxes each event, not the total.
        let rows = ctx.nest_sql(&format!(
            "SELECT block_number, CAST(net AS VARCHAR) AS net, CAST(curators AS VARCHAR) AS curators \
             FROM ( SELECT block_number, CAST(\"tokensCollected\" AS HUGEINT) - CAST(\"tokensCurators\" AS HUGEINT) \
             - (CAST(\"tokensCollected\" AS HUGEINT) // 100) AS net, CAST(\"tokensCurators\" AS HUGEINT) AS curators \
             FROM subgraph_service__query_fees_collected \
             UNION ALL SELECT block_number, CAST(\"queryFees\" AS HUGEINT), CAST(\"curationFees\" AS HUGEINT) \
             FROM staking_legacy__rebate_collected \
             UNION ALL SELECT block_number, CAST(tokens AS HUGEINT) - CAST(\"curationFees\" AS HUGEINT), \
             CAST(\"curationFees\" AS HUGEINT) FROM staking_legacy__allocation_collected\
             ) q WHERE block_number > {lo} AND block_number < {hi} AND block_number <= {}",
            ctx.block
        ))?;
        let mut events = Vec::new();
        for r in &rows {
            events.push((
                int(field(r, "block_number")?, "block_number")?,
                int(field(r, "net")?, "net")?,
                int(field(r, "curators")?, "curators")?,
            ));
        }
        out.insert(b.clone(), (Some(hi), events));
    }
    Ok(out)
}

/// Every block c at which the window's collections from c upward sum to exactly `amount`.
fn tail_cuts(
    hi: Option<i128>,
    events: &[(i128, i128, i128)],
    idx: usize,
    amount: i128,
) -> BTreeSet<i128> {
    let mut cuts = BTreeSet::new();
    if amount == 0 {
        if let Some(hi) = hi {
            cuts.insert(hi);
        }
    }
    let pick = |e: &(i128, i128, i128)| if idx == 1 { e.1 } else { e.2 };
    let blocks: BTreeSet<i128> = events.iter().map(|e| e.0).collect();
    for c in blocks {
        if events.iter().filter(|e| e.0 >= c).map(pick).sum::<i128>() == amount {
            cuts.insert(c);
        }
    }
    cuts
}

/// A run of adjacent epochs, and per boundary inside it: (amount moved, first block moved).
type Shift = (Vec<String>, BTreeMap<i128, (String, i128, i128)>);

/// A run of adjacent epochs that nets to zero, and the amount moved across each boundary in it.
type Run = (Vec<String>, Vec<(String, i128)>);

/// {fee field: exact runs only}.
fn trace_boundary_shifts(
    ctx: &Ctx,
    ep: &Epochs,
    overlap: &[String],
    floor: i128,
) -> R<HashMap<String, Vec<Shift>>> {
    // Per fee field: (run, ordered {boundary: amount moved}).
    let mut runs: Vec<(&str, Vec<Run>)> = Vec::new();
    for (nest_col, sg_col, from_epoch, _) in EPOCH_FIELDS {
        if !FEE_EVENT_INDEX.iter().any(|(c, _)| *c == nest_col) {
            continue;
        }
        let cut = from_epoch.max(floor);
        let window: Vec<String> = overlap.iter().filter(|e| ekey(e) >= cut).cloned().collect();
        let bad = ep.deltas(&window, nest_col, sg_col)?;
        let mut sorted: Vec<&(String, i128)> = bad.iter().collect();
        sorted.sort_by_key(|(e, _)| ekey(e));
        let mut groups: Vec<Vec<String>> = Vec::new();
        for (e, _) in sorted {
            match groups.last_mut() {
                Some(g) if ekey(e) == ekey(g.last().unwrap()) + 1 => g.push(e.clone()),
                _ => groups.push(vec![e.clone()]),
            }
        }
        let mut col_runs = Vec::new();
        for g in groups {
            let mut acc = 0i128;
            let mut moved = Vec::new();
            for e in &g[..g.len() - 1] {
                acc += delta_of(&bad, e).unwrap();
                moved.push(((ekey(e) + 1).to_string(), acc));
            }
            if acc + delta_of(&bad, g.last().unwrap()).unwrap() == 0 {
                col_runs.push((g, moved));
            }
        }
        runs.push((nest_col, col_runs));
    }
    let mut boundaries: Vec<String> = runs
        .iter()
        .flat_map(|(_, rs)| {
            rs.iter()
                .flat_map(|(_, m)| m.iter().map(|(b, _)| b.clone()))
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    boundaries.sort_by_key(|b| ekey(b));
    if boundaries.is_empty() {
        return Ok(HashMap::new());
    }
    let windows = fee_windows(ctx, &boundaries)?;
    let moved_at = |col: &str, b: &str| -> i128 {
        runs.iter()
            .filter(|(c, _)| *c == col)
            .flat_map(|(_, rs)| rs.iter())
            .find_map(|(_, m)| m.iter().find(|(k, _)| k == b).map(|(_, v)| *v))
            .unwrap_or(0)
    };
    let mut common: HashMap<String, BTreeSet<i128>> = HashMap::new();
    for b in &boundaries {
        let (hi, events) = &windows[b];
        let mut cuts: Option<BTreeSet<i128>> = None;
        for (col, idx) in FEE_EVENT_INDEX {
            let c = tail_cuts(*hi, events, idx, moved_at(col, b));
            cuts = Some(match cuts {
                None => c,
                Some(prev) => prev.intersection(&c).copied().collect(),
            });
        }
        common.insert(b.clone(), cuts.unwrap_or_default());
    }
    let mut out = HashMap::new();
    for (col, rs) in runs {
        let mut kept = Vec::new();
        for (g, m) in rs {
            if m.iter().all(|(b, _)| !common[b].is_empty()) {
                let moves = m
                    .iter()
                    .map(|(b, amt)| (ekey(b), (b.clone(), *amt, *common[b].first().unwrap())))
                    .collect();
                kept.push((g, moves));
            }
        }
        out.insert(col.to_string(), kept);
    }
    Ok(out)
}

/// `paymentsEscrowTransactions.id` is `txHash(32 bytes) || logIndex(uint32 little-endian)`.
fn decode_escrow_id(eid: &str) -> R<(String, i128)> {
    let h = eid.strip_prefix("0x").unwrap_or(eid);
    if h.len() != 72 || !h.is_ascii() {
        return Err(die(format!(
            "unexpected escrow id shape '{eid}': expected 36 bytes"
        )));
    }
    let mut le = [0u8; 4];
    for (i, b) in le.iter_mut().enumerate() {
        *b = u8::from_str_radix(&h[64 + 2 * i..66 + 2 * i], 16)
            .map_err(|_| die(format!("escrow id {eid} is not hex")))?;
    }
    Ok((
        format!("0x{}", h[..64].to_lowercase()),
        u32::from_le_bytes(le) as i128,
    ))
}

type Key = (String, i128);

/// Paged read of one escrow source table, keyed the way the subgraph keys it, to each row's collector.
///
/// 70,000+ rows is well past the node's 50,000-row result cap, so this pages by block. The count is
/// taken first and asserted against the assembled set: a page boundary that dropped or double-counted
/// rows would otherwise look exactly like a parity difference, and get reported as one.
fn nest_escrow_ids(ctx: &Ctx, table: &str) -> R<BTreeMap<Key, String>> {
    let block = ctx.block;
    let bounds = ctx.nest_sql(&format!(
        "SELECT min(block_number) AS lo, max(block_number) AS hi, count(*) AS n \
         FROM {table} WHERE block_number <= {block}"
    ))?;
    let n = bounds
        .first()
        .and_then(|b| b.get("n"))
        .filter(|v| !v.is_null());
    let Some(n) = n else {
        return Err(die(format!("{table} has no rows at block {block}")));
    };
    let expect = int(n, "count")?;
    if expect == 0 {
        return Err(die(format!("{table} has no rows at block {block}")));
    }
    let lo = int(field(&bounds[0], "lo")?, "lo")?;
    let hi = int(field(&bounds[0], "hi")?, "hi")?;
    // Half the cap per page, so a dense block range still lands inside it.
    let pages = 1.max(expect / 25000 + 1);
    let step = (hi - lo).div_euclid(pages) + 1;
    let mut out = BTreeMap::new();
    let mut cur = lo;
    while cur <= hi {
        for r in ctx.nest_sql(&format!(
            "SELECT tx_hash, log_index, payer, collector FROM {table} \
             WHERE block_number >= {cur} AND block_number < {} AND block_number <= {block}",
            cur + step
        ))? {
            let tx = match field(&r, "tx_hash")? {
                Value::String(s) => s.to_lowercase(),
                other => return Err(die(format!("tx_hash is not a string: {other}"))),
            };
            out.insert(
                (tx, int(field(&r, "log_index")?, "log_index")?),
                py_str(field(&r, "collector")?).to_lowercase(),
            );
        }
        cur += step;
    }
    if out.len() as i128 != expect {
        return Err(die(format!(
            "{table} paging assembled {} rows but count(*) says {expect}: the pages do not cover the range",
            out.len()
        )));
    }
    Ok(out)
}

/// Recovers the subgraph's log-index base for one escrow type. Fails rather than guessing: the
/// winning candidate must account for at least 99% of the subgraph rows and must beat every other
/// candidate, so an encoding change stops the run instead of reading as a large row difference.
fn derive_offset(
    block: i128,
    label: &str,
    nest_keys: &BTreeSet<Key>,
    sg_keys: &BTreeSet<Key>,
) -> R<(i128, BTreeSet<Key>)> {
    if sg_keys.is_empty() {
        return Err(die(format!(
            "subgraph returned no {label} rows at block {block}"
        )));
    }
    let mut scored: Vec<(usize, i128, BTreeSet<Key>)> = (-2..=2)
        .map(|off| {
            let shifted: BTreeSet<Key> = nest_keys
                .iter()
                .map(|(t, l)| (t.clone(), l + off))
                .collect();
            (shifted.intersection(sg_keys).count(), off, shifted)
        })
        .collect();
    scored.sort_by_key(|s| std::cmp::Reverse(s.0));
    let (best, off) = (scored[0].0, scored[0].1);
    let runner_up = scored[1].0;
    if (best as f64) < 0.99 * sg_keys.len() as f64 || best <= runner_up {
        return Err(die(format!(
            "cannot derive the {label} log-index base at block {block}: {}",
            scored
                .iter()
                .map(|(n, o, _)| format!("offset {o:+} matches {n}"))
                .collect::<Vec<_>>()
                .join(", ")
        )));
    }
    println!(
        "  {label} log-index base: nest log_index {off:+} ({best} of {} subgraph rows)",
        sg_keys.len()
    );
    Ok((off, scored.swap_remove(0).2))
}

fn run() -> R<()> {
    let key = env("GRAPH_API_KEY")?;
    let block = parse_int(&env("BLOCK")?).ok_or_else(|| die("BLOCK is not an integer"))?;
    let gateway = env("GATEWAY")?.trim_end_matches('/').to_string();
    let mut ctx = Ctx {
        url: format!("{gateway}/api/{key}/subgraphs/id/{}", env("NETWORK_SG")?),
        key,
        block,
        head_mode: env("PARITY_MODE")? == "head",
        nest: env("NEST")?.trim_end_matches('/').to_string(),
        failed: false,
    };
    let env_int = |k: &str| -> R<i128> {
        parse_int(&env(k)?).ok_or_else(|| die(format!("{k} is not an integer")))
    };

    let meta = ctx.gql("{ _meta { block { number } } }")?;
    let Some(sg_block) = meta
        .get("_meta")
        .and_then(|m| m.get("block"))
        .and_then(|b| b.get("number"))
        .and_then(int_of)
    else {
        return Err(ctx.rig_fault(&format!(
            "subgraph _meta.block.number missing, so the pin cannot be checked: {}",
            Value::Object(meta.clone())
        )));
    };
    println!("subgraph _meta.block.number={sg_block} pin={block}");
    if sg_block < block && ctx.head_mode {
        return Err(ctx.not_measured(&format!(
            "the subgraph's head {sg_block} is below the nest's"
        )));
    }
    if sg_block < block {
        return Err(ctx.rig_fault(&format!(
            "subgraph head {sg_block} is below pin {block}: a match here would not be a comparison at that block"
        )));
    }

    // --- allocations: Horizon vs Horizon, not all-time allocationCount ---
    let alloc_sg = ctx.page_ids("allocations", "isLegacy: false")?.len() as i128;
    let alloc_nest = env_int("ALLOC_N")?;
    let status = if alloc_nest == alloc_sg { "OK" } else { "DIFF" };
    println!("lodestar_allocations nest={alloc_nest} subgraph_isLegacy_false={alloc_sg} {status}");
    if status == "DIFF" {
        ctx.failed = true;
        println!("  (all-time graphNetwork.allocationCount is a different population and is not compared)");
    }

    // --- disputes: id sets at pinned block, legacy excluded ---
    // lodestar_disputes indexes Arbitrum One (Horizon) dispute manager events directly.
    let mut nest_disputes = BTreeSet::new();
    for r in ctx.nest_sql(&format!(
        "SELECT id FROM lodestar_disputes WHERE created_at_block <= {block}"
    ))? {
        match field(&r, "id")? {
            Value::String(s) => nest_disputes.insert(s.to_lowercase()),
            other => return Err(die(format!("dispute id is not a string: {other}"))),
        };
    }
    // Fetch the complete pinned classification first. Filtering legacy rows here would make
    // `sg_legacy` empty and leave the nest-side exclusion below as rather fine decoration.
    let mut sg_disputes = Vec::new();
    let mut last = String::new();
    loop {
        let w = if last.is_empty() {
            String::new()
        } else {
            format!(", where: {{ id_gt: \"{last}\" }}")
        };
        let data = ctx.gql(&format!(
            "{{ disputes(first: 1000, orderBy: id, orderDirection: asc, block: {{ number: {block} }}{w}) {{ id type isLegacy }} }}"
        ))?;
        let rows = ctx.rows_of(&data, "disputes")?;
        let n = rows.len();
        if n > 0 {
            last = py_str(field(&rows[n - 1], "id")?);
        }
        sg_disputes.extend(rows);
        if n < 1000 {
            break;
        }
    }
    let mut sg_live = BTreeSet::new();
    let mut sg_legacy = BTreeSet::new();
    for d in &sg_disputes {
        let id = match field(d, "id")? {
            Value::String(s) => s.to_lowercase(),
            other => return Err(die(format!("dispute id is not a string: {other}"))),
        };
        if truthy(d.get("isLegacy")) {
            sg_legacy.insert(id);
        } else {
            sg_live.insert(id);
        }
    }
    // Apply the subgraph's pinned legacy classification to the nest set too. The nest view is event
    // derived and carries no `is_legacy` column of its own, so comparing every nest id to a
    // legacy-filtered subgraph set would manufacture nest-only differences.
    let nest_live: BTreeSet<_> = nest_disputes.difference(&sg_legacy).cloned().collect();
    let nest_legacy = nest_disputes.intersection(&sg_legacy).count();
    let only_nest: Vec<_> = nest_live.difference(&sg_live).cloned().collect();
    let only_sg: Vec<_> = sg_live.difference(&nest_disputes).cloned().collect();
    let status = if only_nest.is_empty() && only_sg.is_empty() {
        "OK"
    } else {
        "DIFF"
    };
    println!(
        "lodestar_disputes nest_live={} nest_legacy_excluded={nest_legacy} subgraph_live={} subgraph_legacy_excluded={} {status}",
        nest_live.len(),
        sg_live.len(),
        sg_legacy.len()
    );
    let first20 = |v: &[String]| v.iter().take(20).cloned().collect::<Vec<_>>().join(", ");
    if !only_nest.is_empty() {
        ctx.failed = true;
        println!("  nest-only: {}", first20(&only_nest));
    }
    if !only_sg.is_empty() {
        ctx.failed = true;
        println!("  subgraph-only: {}", first20(&only_sg));
    }

    // --- epochs: field-by-field at the pinned block ---
    // An optional floor an operator can raise, never lower: narrowing the window is a caller's
    // business, widening it past a measured boundary is how a green run stops meaning anything.
    let epoch_floor = match std::env::var("EPOCH_PARITY_FROM")
        .unwrap_or_default()
        .as_str()
    {
        "" => 0,
        s => parse_int(s).ok_or_else(|| die("EPOCH_PARITY_FROM is not an integer"))?,
    };
    let mut nest_epochs: HashMap<String, Row> = HashMap::new();
    for r in ctx.nest_sql(&format!(
        "SELECT * FROM lodestar_epochs WHERE start_block <= {block}"
    ))? {
        let id = py_str(field(&r, "id")?);
        match r {
            Value::Object(o) => nest_epochs.insert(id, o),
            _ => unreachable!(),
        };
    }
    if nest_epochs.is_empty() {
        return Err(die(format!(
            "lodestar_epochs returned no rows at block {block}"
        )));
    }
    // EPOCH_N was counted before the subgraph was asked. If the two reads of the same pinned view
    // disagree the snapshot moved underneath us and nothing below is a comparison.
    let epoch_n = env_int("EPOCH_N")?;
    if nest_epochs.len() as i128 != epoch_n {
        return Err(die(format!(
            "lodestar_epochs changed between reads at block {block}: counted {epoch_n}, then read {}",
            nest_epochs.len()
        )));
    }
    let mut ordered: Vec<String> = nest_epochs.keys().cloned().collect();
    ordered.sort_by_key(|e| ekey(e));
    // The newest epoch at the pin is still open: its end_block is its own last observation and both it
    // and its totals still move. Excluding it is what makes the rest immutable.
    let open_epoch = ordered.last().unwrap().clone();

    let sg_fields: Vec<&str> = EPOCH_FIELDS
        .iter()
        .filter(|f| f.3 == Class::Gate)
        .chain(EPOCH_FIELDS.iter().filter(|f| f.3 == Class::Drift))
        .map(|f| f.1)
        .collect();
    let mut sg_epochs: HashMap<String, Row> = HashMap::new();
    for chunk in ordered.chunks(100) {
        let data = ctx.gql(&format!(
            "{{ epoches(first: 1000, where: {{ id_in: [{}] }}, block: {{ number: {block} }}) {{ id {} }} }}",
            chunk.iter().map(|e| format!("\"{e}\"")).collect::<Vec<_>>().join(","),
            sg_fields.join(" ")
        ))?;
        for r in ctx.rows_of(&data, "epoches")? {
            let id = py_str(field(&r, "id")?);
            if let Value::Object(o) = r {
                sg_epochs.insert(id, o);
            }
        }
    }

    let closed_nest: Vec<String> = ordered
        .iter()
        .filter(|e| **e != open_epoch)
        .cloned()
        .collect();
    // The intersection is where an absent comparison would hide. The Graph Network subgraph holds every
    // epoch the network has had, and the nest holds a Horizon-era subset, so every closed nest epoch
    // must come back. Silently intersecting instead would let the subgraph drop a disagreeing epoch and
    // leave the survivors to report parity.
    let missing: Vec<String> = closed_nest
        .iter()
        .filter(|e| !sg_epochs.contains_key(*e))
        .cloned()
        .collect();
    if !missing.is_empty() {
        ctx.failed = true;
        println!(
            "  MISSING {} of {} closed nest epochs absent from the subgraph response at block {block}: {}",
            missing.len(),
            closed_nest.len(),
            first20(&missing)
        );
    }
    let overlap: Vec<String> = closed_nest
        .iter()
        .filter(|e| sg_epochs.contains_key(*e))
        .cloned()
        .collect();
    if overlap.is_empty() {
        return Err(die(format!(
            "no closed epoch is held by both sides at block {block}, so nothing was compared"
        )));
    }
    let top_closed = overlap.last().unwrap().clone();
    println!(
        "lodestar_epochs nest={} overlap={} open_excluded={open_epoch} floor={}",
        nest_epochs.len(),
        overlap.len() + 1,
        if epoch_floor == 0 {
            "none".to_string()
        } else {
            epoch_floor.to_string()
        }
    );
    let ep = Epochs {
        nest: nest_epochs,
        sg: sg_epochs,
        open: open_epoch,
        top_closed,
    };

    let boundary_shifts = trace_boundary_shifts(&ctx, &ep, &overlap, epoch_floor)?;
    let shifted_epochs: HashMap<&str, BTreeSet<&String>> = boundary_shifts
        .iter()
        .map(|(col, rs)| {
            (
                col.as_str(),
                rs.iter().flat_map(|(g, _)| g.iter()).collect(),
            )
        })
        .collect();
    let is_shifted = |col: &str, e: &String| shifted_epochs.get(col).is_some_and(|s| s.contains(e));

    let mut epoch_known_diff: Vec<String> = Vec::new();
    let mut gate_windows: Vec<usize> = Vec::new();
    for (nest_col, sg_col, from_epoch, klass) in EPOCH_FIELDS {
        let cut = from_epoch.max(epoch_floor);
        let window: Vec<String> = overlap.iter().filter(|e| ekey(e) >= cut).cloned().collect();
        if window.is_empty() {
            return Err(die(format!(
                "{nest_col}: boundary {cut} leaves no epoch to compare (overlap is {}..{})",
                overlap[0],
                overlap[overlap.len() - 1]
            )));
        }
        let bad = ep.deltas(&window, nest_col, sg_col)?;

        if klass == Class::Gate {
            gate_windows.push(window.len());
            let empty = Vec::new();
            let shifts = boundary_shifts.get(nest_col).unwrap_or(&empty);
            let mut unexplained: Vec<&String> = bad
                .iter()
                .map(|(e, _)| e)
                .filter(|e| !is_shifted(nest_col, e))
                .collect();
            let status = if !unexplained.is_empty() {
                "DIFF"
            } else if !bad.is_empty() {
                "KNOWN-DIFF (#1819)"
            } else {
                "OK"
            };
            println!(
                "  {nest_col} {}/{} epochs agree from {cut} {status}",
                window.len() - bad.len(),
                window.len()
            );
            for (g, moves) in shifts {
                println!(
                    "    epochs {}-{} are a boundary shift netting to 0: {} KNOWN-DIFF (#1819)",
                    g[0],
                    g[g.len() - 1],
                    moves
                        .values()
                        .map(|(b, amt, first)| format!(
                            "{amt} of epoch {b} filed in {}, collections from block {first}",
                            ekey(b) - 1
                        ))
                        .collect::<Vec<_>>()
                        .join("; ")
                );
            }
            if !shifts.is_empty() {
                epoch_known_diff.push(format!(
                    "{nest_col}@{}",
                    shifts
                        .iter()
                        .map(|(g, _)| format!("{}-{}", g[0], g[g.len() - 1]))
                        .collect::<Vec<_>>()
                        .join(",")
                ));
            }
            if !unexplained.is_empty() {
                ctx.failed = true;
                unexplained.sort_by_key(|e| ekey(e));
                for e in unexplained.into_iter().take(20) {
                    println!(
                        "    epoch {e} nest={} subgraph={}",
                        py_str(&ep.nest[e][nest_col]),
                        py_str(&ep.sg[e][sg_col])
                    );
                }
            }
        } else {
            // Pair each disagreement with its neighbour. The topmost closed epoch is exempt: its
            // partner is the open epoch, which is excluded from the comparison.
            let mut sorted: Vec<&(String, i128)> = bad.iter().collect();
            sorted.sort_by_key(|(e, _)| ekey(e));
            let (mut paired, mut unpaired, mut edge) = (BTreeSet::new(), Vec::new(), Vec::new());
            for (e, d) in sorted {
                if paired.contains(e) {
                    continue;
                }
                let nxt = (ekey(e) + 1).to_string();
                if delta_of(&bad, &nxt) == Some(-d) {
                    paired.insert(e.clone());
                    paired.insert(nxt);
                } else if *e == ep.top_closed && ep.cancels_into_open_epoch(nest_col, sg_col, *d) {
                    edge.push(e.clone());
                } else {
                    unpaired.push(e.clone());
                }
            }
            if !unpaired.is_empty() {
                ctx.failed = true;
                println!(
                    "  {nest_col} {} of {} disagreements above {cut} are UNPAIRED, net {} - not boundary drift",
                    unpaired.len(),
                    bad.len(),
                    unpaired.iter().map(|e| delta_of(&bad, e).unwrap()).sum::<i128>()
                );
                for e in unpaired.iter().take(20) {
                    println!("    epoch {e} delta={}", delta_of(&bad, e).unwrap());
                }
            } else if !bad.is_empty() {
                epoch_known_diff.push(nest_col.to_string());
                // The paired sum on its own must be exactly zero - that is what makes this drift
                // rather than loss - and folding the exempt top epoch into it would hide it.
                let paired_sum: i128 = paired.iter().filter_map(|e| delta_of(&bad, e)).sum();
                println!(
                    "  {nest_col} {}/{} epochs disagree from {cut}, {} in adjacent pairs summing to {paired_sum} KNOWN-DIFF (#1116)",
                    bad.len(),
                    window.len(),
                    paired.len()
                );
                if paired_sum != 0 {
                    ctx.failed = true;
                    println!("    PAIRS DO NOT CANCEL: sum is {paired_sum}, so this is not boundary drift");
                }
                if let Some(e) = edge.first() {
                    println!(
                        "    epoch {e} carries {}, cancelled by the open epoch {} - checked, not assumed",
                        delta_of(&bad, e).unwrap(),
                        ep.open
                    );
                }
            } else if !ep
                .deltas(
                    &overlap
                        .iter()
                        .filter(|e| ekey(e) >= from_epoch)
                        .cloned()
                        .collect::<Vec<_>>(),
                    nest_col,
                    sg_col,
                )?
                .is_empty()
            {
                // Nothing in the *narrowed* window, but the measured window still disagrees: a caller
                // narrowing the comparison, which says nothing about the classification.
                println!(
                    "  {nest_col} agrees on all {} epochs from {cut}, though the measured window from {from_epoch} still \
                     drifts - narrowed by the floor, not reclassified",
                    window.len()
                );
            } else {
                // Agrees across the **measured** window too, so the classification really has moved.
                ctx.failed = true;
                println!(
                    "  {nest_col} agrees on all {} epochs from {from_epoch}: #1116 has moved, reclassify it as a gate",
                    window.len()
                );
            }
        }

        // **The boundary's claim is that it is the *lowest* epoch at which the field's property
        // holds, so this computes that epoch and asserts the constant equals it.** The drift property
        // is not monotonic, so every candidate is tried rather than reasoned about. Evaluated at
        // `from_epoch`, never at `cut`: an operator raising the floor is doing something supported.
        let property_holds = |ids: &[String]| -> R<bool> {
            if klass == Class::Gate {
                Ok(ep
                    .deltas(ids, nest_col, sg_col)?
                    .iter()
                    .all(|(e, _)| is_shifted(nest_col, e)))
            } else {
                ep.pairing_holds(ids, nest_col, sg_col)
            }
        };
        let mut lowest = None;
        for candidate in &overlap {
            let ids: Vec<String> = overlap
                .iter()
                .filter(|e| ekey(e) >= ekey(candidate))
                .cloned()
                .collect();
            if property_holds(&ids)? {
                lowest = Some(candidate.clone());
                break;
            }
        }
        match lowest {
            None => {
                ctx.failed = true;
                println!(
                    "    BOUNDARY {nest_col}: the property holds at no epoch in the overlap, so {from_epoch} cannot be a \
                     boundary at all"
                );
            }
            Some(l) if ekey(&l) != from_epoch => {
                ctx.failed = true;
                println!(
                    "    BOUNDARY {nest_col}: the lowest epoch at which the property holds is {l}, not the configured \
                     {from_epoch} - the constant is {}",
                    if ekey(&l) < from_epoch {
                        "too high and excludes comparable data"
                    } else {
                        "too low and claims more than it can show"
                    }
                );
            }
            Some(_) => {}
        }
    }
    // start_block/end_block are L2 observations here and L1 epoch boundaries there.
    println!("  start_block/end_block INCOMPARABLE (nest L2 observed, subgraph L1 EpochManager)");
    let epoch_gated_n = gate_windows.iter().min().copied().unwrap_or(0);

    // --- escrow: row-level, at the pinned block ---
    // The log index is **not on the same base for both types**, which is why the offset is derived
    // rather than written down: deposits match the nest's `log_index` exactly, and collections match
    // it plus one. `derive_offset` recovers each one from the data.
    let mut sg_escrow: BTreeMap<Key, String> = BTreeMap::new();
    let mut last = String::new();
    loop {
        let w = if last.is_empty() {
            String::new()
        } else {
            format!(", where: {{ id_gt: \"{last}\" }}")
        };
        let data = ctx.gql(&format!(
            "{{ paymentsEscrowTransactions(first: 1000, orderBy: id, orderDirection: asc, block: {{ number: {block} }}{w}) {{ id type }} }}"
        ))?;
        let rows = ctx.rows_of(&data, "paymentsEscrowTransactions")?;
        for r in &rows {
            let id = match field(r, "id")? {
                Value::String(s) => s.clone(),
                other => return Err(die(format!("escrow id is not a string: {other}"))),
            };
            sg_escrow.insert(decode_escrow_id(&id)?, py_str(field(r, "type")?));
        }
        if rows.len() < 1000 {
            break;
        }
        last = py_str(field(&rows[rows.len() - 1], "id")?);
    }

    println!(
        "lodestar_escrow_transactions nest={} subgraph={}",
        env_int("ESCROW_N")?,
        sg_escrow.len()
    );
    let types = ctx.nest_sql(&format!(
        "SELECT type, count(*) AS n FROM lodestar_escrow_transactions WHERE block_number <= {block} GROUP BY type ORDER BY type"
    ))?;
    let mut type_pairs = Vec::new();
    for t in &types {
        type_pairs.push((py_str(field(t, "type")?), field(t, "n")?.clone()));
    }
    println!(
        "  nest types: {}",
        type_pairs
            .iter()
            .map(|(t, n)| format!("{t}={}", py_str(n)))
            .collect::<Vec<_>>()
            .join(", ")
    );

    // Only the two types the subgraph entity actually models are comparable; it has no Thaw or
    // CancelThaw.
    let mut escrow_known_diff = false;
    let nest_collected_raw = nest_escrow_ids(&ctx, "escrow__escrow_collected")?;
    let sg_collected: BTreeSet<Key> = sg_escrow
        .iter()
        .filter(|(_, t)| *t == "redeem")
        .map(|(k, _)| k.clone())
        .collect();

    // Deposits are a modelled type on both sides and are gated the same way. Fetching them and then
    // comparing only collections would let a missing, extra or mistyped deposit pass as parity.
    let nest_deposits_raw = nest_escrow_ids(&ctx, "escrow__deposit")?;
    let sg_deposits: BTreeSet<Key> = sg_escrow
        .iter()
        .filter(|(_, t)| *t == "deposit")
        .map(|(k, _)| k.clone())
        .collect();
    let (deposit_off, nest_deposits) = derive_offset(
        block,
        "deposit",
        &nest_deposits_raw.keys().cloned().collect(),
        &sg_deposits,
    )?;
    let dep_only_nest: Vec<&Key> = nest_deposits.difference(&sg_deposits).collect();
    let dep_only_sg: Vec<&Key> = sg_deposits.difference(&nest_deposits).collect();
    println!(
        "  deposits nest={} subgraph={} matched={} {}",
        nest_deposits.len(),
        sg_deposits.len(),
        nest_deposits.intersection(&sg_deposits).count(),
        if dep_only_nest.is_empty() && dep_only_sg.is_empty() {
            "OK"
        } else {
            "DIFF"
        }
    );
    if !dep_only_nest.is_empty() {
        ctx.failed = true;
        for (tx, li) in dep_only_nest.iter().take(20) {
            println!("    nest-only deposit {tx} log_index={}", li - deposit_off);
        }
    }
    if !dep_only_sg.is_empty() {
        ctx.failed = true;
        for (tx, li) in dep_only_sg.iter().take(20) {
            println!(
                "    subgraph-only deposit {tx} log_index={}",
                li - deposit_off
            );
        }
    }

    // Every subgraph row must be one of the two modelled types. A third would mean the entity changed
    // under us and the two type filters above silently stopped covering it.
    let unknown_types: BTreeSet<&String> = sg_escrow
        .values()
        .filter(|t| *t != "deposit" && *t != "redeem")
        .collect();
    if !unknown_types.is_empty() {
        ctx.failed = true;
        println!(
            "    subgraph has unmodelled escrow types not compared: {}",
            unknown_types
                .into_iter()
                .cloned()
                .collect::<Vec<_>>()
                .join(", ")
        );
    }

    let (collected_off, nest_collected) = derive_offset(
        block,
        "collected/redeem",
        &nest_collected_raw.keys().cloned().collect(),
        &sg_collected,
    )?;
    let collector_of: HashMap<Key, &String> = nest_collected_raw
        .iter()
        .map(|((t, l), c)| ((t.clone(), l + collected_off), c))
        .collect();
    let only_nest: Vec<&Key> = nest_collected.difference(&sg_collected).collect();
    let only_sg: Vec<&Key> = sg_collected.difference(&nest_collected).collect();
    println!(
        "  collected/redeem nest={} subgraph={} matched={} {}",
        nest_collected.len(),
        sg_collected.len(),
        nest_collected.intersection(&sg_collected).count(),
        if only_nest.is_empty() && only_sg.is_empty() {
            "OK"
        } else {
            "DIFF"
        }
    );
    // The rule is only as good as the address, so every matched row must prove it.
    let not_tally: Vec<&Key> = nest_collected
        .intersection(&sg_collected)
        .filter(|k| collector_of[*k] != GRAPH_TALLY_COLLECTOR)
        .collect();
    if !not_tally.is_empty() {
        ctx.failed = true;
        println!(
            "    {} matched collections were not collected by GraphTallyCollector {GRAPH_TALLY_COLLECTOR}, so the rule is unexplained:",
            not_tally.len()
        );
        for k in not_tally.iter().take(20) {
            println!(
                "      {} log_index={} collector={}",
                k.0,
                k.1 - collected_off,
                collector_of[*k]
            );
        }
    }
    let known_other: Vec<&&Key> = only_nest
        .iter()
        .filter(|k| collector_of[**k] != GRAPH_TALLY_COLLECTOR)
        .collect();
    let unexplained: Vec<&&Key> = only_nest
        .iter()
        .filter(|k| collector_of[**k] == GRAPH_TALLY_COLLECTOR)
        .collect();
    if !known_other.is_empty() {
        escrow_known_diff = true;
        println!(
            "    {} nest-only rows were not collected by GraphTallyCollector, which the subgraph does not record KNOWN-DIFF (#1114, #1983)",
            known_other.len()
        );
        for k in known_other.iter().take(5) {
            println!(
                "      {} log_index={} collector={}",
                k.0,
                k.1 - collected_off,
                collector_of[**k]
            );
        }
    }
    if !unexplained.is_empty() {
        ctx.failed = true;
        println!(
            "    {} nest-only rows were collected by GraphTallyCollector and are unexplained:",
            unexplained.len()
        );
        for k in unexplained.iter().take(20) {
            println!("      {} log_index={}", k.0, k.1 - collected_off);
        }
    }
    if !only_sg.is_empty() {
        ctx.failed = true;
        for (tx, li) in only_sg.iter().take(20) {
            println!("    subgraph-only {tx} log_index={}", li - collected_off);
        }
    }
    // The nest's type vocabulary is closed here on purpose. `Deposit` and `EscrowCollected` are
    // compared above; `Thaw` and `CancelThaw` are the two the subgraph's entity does not model.
    // Anything else is a type nobody has decided about, and excluding it because it is unrecognised is
    // precisely how an unchecked population reads as agreement.
    let compared = ["Deposit", "EscrowCollected"];
    let unmodelled_types = ["Thaw", "CancelThaw"];
    let unexpected: Vec<&(String, Value)> = type_pairs
        .iter()
        .filter(|(t, _)| !compared.contains(&t.as_str()) && !unmodelled_types.contains(&t.as_str()))
        .collect();
    if !unexpected.is_empty() {
        ctx.failed = true;
        println!(
            "    nest escrow types nobody has classified, so they were never compared: {}",
            unexpected
                .iter()
                .map(|(t, n)| format!("{t}={}", py_str(n)))
                .collect::<Vec<_>>()
                .join(", ")
        );
    }
    let mut unmodelled = 0i128;
    for (t, n) in &type_pairs {
        if unmodelled_types.contains(&t.as_str()) {
            unmodelled += int(n, "type count")?;
        }
    }
    if unmodelled != 0 {
        println!(
            "  {unmodelled} nest rows are Thaw/CancelThaw, which the subgraph does not model, and are not compared"
        );
    }

    let mut known = epoch_known_diff;
    if escrow_known_diff {
        known.push("escrow_non_tally_collections".into());
    }
    let summary = env("EPOCH_SUMMARY")?;
    let known = if known.is_empty() {
        "-".to_string()
    } else {
        known.join(",")
    };
    std::fs::write(&summary, format!("{epoch_gated_n} {known}\n"))
        .map_err(|e| die(format!("cannot write {summary}: {e}")))?;

    if ctx.failed {
        return Err(die(format!("nest and subgraph disagree at block {block}")));
    }
    Ok(())
}

fn main() -> ExitCode {
    match std::panic::catch_unwind(run) {
        Ok(Ok(())) => ExitCode::SUCCESS,
        Ok(Err(stop)) => {
            if let Some(m) = stop.msg {
                eprintln!("{m}");
            }
            ExitCode::from(stop.code)
        }
        Err(_) => ExitCode::from(1),
    }
}
