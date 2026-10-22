//! RFC-0070 S0: the time-lapse `sum` fold run over a nest's final sealed segments through Burrmill,
//! with sampled frames checked against Burrmill's own `GROUP BY` over the same segments. Not shipped.
//!
//! Usage: rfc-0070-s0 <nest_dir> <spec.toml> <out_dir> [--check <frames>] [--chunk-rows <n>]
//!
//! Writes `frames.jsonl` and `manifest.json` into `out_dir` and prints one JSON summary line. Exits 1
//! when a sampled frame disagrees with the SQL.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::time::Instant;

use alloy_primitives::I256;
use anyhow::{bail, ensure, Context, Result};
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const FOLD_VERSION: &str = "rfc-0070-s0/sum-1";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    title: String,
    nest: String,
    from_block: u64,
    to_block: u64,
    frames: u64,
    #[serde(default)]
    fps: Option<u32>,
    top: usize,
    quantity: Quantity,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Quantity {
    fold: String,
    sql: String,
}

#[derive(Deserialize)]
struct Segment {
    hash: String,
    from_block: u64,
    to_block: u64,
    rows: u64,
    file: String,
    #[serde(default)]
    provisional: bool,
}

#[derive(Deserialize)]
struct Catalogue {
    tables: BTreeMap<String, Vec<Segment>>,
}

#[derive(Deserialize)]
struct Schema {
    #[serde(default)]
    registry_hash: Option<String>,
    tables: Vec<SchemaTable>,
}

#[derive(Deserialize)]
struct SchemaTable {
    table: String,
    columns: Vec<SchemaColumn>,
}

#[derive(Deserialize)]
struct SchemaColumn {
    name: String,
    storage: String,
}

/// Running sum per key, with an ordered index over the live keys. A key whose sum returns to zero
/// leaves both, so the state is bounded by live keys, not keys ever seen.
#[derive(Default)]
struct Fold {
    value: HashMap<Rc<str>, I256>,
    rank: BTreeSet<(Reverse<I256>, Rc<str>)>,
    total: I256,
    events: u64,
    peak_keys: usize,
}

impl Fold {
    fn apply(&mut self, key: &str, delta: I256) -> Result<()> {
        self.events += 1;
        if delta.is_zero() {
            return Ok(());
        }
        let (k, old) = match self.value.get_key_value(key) {
            Some((k, v)) => (Rc::clone(k), *v),
            None => (Rc::from(key), I256::ZERO),
        };
        let new = old
            .checked_add(delta)
            .with_context(|| format!("key {key} overflows a signed 256-bit sum"))?;
        self.total = self
            .total
            .checked_add(delta)
            .context("the total overflows a signed 256-bit sum")?;
        if !old.is_zero() {
            self.rank.remove(&(Reverse(old), Rc::clone(&k)));
        }
        if new.is_zero() {
            self.value.remove(key);
        } else {
            self.value.insert(Rc::clone(&k), new);
            self.rank.insert((Reverse(new), k));
            self.peak_keys = self.peak_keys.max(self.value.len());
        }
        Ok(())
    }

    fn top(&self, n: usize) -> Vec<(String, String)> {
        self.rank
            .iter()
            .take(n)
            .map(|(Reverse(v), k)| (k.to_string(), v.to_string()))
            .collect()
    }
}

struct Frame {
    close: u64,
    ts: Option<u64>,
    live: usize,
    total: String,
    top: Vec<(String, String)>,
}

impl Frame {
    fn line(&self, i: usize) -> String {
        let top: Vec<String> = self
            .top
            .iter()
            .map(|(k, v)| format!("[{},\"{v}\"]", Value::String(k.clone())))
            .collect();
        let ts = self.ts.map_or("null".to_string(), |t| t.to_string());
        format!(
            "{{\"i\":{i},\"block\":{},\"ts\":{ts},\"live\":{},\"total\":\"{}\",\"top\":[{}]}}\n",
            self.close,
            self.live,
            self.total,
            top.join(",")
        )
    }
}

/// One table the query reads: its declared columns and the final segments that can hold rows at or
/// below the replay's last block.
struct Source<'a> {
    table: String,
    declared: Vec<(String, String)>,
    segments: Vec<&'a Segment>,
}

fn files(nest: &Path, segs: &[&Segment]) -> Result<Vec<(PathBuf, u64)>> {
    segs.iter()
        .map(|s| {
            let p = nest.join("segments").join(&s.file);
            let len = std::fs::metadata(&p)
                .with_context(|| format!("segment {} is missing", p.display()))?
                .len();
            Ok((p, len))
        })
        .collect()
}

fn register(
    engine: &mut burrmill::Engine,
    nest: &Path,
    sources: &[Source],
    window: (u64, u64),
) -> Result<()> {
    let (lo, hi) = window;
    for s in sources {
        let overlapping: Vec<&Segment> = s
            .segments
            .iter()
            .copied()
            .filter(|g| g.from_block <= hi && g.to_block >= lo)
            .collect();
        engine
            .register_facts(
                &s.table,
                &s.declared,
                files(nest, &overlapping)?,
                &[],
                (lo.checked_sub(1), Some(hi)),
            )
            .map_err(|e| anyhow::anyhow!("binding {}: {e}", s.table))?;
    }
    Ok(())
}

fn text(v: &Value, col: &str) -> Result<String> {
    match v.get(col) {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(Value::Number(n)) => Ok(n.to_string()),
        other => bail!("column `{col}` is {other:?}, not a value"),
    }
}

fn int(v: &Value, col: &str) -> Result<u64> {
    v.get(col).and_then(Value::as_u64).with_context(|| {
        format!(
            "column `{col}` is not an unsigned integer: {:?}",
            v.get(col)
        )
    })
}

fn signed_delta(row: &Value, has_sign: bool) -> Result<I256> {
    let raw = text(row, "delta")?;
    let d = I256::from_dec_str(&raw)
        .map_err(|e| anyhow::anyhow!("delta `{raw}` is not a signed 256-bit integer: {e}"))?;
    if !has_sign {
        return Ok(d);
    }
    match row.get("sign").and_then(Value::as_i64) {
        Some(1) => Ok(d),
        Some(-1) => d.checked_neg().context("negated delta overflows"),
        other => bail!("sign is {other:?}; only 1 and -1 are meaningful"),
    }
}

fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    ensure!(
        args.len() >= 4,
        "usage: rfc-0070-s0 <nest_dir> <spec.toml> <out_dir> [--check <frames>] [--chunk-rows <n>]"
    );
    let (nest, spec_path, out) = (
        Path::new(&args[1]),
        Path::new(&args[2]),
        Path::new(&args[3]),
    );
    let mut samples = 20usize;
    let mut chunk_rows = 200_000u64;
    let mut rest = args[4..].iter();
    while let Some(flag) = rest.next() {
        let v = rest.next().context("flag without a value")?;
        match flag.as_str() {
            "--check" => samples = v.parse()?,
            "--chunk-rows" => chunk_rows = v.parse()?,
            _ => bail!("unknown flag {flag}"),
        }
    }

    let spec_bytes = std::fs::read(spec_path)?;
    let spec: Spec = toml::from_str(std::str::from_utf8(&spec_bytes)?)?;
    ensure!(
        spec.quantity.fold == "sum",
        "fold `{}`: S0 builds the `sum` fold only",
        spec.quantity.fold
    );
    ensure!(
        spec.frames > 0 && spec.top > 0,
        "frames and top must be positive"
    );
    ensure!(
        spec.to_block >= spec.from_block,
        "to_block is below from_block"
    );
    let step = (spec.to_block - spec.from_block + 1) / spec.frames;
    ensure!(step > 0, "more frames than blocks in the range");
    let closes: Vec<u64> = (0..spec.frames)
        .map(|i| spec.from_block + (i + 1) * step - 1)
        .collect();
    let end = *closes.last().unwrap();

    let catalogue: Catalogue =
        serde_json::from_slice(&std::fs::read(nest.join("segments/manifest.json"))?)?;
    let schema: Schema = serde_json::from_slice(&std::fs::read(nest.join("schema.json"))?)?;
    let watermark = catalogue
        .tables
        .values()
        .flatten()
        .map(|s| s.to_block)
        .max()
        .context("empty catalogue")?;

    let tables = burrmill::inspect::base_tables(&spec.quantity.sql)
        .context("the quantity's SQL does not parse")?;
    let mut sources = Vec::new();
    for t in &tables {
        let segs = catalogue
            .tables
            .get(t)
            .with_context(|| format!("table {t} has no sealed segments"))?;
        // The hot tail and a provisional segment are never read: the first can reorg, and the
        // second is replaced at the table's next seal.
        let through = segs
            .iter()
            .find(|s| s.provisional)
            .map_or(watermark, |p| p.from_block - 1);
        ensure!(
            end <= through,
            "the replay needs block {end} of {t}, which is final only through {through}"
        );
        let declared = schema
            .tables
            .iter()
            .find(|s| &s.table == t)
            .with_context(|| format!("{t} is not in schema.json"))?
            .columns
            .iter()
            .map(|c| (c.name.clone(), c.storage.clone()))
            .collect();
        let mut segments: Vec<&Segment> = segs
            .iter()
            .filter(|s| !s.provisional && s.from_block <= end)
            .collect();
        segments.sort_by_key(|s| s.from_block);
        sources.push(Source {
            table: t.clone(),
            declared,
            segments,
        });
    }
    let all: Vec<&Segment> = sources.iter().flat_map(|s| s.segments.clone()).collect();
    let start = all
        .iter()
        .map(|s| s.from_block)
        .min()
        .context("no final segments")?;

    // Chunks cut on frame closes, and before the range on segment ends, bounded by rows.
    let mut cuts: Vec<u64> = all
        .iter()
        .map(|s| s.to_block)
        .filter(|b| *b < spec.from_block && *b >= start)
        .chain(closes.iter().copied())
        .collect();
    cuts.sort_unstable();
    cuts.dedup();
    let est = |a: u64, b: u64| -> u64 {
        all.iter()
            .filter(|s| s.from_block <= b && s.to_block >= a)
            .map(|s| s.rows)
            .sum()
    };
    let mut chunks = Vec::new();
    let (mut lo, mut prev) = (start, None::<u64>);
    for &c in &cuts {
        if let Some(p) = prev {
            if est(lo, c) > chunk_rows {
                chunks.push((lo, p));
                lo = p + 1;
            }
        }
        prev = Some(c);
    }
    chunks.push((lo, prev.context("no frame closes")?));

    let mut engine = burrmill::Engine::open_empty().map_err(|e| anyhow::anyhow!("{e}"))?;
    let t0 = Instant::now();
    let mut fold = Fold::default();
    let mut frames: Vec<Frame> = Vec::with_capacity(closes.len());
    let mut ts: Option<u64> = None;
    let mut columns: Option<(bool, bool)> = None;
    let mut last = (0u64, 0u64);
    let (mut encode_secs, mut apply_secs) = (0f64, 0f64);
    let emit = |fold: &Fold, close: u64, ts: Option<u64>, frames: &mut Vec<Frame>| {
        frames.push(Frame {
            close,
            ts,
            live: fold.value.len(),
            total: fold.total.to_string(),
            top: fold.top(spec.top),
        });
    };
    for &(lo, hi) in &chunks {
        register(&mut engine, nest, &sources, (lo, hi))?;
        let (has_sign, has_ts) = match columns {
            Some(c) => c,
            None => {
                let cols: Vec<String> = engine
                    .describe(&spec.quantity.sql)
                    .map_err(|e| anyhow::anyhow!("{e}"))?
                    .into_iter()
                    .map(|(n, _)| n)
                    .collect();
                for c in ["block_number", "log_index", "key", "delta"] {
                    ensure!(cols.iter().any(|n| n == c), "the query has no `{c}` column");
                }
                let c = (
                    cols.iter().any(|n| n == "sign"),
                    cols.iter().any(|n| n == "block_timestamp"),
                );
                columns = Some(c);
                c
            }
        };
        let sql = format!(
            "SELECT * FROM ({}) AS q ORDER BY block_number, log_index, key",
            spec.quantity.sql
        );
        let mut failed: Option<anyhow::Error> = None;
        engine
            .sql_for_each(&sql, |batch| {
                let t = Instant::now();
                let rows = burrmill::df::encode::rows(&batch)?;
                encode_secs += t.elapsed().as_secs_f64();
                let t = Instant::now();
                for row in rows {
                    let r: Result<()> = (|| {
                        let b = int(&row, "block_number")?;
                        let li = int(&row, "log_index")?;
                        ensure!(
                            b >= lo && b <= hi,
                            "row at block {b} outside chunk {lo}..={hi}"
                        );
                        ensure!((b, li) >= last, "rows out of order at block {b}");
                        last = (b, li);
                        while frames.len() < closes.len() && closes[frames.len()] < b {
                            let close = closes[frames.len()];
                            emit(&fold, close, ts, &mut frames);
                        }
                        if has_ts {
                            ts = Some(int(&row, "block_timestamp")?);
                        }
                        fold.apply(&text(&row, "key")?, signed_delta(&row, has_sign)?)
                    })();
                    if let Err(e) = r {
                        failed = Some(e);
                        return Err(burrmill::BurrmillError::Cancelled);
                    }
                }
                apply_secs += t.elapsed().as_secs_f64();
                Ok(())
            })
            .map_err(|e| failed.take().unwrap_or_else(|| anyhow::anyhow!("{e}")))?;
    }
    while frames.len() < closes.len() {
        let close = closes[frames.len()];
        emit(&fold, close, ts, &mut frames);
    }
    let fold_secs = t0.elapsed().as_secs_f64();

    let jsonl: String = frames.iter().enumerate().map(|(i, f)| f.line(i)).collect();
    let frames_sha256 = sha256_hex(jsonl.as_bytes());
    std::fs::create_dir_all(out)?;
    std::fs::write(out.join("frames.jsonl"), &jsonl)?;
    let read: Vec<Value> = sources
        .iter()
        .flat_map(|s| {
            s.segments.iter().map(|g| {
                json!({"table": s.table, "hash": g.hash, "from_block": g.from_block, "to_block": g.to_block})
            })
        })
        .collect();
    let manifest = json!({
        "title": spec.title,
        "nest": spec.nest,
        "registry_hash": schema.registry_hash,
        "spec_sha256": sha256_hex(&spec_bytes),
        "fold_version": FOLD_VERSION,
        "from_block": spec.from_block,
        "to_block": end,
        "step_blocks": step,
        "frames": frames.len(),
        "top": spec.top,
        "fps": spec.fps,
        "segments_read": read,
        "frames_sha256": frames_sha256,
    });
    std::fs::write(
        out.join("manifest.json"),
        serde_json::to_string_pretty(&manifest)? + "\n",
    )?;

    // The check: Burrmill's GROUP BY over every final segment, at evenly spaced frames.
    let t1 = Instant::now();
    let mut mismatches = Vec::new();
    let mut checked = Vec::new();
    if samples > 0 {
        let (n, m) = (frames.len(), samples.min(frames.len()));
        let mut picks: Vec<usize> = (0..m)
            .map(|k| if m == 1 { n - 1 } else { k * (n - 1) / (m - 1) })
            .collect();
        picks.dedup();
        register(&mut engine, nest, &sources, (0, end))?;
        let (has_sign, _) = columns.unwrap();
        let v = if has_sign {
            "CASE WHEN sign = -1 THEN -CAST(delta AS DECIMAL(38,0)) ELSE CAST(delta AS DECIMAL(38,0)) END"
        } else {
            "CAST(delta AS DECIMAL(38,0))"
        };
        for i in picks {
            let f = &frames[i];
            let grouped = format!(
                "SELECT key, SUM(v) AS s FROM (SELECT key, {v} AS v FROM ({}) AS q \
                 WHERE block_number <= {}) AS d GROUP BY key HAVING SUM(v) <> 0",
                spec.quantity.sql, f.close
            );
            let top_sql = format!(
                "SELECT key, s FROM ({grouped}) AS g ORDER BY s DESC, key LIMIT {}",
                spec.top
            );
            let agg_sql = format!("SELECT COUNT(*) AS live, SUM(s) AS total FROM ({grouped}) AS g");
            let mut top = Vec::new();
            for b in engine.sql(&top_sql).map_err(|e| anyhow::anyhow!("{e}"))? {
                for r in burrmill::df::encode::rows(&b).map_err(|e| anyhow::anyhow!("{e}"))? {
                    top.push((text(&r, "key")?, text(&r, "s")?));
                }
            }
            let agg = engine.sql(&agg_sql).map_err(|e| anyhow::anyhow!("{e}"))?;
            let r = &burrmill::df::encode::rows(&agg[0]).map_err(|e| anyhow::anyhow!("{e}"))?[0];
            let live = int(r, "live")? as usize;
            let total = match r.get("total") {
                Some(Value::Null) => "0".to_string(),
                _ => text(r, "total")?,
            };
            let same = top == f.top && live == f.live && total == f.total;
            checked.push(json!({"i": i, "block": f.close, "live": live, "same": same}));
            if !same {
                mismatches.push(json!({
                    "i": i, "block": f.close,
                    "fold": {"live": f.live, "total": f.total, "top": f.top},
                    "sql": {"live": live, "total": total, "top": top},
                }));
            }
        }
    }
    let summary = json!({
        "title": spec.title,
        "frames_sha256": frames_sha256,
        "frames": frames.len(),
        "chunks": chunks.len(),
        "segments_read": all.len(),
        "events": fold.events,
        "live_keys_end": fold.value.len(),
        "live_keys_peak": fold.peak_keys,
        "fold_secs": fold_secs,
        "encode_secs": encode_secs,
        "apply_secs": apply_secs,
        "check_secs": t1.elapsed().as_secs_f64(),
        "checked": checked,
        "mismatches": mismatches,
    });
    println!("{summary}");
    if !mismatches.is_empty() {
        std::process::exit(1);
    }
    Ok(())
}
