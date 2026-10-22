//! Usage: rfc-0070-s1 <nest_dir> <spec.toml> <out_dir> [--check <frames>] [--chunk-rows <n>]
//!
//! Writes `frames.jsonl` and `manifest.json` into `out_dir` and prints one JSON summary line. Exits 1
//! when a sampled frame disagrees with Burrmill's own grouping of the same rows.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{anyhow, bail, ensure, Context, Result};
use nuthatch_timelapse::{manifest::identity, Plan, Replay};
use rfc_0070_s1::{check_sql, is_input, prepare};
use serde_json::{json, Value};

fn inputs(root: &Path) -> Result<BTreeMap<String, Vec<u8>>> {
    let mut out = BTreeMap::new();
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        for e in std::fs::read_dir(&dir)? {
            let e = e?;
            let path = e.path();
            let rel = path
                .strip_prefix(root)?
                .to_string_lossy()
                .replace('\\', "/");
            if !is_input(&rel) {
                continue;
            }
            let ft = e.file_type()?;
            if ft.is_dir() {
                stack.push(path);
            } else if ft.is_file() {
                out.insert(rel, std::fs::read(&path)?);
            }
        }
    }
    Ok(out)
}

fn files(nest: &Path, segs: &[&nuthatch_timelapse::Segment]) -> Result<Vec<(PathBuf, u64)>> {
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

fn bind(
    engine: &mut burrmill::Engine,
    nest: &Path,
    p: &rfc_0070_s1::Prepared,
    (lo, hi): (u64, u64),
) -> Result<()> {
    for s in &p.plan.sources {
        let segs: Vec<_> = Plan::overlapping(s, lo, hi).collect();
        engine
            .register_facts(
                &s.table,
                &p.declared[&s.table],
                files(nest, &segs)?,
                &[],
                (lo.checked_sub(1), Some(hi)),
            )
            .map_err(|e| anyhow!("binding {}: {e}", s.table))?;
    }
    Ok(())
}

fn text(v: &Value) -> Result<String> {
    match v {
        Value::String(s) => Ok(s.clone()),
        Value::Number(n) => Ok(n.to_string()),
        Value::Null => Ok("0".into()),
        other => bail!("{other} is not a value"),
    }
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    ensure!(
        args.len() >= 4,
        "usage: rfc-0070-s1 <nest_dir> <spec.toml> <out_dir> [--check <frames>] [--chunk-rows <n>]"
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
    let p = prepare(
        std::str::from_utf8(&spec_bytes)?,
        &std::fs::read(nest.join("segments/manifest.json"))?,
        &std::fs::read(nest.join("schema.json"))?,
        chunk_rows,
    )?;
    let id = identity(inputs(nest)?)?;

    let mut engine = burrmill::Engine::open_empty().map_err(|e| anyhow!("{e}"))?;
    let t0 = Instant::now();
    let mut replay = Replay::new(&p.spec, &p.plan, &p.shape);
    let mut fold_secs = 0f64;
    let chunk_sql = Plan::chunk_sql(&p.spec.quantity.sql);
    for &(lo, hi) in &p.plan.chunks {
        bind(&mut engine, nest, &p, (lo, hi))?;
        replay.begin_chunk(lo, hi);
        let mut failed = None;
        engine
            .sql_for_each(&chunk_sql, |batch| {
                let t = Instant::now();
                let r = replay.push_batch(&batch);
                fold_secs += t.elapsed().as_secs_f64();
                r.map_err(|e| {
                    failed = Some(e);
                    burrmill::BurrmillError::Cancelled
                })
            })
            .map_err(|e| failed.take().unwrap_or_else(|| anyhow!("{e}")))?;
    }
    let rec = replay.finish();
    let replay_secs = t0.elapsed().as_secs_f64();

    std::fs::create_dir_all(out)?;
    std::fs::write(out.join("frames.jsonl"), &rec.jsonl)?;
    let manifest = rfc_0070_s1::manifest(&p, &spec_bytes, &rec, Some(&id));
    std::fs::write(out.join("manifest.json"), manifest.to_json())?;

    let t1 = Instant::now();
    let mut mismatches = Vec::new();
    let mut checked = 0;
    if samples > 0 {
        let (n, m) = (rec.frames.len(), samples.min(rec.frames.len()));
        let mut picks: Vec<usize> = (0..m)
            .map(|k| if m == 1 { n - 1 } else { k * (n - 1) / (m - 1) })
            .collect();
        picks.dedup();
        bind(&mut engine, nest, &p, (0, p.plan.end))?;
        for i in picks {
            let f = &rec.frames[i];
            let (top_sql, agg_sql) = check_sql(&p.spec, p.shape.has_sign, f.block);
            let mut top = Vec::new();
            for b in engine.sql(&top_sql).map_err(|e| anyhow!("{e}"))? {
                for r in burrmill::df::encode::rows(&b).map_err(|e| anyhow!("{e}"))? {
                    top.push((text(&r["key"])?, text(&r["s"])?));
                }
            }
            let agg = engine.sql(&agg_sql).map_err(|e| anyhow!("{e}"))?;
            let r = &burrmill::df::encode::rows(&agg[0]).map_err(|e| anyhow!("{e}"))?[0];
            let live = r["live"].as_u64().context("live")? as usize;
            let total = text(&r["total"])?;
            checked += 1;
            if !(top == f.top && live == f.live && total == f.total) {
                mismatches.push(json!({
                    "i": i, "block": f.block,
                    "fold": {"live": f.live, "total": f.total, "top": f.top},
                    "sql": {"live": live, "total": total, "top": top},
                }));
            }
        }
    }
    println!(
        "{}",
        json!({
            "title": p.spec.title,
            "frames_sha256": rec.frames_sha256,
            "frames": rec.frames.len(),
            "chunks": p.plan.chunks.len(),
            "first_block": p.plan.start,
            "segments_read": p.plan.sources.iter().map(|s| s.segments.len()).sum::<usize>(),
            "events": rec.events,
            "live_keys_end": rec.live_keys_end,
            "live_keys_peak": rec.live_keys_peak,
            "replay_secs": replay_secs,
            "fold_secs": fold_secs,
            "check_secs": t1.elapsed().as_secs_f64(),
            "checked": checked,
            "mismatches": mismatches,
            "nid": id.nid,
            "data_identity": id.data_identity,
        })
    );
    if !mismatches.is_empty() {
        std::process::exit(1);
    }
    Ok(())
}
