//! The same pass in wasm, over segments read through JavaScript, as a tab would run it.

use std::collections::BTreeMap;
use std::sync::Arc;

use js_sys::Function;
use nuthatch_timelapse::{manifest::identity, Plan, Replay};
use object_store::path::Path;
use object_store::ObjectStoreExt;
use serde_json::json;
use wasm_bindgen::prelude::*;

use crate::js_store::JsStore;

fn err(e: impl std::fmt::Display) -> JsError {
    JsError::new(&e.to_string())
}

#[wasm_bindgen(js_name = wasmMemory)]
pub fn wasm_memory() -> JsValue {
    wasm_bindgen::memory()
}

/// `size(key)` and `read(key, offset, length)` read the nest directory; `inputs` is a JSON object of
/// each authored input's path to its bytes in hex, empty for a mirror, which publishes none, and then
/// the manifest's NID and data identity are null. `progress(done, total)` is called after each chunk.
/// Answers a JSON summary holding the frames and the manifest.
#[wasm_bindgen]
#[allow(clippy::too_many_arguments)]
pub async fn replay(
    size: Function,
    read: Function,
    spec: String,
    manifest: String,
    schema: String,
    inputs: String,
    chunk_rows: f64,
    progress: Option<Function>,
) -> Result<String, JsError> {
    let p = crate::prepare(
        &spec,
        manifest.as_bytes(),
        schema.as_bytes(),
        chunk_rows as u64,
    )
    .map_err(err)?;
    let inputs: BTreeMap<String, String> = serde_json::from_str(&inputs).map_err(err)?;
    let inputs = inputs
        .into_iter()
        .map(|(k, v)| Ok((k, hex::decode(v)?)))
        .collect::<Result<BTreeMap<_, _>, hex::FromHexError>>()
        .map_err(err)?;
    let id = if inputs.is_empty() {
        None
    } else {
        Some(identity(inputs).map_err(err)?)
    };

    let store = Arc::new(JsStore::new(size, read));
    let mut engine = burrmill::Engine::open_empty().map_err(err)?;
    engine.attach_store(store.clone()).map_err(err)?;
    let t0 = js_sys::Date::now();
    let mut fold_ms = 0f64;
    let mut replay = Replay::new(&p.spec, &p.plan, &p.shape);
    let chunk_sql = Plan::chunk_sql(&p.spec.quantity.sql);
    let mut done = 0usize;
    for &(lo, hi) in &p.plan.chunks {
        store.release();
        for s in &p.plan.sources {
            let mut keys = Vec::new();
            for g in Plan::overlapping(s, lo, hi) {
                let key = format!("segments/{}", g.file);
                let meta = store.head(&Path::from(key.as_str())).await.map_err(err)?;
                keys.push((key, meta.size));
            }
            engine
                .register_store_facts(
                    &s.table,
                    &p.declared[&s.table],
                    keys,
                    &[],
                    (lo.checked_sub(1), Some(hi)),
                )
                .await
                .map_err(|e| err(format!("binding {}: {e}", s.table)))?;
        }
        replay.begin_chunk(lo, hi);
        let mut failed = None;
        let ran = engine
            .sql_async_for_each(&chunk_sql, |batch| {
                let t = js_sys::Date::now();
                let r = replay.push_batch(&batch);
                fold_ms += js_sys::Date::now() - t;
                r.map_err(|e| {
                    failed = Some(e);
                    burrmill::BurrmillError::Cancelled
                })
            })
            .await;
        if let Some(e) = failed {
            return Err(err(format!("{e:#}")));
        }
        ran.map_err(err)?;
        if let Some(f) = &progress {
            done += 1;
            let _ = f.call2(
                &JsValue::NULL,
                &JsValue::from_f64(done as f64),
                &JsValue::from_f64(p.plan.chunks.len() as f64),
            );
        }
    }
    store.release();
    let rec = replay.finish();
    let replay_ms = js_sys::Date::now() - t0;
    let m = crate::manifest(&p, spec.as_bytes(), &rec, id.as_ref());
    Ok(json!({
        "title": p.spec.title,
        "frames_sha256": rec.frames_sha256,
        "frames": rec.frames.len(),
        "chunks": p.plan.chunks.len(),
        "events": rec.events,
        "live_keys_end": rec.live_keys_end,
        "live_keys_peak": rec.live_keys_peak,
        "replay_secs": replay_ms / 1e3,
        "fold_secs": fold_ms / 1e3,
        "nid": id.as_ref().map(|i| &i.nid),
        "data_identity": id.as_ref().map(|i| &i.data_identity),
        "jsonl": rec.jsonl,
        "manifest": m.to_json(),
    })
    .to_string())
}
