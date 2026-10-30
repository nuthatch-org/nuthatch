//! RFC-0075 S3: where a tab's sealed segments go. The node's floor, so a table's newest segment stays
//! provisional and is folded back in through `nuthatch_seal::segment_rows`, as the node's is.
//!
//! RFC-0077 S3: over a store, the segments and their catalogue live in the store's own file, in
//! tables beside the hot rows, so a reopened nest has its sealed history. A seal is synchronous
//! inside the window's commit, and a browser opens a file only asynchronously, so a file per
//! segment could only be written behind the seal, after the hot rows it holds were pruned.

use anyhow::{bail, Context, Result};
use nuthatch_core::redb_store::Store;
use nuthatch_core::restart::SealedHistory;
use nuthatch_core::seal::{Manifest, SegmentSink, Sinks};
use nuthatch_core::store::SEALED_THROUGH_KEY;
use nuthatch_decode::registry::{DecodedRow, TableSchema};
use nuthatch_seal::cut::{Segment, SEAL_TABLE_FLOOR};
use redb::{ReadableTable, TableDefinition};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Mutex;

/// Each segment's bytes by its file name, as `segments/` holds them on a node.
const SEGMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("cellar_segments");
/// The catalogue, as `manifest.json` on a node.
const CATALOGUE: TableDefinition<&str, &str> = TableDefinition::new("cellar_manifest");
const MANIFEST: &str = "manifest.json";

#[derive(Default)]
pub struct Cellar {
    /// Where segments are kept; none holds them in memory only.
    store: Option<Store>,
    manifest: Mutex<Manifest>,
    /// Every file without a store; with one, those published and not yet installed.
    files: Mutex<HashMap<String, Vec<u8>>>,
}

struct Open<'a>(&'a Cellar);

impl SegmentSink for Open<'_> {
    fn manifest(&mut self) -> Result<Manifest> {
        Ok(self.0.manifest.lock().unwrap().clone())
    }

    fn intact(&self, s: &Segment) -> bool {
        self.0
            .read(&s.file)
            .ok()
            .flatten()
            .is_some_and(|b| nuthatch_seal::content_hash(&b) == s.hash)
    }

    fn restore(&mut self, s: &Segment, bytes: &[u8]) -> Result<()> {
        match &self.0.store {
            Some(store) => {
                let wtx = store.database().begin_write()?;
                wtx.open_table(SEGMENTS)?.insert(s.file.as_str(), bytes)?;
                wtx.commit()?;
            }
            None => {
                self.0
                    .files
                    .lock()
                    .unwrap()
                    .insert(s.file.clone(), bytes.to_vec());
            }
        }
        Ok(())
    }

    fn rows(&self, s: &Segment) -> Result<Vec<Value>> {
        let Some(bytes) = self.0.read(&s.file)? else {
            bail!("segment {} is not in the cellar", s.file);
        };
        nuthatch_seal::segment_rows(bytes.into())
    }

    fn table_floor(&self) -> usize {
        SEAL_TABLE_FLOOR
    }

    fn publish(&mut self, file: &str, _: &str, bytes: &[u8], _: Option<&Segment>) -> Result<()> {
        self.0
            .files
            .lock()
            .unwrap()
            .insert(file.into(), bytes.to_vec());
        Ok(())
    }

    fn install(&mut self, manifest: &Manifest) -> Result<()> {
        let named: Vec<String> = manifest
            .tables
            .values()
            .flatten()
            .map(|s| s.file.clone())
            .collect();
        let mut files = self.0.files.lock().unwrap();
        match &self.0.store {
            // The new segments, the catalogue naming them and the removal of what it no longer
            // names commit together, before the seal prunes hot.
            Some(store) => {
                let wtx = store.database().begin_write()?;
                {
                    let mut segments = wtx.open_table(SEGMENTS)?;
                    for (file, bytes) in files.iter() {
                        segments.insert(file.as_str(), bytes.as_slice())?;
                    }
                    let stale: Vec<String> = segments
                        .iter()?
                        .map(|r| r.map(|(k, _)| k.value().to_string()))
                        .collect::<Result<_, _>>()?;
                    for file in stale.iter().filter(|f| !named.contains(f)) {
                        segments.remove(file.as_str())?;
                    }
                    wtx.open_table(CATALOGUE)?
                        .insert(MANIFEST, serde_json::to_string(manifest)?.as_str())?;
                }
                wtx.commit()?;
                files.clear();
            }
            None => files.retain(|f, _| named.contains(f)),
        }
        *self.0.manifest.lock().unwrap() = manifest.clone();
        Ok(())
    }
}

impl Sinks for Cellar {
    fn open(&self) -> Box<dyn SegmentSink + '_> {
        Box::new(Open(self))
    }
}

impl SealedHistory for Cellar {
    fn manifest(&self) -> Result<Manifest> {
        Ok(Cellar::manifest(self))
    }

    fn segment_rows(&self, segment: &Segment, schema: &TableSchema) -> Result<Vec<DecodedRow>> {
        let bytes = self
            .read(&segment.file)?
            .with_context(|| format!("segment {} is not in the cellar", segment.file))?;
        nuthatch_seal::segment_rows(bytes.into())?
            .iter()
            .map(|v| DecodedRow::from_stored(v, schema))
            .collect()
    }

    fn segment_columns(
        &self,
        segment: &Segment,
        columns: &[&str],
        visit: &mut dyn FnMut(Value),
    ) -> Result<()> {
        let bytes = self
            .read(&segment.file)?
            .with_context(|| format!("segment {} is not in the cellar", segment.file))?;
        for row in nuthatch_seal::segment_rows(bytes.into())? {
            let Value::Object(mut row) = row else {
                continue;
            };
            row.retain(|k, v| columns.contains(&k.as_str()) && !v.is_null());
            visit(Value::Object(row));
        }
        Ok(())
    }
}

impl Cellar {
    /// The segments `store` holds, which survive it being closed and opened again. A store that
    /// sealed before its segments were kept there has lost them, and is refused.
    pub fn over(store: Store) -> Result<Cellar> {
        let wtx = store.database().begin_write()?;
        let manifest = {
            wtx.open_table(SEGMENTS)?;
            let mut catalogue = wtx.open_table(CATALOGUE)?;
            let held = catalogue.get(MANIFEST)?.map(|v| v.value().to_string());
            match held {
                Some(json) => serde_json::from_str(&json).context("the cellar's manifest")?,
                None => {
                    if let Some(through) = store.get_meta(SEALED_THROUGH_KEY)? {
                        bail!(
                            "this store sealed through block {through} before its segments were \
                             kept beside it (RFC-0077 S3), so its sealed history is gone: open it fresh"
                        );
                    }
                    let empty = Manifest::default();
                    catalogue.insert(MANIFEST, serde_json::to_string(&empty)?.as_str())?;
                    empty
                }
            }
        };
        wtx.commit()?;
        Ok(Cellar {
            store: Some(store),
            manifest: Mutex::new(manifest),
            files: Mutex::default(),
        })
    }

    fn read(&self, name: &str) -> Result<Option<Vec<u8>>> {
        if let Some(b) = self.files.lock().unwrap().get(name) {
            return Ok(Some(b.clone()));
        }
        let Some(store) = &self.store else {
            return Ok(None);
        };
        let rtx = store.database().begin_read()?;
        let table = rtx.open_table(SEGMENTS)?;
        let bytes = table.get(name)?.map(|v| v.value().to_vec());
        Ok(bytes)
    }

    pub fn manifest(&self) -> Manifest {
        self.manifest.lock().unwrap().clone()
    }

    pub fn file(&self, name: &str) -> Option<Vec<u8>> {
        self.read(name).ok().flatten()
    }

    /// Every table's segments as the gate compares them, each with the hash of the bytes held.
    pub fn report(&self) -> Value {
        let tables: serde_json::Map<String, Value> = self
            .manifest()
            .tables
            .iter()
            .map(|(t, segs)| {
                let segs = segs
                    .iter()
                    .map(|s| {
                        json!({
                            "from_block": s.from_block,
                            "to_block": s.to_block,
                            "rows": s.rows,
                            "hash": s.hash,
                            "provisional": s.provisional,
                            "bytes_hash": self.file(&s.file).map(|b| nuthatch_seal::content_hash(&b)),
                        })
                    })
                    .collect();
                (t.clone(), Value::Array(segs))
            })
            .collect();
        Value::Object(tables)
    }

    /// Each table's first sealed row, read back from its segment: what a hash mismatch is read by.
    pub fn first_rows(&self) -> Value {
        let rows: serde_json::Map<String, Value> = self
            .manifest()
            .tables
            .iter()
            .filter_map(|(t, segs)| {
                let bytes = self.file(&segs.first()?.file)?;
                let row = nuthatch_seal::segment_rows(bytes.into())
                    .ok()?
                    .into_iter()
                    .next()?;
                Some((t.clone(), row))
            })
            .collect();
        Value::Object(rows)
    }

    /// The bytes of every segment the catalogue names.
    pub fn bytes(&self) -> usize {
        self.manifest()
            .tables
            .values()
            .flatten()
            .filter_map(|s| self.file(&s.file))
            .map(|b| b.len())
            .sum()
    }
}
