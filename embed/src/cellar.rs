//! RFC-0075 S3: where a tab's sealed segments go. The node's floor, so a table's newest segment stays
//! provisional and is folded back in through `nuthatch_seal::segment_rows`, as the node's is.

use anyhow::{bail, Result};
use nuthatch_core::seal::{Manifest, SegmentSink, Sinks};
use nuthatch_seal::cut::{Segment, SEAL_TABLE_FLOOR};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Mutex;

#[derive(Default)]
pub struct Cellar {
    manifest: Mutex<Manifest>,
    files: Mutex<HashMap<String, Vec<u8>>>,
}

struct Open<'a>(&'a Cellar);

impl SegmentSink for Open<'_> {
    fn manifest(&mut self) -> Result<Manifest> {
        Ok(self.0.manifest.lock().unwrap().clone())
    }

    fn intact(&self, s: &Segment) -> bool {
        self.0
            .files
            .lock()
            .unwrap()
            .get(&s.file)
            .is_some_and(|b| nuthatch_seal::content_hash(b) == s.hash)
    }

    fn restore(&mut self, s: &Segment, bytes: &[u8]) -> Result<()> {
        self.0
            .files
            .lock()
            .unwrap()
            .insert(s.file.clone(), bytes.to_vec());
        Ok(())
    }

    fn rows(&self, s: &Segment) -> Result<Vec<Value>> {
        let Some(bytes) = self.0.files.lock().unwrap().get(&s.file).cloned() else {
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
        self.0
            .files
            .lock()
            .unwrap()
            .retain(|f, _| named.contains(f));
        *self.0.manifest.lock().unwrap() = manifest.clone();
        Ok(())
    }
}

impl Sinks for Cellar {
    fn open(&self) -> Box<dyn SegmentSink + '_> {
        Box::new(Open(self))
    }
}

impl Cellar {
    pub fn manifest(&self) -> Manifest {
        self.manifest.lock().unwrap().clone()
    }

    pub fn file(&self, name: &str) -> Option<Vec<u8>> {
        self.files.lock().unwrap().get(name).cloned()
    }

    /// Every table's segments as the gate compares them, each with the hash of the bytes held.
    pub fn report(&self) -> Value {
        let files = self.files.lock().unwrap();
        let tables: serde_json::Map<String, Value> = self
            .manifest
            .lock()
            .unwrap()
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
                            "bytes_hash": files.get(&s.file).map(|b| nuthatch_seal::content_hash(b)),
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
        let files = self.files.lock().unwrap();
        let manifest = self.manifest.lock().unwrap();
        let rows: serde_json::Map<String, Value> = manifest
            .tables
            .iter()
            .filter_map(|(t, segs)| {
                let bytes = files.get(&segs.first()?.file)?.clone();
                let row = nuthatch_seal::segment_rows(bytes.into())
                    .ok()?
                    .into_iter()
                    .next()?;
                Some((t.clone(), row))
            })
            .collect();
        Value::Object(rows)
    }

    pub fn bytes(&self) -> usize {
        self.files.lock().unwrap().values().map(Vec::len).sum()
    }
}
