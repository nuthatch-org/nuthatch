//! RFC-0075 S3: where a tab's sealed segments go. The node's floor, so a table's newest segment stays
//! provisional and is folded back in through `nuthatch_seal::segment_rows`, as the node's is.
//!
//! RFC-0077 S3: over a store, the segments and their catalogue live in the store's own file, in
//! tables beside the hot rows, so a reopened nest has its sealed history. A seal is synchronous
//! inside the window's commit, and a browser opens a file only asynchronously, so a file per
//! segment could only be written behind the seal, after the hot rows it holds were pruned.
//!
//! RFC-0077 S4: history hydrated from a mirror sits in a pack, one host file the segments are
//! appended to, and is read by range; the store records where each one starts and ends.

use anyhow::{anyhow, bail, Context, Result};
use nuthatch_core::redb_store::Store;
use nuthatch_core::restart::SealedHistory;
use nuthatch_core::seal::{Manifest, SegmentSink, Sinks};
use nuthatch_core::store::SEALED_THROUGH_KEY;
use nuthatch_decode::registry::{DecodedRow, TableSchema};
use nuthatch_seal::cut::{Segment, SEAL_TABLE_FLOOR};
use redb::{ReadableTable, ReadableTableMetadata, StorageBackend, TableDefinition};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Mutex};

/// Each segment's bytes by its file name, as `segments/` holds them on a node.
const SEGMENTS: TableDefinition<&str, &[u8]> = TableDefinition::new("cellar_segments");
/// The catalogue, as `manifest.json` on a node.
const CATALOGUE: TableDefinition<&str, &str> = TableDefinition::new("cellar_manifest");
/// A mirrored segment's place in the pack: its offset and length.
const PACKED: TableDefinition<&str, (u64, u64)> = TableDefinition::new("cellar_packed");
const MANIFEST: &str = "manifest.json";

#[derive(Default)]
pub struct Cellar {
    /// Where segments are kept; none holds them in memory only.
    store: Option<Store>,
    /// The host file mirrored segments were appended to.
    pack: Option<Arc<dyn StorageBackend>>,
    manifest: Mutex<Manifest>,
    /// Every file without a store; with one, those published and not yet installed.
    files: Mutex<HashMap<String, Vec<u8>>>,
}

impl std::fmt::Debug for Cellar {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Cellar")
    }
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
                    // A mirrored tail the seal folded in is no longer named; its bytes stay in the
                    // pack, which only grows.
                    let mut packed = wtx.open_table(PACKED)?;
                    let stale: Vec<String> = packed
                        .iter()?
                        .map(|r| r.map(|(k, _)| k.value().to_string()))
                        .collect::<Result<_, _>>()?;
                    for file in stale.iter().filter(|f| !named.contains(f)) {
                        packed.remove(file.as_str())?;
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
        Self::over_with_pack(store, None)
    }

    /// [`Self::over`], with mirrored history read from `pack`. A store whose catalogue places
    /// segments in a pack is refused without one.
    pub fn over_with_pack(store: Store, pack: Option<Arc<dyn StorageBackend>>) -> Result<Cellar> {
        let wtx = store.database().begin_write()?;
        let manifest = {
            wtx.open_table(SEGMENTS)?;
            let packed = wtx.open_table(PACKED)?.len()?;
            if packed > 0 && pack.is_none() {
                bail!(
                    "this nest's sealed history was hydrated from a mirror into a pack beside its \
                     store, and it was opened without one: open it with the same `mirror`"
                );
            }
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
            pack,
            manifest: Mutex::new(manifest),
            files: Mutex::default(),
        })
    }

    fn packed(&self, name: &str) -> Result<Option<(u64, u64)>> {
        let Some(store) = &self.store else {
            return Ok(None);
        };
        let rtx = store.database().begin_read()?;
        let table = rtx.open_table(PACKED)?;
        let at = table.get(name)?.map(|v| v.value());
        Ok(at)
    }

    fn pack(&self) -> Result<&Arc<dyn StorageBackend>> {
        self.pack
            .as_ref()
            .ok_or_else(|| anyhow!("a mirrored segment is named, and no pack is open"))
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
        if let Some(v) = table.get(name)? {
            return Ok(Some(v.value().to_vec()));
        }
        drop(table);
        drop(rtx);
        match self.packed(name)? {
            Some((at, len)) => Ok(Some(self.pack()?.read(at, len as usize)?)),
            None => Ok(None),
        }
    }

    /// A segment's length, without reading it where it is packed.
    pub fn len_of(&self, name: &str) -> Result<Option<u64>> {
        if let Some((_, len)) = self.packed(name)? {
            return Ok(Some(len));
        }
        Ok(self.read(name)?.map(|b| b.len() as u64))
    }

    /// `range` of a segment's bytes, read by range from the pack where it is packed.
    pub fn read_range(&self, name: &str, range: Range<u64>) -> Result<Option<Vec<u8>>> {
        if let Some((at, len)) = self.packed(name)? {
            if range.end > len || range.start > range.end {
                bail!("{name}: bytes {range:?} of {len}");
            }
            let n = (range.end - range.start) as usize;
            return Ok(Some(self.pack()?.read(at + range.start, n)?));
        }
        Ok(self
            .read(name)?
            .map(|b| b[range.start as usize..(range.end as usize).min(b.len())].to_vec()))
    }

    /// Forget what a hydration that did not finish placed, so it can start again.
    pub fn reset_packed(&self) -> Result<()> {
        let store = self.store.as_ref().context("a pack needs a store")?;
        let wtx = store.database().begin_write()?;
        {
            let mut packed = wtx.open_table(PACKED)?;
            let names: Vec<String> = packed
                .iter()?
                .map(|r| r.map(|(k, _)| k.value().to_string()))
                .collect::<Result<_, _>>()?;
            for n in names {
                packed.remove(n.as_str())?;
            }
            wtx.open_table(CATALOGUE)?.insert(
                MANIFEST,
                serde_json::to_string(&Manifest::default())?.as_str(),
            )?;
        }
        wtx.commit()?;
        self.pack()?.set_len(0)?;
        *self.manifest.lock().unwrap() = Manifest::default();
        Ok(())
    }

    /// Append `bytes` to the pack; where it went.
    pub fn append(&self, bytes: &[u8]) -> Result<(u64, u64)> {
        let pack = self.pack()?;
        let at = pack.len()?;
        pack.write(at, bytes)?;
        Ok((at, bytes.len() as u64))
    }

    /// The SHA-256 of `len` bytes of the pack at `at`, read a slice at a time.
    pub fn pack_hash(&self, at: u64, len: u64) -> Result<String> {
        use sha2::Digest;
        const SLICE: u64 = 4 << 20;
        let pack = self.pack()?;
        let mut h = sha2::Sha256::new();
        let mut done = 0;
        while done < len {
            let n = (len - done).min(SLICE);
            h.update(pack.read(at + done, n as usize)?);
            done += n;
        }
        Ok(hex::encode(h.finalize()))
    }

    /// Every segment the catalogue places in the pack, read back against its content address: how
    /// many, and their bytes.
    pub fn check_packed(&self) -> Result<(usize, u64)> {
        let (mut count, mut bytes) = (0, 0);
        for s in self.manifest().tables.values().flatten() {
            let Some((at, len)) = self.packed(&s.file)? else {
                continue;
            };
            if self.pack_hash(at, len)? != s.hash {
                bail!("{} in the pack does not hash to its content address", s.file);
            }
            count += 1;
            bytes += len;
        }
        Ok((count, bytes))
    }

    /// Make the pack durable, then install `manifest` naming the segments `placed` put in it, in one
    /// transaction.
    pub fn install_packed(&self, manifest: &Manifest, placed: &[(String, u64, u64)]) -> Result<()> {
        self.pack()?.sync_data(false)?;
        let store = self.store.as_ref().context("a pack needs a store")?;
        let wtx = store.database().begin_write()?;
        {
            let mut packed = wtx.open_table(PACKED)?;
            for (name, at, len) in placed {
                packed.insert(name.as_str(), (*at, *len))?;
            }
            wtx.open_table(CATALOGUE)?
                .insert(MANIFEST, serde_json::to_string(manifest)?.as_str())?;
        }
        wtx.commit()?;
        *self.manifest.lock().unwrap() = manifest.clone();
        Ok(())
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
    pub fn bytes(&self) -> u64 {
        self.manifest()
            .tables
            .values()
            .flatten()
            .filter_map(|s| self.len_of(&s.file).ok().flatten())
            .sum()
    }
}

/// The cellar as the object store Burrmill reads segments from, by range, under `segments/`.
#[derive(Debug, Clone)]
pub struct Segments(pub Arc<Cellar>);

impl std::fmt::Display for Segments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("cellar")
    }
}

pub const SEGMENTS_PREFIX: &str = "segments/";

fn store_err(e: impl std::fmt::Display) -> object_store::Error {
    object_store::Error::Generic {
        store: "cellar",
        source: e.to_string().into(),
    }
}

impl Segments {
    fn name(location: &object_store::path::Path) -> object_store::Result<&str> {
        location
            .as_ref()
            .strip_prefix(SEGMENTS_PREFIX)
            .ok_or_else(|| object_store::Error::NotFound {
                path: location.to_string(),
                source: "not a segment".into(),
            })
    }

    fn meta(
        &self,
        location: &object_store::path::Path,
    ) -> object_store::Result<object_store::ObjectMeta> {
        let name = Self::name(location)?;
        let size = self.0.len_of(name).map_err(store_err)?.ok_or_else(|| {
            object_store::Error::NotFound {
                path: location.to_string(),
                source: "not in the cellar".into(),
            }
        })?;
        Ok(object_store::ObjectMeta {
            location: location.clone(),
            last_modified: Default::default(),
            size,
            e_tag: None,
            version: None,
        })
    }

    fn unsupported(operation: &str) -> object_store::Error {
        object_store::Error::NotImplemented {
            operation: operation.into(),
            implementer: "cellar".into(),
        }
    }
}

#[async_trait::async_trait]
impl object_store::ObjectStore for Segments {
    async fn put_opts(
        &self,
        _: &object_store::path::Path,
        _: object_store::PutPayload,
        _: object_store::PutOptions,
    ) -> object_store::Result<object_store::PutResult> {
        Err(Self::unsupported("put"))
    }

    async fn put_multipart_opts(
        &self,
        _: &object_store::path::Path,
        _: object_store::PutMultipartOptions,
    ) -> object_store::Result<Box<dyn object_store::MultipartUpload>> {
        Err(Self::unsupported("put_multipart"))
    }

    async fn get_opts(
        &self,
        location: &object_store::path::Path,
        options: object_store::GetOptions,
    ) -> object_store::Result<object_store::GetResult> {
        let meta = self.meta(location)?;
        let range = match &options.range {
            Some(r) => r.as_range(meta.size).map_err(store_err)?,
            None => 0..meta.size,
        };
        let bytes = if options.head {
            bytes::Bytes::new()
        } else {
            self.0
                .read_range(Self::name(location)?, range.clone())
                .map_err(store_err)?
                .map(bytes::Bytes::from)
                .unwrap_or_default()
        };
        Ok(object_store::GetResult {
            payload: object_store::GetResultPayload::Stream(Box::pin(futures::stream::once(
                async move { Ok(bytes) },
            ))),
            meta,
            range,
            attributes: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        locations: futures::stream::BoxStream<
            'static,
            object_store::Result<object_store::path::Path>,
        >,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::path::Path>> {
        use futures::StreamExt;
        locations.map(|_| Err(Self::unsupported("delete"))).boxed()
    }

    fn list(
        &self,
        _: Option<&object_store::path::Path>,
    ) -> futures::stream::BoxStream<'static, object_store::Result<object_store::ObjectMeta>> {
        use futures::StreamExt;
        futures::stream::once(async { Err(Self::unsupported("list")) }).boxed()
    }

    async fn list_with_delimiter(
        &self,
        _: Option<&object_store::path::Path>,
    ) -> object_store::Result<object_store::ListResult> {
        Err(Self::unsupported("list_with_delimiter"))
    }

    async fn copy_opts(
        &self,
        _: &object_store::path::Path,
        _: &object_store::path::Path,
        _: object_store::CopyOptions,
    ) -> object_store::Result<()> {
        Err(Self::unsupported("copy"))
    }
}
