//! Feed v2 (RFC-0076 FR17): `GET /_admin/feed?feed=2` as length-prefixed frames, negotiated on
//! `hello`, with v1's newline-delimited JSON kept for old followers and for inspection.
//!
//! Every frame is `u32` big-endian payload length, one kind byte, then the payload. `hello`,
//! `mount`, `heartbeat` and the other small lines are one [`JSON`] frame each. A snapshot, which is
//! streamed as it is read and never held whole, goes as [`PART`] frames of v1's JSON ending in an
//! [`END`]. A delta is one [`DELTA`] frame: its dataset first, so a follower knows the delta's
//! budget before reading the rest, then [`encode_delta`]'s body.
//!
//! **A delta's puts are Arrow IPC record batches, one per entity table.** A stored row is a JSON
//! object; each batch has the key, then one Utf8 column per field holding the field's JSON text,
//! and a row is put back together from them byte for byte. A row that would not come back exactly
//! (escaped keys, whitespace, a field order its batch cannot keep) travels whole in the `_row`
//! column, so decoding is exact by construction, not by assumption. Drops, meta and checkpoint
//! changes are compact tagged records, in the commit's order with the puts.

use crate::feed::{Delta, Op};
use anyhow::{bail, ensure, Context, Result};
use arrow::array::{Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use serde_json::value::RawValue;
use std::collections::HashMap;
use std::sync::Arc;

/// The format version v2 speaks in `hello`.
pub const FEED_V2: u32 = 2;
/// The content type a writer answers a v2 feed with, which is how a follower knows which reader to
/// use: a writer that predates v2 ignores `?feed=2` and answers NDJSON.
pub const CONTENT_TYPE: &str = "application/vnd.nuthatch.feed-v2";

pub const JSON: u8 = 1;
pub const PART: u8 = 2;
pub const END: u8 = 3;
pub const DELTA: u8 = 4;

/// The frame header's length: the payload length and the kind.
pub const HEADER: usize = 5;
/// The most a single [`JSON`] frame may carry; anything larger is sent as parts.
pub const MAX_JSON_FRAME: u32 = 16 << 20;
/// The most a dataset name in a [`DELTA`] frame may be.
const MAX_DATASET: usize = 4096;

/// A frame header for a payload of `len` bytes.
pub fn header(kind: u8, len: usize) -> [u8; HEADER] {
    let n = (len as u32).to_be_bytes();
    [n[0], n[1], n[2], n[3], kind]
}

/// One whole frame.
pub fn frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEADER + payload.len());
    out.extend_from_slice(&header(kind, payload.len()));
    out.extend_from_slice(payload);
    out
}

/// A [`DELTA`] frame for `dataset` around an [`encode_delta`] body.
pub fn delta_frame(dataset: &str, body: &[u8]) -> Vec<u8> {
    let len = 2 + dataset.len() + body.len();
    let mut out = Vec::with_capacity(HEADER + len);
    out.extend_from_slice(&header(DELTA, len));
    out.extend_from_slice(&(dataset.len() as u16).to_be_bytes());
    out.extend_from_slice(dataset.as_bytes());
    out.extend_from_slice(body);
    out
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push((v as u8) | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

fn string(out: &mut Vec<u8>, s: &str) {
    varint(out, s.len() as u64);
    out.extend_from_slice(s.as_bytes());
}

struct Cursor<'a> {
    buf: &'a [u8],
    at: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Result<&'a [u8]> {
        let end = self.at.checked_add(n).context("a delta frame overflows")?;
        ensure!(end <= self.buf.len(), "a delta frame ends early");
        let out = &self.buf[self.at..end];
        self.at = end;
        Ok(out)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into()?))
    }

    fn varint(&mut self) -> Result<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.u8()?;
            v |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Ok(v);
            }
        }
        bail!("a varint in a delta frame runs past 64 bits")
    }

    fn string(&mut self) -> Result<String> {
        let n = self.varint()? as usize;
        Ok(std::str::from_utf8(self.take(n)?)
            .context("a string in a delta frame is not UTF-8")?
            .to_string())
    }
}

const RESYNC: u8 = 1;
const CATALOGUE: u8 = 2;

const OP_PUT: u8 = b'P';
const OP_DROP: u8 = b'D';
const OP_META: u8 = b'M';
const OP_UNMETA: u8 = b'U';
const OP_BLOCK: u8 = b'B';
const OP_UNBLOCK: u8 = b'b';
const OP_UNBLOCK_FROM: u8 = b'F';

/// A row's fields in the order its text has them, borrowed from it; `None` when the row is not an
/// object of plainly written keys, which then travels whole.
fn fields(row: &str) -> Option<Vec<(&str, &RawValue)>> {
    struct Visit;
    impl<'de> serde::de::Visitor<'de> for Visit {
        type Value = Vec<(&'de str, &'de RawValue)>;
        fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("an object")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(
            self,
            mut map: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            let mut out = Vec::new();
            while let Some(k) = map.next_key::<&'de str>()? {
                out.push((k, map.next_value::<&'de RawValue>()?));
            }
            Ok(out)
        }
    }
    use serde::Deserializer as _;
    let mut de = serde_json::Deserializer::from_str(row);
    let got = (&mut de).deserialize_map(Visit).ok()?;
    de.end().ok()?;
    Some(got)
}

/// What a decoder makes of a row's fields, which the encoder checks a row against before sending
/// it in pieces.
fn assemble<'a>(fields: impl Iterator<Item = (&'a str, &'a str)>) -> String {
    let mut out = String::from("{");
    for (i, (k, v)) in fields.enumerate() {
        if i > 0 {
            out.push(',');
        }
        out.push_str(&serde_json::Value::String(k.to_string()).to_string());
        out.push(':');
        out.push_str(v);
    }
    out.push('}');
    out
}

/// One entity table's puts as columns.
struct Batch<'a> {
    keys: Vec<&'a str>,
    /// Field names in the order first seen, and each one's column.
    columns: Vec<(&'a str, Vec<Option<&'a str>>)>,
    index: HashMap<&'a str, usize>,
    whole: Vec<Option<&'a str>>,
}

impl<'a> Batch<'a> {
    fn new() -> Self {
        Batch {
            keys: Vec::new(),
            columns: Vec::new(),
            index: HashMap::new(),
            whole: Vec::new(),
        }
    }

    fn push(&mut self, key: &'a str, row: &'a str, fields: Option<Vec<(&'a str, &'a RawValue)>>) {
        let n = self.keys.len();
        self.keys.push(key);
        let split = fields.filter(|f| {
            // Fields must keep the batch's column order, so a row put back together from the columns
            // lists them as its own text does.
            let mut last = None;
            let mut seen = std::collections::HashSet::new();
            for (k, _) in f {
                if !seen.insert(*k) {
                    return false;
                }
                let at = self.index.get(k).copied();
                match (at, last) {
                    (Some(at), Some(l)) if at <= l => return false,
                    (Some(at), _) => last = Some(at),
                    (None, _) => last = Some(usize::MAX),
                }
            }
            assemble(f.iter().map(|(k, v)| (*k, v.get()))) == row
        });
        match split {
            Some(f) => {
                self.whole.push(None);
                for (k, v) in f {
                    let at = match self.index.get(k) {
                        Some(&at) => at,
                        None => {
                            self.index.insert(k, self.columns.len());
                            self.columns.push((k, vec![None; n]));
                            self.columns.len() - 1
                        }
                    };
                    self.columns[at].1.push(Some(v.get()));
                }
            }
            None => self.whole.push(Some(row)),
        }
        for (_, col) in &mut self.columns {
            col.resize(n + 1, None);
        }
    }

    fn record_batch(self) -> Result<RecordBatch> {
        let mut fields = vec![
            Field::new("_key", DataType::Utf8, false),
            Field::new("_row", DataType::Utf8, true),
        ];
        let mut arrays: Vec<Arc<dyn Array>> = vec![
            Arc::new(StringArray::from(self.keys)),
            Arc::new(StringArray::from(self.whole)),
        ];
        for (name, col) in self.columns {
            fields.push(Field::new(name, DataType::Utf8, true));
            arrays.push(Arc::new(StringArray::from(col)));
        }
        Ok(RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays)?)
    }
}

/// The table a row names, as its JSON text, for grouping; rows without one share a batch.
fn table_of<'a>(fields: Option<&Vec<(&'a str, &'a RawValue)>>) -> &'a str {
    fields
        .and_then(|f| f.iter().find(|(k, _)| *k == "table"))
        .map_or("", |(_, v)| v.get())
}

/// A batch as an Arrow IPC stream, LZ4-compressed whole behind its decompressed size, which a
/// follower checks against what its dataset has left before decompressing anything.
fn ipc(batch: &RecordBatch) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut w = arrow::ipc::writer::StreamWriter::try_new(&mut out, &batch.schema())?;
    w.write(batch)?;
    w.finish()?;
    drop(w);
    Ok(lz4_flex::block::compress_prepend_size(&out))
}

/// A delta whose decoded rows would come to more than its dataset has left. Nothing past the
/// limit was allocated.
#[derive(Debug)]
pub struct TooLarge {
    pub needed: u64,
    pub limit: u64,
}

impl std::fmt::Display for TooLarge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a delta decoding to more than {} bytes, its limit",
            self.limit
        )
    }
}

impl std::error::Error for TooLarge {}

/// What decoding may still allocate.
struct Allowance {
    left: Option<u64>,
    limit: u64,
    used: u64,
}

impl Allowance {
    fn fits(&self, n: u64) -> Result<()> {
        if self.left.is_some_and(|left| n > left) {
            return Err(TooLarge {
                needed: self.used.saturating_add(n),
                limit: self.limit,
            }
            .into());
        }
        Ok(())
    }

    fn take(&mut self, n: u64) -> Result<()> {
        self.used = self.used.saturating_add(n);
        if let Some(left) = &mut self.left {
            match left.checked_sub(n) {
                Some(l) => *left = l,
                None => {
                    return Err(TooLarge {
                        needed: self.used,
                        limit: self.limit,
                    }
                    .into())
                }
            }
        }
        Ok(())
    }
}

/// A delta's v2 body: header, catalogue, a batch per entity table, then the ops in order.
pub fn encode_delta(delta: &Delta) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.extend_from_slice(&delta.epoch.to_be_bytes());
    out.extend_from_slice(&delta.generation.to_be_bytes());
    let mut flags = 0;
    if delta.resync {
        flags |= RESYNC;
    }
    if delta.catalogue.is_some() {
        flags |= CATALOGUE;
    }
    out.push(flags);
    if let Some(c) = &delta.catalogue {
        use sha2::Digest;
        out.extend_from_slice(&sha2::Sha256::digest(c.as_bytes()));
        string(&mut out, c);
    }
    let mut tables: Vec<&str> = Vec::new();
    let mut batches: HashMap<&str, Batch> = HashMap::new();
    let mut order: Vec<u64> = Vec::new();
    for op in &delta.ops {
        if let Op::Put { key, row } = op {
            let f = fields(row);
            let table = table_of(f.as_ref());
            let at = match tables.iter().position(|t| *t == table) {
                Some(at) => at,
                None => {
                    tables.push(table);
                    tables.len() - 1
                }
            };
            batches.entry(table).or_insert_with(Batch::new).push(key, row, f);
            order.push(at as u64);
        }
    }
    varint(&mut out, tables.len() as u64);
    for t in &tables {
        let batch = batches.remove(t).context("a table with no batch")?;
        let bytes = ipc(&batch.record_batch()?)?;
        varint(&mut out, bytes.len() as u64);
        out.extend_from_slice(&bytes);
    }
    varint(&mut out, delta.ops.len() as u64);
    let mut puts = order.into_iter();
    for op in &delta.ops {
        match op {
            Op::Put { .. } => {
                out.push(OP_PUT);
                varint(&mut out, puts.next().context("a put with no batch")?);
            }
            Op::Drop { from, to } => {
                out.push(OP_DROP);
                string(&mut out, from);
                match to {
                    Some(to) => {
                        out.push(1);
                        string(&mut out, to);
                    }
                    None => out.push(0),
                }
            }
            Op::Meta { key, value } => {
                out.push(OP_META);
                string(&mut out, key);
                string(&mut out, value);
            }
            Op::Unmeta { key } => {
                out.push(OP_UNMETA);
                string(&mut out, key);
            }
            Op::Block { key, record } => {
                out.push(OP_BLOCK);
                string(&mut out, key);
                string(&mut out, record);
            }
            Op::Unblock { key } => {
                out.push(OP_UNBLOCK);
                string(&mut out, key);
            }
            Op::UnblockFrom { key } => {
                out.push(OP_UNBLOCK_FROM);
                string(&mut out, key);
            }
        }
    }
    Ok(out)
}

/// One batch's rows, back as `(key, row)` in the order they were put: its decompressed size and
/// then its rows' are taken from `allow` before either is allocated.
fn rows_of(bytes: &[u8], allow: &mut Allowance) -> Result<Vec<(String, String)>> {
    let (size, _) = lz4_flex::block::uncompressed_size(bytes)
        .map_err(|e| anyhow::anyhow!("a delta batch with no size: {e}"))?;
    // Freed once the rows are built from it, so held to what is left rather than taken from it.
    allow.fits(size as u64)?;
    let ipc = lz4_flex::block::decompress_size_prepended(bytes)
        .map_err(|e| anyhow::anyhow!("a delta batch that does not decompress: {e}"))?;
    let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(ipc), None)?;
    let mut out = Vec::new();
    for batch in reader {
        let batch = batch?;
        let col = |i: usize| -> Result<&StringArray> {
            batch
                .column(i)
                .as_any()
                .downcast_ref::<StringArray>()
                .context("a delta batch column is not Utf8")
        };
        ensure!(batch.num_columns() >= 2, "a delta batch has no key column");
        let (keys, whole) = (col(0)?, col(1)?);
        let schema = batch.schema();
        let data: Vec<(&str, &StringArray)> = (2..batch.num_columns())
            .map(|i| Ok((schema.field(i).name().as_str(), col(i)?)))
            .collect::<Result<_>>()?;
        // What the rows come to once put back together, from the columns' lengths alone.
        let mut bytes = 0u64;
        for r in 0..batch.num_rows() {
            bytes += if keys.is_valid(r) { keys.value_length(r) as u64 } else { 0 };
            if whole.is_valid(r) {
                bytes += whole.value_length(r) as u64;
                continue;
            }
            bytes += 2;
            for (k, c) in &data {
                if c.is_valid(r) {
                    // Quotes, colon, comma and the escapes a name could take at most.
                    bytes += 6 * k.len() as u64 + 4 + c.value_length(r) as u64;
                }
            }
        }
        allow.take(bytes)?;
        for r in 0..batch.num_rows() {
            ensure!(keys.is_valid(r), "a delta row has no key");
            let row = if whole.is_valid(r) {
                whole.value(r).to_string()
            } else {
                assemble(
                    data.iter()
                        .filter(|(_, c)| c.is_valid(r))
                        .map(|(k, c)| (*k, c.value(r))),
                )
            };
            out.push((keys.value(r).to_string(), row));
        }
    }
    Ok(out)
}

/// [`encode_delta`] undone, allocating for its rows no more than `limit` bytes past what the body
/// itself holds, or failing with [`TooLarge`] before it would.
pub fn decode_delta(body: &[u8], limit: Option<u64>) -> Result<Delta> {
    let mut allow = Allowance {
        left: limit,
        limit: limit.unwrap_or(u64::MAX),
        used: 0,
    };
    let mut c = Cursor { buf: body, at: 0 };
    let epoch = c.u64()?;
    let generation = c.u64()?;
    let flags = c.u8()?;
    let catalogue = if flags & CATALOGUE != 0 {
        use sha2::Digest;
        let want = c.take(32)?.to_vec();
        let raw = c.string()?;
        ensure!(
            sha2::Sha256::digest(raw.as_bytes()).as_slice() == want.as_slice(),
            "a delta's catalogue does not match the hash its header carries"
        );
        Some(raw)
    } else {
        None
    };
    let tables = c.varint()? as usize;
    let mut batches = Vec::with_capacity(tables.min(1024));
    for _ in 0..tables {
        let n = c.varint()? as usize;
        batches.push(rows_of(c.take(n)?, &mut allow)?.into_iter());
    }
    let n = c.varint()? as usize;
    let mut ops = Vec::with_capacity(n.min(1 << 16));
    for _ in 0..n {
        let op = match c.u8()? {
            OP_PUT => {
                let at = c.varint()? as usize;
                let (key, row) = batches
                    .get_mut(at)
                    .and_then(Iterator::next)
                    .context("a put names a row its batch does not have")?;
                Op::Put { key, row }
            }
            OP_DROP => {
                let from = c.string()?;
                let to = match c.u8()? {
                    0 => None,
                    _ => Some(c.string()?),
                };
                Op::Drop { from, to }
            }
            OP_META => Op::Meta {
                key: c.string()?,
                value: c.string()?,
            },
            OP_UNMETA => Op::Unmeta { key: c.string()? },
            OP_BLOCK => Op::Block {
                key: c.string()?,
                record: c.string()?,
            },
            OP_UNBLOCK => Op::Unblock { key: c.string()? },
            OP_UNBLOCK_FROM => Op::UnblockFrom { key: c.string()? },
            other => bail!("unknown op {other} in a delta frame"),
        };
        ops.push(op);
    }
    ensure!(c.at == body.len(), "a delta frame has bytes past its ops");
    ensure!(
        batches.iter_mut().all(|b| b.next().is_none()),
        "a delta batch has rows no put names"
    );
    Ok(Delta {
        epoch,
        generation,
        ops,
        catalogue,
        resync: flags & RESYNC != 0,
        ..Delta::default()
    })
}

/// A [`DELTA`] frame's dataset, read from the start of its payload.
pub fn split_dataset(payload_head: &[u8]) -> Result<usize> {
    ensure!(payload_head.len() >= 2, "a delta frame with no dataset");
    let n = u16::from_be_bytes([payload_head[0], payload_head[1]]) as usize;
    ensure!(n <= MAX_DATASET, "a delta frame's dataset name is too long");
    Ok(n)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(block: u64, i: u64, table: &str, extra: &str) -> (String, String) {
        (
            format!("{block:012}-{i:06}"),
            format!(
                "{{\"block_number\":{block},\"log_index\":{i},\"table\":\"{table}\",\"from\":\"0x{:040x}\",\"value\":\"{}\"{extra}}}",
                block * 7 + i,
                block * 1_000_000 + i
            ),
        )
    }

    fn sample() -> Delta {
        let mut ops = vec![Op::Drop {
            from: "000000000009-000000".into(),
            to: None,
        }];
        for i in 0..20 {
            let (key, row) = row(10, i, if i % 3 == 0 { "approval" } else { "transfer" }, "");
            ops.push(Op::Put { key, row });
        }
        // Rows a batch cannot split: escaped key, whitespace, an order against the batch's, not an
        // object, a missing table, unicode.
        ops.push(Op::Put {
            key: "000000000010-000100".into(),
            row: r#"{"block_number":10,"table":"transfer","fr\u006fm":"x"}"#.into(),
        });
        ops.push(Op::Put {
            key: "000000000010-000101".into(),
            row: r#"{"block_number": 10, "table":"transfer"}"#.into(),
        });
        ops.push(Op::Put {
            key: "000000000010-000102".into(),
            row: r#"{"table":"transfer","block_number":10}"#.into(),
        });
        ops.push(Op::Put {
            key: "000000000010-000103".into(),
            row: "[1,2,3]".into(),
        });
        ops.push(Op::Put {
            key: "000000000010-000104".into(),
            row: r#"{"block_number":10,"note":"ünïcødé \"q\" null","v":null,"o":{"a":[1,{"b":2}]}}"#
                .into(),
        });
        ops.push(Op::Put {
            key: "000000000010-000105".into(),
            row: "not json at all".into(),
        });
        ops.push(Op::Meta {
            key: "last_block".into(),
            value: "10".into(),
        });
        ops.push(Op::Block {
            key: "000000000010".into(),
            record: "0xabc|1700000010".into(),
        });
        ops.push(Op::Unblock {
            key: "000000000001".into(),
        });
        ops.push(Op::UnblockFrom {
            key: "000000000011".into(),
        });
        ops.push(Op::Unmeta {
            key: "gone".into(),
        });
        ops.push(Op::Drop {
            from: "000000000001-000000".into(),
            to: Some("000000000002-999999".into()),
        });
        Delta {
            epoch: 0xdead_beef,
            generation: 42,
            ops,
            catalogue: Some(r#"{"manifest_version":1,"tables":{}}"#.into()),
            resync: false,
            ..Delta::default()
        }
    }

    #[test]
    fn a_v2_delta_decodes_to_exactly_the_delta_encoded() {
        let d = sample();
        let body = encode_delta(&d).unwrap();
        let back = decode_delta(&body, None).unwrap();
        assert_eq!(back.ops, d.ops);
        assert_eq!(
            (back.epoch, back.generation, &back.catalogue, back.resync),
            (d.epoch, d.generation, &d.catalogue, d.resync)
        );
        let resync = Delta {
            resync: true,
            ..Delta::default()
        };
        assert!(decode_delta(&encode_delta(&resync).unwrap(), None).unwrap().resync);
    }

    /// Generated rows in every shape a field may take, in random orders and mixes: what comes back
    /// is what went in, every time.
    #[test]
    fn random_deltas_round_trip() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        let values = [
            "1",
            "-3.5e7",
            "\"0xabc\"",
            "null",
            "true",
            "[]",
            "{\"x\":\"\\\"\"}",
            "\"\\u00e9\"",
        ];
        let names = ["table", "a", "b", "block_number", "c", "d"];
        for _ in 0..300 {
            let mut ops = Vec::new();
            for i in 0..(next() % 40) {
                if next() % 9 == 0 {
                    ops.push(Op::Meta {
                        key: format!("m{}", next() % 3),
                        value: format!("{}", next()),
                    });
                    continue;
                }
                let mut row = String::from("{");
                let mut first = true;
                for name in names {
                    if next() % 3 == 0 {
                        continue;
                    }
                    if !first {
                        row.push(',');
                    }
                    first = false;
                    let v = if name == "table" {
                        format!("\"t{}\"", next() % 3)
                    } else {
                        values[(next() % values.len() as u64) as usize].to_string()
                    };
                    row.push_str(&format!("\"{name}\":{v}"));
                }
                row.push('}');
                ops.push(Op::Put {
                    key: format!("{:012}-{i:06}", next() % 100),
                    row,
                });
            }
            let d = Delta {
                epoch: next(),
                generation: next(),
                ops,
                ..Delta::default()
            };
            let back = decode_delta(&encode_delta(&d).unwrap(), None).unwrap();
            assert_eq!(back.ops, d.ops);
        }
    }

    #[test]
    fn a_tampered_catalogue_is_refused() {
        let d = sample();
        let mut body = encode_delta(&d).unwrap();
        // The catalogue's first byte sits after epoch, generation, flags, hash and its length.
        body[8 + 8 + 1 + 32 + 1] ^= 1;
        assert!(decode_delta(&body, None).is_err());
    }
}
