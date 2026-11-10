//! An Arrow IPC stream from outside, refused where arrow-ipc 59.3's `StreamReader` would panic or
//! abort on it rather than err (#2811). The tab is built panic=abort, so the reader must never see one.
//!
//! The schema is checked as a flatbuffer, because converting it is where most of the panics are
//! (`convert::get_data_type`), then each batch's nodes and buffers are walked in the order the
//! reader takes them. Whatever the reader refuses with an error is left to it.

use std::collections::VecDeque;

use arrow_data::BufferSpec;
use arrow_ipc::{
    DateUnit, Endianness, IntervalUnit, MessageHeader, MetadataVersion, Precision, TimeUnit, Type,
    UnionMode,
};
use arrow_schema::{DataType, Schema};

const CONTINUATION: [u8; 4] = [0xff; 4];

/// `Err` naming what in `stream` the reader would panic or abort on; `Ok` leaves it to the reader.
pub fn check(stream: &[u8]) -> Result<(), String> {
    let mut rest = stream;
    let mut schema: Option<Schema> = None;
    loop {
        let Some(mut len) = take4(&mut rest) else {
            return Ok(());
        };
        if len == CONTINUATION {
            let Some(l) = take4(&mut rest) else {
                return Ok(());
            };
            len = l;
        }
        let Ok(len) = usize::try_from(i32::from_le_bytes(len)) else {
            return Ok(());
        };
        if len == 0 {
            return Ok(());
        }
        // The reader allocates both lengths before reading either, so a length past the input is
        // an allocation of up to 2 GiB for the metadata and of anything at all for the body.
        if len > rest.len() {
            return Err(format!(
                "a message of {len} bytes where {} remain",
                rest.len()
            ));
        }
        let (meta, after) = rest.split_at(len);
        rest = after;
        let Ok(message) = arrow_ipc::root_as_message(meta) else {
            return Ok(());
        };
        let body = message.bodyLength();
        let Some(body) = usize::try_from(body).ok().filter(|b| *b <= rest.len()) else {
            return Err(format!(
                "a message body of {body} bytes where {} remain",
                rest.len()
            ));
        };
        rest = &rest[body..];

        let header = message.header_type();
        if header == MessageHeader::Schema {
            let Some(fb) = message.header_as_schema() else {
                return Ok(());
            };
            schema_fb(fb)?;
            // A second schema is converted before it is refused, so it is checked all the same.
            if schema.is_some() {
                return Ok(());
            }
            schema = Some(arrow_ipc::convert::fb_to_schema(fb));
            continue;
        }
        let Some(schema) = &schema else {
            return Ok(());
        };
        let walked = if header == MessageHeader::RecordBatch {
            let Some(batch) = message.header_as_record_batch() else {
                return Ok(());
            };
            walk(
                batch,
                schema.fields().iter().map(|f| f.data_type()),
                body,
                message.version(),
            )?
        } else if header == MessageHeader::DictionaryBatch {
            let Some(dict) = message.header_as_dictionary_batch() else {
                return Ok(());
            };
            #[allow(deprecated)]
            let using = schema.fields_with_dict_id(dict.id());
            let Some(DataType::Dictionary(_, value)) = using.first().map(|f| f.data_type()) else {
                return Ok(());
            };
            let Some(batch) = dict.data() else {
                return Err(format!("dictionary batch {} has no data", dict.id()));
            };
            walk(
                batch,
                std::iter::once(value.as_ref()),
                body,
                message.version(),
            )?
        } else {
            false
        };
        if !walked {
            return Ok(());
        }
    }
}

fn take4(rest: &mut &[u8]) -> Option<[u8; 4]> {
    let (head, tail) = rest.split_first_chunk::<4>()?;
    *rest = tail;
    Some(*head)
}

fn schema_fb(fb: arrow_ipc::Schema) -> Result<(), String> {
    let Some(fields) = fb.fields() else {
        return Err("a schema with no field list".into());
    };
    for f in fields {
        if f.type_type() == Type::Decimal && fb.endianness() == Endianness::Big {
            return Err("a big-endian decimal".into());
        }
        field_fb(f)?;
    }
    Ok(())
}

fn int_ok(width: i32) -> bool {
    matches!(width, 8 | 16 | 32 | 64)
}

/// What `convert::get_data_type` would panic on in `f` or under it.
fn field_fb(f: arrow_ipc::Field) -> Result<(), String> {
    let name = f.name().unwrap_or_default();
    let refuse = |why: &str| Err(format!("field `{name}`: {why}"));
    if let Some(d) = f.dictionary() {
        match d.indexType() {
            Some(i) if int_ok(i.bitWidth()) => {}
            Some(i) => return refuse(&format!("a dictionary index {} bits wide", i.bitWidth())),
            None => return refuse("a dictionary with no index type"),
        }
    }
    let ty = f.type_type();
    let children = f.children();
    let count = children.map_or(0, |c| c.len());
    let absent = || refuse(&format!("{ty:?} with no type table"));
    match ty {
        Type::Null
        | Type::Bool
        | Type::Binary
        | Type::BinaryView
        | Type::LargeBinary
        | Type::Utf8
        | Type::Utf8View
        | Type::LargeUtf8
        | Type::Struct_ => {}
        Type::Int => match f.type_as_int() {
            Some(i) if int_ok(i.bitWidth()) => {}
            Some(i) => return refuse(&format!("an Int {} bits wide", i.bitWidth())),
            None => return absent(),
        },
        // A negative width converts, then panics in arrow-data's layout of it.
        Type::FixedSizeBinary => match f.type_as_fixed_size_binary() {
            Some(b) if b.byteWidth() >= 0 => {}
            Some(b) => return refuse(&format!("a FixedSizeBinary {} bytes wide", b.byteWidth())),
            None => return absent(),
        },
        Type::FloatingPoint => match f.type_as_floating_point().map(|p| p.precision()) {
            Some(Precision::HALF | Precision::SINGLE | Precision::DOUBLE) => {}
            Some(p) => return refuse(&format!("a float of precision {p:?}")),
            None => return absent(),
        },
        Type::Date => match f.type_as_date().map(|d| d.unit()) {
            Some(DateUnit::DAY | DateUnit::MILLISECOND) => {}
            Some(u) => return refuse(&format!("a date in {u:?}")),
            None => return absent(),
        },
        Type::Time => match f.type_as_time().map(|t| (t.bitWidth(), t.unit())) {
            Some((32, TimeUnit::SECOND | TimeUnit::MILLISECOND))
            | Some((64, TimeUnit::MICROSECOND | TimeUnit::NANOSECOND)) => {}
            Some((w, u)) => return refuse(&format!("a time {w} bits wide in {u:?}")),
            None => return absent(),
        },
        Type::Timestamp | Type::Duration => {
            let unit = match ty {
                Type::Timestamp => f.type_as_timestamp().map(|t| t.unit()),
                _ => f.type_as_duration().map(|d| d.unit()),
            };
            match unit {
                Some(
                    TimeUnit::SECOND
                    | TimeUnit::MILLISECOND
                    | TimeUnit::MICROSECOND
                    | TimeUnit::NANOSECOND,
                ) => {}
                Some(u) => return refuse(&format!("a {ty:?} in {u:?}")),
                None => return absent(),
            }
        }
        Type::Interval => match f.type_as_interval().map(|i| i.unit()) {
            Some(
                IntervalUnit::YEAR_MONTH | IntervalUnit::DAY_TIME | IntervalUnit::MONTH_DAY_NANO,
            ) => {}
            Some(u) => return refuse(&format!("an interval in {u:?}")),
            None => return absent(),
        },
        Type::List | Type::LargeList | Type::ListView | Type::LargeListView => {
            if children.is_none() || count != 1 {
                return refuse(&format!("a {ty:?} with {count} children"));
            }
        }
        Type::FixedSizeList | Type::Map => {
            let table = match ty {
                Type::FixedSizeList => f.type_as_fixed_size_list().is_some(),
                _ => f.type_as_map().is_some(),
            };
            if !table {
                return absent();
            }
            if children.is_none() || count != 1 {
                return refuse(&format!("a {ty:?} with {count} children"));
            }
        }
        Type::RunEndEncoded => {
            if children.is_none() || count != 2 {
                return refuse(&format!("a RunEndEncoded with {count} children"));
            }
        }
        Type::Decimal => {
            let Some(d) = f.type_as_decimal() else {
                return absent();
            };
            if u8::try_from(d.precision()).is_err() || i8::try_from(d.scale()).is_err() {
                return refuse(&format!(
                    "a decimal of precision {} and scale {}",
                    d.precision(),
                    d.scale()
                ));
            }
            if !matches!(d.bitWidth(), 32 | 64 | 128 | 256) {
                return refuse(&format!("a decimal {} bits wide", d.bitWidth()));
            }
        }
        Type::Union => {
            let Some(u) = f.type_as_union() else {
                return absent();
            };
            if !matches!(u.mode(), UnionMode::Sparse | UnionMode::Dense) {
                return refuse(&format!("a union in mode {:?}", u.mode()));
            }
            // An empty union converts, then panics where the reader makes an empty array of it.
            if count == 0 {
                return refuse("a union with no children");
            }
            match u.typeIds() {
                None if count > 128 => {
                    return refuse(&format!("a union of {count} children"));
                }
                None => {}
                Some(ids) => {
                    let mut seen = 0u128;
                    for id in ids.iter().map(|i| i as i8) {
                        if id < 0 || seen & (1 << id) != 0 {
                            return refuse(&format!("a union type id {id}"));
                        }
                        seen |= 1 << id;
                    }
                    if ids.len() != count {
                        return refuse(&format!(
                            "a union of {count} children and {} type ids",
                            ids.len()
                        ));
                    }
                }
            }
        }
        t => return refuse(&format!("type {t:?}")),
    }
    for child in children.into_iter().flatten() {
        field_fb(child)?;
    }
    Ok(())
}

/// Walks `batch` for `types` as `RecordBatchDecoder::create_array` reads it. `Ok(false)` where the
/// reader errs first, which ends the stream.
fn walk<'a>(
    batch: arrow_ipc::RecordBatch,
    types: impl Iterator<Item = &'a DataType>,
    body: usize,
    version: MetadataVersion,
) -> Result<bool, String> {
    if batch.compression().is_some() {
        return Err("a compressed batch, which nothing here writes".into());
    }
    let (Some(nodes), Some(buffers)) = (batch.nodes(), batch.buffers()) else {
        return Ok(false);
    };
    let mut w = Walk {
        nodes: nodes
            .iter()
            .map(|n| (n.length() as usize, n.null_count() > 0))
            .collect(),
        buffers: buffers
            .iter()
            .map(|b| (b.offset() as usize, b.length() as usize))
            .collect(),
        counts: batch.variadicBufferCounts().into_iter().flatten().collect(),
        body,
        v5: version >= MetadataVersion::V5,
    };
    for ty in types {
        match w.array(ty) {
            Ok(()) => {}
            Err(Stop::Reader) => return Ok(false),
            Err(Stop::Refuse(why)) => return Err(why),
        }
    }
    if !w.counts.is_empty() {
        return Err(format!(
            "{} variadic buffer counts no column uses",
            w.counts.len()
        ));
    }
    Ok(true)
}

enum Stop {
    Reader,
    Refuse(String),
}

struct Walk {
    /// Each node's length, and whether the reader takes its validity bitmap.
    nodes: VecDeque<(usize, bool)>,
    buffers: VecDeque<(usize, usize)>,
    counts: VecDeque<i64>,
    body: usize,
    v5: bool,
}

impl Walk {
    fn node(&mut self) -> Result<(usize, bool), Stop> {
        self.nodes.pop_front().ok_or(Stop::Reader)
    }

    /// A buffer's offset and length, both inside the body.
    fn buffer(&mut self) -> Result<(usize, usize), Stop> {
        let (offset, len) = self.buffers.pop_front().ok_or(Stop::Reader)?;
        if offset.saturating_add(len) > self.body {
            return Err(Stop::Refuse(format!(
                "a buffer of {len} bytes at {offset} in a body of {}",
                self.body
            )));
        }
        Ok((offset, len))
    }

    /// The `n` buffers after `ty`'s validity bitmap. arrow-data reads a fixed-width one as a slice of
    /// its elements and asserts there is no remainder.
    fn data(&mut self, ty: &DataType, n: usize) -> Result<(), Stop> {
        let layout = arrow_data::layout(ty);
        for i in 0..n {
            let (_, len) = self.buffer()?;
            if let Some(BufferSpec::FixedWidth { byte_width, .. }) = layout.buffers.get(i) {
                if *byte_width > 0 && len % byte_width != 0 {
                    return Err(Stop::Refuse(format!(
                        "a {ty} buffer of {len} bytes, which is not whole {byte_width}-byte values"
                    )));
                }
            }
        }
        Ok(())
    }

    /// `BooleanBuffer::new` asserts a validity bitmap covers the column it is taken for.
    fn bitmap(node: (usize, bool), bytes: usize) -> Result<(), Stop> {
        match node {
            (len, true) if len > bytes.saturating_mul(8) => Err(Stop::Refuse(format!(
                "{len} rows with a {bytes}-byte validity bitmap"
            ))),
            _ => Ok(()),
        }
    }

    fn array(&mut self, ty: &DataType) -> Result<(), Stop> {
        let refuse = |why: String| Err(Stop::Refuse(why));
        match ty {
            DataType::Utf8 | DataType::Binary | DataType::LargeBinary | DataType::LargeUtf8 => {
                let node = self.node()?;
                let (_, valid) = self.buffer()?;
                self.data(ty, 2)?;
                Self::bitmap(node, valid)
            }
            DataType::BinaryView | DataType::Utf8View => {
                let count = self.counts.pop_front().ok_or(Stop::Reader)?;
                let Some(n) = count.checked_add(2).filter(|_| count >= 0) else {
                    return refuse(format!("a variadic buffer count of {count}"));
                };
                let (_, valid) = self.buffer()?;
                self.data(ty, 1)?;
                for _ in 2..n {
                    self.buffer()?;
                }
                let node = self.node()?;
                Self::bitmap(node, valid)
            }
            DataType::List(child) | DataType::LargeList(child) | DataType::Map(child, _) => {
                let node = self.node()?;
                let (_, valid) = self.buffer()?;
                self.data(ty, 1)?;
                self.array(child.data_type())?;
                Self::bitmap(node, valid)
            }
            DataType::ListView(child) | DataType::LargeListView(child) => {
                let node = self.node()?;
                let (_, valid) = self.buffer()?;
                self.data(ty, 2)?;
                self.array(child.data_type())?;
                Self::bitmap(node, valid)
            }
            DataType::FixedSizeList(child, size) => {
                let node = self.node()?;
                let (_, valid) = self.buffer()?;
                self.array(child.data_type())?;
                Self::bitmap(node, valid)?;
                match usize::try_from(*size) {
                    Ok(size) if node.0.checked_mul(size).is_none() => {
                        refuse(format!("{} lists of {size}", node.0))
                    }
                    _ => Ok(()),
                }
            }
            DataType::Struct(fields) => {
                let node = self.node()?;
                let (_, valid) = self.buffer()?;
                for f in fields {
                    self.array(f.data_type())?;
                }
                Self::bitmap(node, valid)
            }
            DataType::RunEndEncoded(runs, values) => {
                self.node()?;
                self.array(runs.data_type())?;
                self.array(values.data_type())
            }
            DataType::Union(fields, mode) => {
                let (len, _) = self.node()?;
                if !self.v5 {
                    self.buffer()?;
                }
                let (_, ids) = self.buffer()?;
                if len > ids {
                    return refuse(format!("{len} union rows with {ids} type ids"));
                }
                if *mode == arrow_schema::UnionMode::Dense {
                    let (at, offsets) = self.buffer()?;
                    if at % 4 != 0 || len.checked_mul(4).is_none_or(|n| n > offsets) {
                        return refuse(format!(
                            "{len} dense union rows with {offsets} bytes of offsets at {at}"
                        ));
                    }
                }
                for (_, f) in fields.iter() {
                    self.array(f.data_type())?;
                }
                Ok(())
            }
            DataType::Null => self.node().map(drop),
            // Dictionary keys and every fixed-width type: a validity bitmap and the values.
            _ => {
                let node = self.node()?;
                let (_, valid) = self.buffer()?;
                self.data(ty, 1)?;
                Self::bitmap(node, valid)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{new_null_array, ArrayRef, Int64Array, RecordBatch, StringViewArray};
    use arrow_ipc as fb;
    use arrow_ipc::writer::StreamWriter;
    use arrow_schema::{Field, Fields, IntervalUnit as Iu, TimeUnit as Tu, UnionFields};
    use flatbuffers::{FlatBufferBuilder, UnionWIPOffset, WIPOffset};
    use std::sync::Arc;

    type Fbb = FlatBufferBuilder<'static>;
    type FbField = WIPOffset<fb::Field<'static>>;
    type Table = Option<WIPOffset<UnionWIPOffset>>;

    fn field(fbb: &mut Fbb, ty: fb::Type, table: Table, children: Option<&[FbField]>) -> FbField {
        let name = fbb.create_string("c");
        let children = children.map(|c| fbb.create_vector(c));
        fb::Field::create(
            fbb,
            &fb::FieldArgs {
                name: Some(name),
                nullable: true,
                type_type: ty,
                type_: table,
                children,
                ..Default::default()
            },
        )
    }

    fn int(fbb: &mut Fbb, width: i32) -> Table {
        Some(
            fb::Int::create(
                fbb,
                &fb::IntArgs {
                    bitWidth: width,
                    is_signed: true,
                },
            )
            .as_union_value(),
        )
    }

    fn int_field(fbb: &mut Fbb) -> FbField {
        let t = int(fbb, 64);
        field(fbb, fb::Type::Int, t, None)
    }

    fn message(
        mut fbb: Fbb,
        header_type: MessageHeader,
        header: WIPOffset<UnionWIPOffset>,
        body: i64,
    ) -> Vec<u8> {
        let m = fb::Message::create(
            &mut fbb,
            &fb::MessageArgs {
                version: MetadataVersion::V5,
                header_type,
                header: Some(header),
                bodyLength: body,
                custom_metadata: None,
            },
        );
        fbb.finish(m, None);
        let meta = fbb.finished_data();
        let pad = (8 - meta.len() % 8) % 8;
        let mut out = CONTINUATION.to_vec();
        out.extend(((meta.len() + pad) as i32).to_le_bytes());
        out.extend(meta);
        out.extend(vec![0; pad]);
        out
    }

    fn schema_with(
        fields: impl FnOnce(&mut Fbb) -> Option<Vec<FbField>>,
        endianness: Endianness,
    ) -> Vec<u8> {
        let mut fbb = Fbb::new();
        let fields = fields(&mut fbb).map(|f| fbb.create_vector(&f));
        let s = fb::Schema::create(
            &mut fbb,
            &fb::SchemaArgs {
                endianness,
                fields,
                ..Default::default()
            },
        );
        message(fbb, MessageHeader::Schema, s.as_union_value(), 0)
    }

    /// A stream whose schema is the one field `build` makes.
    fn one(build: impl FnOnce(&mut Fbb) -> FbField) -> Vec<u8> {
        schema_with(|fbb| Some(vec![build(fbb)]), Endianness::Little)
    }

    /// What arrow-ipc writes for `schema`, without an end of stream.
    fn written(schema: &arrow_schema::Schema) -> Vec<u8> {
        StreamWriter::try_new(Vec::new(), schema)
            .unwrap()
            .get_ref()
            .clone()
    }

    #[derive(Default)]
    struct Batch<'s> {
        len: i64,
        nodes: &'s [(i64, i64)],
        buffers: &'s [(i64, i64)],
        counts: Option<&'s [i64]>,
        body: usize,
        compressed: bool,
    }

    fn batch(b: Batch) -> Vec<u8> {
        let mut fbb = Fbb::new();
        let nodes: Vec<_> = b
            .nodes
            .iter()
            .map(|&(l, n)| fb::FieldNode::new(l, n))
            .collect();
        let buffers: Vec<_> = b
            .buffers
            .iter()
            .map(|&(o, l)| fb::Buffer::new(o, l))
            .collect();
        let nodes = fbb.create_vector(&nodes);
        let buffers = fbb.create_vector(&buffers);
        let counts = b.counts.map(|c| fbb.create_vector(c));
        let compression = b.compressed.then(|| {
            fb::BodyCompression::create(
                &mut fbb,
                &fb::BodyCompressionArgs {
                    codec: fb::CompressionType::LZ4_FRAME,
                    ..Default::default()
                },
            )
        });
        let rb = fb::RecordBatch::create(
            &mut fbb,
            &fb::RecordBatchArgs {
                length: b.len,
                nodes: Some(nodes),
                buffers: Some(buffers),
                compression,
                variadicBufferCounts: counts,
            },
        );
        let mut out = message(
            fbb,
            MessageHeader::RecordBatch,
            rb.as_union_value(),
            b.body as i64,
        );
        out.extend(vec![0; b.body]);
        out
    }

    /// The reader panics on `stream`, and `check` refuses it first.
    fn refused(stream: &[u8]) -> String {
        let read = std::panic::catch_unwind(|| {
            let r = arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(stream), None)?;
            r.collect::<Result<Vec<_>, _>>()
        });
        assert!(read.is_err(), "the reader did not panic: {read:?}");
        check(stream).expect_err("check passed a stream the reader panics on")
    }

    #[test]
    fn a_field_of_type_none_is_refused() {
        // The attach_ipc fuzz target's first finding.
        let why = refused(&one(|fbb| field(fbb, fb::Type::NONE, None, None)));
        assert!(why.contains("NONE"), "{why}");
        refused(&one(|fbb| {
            let t = int(fbb, 64);
            field(fbb, fb::Type(99), t, None)
        }));
    }

    #[test]
    fn a_schema_with_no_field_list_is_refused() {
        refused(&schema_with(|_| None, Endianness::Little));
    }

    #[test]
    fn a_big_endian_decimal_is_refused() {
        refused(&schema_with(
            |fbb| {
                let t = fb::Decimal::create(
                    fbb,
                    &fb::DecimalArgs {
                        precision: 10,
                        scale: 2,
                        bitWidth: 128,
                    },
                );
                Some(vec![field(
                    fbb,
                    fb::Type::Decimal,
                    Some(t.as_union_value()),
                    None,
                )])
            },
            Endianness::Big,
        ));
    }

    #[test]
    fn a_dictionary_with_no_index_type_or_a_bad_one_is_refused() {
        for index in [None, Some(12)] {
            refused(&one(|fbb| {
                let index_type = index.map(|w| {
                    fb::Int::create(
                        fbb,
                        &fb::IntArgs {
                            bitWidth: w,
                            is_signed: true,
                        },
                    )
                });
                let d = fb::DictionaryEncoding::create(
                    fbb,
                    &fb::DictionaryEncodingArgs {
                        id: 0,
                        indexType: index_type,
                        ..Default::default()
                    },
                );
                let name = fbb.create_string("c");
                let utf8 = fb::Utf8::create(fbb, &fb::Utf8Args {}).as_union_value();
                fb::Field::create(
                    fbb,
                    &fb::FieldArgs {
                        name: Some(name),
                        type_type: fb::Type::Utf8,
                        type_: Some(utf8),
                        dictionary: Some(d),
                        ..Default::default()
                    },
                )
            }));
        }
    }

    #[test]
    fn an_int_of_a_bad_width_is_refused() {
        refused(&one(|fbb| {
            let t = int(fbb, 7);
            field(fbb, fb::Type::Int, t, None)
        }));
    }

    #[test]
    fn a_unit_or_precision_out_of_range_is_refused() {
        refused(&one(|fbb| {
            let t = fb::FloatingPoint::create(
                fbb,
                &fb::FloatingPointArgs {
                    precision: fb::Precision(3),
                },
            );
            field(fbb, fb::Type::FloatingPoint, Some(t.as_union_value()), None)
        }));
        refused(&one(|fbb| {
            let t = fb::Date::create(
                fbb,
                &fb::DateArgs {
                    unit: fb::DateUnit(2),
                },
            );
            field(fbb, fb::Type::Date, Some(t.as_union_value()), None)
        }));
        for (width, unit) in [
            (32, fb::TimeUnit::NANOSECOND),
            (64, fb::TimeUnit::SECOND),
            (16, fb::TimeUnit::SECOND),
        ] {
            refused(&one(|fbb| {
                let t = fb::Time::create(
                    fbb,
                    &fb::TimeArgs {
                        unit,
                        bitWidth: width,
                    },
                );
                field(fbb, fb::Type::Time, Some(t.as_union_value()), None)
            }));
        }
        refused(&one(|fbb| {
            let t = fb::Timestamp::create(
                fbb,
                &fb::TimestampArgs {
                    unit: fb::TimeUnit(4),
                    timezone: None,
                },
            );
            field(fbb, fb::Type::Timestamp, Some(t.as_union_value()), None)
        }));
        refused(&one(|fbb| {
            let t = fb::Interval::create(
                fbb,
                &fb::IntervalArgs {
                    unit: fb::IntervalUnit(3),
                },
            );
            field(fbb, fb::Type::Interval, Some(t.as_union_value()), None)
        }));
        refused(&one(|fbb| {
            let t = fb::Duration::create(
                fbb,
                &fb::DurationArgs {
                    unit: fb::TimeUnit(-1),
                },
            );
            field(fbb, fb::Type::Duration, Some(t.as_union_value()), None)
        }));
    }

    /// The type table `ty` must carry for the flatbuffer verifier to pass it.
    fn table(fbb: &mut Fbb, ty: fb::Type) -> Table {
        use fb::Type as T;
        Some(match ty {
            T::List => fb::List::create(fbb, &fb::ListArgs {}).as_union_value(),
            T::LargeList => fb::LargeList::create(fbb, &fb::LargeListArgs {}).as_union_value(),
            T::ListView => fb::ListView::create(fbb, &fb::ListViewArgs {}).as_union_value(),
            T::LargeListView => {
                fb::LargeListView::create(fbb, &fb::LargeListViewArgs {}).as_union_value()
            }
            T::FixedSizeList => {
                fb::FixedSizeList::create(fbb, &fb::FixedSizeListArgs { listSize: 2 })
                    .as_union_value()
            }
            T::Map => fb::Map::create(fbb, &fb::MapArgs { keysSorted: false }).as_union_value(),
            T::RunEndEncoded => {
                fb::RunEndEncoded::create(fbb, &fb::RunEndEncodedArgs {}).as_union_value()
            }
            t => unreachable!("{t:?}"),
        })
    }

    fn with_children(ty: fb::Type, n: Option<usize>) -> Vec<u8> {
        one(|fbb| {
            let t = table(fbb, ty);
            let children: Option<Vec<_>> = n.map(|n| (0..n).map(|_| int_field(fbb)).collect());
            field(fbb, ty, t, children.as_deref())
        })
    }

    #[test]
    fn a_list_without_exactly_one_child_is_refused() {
        use fb::Type as T;
        for ty in [
            T::List,
            T::LargeList,
            T::ListView,
            T::LargeListView,
            T::FixedSizeList,
            T::Map,
        ] {
            for n in [None, Some(0), Some(2)] {
                refused(&with_children(ty, n));
            }
        }
    }

    #[test]
    fn a_run_end_encoding_without_two_children_is_refused() {
        for n in [None, Some(1), Some(3)] {
            refused(&with_children(fb::Type::RunEndEncoded, n));
        }
    }

    #[test]
    fn a_decimal_out_of_range_is_refused() {
        for (precision, scale, bit_width) in [(300, 2, 128), (10, 200, 128), (10, 2, 96)] {
            refused(&one(|fbb| {
                let t = fb::Decimal::create(
                    fbb,
                    &fb::DecimalArgs {
                        precision,
                        scale,
                        bitWidth: bit_width,
                    },
                );
                field(fbb, fb::Type::Decimal, Some(t.as_union_value()), None)
            }));
        }
    }

    fn union(fbb: &mut Fbb, mode: fb::UnionMode, ids: Option<&[i32]>, children: usize) -> FbField {
        let ids = ids.map(|i| fbb.create_vector(i));
        let t = fb::Union::create(fbb, &fb::UnionArgs { mode, typeIds: ids });
        let children: Vec<_> = (0..children).map(|_| int_field(fbb)).collect();
        field(
            fbb,
            fb::Type::Union,
            Some(t.as_union_value()),
            Some(&children),
        )
    }

    #[test]
    fn a_union_arrow_cannot_build_is_refused() {
        refused(&one(|fbb| union(fbb, fb::UnionMode(2), None, 1)));
        for ids in [&[-1, 1][..], &[1, 1], &[0], &[0, 1, 2], &[200, 1]] {
            refused(&one(|fbb| union(fbb, fb::UnionMode::Sparse, Some(ids), 2)));
        }
        refused(&one(|fbb| union(fbb, fb::UnionMode::Sparse, None, 129)));
    }

    #[test]
    fn a_union_with_no_children_is_refused() {
        // It converts, then panics as the empty dictionary of a column whose dictionary never came.
        let mut stream = one(|fbb| {
            let t = fb::Union::create(
                fbb,
                &fb::UnionArgs {
                    mode: fb::UnionMode::Sparse,
                    typeIds: None,
                },
            );
            let index_type = fb::Int::create(
                fbb,
                &fb::IntArgs {
                    bitWidth: 32,
                    is_signed: true,
                },
            );
            let d = fb::DictionaryEncoding::create(
                fbb,
                &fb::DictionaryEncodingArgs {
                    id: 0,
                    indexType: Some(index_type),
                    ..Default::default()
                },
            );
            fb::Field::create(
                fbb,
                &fb::FieldArgs {
                    type_type: fb::Type::Union,
                    type_: Some(t.as_union_value()),
                    dictionary: Some(d),
                    ..Default::default()
                },
            )
        });
        stream.extend(batch(Batch {
            nodes: &[(0, 0)],
            buffers: &[(0, 0), (0, 0)],
            ..Default::default()
        }));
        refused(&stream);
    }

    #[test]
    fn a_negative_fixed_size_binary_is_refused() {
        let mut stream = one(|fbb| {
            let t = fb::FixedSizeBinary::create(fbb, &fb::FixedSizeBinaryArgs { byteWidth: -1 });
            field(
                fbb,
                fb::Type::FixedSizeBinary,
                Some(t.as_union_value()),
                None,
            )
        });
        stream.extend(batch(Batch {
            nodes: &[(0, 0)],
            buffers: &[(0, 0), (0, 0)],
            ..Default::default()
        }));
        refused(&stream);
    }

    fn int64() -> Vec<u8> {
        written(&arrow_schema::Schema::new(vec![Field::new(
            "i",
            DataType::Int64,
            true,
        )]))
    }

    #[test]
    fn a_buffer_past_the_body_is_refused() {
        let mut stream = int64();
        stream.extend(batch(Batch {
            len: 1,
            nodes: &[(1, 0)],
            buffers: &[(0, 0), (0, 64)],
            body: 8,
            ..Default::default()
        }));
        refused(&stream);
        let mut stream = int64();
        stream.extend(batch(Batch {
            len: 1,
            nodes: &[(1, 0)],
            buffers: &[(0, 0), (-8, 8)],
            body: 8,
            ..Default::default()
        }));
        refused(&stream);
    }

    #[test]
    fn variadic_counts_no_column_uses_are_refused() {
        let mut stream = int64();
        stream.extend(batch(Batch {
            len: 1,
            nodes: &[(1, 0)],
            buffers: &[(0, 0), (0, 8)],
            counts: Some(&[1]),
            body: 8,
            ..Default::default()
        }));
        refused(&stream);
    }

    #[test]
    fn a_negative_variadic_count_is_refused() {
        let mut stream = written(&arrow_schema::Schema::new(vec![Field::new(
            "v",
            DataType::Utf8View,
            true,
        )]));
        stream.extend(batch(Batch {
            nodes: &[(0, 0)],
            counts: Some(&[-2]),
            ..Default::default()
        }));
        refused(&stream);
    }

    fn union_of(mode: arrow_schema::UnionMode) -> Vec<u8> {
        let fields = UnionFields::try_new([0], [Field::new("a", DataType::Int8, true)]).unwrap();
        written(&arrow_schema::Schema::new(vec![Field::new(
            "u",
            DataType::Union(fields, mode),
            false,
        )]))
    }

    #[test]
    fn union_type_ids_shorter_than_the_column_are_refused() {
        let mut stream = union_of(arrow_schema::UnionMode::Sparse);
        stream.extend(batch(Batch {
            len: 4,
            nodes: &[(4, 0), (4, 0)],
            buffers: &[(0, 0), (0, 0), (0, 4)],
            body: 8,
            ..Default::default()
        }));
        refused(&stream);
    }

    #[test]
    fn misaligned_dense_union_offsets_are_refused() {
        let mut stream = union_of(arrow_schema::UnionMode::Dense);
        stream.extend(batch(Batch {
            len: 1,
            nodes: &[(1, 0), (1, 0)],
            buffers: &[(0, 1), (1, 4), (0, 0), (8, 1)],
            body: 16,
            ..Default::default()
        }));
        refused(&stream);
    }

    #[test]
    fn a_fixed_width_buffer_of_part_values_is_refused() {
        let mut stream = written(&arrow_schema::Schema::new(vec![Field::new(
            "s",
            DataType::Utf8,
            true,
        )]));
        stream.extend(batch(Batch {
            len: 1,
            nodes: &[(1, 0)],
            buffers: &[(0, 0), (0, 9), (16, 1)],
            body: 24,
            ..Default::default()
        }));
        refused(&stream);
    }

    #[test]
    fn a_validity_bitmap_shorter_than_its_column_is_refused() {
        let mut stream = int64();
        stream.extend(batch(Batch {
            len: 254,
            nodes: &[(254, 1)],
            buffers: &[(0, 1), (0, 2032)],
            body: 2032,
            ..Default::default()
        }));
        refused(&stream);

        let fields = Fields::from(vec![Field::new("a", DataType::Int64, true)]);
        let mut stream = written(&arrow_schema::Schema::new(vec![Field::new(
            "s",
            DataType::Struct(fields),
            true,
        )]));
        stream.extend(batch(Batch {
            len: 16,
            nodes: &[(16, 1), (16, 0)],
            buffers: &[(0, 0), (0, 0), (0, 128)],
            body: 128,
            ..Default::default()
        }));
        refused(&stream);
    }

    #[test]
    fn a_fixed_size_list_whose_length_overflows_is_refused() {
        let item = Arc::new(Field::new("item", DataType::Int8, true));
        let mut stream = written(&arrow_schema::Schema::new(vec![Field::new(
            "l",
            DataType::FixedSizeList(item, 4),
            true,
        )]));
        stream.extend(batch(Batch {
            nodes: &[(i64::MAX, 0), (0, 0)],
            buffers: &[(0, 0), (0, 0), (0, 0)],
            ..Default::default()
        }));
        refused(&stream);
    }

    #[test]
    fn a_dictionary_batch_with_no_data_is_refused() {
        let dict = DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8));
        let mut stream = written(&arrow_schema::Schema::new(vec![Field::new(
            "d", dict, true,
        )]));
        let mut fbb = Fbb::new();
        let d = fb::DictionaryBatch::create(
            &mut fbb,
            &fb::DictionaryBatchArgs {
                id: 0,
                data: None,
                isDelta: false,
            },
        );
        stream.extend(message(
            fbb,
            MessageHeader::DictionaryBatch,
            d.as_union_value(),
            0,
        ));
        refused(&stream);
    }

    // The reader allocates these before reading them, which aborts rather than panics.
    #[test]
    fn lengths_past_the_input_are_refused() {
        let mut stream = int64();
        stream.extend(CONTINUATION);
        stream.extend(i32::MAX.to_le_bytes());
        assert!(check(&stream).unwrap_err().contains("message of"));
        for body in [-1, i64::MAX, 64] {
            let mut stream = int64();
            let mut fbb = Fbb::new();
            let rb = fb::RecordBatch::create(&mut fbb, &fb::RecordBatchArgs::default());
            stream.extend(message(
                fbb,
                MessageHeader::RecordBatch,
                rb.as_union_value(),
                body,
            ));
            stream.extend([0; 8]);
            assert!(check(&stream).unwrap_err().contains("body of"));
        }
    }

    #[test]
    fn a_compressed_batch_is_refused() {
        let mut stream = int64();
        stream.extend(batch(Batch {
            len: 1,
            nodes: &[(1, 0)],
            buffers: &[(0, 0), (0, 16)],
            body: 16,
            compressed: true,
            ..Default::default()
        }));
        assert!(check(&stream).unwrap_err().contains("compressed"));
    }

    #[test]
    fn what_the_writer_writes_passes() {
        let dense = UnionFields::try_new(
            [0, 3],
            [
                Field::new("a", DataType::Int32, true),
                Field::new("b", DataType::Utf8, true),
            ],
        )
        .unwrap();
        let item = || Arc::new(Field::new("item", DataType::Int32, true));
        let entries = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("k", DataType::Utf8, false),
                Field::new("v", DataType::Int64, true),
            ])),
            false,
        ));
        let types = [
            DataType::Null,
            DataType::Boolean,
            DataType::Int8,
            DataType::UInt16,
            DataType::Int32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::BinaryView,
            DataType::FixedSizeBinary(20),
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(Tu::Second),
            DataType::Time32(Tu::Millisecond),
            DataType::Time64(Tu::Microsecond),
            DataType::Time64(Tu::Nanosecond),
            DataType::Timestamp(Tu::Millisecond, Some("UTC".into())),
            DataType::Duration(Tu::Second),
            DataType::Interval(Iu::YearMonth),
            DataType::Interval(Iu::DayTime),
            DataType::Interval(Iu::MonthDayNano),
            DataType::Decimal32(9, 2),
            DataType::Decimal64(18, 4),
            DataType::Decimal128(38, 10),
            DataType::Decimal256(76, -5),
            DataType::List(item()),
            DataType::LargeList(item()),
            DataType::FixedSizeList(item(), 3),
            DataType::Struct(Fields::from(vec![Field::new(
                "x",
                DataType::Utf8View,
                true,
            )])),
            DataType::Map(entries, false),
            DataType::Union(dense, arrow_schema::UnionMode::Dense),
            DataType::Dictionary(Box::new(DataType::UInt8), Box::new(DataType::Utf8)),
        ];
        let mut fields: Vec<Field> = types
            .iter()
            .enumerate()
            .map(|(i, t)| Field::new(format!("c{i}"), t.clone(), true))
            .collect();
        let mut columns: Vec<ArrayRef> = types.iter().map(|t| new_null_array(t, 3)).collect();
        fields.push(Field::new("i", DataType::Int64, true));
        columns.push(Arc::new(Int64Array::from(vec![Some(1), None, Some(3)])));
        fields.push(Field::new("v", DataType::Utf8View, true));
        columns.push(Arc::new(StringViewArray::from(vec![
            Some("a string longer than twelve bytes"),
            None,
            Some("short"),
        ])));
        let schema = Arc::new(arrow_schema::Schema::new(fields));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut w = StreamWriter::try_new(Vec::new(), &schema).unwrap();
        w.write(&batch).unwrap();
        w.write(&batch).unwrap();
        let stream = w.into_inner().unwrap();

        check(&stream).unwrap();
        let read =
            arrow_ipc::reader::StreamReader::try_new(std::io::Cursor::new(&stream), None).unwrap();
        assert_eq!(
            read.collect::<Result<Vec<_>, _>>().unwrap(),
            vec![batch.clone(), batch]
        );
    }
}
