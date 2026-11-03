//! Sizing a slice of an answer before it goes into an Arrow IPC stream, shared by the node's and the
//! engine's `collect_ipc` so both hold the result byte cap the same way (#2589, #2599, #2616).
//!
//! The IPC writer writes a sliced view array's variadic buffers whole, so uncompacted one row carries
//! the batch's strings, at the top level or inside a list or a struct (#2631). A slice is sized from
//! its views without copying, and only the slice that is written is compacted.

use std::sync::Arc;

use arrow_array::builder::OffsetBufferBuilder;
use arrow_array::cast::AsArray;
use arrow_array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait, RecordBatch, StructArray};
use arrow_schema::{ArrowError, DataType, FieldRef};

/// `part` with every view array's data buffers cut down to the rows it holds, inside lists and
/// structs too.
pub fn compact(part: &RecordBatch) -> Result<RecordBatch, ArrowError> {
    if !part.columns().iter().any(|c| holds_view(c.data_type())) {
        return Ok(part.clone());
    }
    let columns: Vec<ArrayRef> = part.columns().iter().map(compact_array).collect();
    RecordBatch::try_new(part.schema(), columns)
}

/// What `part` takes in an IPC stream once compacted, worked out from the uncompacted slice so a
/// slice too big to write whole is never copied. A type whose slice cannot be sized counts its whole
/// buffers, which errs toward fewer rows at once.
pub fn slice_bytes(part: &RecordBatch) -> usize {
    part.columns()
        .iter()
        .map(estimate)
        .fold(0usize, usize::saturating_add)
}

fn compact_array(a: &ArrayRef) -> ArrayRef {
    if !holds_view(a.data_type()) {
        return a.clone();
    }
    match a.data_type() {
        DataType::Utf8View => Arc::new(a.as_string_view().gc()),
        DataType::BinaryView => Arc::new(a.as_binary_view().gc()),
        DataType::List(f) => compact_list(a.as_list::<i32>(), f),
        DataType::LargeList(f) => compact_list(a.as_list::<i64>(), f),
        DataType::Struct(fields) => {
            let s = a.as_struct();
            let columns = s.columns().iter().map(compact_array).collect();
            Arc::new(StructArray::new(
                fields.clone(),
                columns,
                s.nulls().cloned(),
            ))
        }
        // A map or a fixed-size list of views is written as it is, and `estimate` counts it whole.
        _ => a.clone(),
    }
}

/// A sliced list keeps its whole child; the rows it holds are the child's span between its first
/// and last offsets, re-based to zero.
fn compact_list<O: OffsetSizeTrait>(l: &GenericListArray<O>, field: &FieldRef) -> ArrayRef {
    let (values, offsets) = held_values(l);
    let mut rebased = OffsetBufferBuilder::<O>::new(l.len());
    for w in offsets.windows(2) {
        rebased.push_length((w[1] - w[0]).as_usize());
    }
    Arc::new(GenericListArray::new(
        field.clone(),
        rebased.finish(),
        compact_array(&values),
        l.nulls().cloned(),
    ))
}

fn held_values<O: OffsetSizeTrait>(l: &GenericListArray<O>) -> (ArrayRef, &[O]) {
    let offsets: &[O] = l.value_offsets();
    let first = offsets.first().map_or(0, |o| o.as_usize());
    let last = offsets.last().map_or(0, |o| o.as_usize());
    (l.values().slice(first, last - first), offsets)
}

fn estimate(a: &ArrayRef) -> usize {
    let nulls = a.nulls().map_or(0, |n| n.buffer().len());
    let own = match a.data_type() {
        DataType::Utf8View => views(a.as_string_view().views()),
        DataType::BinaryView => views(a.as_binary_view().views()),
        DataType::List(_) => list(a.as_list::<i32>()),
        DataType::LargeList(_) => list(a.as_list::<i64>()),
        DataType::Struct(_) => a
            .as_struct()
            .columns()
            .iter()
            .map(estimate)
            .fold(0usize, usize::saturating_add),
        _ => {
            let data = a.to_data();
            if holds_view(a.data_type()) {
                data.get_buffer_memory_size()
            } else {
                data.get_slice_memory_size()
                    .unwrap_or_else(|_| data.get_buffer_memory_size())
            }
        }
    };
    own.saturating_add(nulls)
}

/// A view is 16 bytes, and a value longer than 12 bytes is held in a data buffer besides.
fn views(v: &[u128]) -> usize {
    v.iter()
        .map(|&view| {
            let len = view as u32 as usize;
            16 + if len > 12 { len } else { 0 }
        })
        .fold(0usize, usize::saturating_add)
}

fn list<O: OffsetSizeTrait>(l: &GenericListArray<O>) -> usize {
    let (values, offsets) = held_values(l);
    (offsets.len() * O::get_byte_width()).saturating_add(estimate(&values))
}

fn holds_view(t: &DataType) -> bool {
    match t {
        DataType::Utf8View | DataType::BinaryView => true,
        DataType::List(f)
        | DataType::LargeList(f)
        | DataType::FixedSizeList(f, _)
        | DataType::ListView(f)
        | DataType::LargeListView(f)
        | DataType::Map(f, _) => holds_view(f.data_type()),
        DataType::Struct(fields) => fields.iter().any(|f| holds_view(f.data_type())),
        DataType::Union(fields, _) => fields.iter().any(|(_, f)| holds_view(f.data_type())),
        DataType::Dictionary(_, v) => holds_view(v),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::builder::{ListBuilder, StringViewBuilder};
    use arrow_array::StringViewArray;
    use arrow_schema::{Field, Schema};

    fn batch_of(col: ArrayRef) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            col.data_type().clone(),
            true,
        )]));
        RecordBatch::try_new(schema, vec![col]).unwrap()
    }

    #[test]
    fn a_compacted_view_slice_is_sized_by_its_own_strings() {
        let big = "A".repeat(100_000);
        let col = StringViewArray::from_iter_values(std::iter::repeat_n(big.as_str(), 32));
        let batch = batch_of(Arc::new(col));
        let one = batch.slice(0, 1);
        let bytes = slice_bytes(&one);
        assert!(
            (100_000..200_000).contains(&bytes),
            "one 100 KB row sized at {bytes}"
        );
        let compacted = compact(&one).unwrap();
        assert!(compacted.column(0).to_data().get_buffer_memory_size() < 200_000);
    }

    #[test]
    fn a_list_of_views_is_sized_and_compacted_by_the_rows_it_holds() {
        let big = "A".repeat(100_000);
        let mut b = ListBuilder::new(StringViewBuilder::new());
        for _ in 0..32 {
            b.values().append_value(&big);
            b.append(true);
        }
        let batch = batch_of(Arc::new(b.finish()));
        let one = batch.slice(5, 1);
        let bytes = slice_bytes(&one);
        assert!(
            (100_000..200_000).contains(&bytes),
            "one 100 KB row sized at {bytes}"
        );
        let compacted = compact(&one).unwrap();
        let held = compacted.column(0).to_data().get_buffer_memory_size();
        let child: usize = compacted.column(0).to_data().child_data()[0]
            .buffers()
            .iter()
            .map(|b| b.capacity())
            .sum();
        assert!(
            held + child < 300_000,
            "a compacted row holds {held} + {child}"
        );
        assert_eq!(compacted.column(0).as_list::<i32>().value(0).len(), 1);
    }
}
