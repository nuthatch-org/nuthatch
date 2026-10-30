//! RFC-0041 × RFC-0045: an offchain snapshot table as an entity input (#1437).
//!
//! A chain source is bound from the decode registry and fed decoded windows. An offchain source is
//! bound from its retained snapshots' own Parquet schemas and fed whole snapshots, append-only: no
//! block height, no reorg, no retraction. Its rows are the rows `offchain__<table>` answers with on
//! `/sql`, which is `read_parquet(<present snapshots>, union_by_name=true)`, so a column one snapshot
//! lacks reads NULL for that snapshot's rows here as well.

use crate::entity_row::{Row, Scalar};
use anyhow::{anyhow, bail, Context, Result};
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::datatypes::{
    DataType, Int16Type, Int32Type, Int64Type, Int8Type, SchemaRef, UInt16Type, UInt32Type,
    UInt64Type, UInt8Type,
};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};

pub use nuthatch_core::entity_offchain::{
    table_of, Column, Kind, OffchainSnapshot, OffchainTables, OFFCHAIN_NAMESPACE,
};

/// Whether an entity's SQL reads an offchain table. Admission charges such an entity for the
/// offchain input it may hold (RFC-0041 §7), so it must agree with what `start_entities` binds.
pub fn reads_offchain(plan: &crate::entity_plan::Plan) -> bool {
    std::iter::once(&plan.left)
        .chain(plan.join.as_ref().map(|j| &j.right))
        .any(|s| table_of(&s.table).is_some())
}

/// What a Parquet column's type becomes in an entity, or why it is refused.
fn kind_of(ty: &DataType) -> std::result::Result<Kind, &'static str> {
    Ok(match ty {
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => Kind::Str,
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::Int64
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => Kind::Int,
        DataType::Boolean => Kind::Bool,
        DataType::Float16 | DataType::Float32 | DataType::Float64 => {
            return Err(
                "floating point is not exact (RFC-0041 §3.3); publish a scaled \
                        integer such as price_e8, or text and CAST it",
            )
        }
        DataType::Decimal32(..)
        | DataType::Decimal64(..)
        | DataType::Decimal128(..)
        | DataType::Decimal256(..) => {
            return Err(
                "entities have no decimal type yet; publish a scaled integer such \
                        as price_e8",
            )
        }
        _ => return Err("an entity reads text, integer and boolean columns only"),
    })
}

/// One retained snapshot whose segment is present.
#[derive(Clone, Debug)]
pub struct Snapshot {
    pub hash: String,
    path: PathBuf,
    schema: SchemaRef,
    rows: u64,
    bytes: u64,
}

/// An offchain table as the entity binder sees it: its present snapshots, in manifest order.
#[derive(Clone, Debug)]
pub struct Table {
    snapshots: Vec<Snapshot>,
}

impl Table {
    /// The content hashes of the snapshots that make up this table, in the order they were
    /// appended. This is the table's version, which an entity reports as what it has applied.
    pub fn version(&self) -> Vec<String> {
        self.snapshots.iter().map(|s| s.hash.clone()).collect()
    }

    pub fn snapshots(&self) -> &[Snapshot] {
        &self.snapshots
    }

    /// Every column any snapshot carries, in first-appearance order.
    fn column_names(&self) -> Vec<String> {
        let mut names: Vec<String> = Vec::new();
        for s in &self.snapshots {
            for f in s.schema.fields() {
                if !names.contains(f.name()) {
                    names.push(f.name().clone());
                }
            }
        }
        names
    }

    /// Resolve the columns an entity reads, or refuse at load.
    pub fn bind(&self, source: &str, columns: &[String]) -> Result<Vec<Column>> {
        columns
            .iter()
            .map(|name| {
                let mut kind: Option<(Kind, &str)> = None;
                for s in &self.snapshots {
                    let Ok(field) = s.schema.field_with_name(name) else {
                        continue;
                    };
                    let k = kind_of(field.data_type()).map_err(|why| {
                        anyhow!(
                            "{source}.{name} is {} in snapshot {}: {why}",
                            field.data_type(),
                            short(&s.hash)
                        )
                    })?;
                    match kind {
                        None => kind = Some((k, &s.hash)),
                        Some((seen, at)) if seen != k => bail!(
                            "{source}.{name} is {} in snapshot {} and {} in snapshot {}. An entity \
                             reads one type per column",
                            seen.name(),
                            short(at),
                            k.name(),
                            short(&s.hash)
                        ),
                        Some(_) => {}
                    }
                }
                let (kind, _) = kind.ok_or_else(|| {
                    anyhow!(
                        "no column {name} in {source}. Its columns are: {}",
                        self.column_names().join(", ")
                    )
                })?;
                Ok(Column {
                    name: name.clone(),
                    kind,
                })
            })
            .collect()
    }
}

impl Snapshot {
    /// From the Parquet footer, so a feed can be bounded before any row is read.
    pub fn row_count(&self) -> u64 {
        self.rows
    }

    /// Uncompressed, from the Parquet footer, for the same reason.
    pub fn byte_size(&self) -> u64 {
        self.bytes
    }

    /// This snapshot's rows as entity rows of `columns`, in order.
    ///
    /// The bytes are checked against the content hash first. An entity reports the hashes it has
    /// applied, and that claim is only true if these are the bytes the hash names.
    pub fn rows(&self, columns: &[Column]) -> Result<Vec<Row>> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let bytes = std::fs::read(&self.path)
            .with_context(|| format!("reading offchain snapshot {}", self.path.display()))?;
        let actual = hex::encode(Sha256::digest(&bytes));
        if actual != self.hash {
            bail!(
                "offchain snapshot {} hashes to {}, not the {} its manifest records",
                self.path.display(),
                short(&actual),
                short(&self.hash)
            );
        }
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .and_then(|b| b.build())
            .with_context(|| format!("reading offchain snapshot {}", self.path.display()))?;
        let mut out = Vec::new();
        for batch in reader {
            out.extend(batch_rows(&batch?, columns)?);
        }
        Ok(out)
    }
}

/// The nest's offchain catalogue, with snapshot schemas read only for the tables asked about.
pub struct Tables {
    dir: PathBuf,
    catalogue: crate::offchain::Catalogue,
}

impl Tables {
    /// No offchain tables, for a caller that binds chain sources only.
    pub fn none() -> Self {
        Self {
            dir: PathBuf::new(),
            catalogue: Default::default(),
        }
    }

    pub fn load(dir: &Path) -> Result<Self> {
        Ok(Self {
            dir: dir.to_path_buf(),
            catalogue: crate::offchain::load(dir)?,
        })
    }

    /// `table` as the `/sql` view defines it, or `None` when that view would not exist: no such
    /// table, or none of its snapshots present.
    pub fn table(&self, table: &str) -> Result<Option<Table>> {
        let mut snapshots = Vec::new();
        for s in self.retained(table) {
            snapshots.extend(self.open(s)?);
        }
        Ok((!snapshots.is_empty()).then_some(Table { snapshots }))
    }

    /// The content hashes of `table`'s present snapshots, in append order: what [`Table::version`]
    /// returns, without reading any snapshot's footer.
    pub fn version(&self, table: &str) -> Result<Vec<String>> {
        let mut version = Vec::new();
        for s in self.retained(table) {
            let path = crate::offchain::segment_path(&self.dir, s);
            match std::fs::metadata(&path) {
                Ok(_) => version.push(s.hash.clone()),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e).with_context(|| format!("reading {}", path.display())),
            }
        }
        Ok(version)
    }

    /// The present snapshots of `table` among `hashes`, in append order.
    pub fn snapshots(&self, table: &str, hashes: &[String]) -> Result<Vec<Snapshot>> {
        let mut out = Vec::new();
        for s in self.retained(table).filter(|s| hashes.contains(&s.hash)) {
            out.extend(self.open(s)?);
        }
        Ok(out)
    }

    /// Case-insensitive, as `/sql` resolves `offchain__<table>`. `offchain drop` refuses a case-only
    /// variant, so at most one name matches.
    fn retained(&self, table: &str) -> impl Iterator<Item = &crate::offchain::Snapshot> {
        self.catalogue
            .tables
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(table))
            .map(|(_, snapshots)| snapshots)
            .into_iter()
            .flatten()
    }

    /// A retained snapshot, or `None` when its segment is absent, as the `/sql` view treats it.
    fn open(&self, s: &crate::offchain::Snapshot) -> Result<Option<Snapshot>> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let path = crate::offchain::segment_path(&self.dir, s);
        let file = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e).with_context(|| format!("opening {}", path.display())),
        };
        let footer = ParquetRecordBatchReaderBuilder::try_new(file)
            .with_context(|| format!("reading the schema of {}", path.display()))?;
        Ok(Some(Snapshot {
            hash: s.hash.clone(),
            schema: footer.schema().clone(),
            rows: u64::try_from(footer.metadata().file_metadata().num_rows()).unwrap_or(u64::MAX),
            bytes: footer
                .metadata()
                .row_groups()
                .iter()
                .map(|g| u64::try_from(g.total_byte_size()).unwrap_or(u64::MAX))
                .fold(0, u64::saturating_add),
            path,
        }))
    }

    pub fn names(&self) -> Vec<&str> {
        self.catalogue.tables.keys().map(String::as_str).collect()
    }
}

impl OffchainTables for Tables {
    fn bind(&self, table: &str, source: &str, columns: &[String]) -> Result<Option<Vec<Column>>> {
        match self.table(table)? {
            Some(found) => found.bind(source, columns).map(Some),
            None => Ok(None),
        }
    }

    fn names(&self) -> Vec<&str> {
        Tables::names(self)
    }
}

impl OffchainSnapshot for Snapshot {
    fn rows(&self, columns: &[Column]) -> Result<Vec<Row>> {
        Snapshot::rows(self, columns)
    }
}

fn batch_rows(batch: &RecordBatch, columns: &[Column]) -> Result<Vec<Row>> {
    let arrays = columns
        .iter()
        .map(|c| {
            let Ok(i) = batch.schema().index_of(&c.name) else {
                return Ok(None);
            };
            let array = batch.column(i).clone();
            match kind_of(array.data_type()) {
                Ok(k) if k == c.kind => Ok(Some(array)),
                _ => bail!(
                    "{} is {} in this snapshot but was bound as {}; the entity must be rebuilt",
                    c.name,
                    array.data_type(),
                    c.kind.name()
                ),
            }
        })
        .collect::<Result<Vec<_>>>()?;
    (0..batch.num_rows())
        .map(|r| {
            arrays
                .iter()
                .map(|a| a.as_ref().map_or(Ok(Scalar::Null), |a| cell(a.as_ref(), r)))
                .collect::<Result<Vec<_>>>()
                .map(Row)
        })
        .collect()
}

fn cell(a: &dyn Array, i: usize) -> Result<Scalar> {
    if a.is_null(i) {
        return Ok(Scalar::Null);
    }
    Ok(match a.data_type() {
        DataType::Utf8 => Scalar::Str(a.as_string::<i32>().value(i).to_string()),
        DataType::LargeUtf8 => Scalar::Str(a.as_string::<i64>().value(i).to_string()),
        DataType::Utf8View => Scalar::Str(a.as_string_view().value(i).to_string()),
        DataType::Boolean => Scalar::Bool(a.as_boolean().value(i)),
        DataType::Int8 => Scalar::Int(a.as_primitive::<Int8Type>().value(i).into()),
        DataType::Int16 => Scalar::Int(a.as_primitive::<Int16Type>().value(i).into()),
        DataType::Int32 => Scalar::Int(a.as_primitive::<Int32Type>().value(i).into()),
        DataType::Int64 => Scalar::Int(a.as_primitive::<Int64Type>().value(i).into()),
        DataType::UInt8 => Scalar::Int(a.as_primitive::<UInt8Type>().value(i).into()),
        DataType::UInt16 => Scalar::Int(a.as_primitive::<UInt16Type>().value(i).into()),
        DataType::UInt32 => Scalar::Int(a.as_primitive::<UInt32Type>().value(i).into()),
        DataType::UInt64 => Scalar::Int(a.as_primitive::<UInt64Type>().value(i).into()),
        other => bail!("{other} is not an offchain type an entity reads"),
    })
}

/// What a change in a table's snapshots means for an entity that has applied `applied`.
#[derive(Debug, PartialEq, Eq)]
pub enum Advance<'a> {
    Unchanged,
    /// The current version extends the applied one; feed these snapshots at `+1`.
    Append(&'a [String]),
    /// A snapshot was replaced, removed or reordered. Offchain rows have no retraction path, so the
    /// entity is rebuilt from every retained snapshot rather than patched.
    Rebuild,
}

pub fn advance<'a>(applied: &[String], current: &'a [String]) -> Advance<'a> {
    match current.strip_prefix(applied) {
        Some([]) => Advance::Unchanged,
        Some(new) => Advance::Append(new),
        None => Advance::Rebuild,
    }
}

fn short(hash: &str) -> &str {
    &hash[..hash.len().min(12)]
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{BooleanArray, Float64Array, Int64Array, StringArray, UInt32Array};
    use arrow::datatypes::{Field, Schema};
    use std::sync::Arc;

    fn parquet(batch: &RecordBatch) -> Vec<u8> {
        let mut out = Vec::new();
        let mut w = parquet::arrow::ArrowWriter::try_new(&mut out, batch.schema(), None).unwrap();
        w.write(batch).unwrap();
        w.close().unwrap();
        out
    }

    /// Seal `batch` as a snapshot of `table` the way `offchain drop` does, and return its hash.
    fn seal(dir: &Path, table: &str, batch: &RecordBatch) -> String {
        let src = dir.join(format!("{table}-{}.parquet", batch.num_rows()));
        std::fs::write(&src, parquet(batch)).unwrap();
        crate::offchain::drop_file(dir, &src, table).unwrap();
        crate::offchain::load(dir).unwrap().tables[table]
            .last()
            .unwrap()
            .hash
            .clone()
    }

    fn batch(columns: Vec<(&str, Arc<dyn Array>)>) -> RecordBatch {
        let fields: Vec<Field> = columns
            .iter()
            .map(|(n, a)| Field::new(*n, a.data_type().clone(), true))
            .collect();
        RecordBatch::try_new(
            Arc::new(Schema::new(fields)),
            columns.into_iter().map(|(_, a)| a).collect(),
        )
        .unwrap()
    }

    fn prices(symbols: &[&str], e8: &[i64]) -> RecordBatch {
        batch(vec![
            ("symbol", Arc::new(StringArray::from(symbols.to_vec()))),
            ("price_e8", Arc::new(Int64Array::from(e8.to_vec()))),
        ])
    }

    fn cols(names: &[&str]) -> Vec<String> {
        names.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn a_source_names_an_offchain_table_only_through_the_namespace() {
        assert_eq!(table_of("offchain__prices"), Some("prices"));
        assert_eq!(table_of("OFFCHAIN__prices"), Some("prices"));
        assert_eq!(table_of("offchain__"), None);
        assert_eq!(table_of("usdc__transfer"), None);
        assert_eq!(table_of("offchain_prices"), None);
        assert_eq!(
            table_of("offchain_é"),
            None,
            "a boundary inside a character"
        );
    }

    #[test]
    fn a_snapshot_binds_and_reads_as_exact_rows() {
        let dir = tempfile::tempdir().unwrap();
        let hash = seal(
            dir.path(),
            "prices",
            &prices(&["ETH", "BTC"], &[250_000_000_000, 7]),
        );
        let tables = Tables::load(dir.path()).unwrap();
        let table = tables.table("prices").unwrap().expect("one snapshot");
        assert_eq!(table.version(), vec![hash]);

        let bound = table
            .bind("offchain__prices", &cols(&["price_e8", "symbol"]))
            .unwrap();
        assert_eq!(
            bound,
            vec![
                Column {
                    name: "price_e8".into(),
                    kind: Kind::Int
                },
                Column {
                    name: "symbol".into(),
                    kind: Kind::Str
                },
            ]
        );
        assert_eq!(
            table.snapshots()[0].rows(&bound).unwrap(),
            vec![
                Row(vec![
                    Scalar::Int(250_000_000_000),
                    Scalar::Str("ETH".into())
                ]),
                Row(vec![Scalar::Int(7), Scalar::Str("BTC".into())]),
            ]
        );
    }

    /// `union_by_name`: a column a snapshot lacks is NULL for that snapshot's rows, not an error and
    /// not a shifted column.
    #[test]
    fn a_column_one_snapshot_lacks_reads_null_there() {
        let dir = tempfile::tempdir().unwrap();
        seal(dir.path(), "prices", &prices(&["ETH"], &[1]));
        seal(
            dir.path(),
            "prices",
            &batch(vec![
                ("symbol", Arc::new(StringArray::from(vec!["ETH"]))),
                ("price_e8", Arc::new(Int64Array::from(vec![2]))),
                ("venue", Arc::new(StringArray::from(vec!["cex"]))),
            ]),
        );
        let table = Tables::load(dir.path())
            .unwrap()
            .table("prices")
            .unwrap()
            .unwrap();
        let bound = table
            .bind("offchain__prices", &cols(&["venue", "price_e8"]))
            .unwrap();
        let rows: Vec<Row> = table
            .snapshots()
            .iter()
            .flat_map(|s| s.rows(&bound).unwrap())
            .collect();
        assert_eq!(
            rows,
            vec![
                Row(vec![Scalar::Null, Scalar::Int(1)]),
                Row(vec![Scalar::Str("cex".into()), Scalar::Int(2)]),
            ]
        );
    }

    #[test]
    fn an_unknown_column_is_refused_with_the_columns_there_are() {
        let dir = tempfile::tempdir().unwrap();
        seal(dir.path(), "prices", &prices(&["ETH"], &[1]));
        let table = Tables::load(dir.path())
            .unwrap()
            .table("prices")
            .unwrap()
            .unwrap();
        let err = format!(
            "{:#}",
            table
                .bind("offchain__prices", &cols(&["price"]))
                .unwrap_err()
        );
        assert!(err.contains("no column price in offchain__prices"), "{err}");
        assert!(err.contains("symbol, price_e8"), "{err}");
    }

    #[test]
    fn inexact_and_unsupported_types_are_refused_at_load() {
        let dir = tempfile::tempdir().unwrap();
        seal(
            dir.path(),
            "prices",
            &batch(vec![
                ("price", Arc::new(Float64Array::from(vec![2500.5]))),
                (
                    "at",
                    Arc::new(arrow::array::TimestampSecondArray::from(vec![
                        1_700_000_000,
                    ])),
                ),
            ]),
        );
        let table = Tables::load(dir.path())
            .unwrap()
            .table("prices")
            .unwrap()
            .unwrap();
        let float = format!(
            "{:#}",
            table
                .bind("offchain__prices", &cols(&["price"]))
                .unwrap_err()
        );
        assert!(float.contains("floating point"), "{float}");
        let ts = format!(
            "{:#}",
            table.bind("offchain__prices", &cols(&["at"])).unwrap_err()
        );
        assert!(ts.contains("text, integer and boolean"), "{ts}");
    }

    /// One column, two kinds across snapshots, is refused rather than resolved by a rule the SQL
    /// engine and the entity might not share. Two integer widths are one kind and bind.
    #[test]
    fn a_column_whose_kind_differs_between_snapshots_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        seal(dir.path(), "prices", &prices(&["ETH"], &[1]));
        seal(
            dir.path(),
            "prices",
            &batch(vec![("price_e8", Arc::new(UInt32Array::from(vec![2u32])))]),
        );
        let table = Tables::load(dir.path())
            .unwrap()
            .table("prices")
            .unwrap()
            .unwrap();
        assert_eq!(
            table
                .bind("offchain__prices", &cols(&["price_e8"]))
                .unwrap(),
            vec![Column {
                name: "price_e8".into(),
                kind: Kind::Int
            }]
        );

        seal(
            dir.path(),
            "prices",
            &batch(vec![("symbol", Arc::new(BooleanArray::from(vec![true])))]),
        );
        let table = Tables::load(dir.path())
            .unwrap()
            .table("prices")
            .unwrap()
            .unwrap();
        let err = format!(
            "{:#}",
            table
                .bind("offchain__prices", &cols(&["symbol"]))
                .unwrap_err()
        );
        assert!(
            err.contains("text in snapshot") && err.contains("boolean in snapshot"),
            "{err}"
        );
    }

    /// The view skips a snapshot whose segment is gone, so the entity does too, and the version
    /// says so: a deleted snapshot is a different table, which is a rebuild.
    #[test]
    fn a_missing_segment_is_absent_from_the_table_and_its_version() {
        let dir = tempfile::tempdir().unwrap();
        let first = seal(dir.path(), "prices", &prices(&["ETH"], &[1]));
        let second = seal(dir.path(), "prices", &prices(&["ETH"], &[2]));
        let tables = Tables::load(dir.path()).unwrap();
        let catalogue = crate::offchain::load(dir.path()).unwrap();
        std::fs::remove_file(crate::offchain::segment_path(
            dir.path(),
            &catalogue.tables["prices"][0],
        ))
        .unwrap();
        let version = tables.table("prices").unwrap().unwrap().version();
        assert_eq!(version, vec![second.clone()]);
        assert_eq!(advance(&[first, second], &version), Advance::Rebuild);
        assert!(Tables::load(dir.path())
            .unwrap()
            .table("absent")
            .unwrap()
            .is_none());
    }

    /// `/sql` reads `offchain__prices` for a table dropped as `Prices`, so an entity must too.
    #[test]
    fn an_offchain_table_resolves_case_insensitively_as_duckdb_does() {
        let dir = tempfile::tempdir().unwrap();
        let hash = seal(dir.path(), "Prices", &prices(&["ETH"], &[1]));
        let tables = Tables::load(dir.path()).unwrap();
        let table = tables
            .table("prices")
            .unwrap()
            .expect("found despite the case");
        assert_eq!(table.version(), vec![hash.clone()]);
        assert_eq!(tables.version("prices").unwrap(), vec![hash.clone()]);
        assert_eq!(tables.snapshots("PRICES", &[hash]).unwrap().len(), 1);
    }

    #[test]
    fn a_segment_whose_bytes_changed_is_refused_not_read() {
        let dir = tempfile::tempdir().unwrap();
        seal(dir.path(), "prices", &prices(&["ETH"], &[1]));
        let table = Tables::load(dir.path())
            .unwrap()
            .table("prices")
            .unwrap()
            .unwrap();
        let catalogue = crate::offchain::load(dir.path()).unwrap();
        let path = crate::offchain::segment_path(dir.path(), &catalogue.tables["prices"][0]);
        std::fs::write(&path, parquet(&prices(&["ETH"], &[999]))).unwrap();
        let bound = table
            .bind("offchain__prices", &cols(&["price_e8"]))
            .unwrap();
        let err = format!("{:#}", table.snapshots()[0].rows(&bound).unwrap_err());
        assert!(err.contains("not the"), "{err}");
    }

    #[test]
    fn only_an_extension_of_the_applied_snapshots_is_a_delta() {
        let v = |s: &[&str]| s.iter().map(|x| x.to_string()).collect::<Vec<_>>();
        assert_eq!(
            advance(&v(&["a", "b"]), &v(&["a", "b"])),
            Advance::Unchanged
        );
        assert_eq!(
            advance(&v(&[]), &v(&["a"])),
            Advance::Append(&v(&["a"])[..])
        );
        assert_eq!(
            advance(&v(&["a"]), &v(&["a", "b", "c"])),
            Advance::Append(&v(&["b", "c"])[..])
        );
        assert_eq!(advance(&v(&["a", "b"]), &v(&["a", "c"])), Advance::Rebuild);
        assert_eq!(advance(&v(&["a", "b"]), &v(&["a"])), Advance::Rebuild);
        assert_eq!(advance(&v(&["a", "b"]), &v(&["b", "a"])), Advance::Rebuild);
    }
}

/// The entity binder (`nuthatch_core::entity_bind`) over this module's file-backed tables (#1437).
#[cfg(test)]
mod through_the_binder {
    use crate::entity_bind::Binding;
    use crate::entity_circuit::EntityCircuit;
    use crate::entity_expr::Expr;
    use crate::entity_offchain::Tables;
    use crate::entity_plan::{Agg, Join, Plan, Source};
    use crate::entity_row::{Row, Scalar};
    use crate::registry::{ContractSpec, DecodeRegistry, DecodedRow};
    use crate::rpc::Log;

    const ERC20: &str = r#"[
        {"type":"event","name":"Transfer","inputs":[
            {"name":"from","type":"address","indexed":true},
            {"name":"to","type":"address","indexed":true},
            {"name":"value","type":"uint256","indexed":false}],"anonymous":false}
    ]"#;
    const TRANSFER_TOPIC0: &str =
        "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";
    const TOKEN: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";
    const ALICE: &str = "0x1111111111111111111111111111111111111111";
    const BOB: &str = "0x2222222222222222222222222222222222222222";

    fn registry() -> DecodeRegistry {
        let abi: alloy_json_abi::JsonAbi = serde_json::from_str(ERC20).unwrap();
        DecodeRegistry::build(vec![ContractSpec {
            alias: "usdc".into(),
            address: TOKEN.parse().unwrap(),
            abi,
            events: Vec::new(),
        }])
        .unwrap()
    }

    fn transfer(from: &str, to: &str, value: &str, block: u64, li: u64) -> Log {
        let topic = |a: &str| format!("0x{:0>64}", a.trim_start_matches("0x"));
        Log {
            address: TOKEN.into(),
            topics: vec![TRANSFER_TOPIC0.into(), topic(from), topic(to)],
            data: format!("0x{value:0>64}"),
            block_number: block,
            block_hash: "0xbh".into(),
            tx_hash: "0xtx".into(),
            log_index: li,
        }
    }

    fn decode(reg: &DecodeRegistry, logs: &[Log]) -> Vec<DecodedRow> {
        logs.iter()
            .map(|l| reg.decode(l).unwrap().unwrap())
            .collect()
    }

    fn transfer_source(columns: &[&str]) -> Source {
        Source {
            table: "usdc__transfer".into(),
            columns: columns.iter().map(|c| c.to_string()).collect(),
        }
    }

    /// Seal one `tiers(account, tier)` snapshot the way `offchain drop` does.
    fn tiers(dir: &std::path::Path, rows: &[(&str, i64)]) {
        use arrow::array::{Int64Array, RecordBatch, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        let batch = RecordBatch::try_new(
            std::sync::Arc::new(Schema::new(vec![
                Field::new("account", DataType::Utf8, false),
                Field::new("tier", DataType::Int64, false),
            ])),
            vec![
                std::sync::Arc::new(StringArray::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                std::sync::Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let mut bytes = Vec::new();
        let mut w = parquet::arrow::ArrowWriter::try_new(&mut bytes, batch.schema(), None).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        let src = dir.join("tiers.parquet");
        std::fs::write(&src, bytes).unwrap();
        crate::offchain::drop_file(dir, &src, "tiers").unwrap();
    }

    /// `SELECT t.tier, SUM(x.value) FROM usdc__transfer x JOIN offchain__tiers t ON x.to = t.account
    /// GROUP BY t.tier`
    fn received_by_tier() -> Plan {
        Plan {
            left: transfer_source(&["to", "value"]),
            left_filter: None,
            join: Some(Join {
                right: Source {
                    table: "offchain__tiers".into(),
                    columns: vec!["account".into(), "tier".into()],
                },
                right_filter: None,
                on: (0, 0),
            }),
            key: vec![Expr::Column(3)],
            aggregates: vec![Agg::Sum(Expr::Column(1))],
        }
    }

    /// #1437: a chain source and an offchain source in one entity. Decoded windows feed only the
    /// chain side and snapshots only the offchain side, and the circuit agrees with the evaluator.
    #[test]
    fn an_offchain_snapshot_joins_a_decoded_window_through_the_binding_and_the_circuit() {
        let reg = registry();
        let dir = tempfile::tempdir().unwrap();
        tiers(dir.path(), &[(BOB, 1), (ALICE, 2)]);
        let offchain = Tables::load(dir.path()).unwrap();
        let plan = received_by_tier();
        let binding = Binding::bind_with_offchain(&plan, &reg, &offchain).unwrap();
        assert_eq!(binding.offchain_tables(), vec!["tiers"]);

        let (left, right) = binding
            .window(&decode(
                &reg,
                &[
                    transfer(ALICE, BOB, "64", 10, 0),
                    transfer(BOB, ALICE, "5", 11, 0),
                ],
            ))
            .unwrap();
        assert_eq!(
            (left.len(), right.len()),
            (2, 0),
            "a window never feeds the offchain side"
        );

        let snapshot = &offchain.table("tiers").unwrap().unwrap().snapshots()[0].clone();
        let (none, tiers) = binding.snapshot("tiers", snapshot).unwrap();
        assert_eq!(
            (none.len(), tiers.len()),
            (0, 2),
            "a snapshot never feeds the chain side"
        );
        let (a, b) = binding.snapshot("other", snapshot).unwrap();
        assert!(
            a.is_empty() && b.is_empty(),
            "a snapshot of another table feeds nothing"
        );

        let weighted = |rows: &[Row]| rows.iter().map(|r| (r.clone(), 1)).collect::<Vec<_>>();
        let mut circuit = EntityCircuit::build(plan.clone()).unwrap();
        let mut relation = crate::entity_plan::Relation::new();
        circuit
            .apply(&weighted(&left), &weighted(&tiers), &mut relation)
            .unwrap();
        assert_eq!(
            relation.get(&Row(vec![Scalar::Int(1)])),
            Some(&Row(vec![Scalar::Int(100)])),
            "0x64 to bob, who is tier 1"
        );
        assert_eq!(relation, plan.evaluate(&left, &tiers).unwrap());
    }

    #[test]
    fn an_unknown_offchain_table_names_the_ones_there_are() {
        let reg = registry();
        let dir = tempfile::tempdir().unwrap();
        tiers(dir.path(), &[(BOB, 1)]);
        let mut plan = received_by_tier();
        plan.join.as_mut().unwrap().right.table = "offchain__tier".into();
        let err = format!(
            "{:#}",
            Binding::bind_with_offchain(&plan, &reg, &Tables::load(dir.path()).unwrap())
                .unwrap_err()
        );
        assert!(err.contains("Offchain tables: offchain__tiers"), "{err}");
    }
}
