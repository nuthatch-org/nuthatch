//! RFC-0041 §4 step 4: Nuthatch's dynamic row representation for entity circuits (#870).
//!
//! The slice-zero spike built its circuit over concrete Rust structs - `DelegationFact` with three
//! named fields - and said so: *"the Rust representation is fixed for this vertical spike"*. That is
//! why nothing can convert a decoded window into an entity's input relations for any plan but the one
//! admitted: the shape is in the type system rather than in the plan.
//!
//! §4 step 4 asks instead for a circuit *"over Nuthatch's dynamic row representation"*, so the plan
//! carries the shape and the types do not. That means one row type, positional, with the column
//! meaning fixed by the plan rather than by a struct field.
//!
//! **This module exists to answer one question before any lowering is written:** can such a type
//! satisfy DBSP's operator bounds at all? A ZSet key must be `Clone + Ord + Hash + SizeOf + rkyv`-
//! archivable and more besides, and a dynamic enum over `Vec` is exactly the shape those bounds are
//! least forgiving about. If it cannot, the whole plan-driven direction needs rethinking and that is
//! an RFC-level finding rather than an implementation detail.

use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use size_of::SizeOf;
use std::fmt;

/// One exact scalar. Deliberately narrow: RFC-0041 §3.3 admits integer and decimal arithmetic and
/// **refuses floating-point aggregation**, so there is no float variant to be tempted by.
///
/// `Int(i128)` carries token amounts. Decimal scale lives in the plan's column metadata rather than
/// in the value, so two rows of one column cannot disagree about scale - the mistake that makes
/// exact arithmetic quietly inexact.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    SizeOf,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive_attr(derive(PartialEq, Eq, PartialOrd, Ord))]
pub enum Scalar {
    /// Ordered first, so `NULLS FIRST` is the representation's own default rather than an accident
    /// of derive order that nobody wrote down. RFC-0042's research lists NULL ordering as a
    /// cross-engine divergence to pin explicitly; this pins it.
    #[default]
    Null,
    Bool(bool),
    Int(i128),
    Str(String),
}

impl Scalar {
    pub fn as_int(&self) -> Option<i128> {
        match self {
            Scalar::Int(i) => Some(*i),
            _ => None,
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Scalar::Null)
    }
}

impl fmt::Display for Scalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Scalar::Null => write!(f, "NULL"),
            Scalar::Bool(b) => write!(f, "{b}"),
            Scalar::Int(i) => write!(f, "{i}"),
            Scalar::Str(s) => write!(f, "{s}"),
        }
    }
}

/// A positional row. Column meaning comes from the plan, not from field names.
#[derive(
    Clone,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    Default,
    SizeOf,
    Archive,
    RkyvSerialize,
    RkyvDeserialize,
)]
#[archive_attr(derive(PartialEq, Eq, PartialOrd, Ord))]
pub struct Row(pub Vec<Scalar>);

// The two dbsp bounds that are not derivable. Neither type is ever a SQL NULL *wrapper* - a null
// column is `Scalar::Null` inside the row, not an absent row - and neither is a numeric key a
// roaring bitmap filter could index, so both are the "not applicable" impls dbsp provides for
// exactly this case.
dbsp::never_none!(Scalar, Row);
dbsp::never_roaring_filter!(Scalar, Row);

impl Row {
    pub fn get(&self, i: usize) -> &Scalar {
        self.0.get(i).unwrap_or(&Scalar::Null)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Project to a new row by column index - the operation every lowered projection is.
    pub fn project(&self, cols: &[usize]) -> Row {
        Row(cols.iter().map(|&i| self.get(i).clone()).collect())
    }
}

impl FromIterator<Scalar> for Row {
    fn from_iter<I: IntoIterator<Item = Scalar>>(it: I) -> Self {
        Row(it.into_iter().collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn null_sorts_first_and_that_is_deliberate() {
        let mut v = [
            Scalar::Int(1),
            Scalar::Null,
            Scalar::Str("a".into()),
            Scalar::Bool(true),
        ];
        v.sort();
        assert_eq!(v[0], Scalar::Null, "NULLS FIRST is the pinned default");
    }

    #[test]
    fn rows_order_lexicographically_by_column() {
        let a = Row(vec![Scalar::Int(1), Scalar::Str("b".into())]);
        let b = Row(vec![Scalar::Int(1), Scalar::Str("c".into())]);
        let c = Row(vec![Scalar::Int(2), Scalar::Str("a".into())]);
        assert!(a < b && b < c);
    }

    #[test]
    fn a_missing_column_reads_as_null_rather_than_panicking() {
        // A plan that projects a column a row does not have is a bug, but it must not take the
        // circuit thread down mid-window - the entity faults through its health flag instead.
        let r = Row(vec![Scalar::Int(1)]);
        assert_eq!(r.get(0), &Scalar::Int(1));
        assert_eq!(r.get(9), &Scalar::Null);
    }

    #[test]
    fn projection_is_by_index() {
        let r = Row(vec![
            Scalar::Int(1),
            Scalar::Str("x".into()),
            Scalar::Bool(true),
        ]);
        assert_eq!(
            r.project(&[2, 0]),
            Row(vec![Scalar::Bool(true), Scalar::Int(1)])
        );
    }

    #[test]
    fn i128_survives_a_round_trip_at_full_width() {
        // Token sums are the reason this is i128 and not i64. §3.3 refuses floating-point
        // aggregation precisely so this stays exact.
        let big = Scalar::Int(i128::MAX);
        assert_eq!(big.as_int(), Some(i128::MAX));
        assert_eq!(format!("{big}"), i128::MAX.to_string());
    }
    /// **The question this module exists to answer.** Build a real circuit over `Row`: an indexed
    /// ZSet keyed by `Row`, an equijoin against a second relation, and a linear aggregate. If DBSP's
    /// operator bounds reject a dynamic type, the plan-driven direction in §4 step 4 needs
    /// rethinking, and that is an RFC finding rather than an implementation detail.
    #[test]
    fn dbsp_accepts_a_dynamic_row_as_a_zset_key_and_value() {
        use dbsp::utils::Tup2;
        use dbsp::{IndexedZSetReader, OrdZSet, OutputHandle, RootCircuit, Runtime};

        type Handles = (
            (
                dbsp::IndexedZSetHandle<Row, Row>,
                dbsp::IndexedZSetHandle<Row, Row>,
            ),
            OutputHandle<OrdZSet<Tup2<Row, i128>>>,
        );

        let build = |circuit: &mut RootCircuit| -> anyhow::Result<Handles> {
            let (left, lh) = circuit.add_input_indexed_zset::<Row, Row>();
            let (right, rh) = circuit.add_input_indexed_zset::<Row, Row>();
            // filter: keep rows whose first payload column is a positive integer
            let kept = left.filter(|(_, v)| v.get(0).as_int().is_some_and(|i| i > 0));
            // inner equijoin on the index key, projecting key + amount
            let joined = kept.join_index(&right, |k: &Row, v: &Row, _r: &Row| {
                std::iter::once((k.clone(), v.get(0).as_int().unwrap_or(0)))
            });
            let totals = joined.aggregate_linear(|amount: &i128| *amount);
            let out = totals
                .map(|(k, amount): (&Row, &i128)| Tup2(k.clone(), *amount))
                .output();
            Ok(((lh, rh), out))
        };

        let (mut circuit, ((left, right), out)) = Runtime::init_circuit(1, build).unwrap();

        let key = |s: &str| Row(vec![Scalar::Str(s.into())]);
        let amt = |i: i128| Row(vec![Scalar::Int(i)]);

        left.append(&mut vec![
            Tup2(key("i1"), Tup2(amt(7), 1)),
            Tup2(key("i1"), Tup2(amt(5), 1)),
            Tup2(key("i2"), Tup2(amt(11), 1)),
            Tup2(key("i1"), Tup2(amt(-3), 1)),
        ]);
        right.append(&mut vec![Tup2(
            key("i1"),
            Tup2(Row(vec![Scalar::Bool(true)]), 1),
        )]);
        circuit.transaction().unwrap();

        let mut got: Vec<(String, i128)> = Vec::new();
        out.consolidate()
            .iter()
            .for_each(|(Tup2(k, v), (), w): (Tup2<Row, i128>, (), i64)| {
                if w > 0 {
                    got.push((k.get(0).to_string(), v));
                }
            });
        got.sort();

        assert_eq!(
            got,
            vec![("i1".to_string(), 12)],
            "i1 sums 7+5 (the -3 is filtered), i2 has no join partner"
        );
    }
}
