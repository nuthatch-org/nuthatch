//! RFC-0041 §3.3: the relational plan an entity lowers to, and its batch evaluator (#870).
//!
//! Two things live here, and the second is the reference for the first.
//!
//! [`Plan`] is the lowered shape: named inputs, per-input filters, an optional inner equijoin, a
//! grouping key and the admitted aggregates. It is deliberately not a general relational algebra -
//! §3.3 admits a small subset and anything outside it is refused at validation (#836) rather than
//! half-supported here.
//!
//! [`Plan::evaluate`] folds a complete set of rows through that plan in one pass. **That is not a
//! substitute for the circuit; it is the thing the circuit must agree with.** RFC-0041 §8 requires
//! "incremental result == batch recomputation over full history", and RFC-0042's Tier 1 research
//! named exactly this pairing as how the DuckDB oracle was retired: two independent
//! implementations that must agree, rather than one implementation and a hope.
//!
//! It has a second job. §5.3's warm restart computes "one finalized seed relation" from sealed
//! facts, which would otherwise mean a SQL engine query. A native batch evaluator is that seed without the
//! engine - which is why this is worth building before the circuit rather than after.

use crate::entity_expr::{admits, Expr};
use crate::entity_row::{Row, Scalar};
use anyhow::{bail, Result};
use std::collections::BTreeMap;

/// One admitted aggregate. §3.3: `count`, `sum`, `min`, `max`, and **`avg` represented as sum plus
/// count** - carrying the pair rather than a running mean is what keeps it exact and what makes it
/// maintainable under retraction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Agg {
    Count,
    Sum(Expr),
    Min(Expr),
    Max(Expr),
    Avg(Expr),
}

/// An input relation: the decoded table it reads and the columns the plan expects, in order. The
/// binder resolves these against `DecodeRegistry`; the evaluator only ever indexes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Source {
    pub table: String,
    pub columns: Vec<String>,
}

/// An inner equijoin on one named column from each side. §3.3 admits inner equijoins with named
/// keys; outer joins are refused, and a non-equi predicate is not a join here at all.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Join {
    pub right: Source,
    pub right_filter: Option<Expr>,
    /// Column index into the left row, and into the right row.
    pub on: (usize, usize),
}

/// The lowered entity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Plan {
    pub left: Source,
    pub left_filter: Option<Expr>,
    pub join: Option<Join>,
    /// Grouping key expressions, evaluated against the joined row. Also the entity's declared key.
    pub key: Vec<Expr>,
    pub aggregates: Vec<Agg>,
}

/// A grouped result: key row to aggregate row, ordered by key.
pub type Relation = BTreeMap<Row, Row>;

/// Running state for one group. `Avg` carries its sum and count separately (§3.3) so retraction is a
/// subtraction on both rather than an irreversible division.
#[derive(Debug, Default, Clone)]
struct Acc {
    count: i128,
    sum: Option<i128>,
    min: Option<Scalar>,
    max: Option<Scalar>,
    avg_sum: i128,
    avg_count: i128,
}

impl Plan {
    /// The joined row for a left row and an optional right row: left columns then right columns.
    /// Column indices in `key` and the aggregates are into this concatenation, which is why the
    /// binder must know both widths.
    fn joined(left: &Row, right: Option<&Row>) -> Row {
        match right {
            None => left.clone(),
            Some(r) => Row(left.0.iter().chain(r.0.iter()).cloned().collect()),
        }
    }

    /// Fold a complete set of rows through the plan.
    ///
    /// `left` and `right` are the current `+1` facts of each input - not weighted deltas. This is
    /// the batch side of the §8 invariant; the incremental side takes weights.
    pub fn evaluate(&self, left: &[Row], right: &[Row]) -> Result<Relation> {
        // Index the right side by its join column once, rather than per left row. An entity with a
        // wide dimension table joined per row is the shape that turns a linear fold quadratic.
        let mut index: BTreeMap<Scalar, Vec<&Row>> = BTreeMap::new();
        if let Some(j) = &self.join {
            for r in right {
                if let Some(f) = &j.right_filter {
                    if !admits(f, r)? {
                        continue;
                    }
                }
                index.entry(r.get(j.on.1).clone()).or_default().push(r);
            }
        }

        let mut groups: BTreeMap<Row, Acc> = BTreeMap::new();
        for l in left {
            if let Some(f) = &self.left_filter {
                if !admits(f, l)? {
                    continue;
                }
            }
            match &self.join {
                None => self.accumulate(&mut groups, &Self::joined(l, None))?,
                Some(j) => {
                    // A NULL join key matches nothing, in SQL and here. Without this, every row with
                    // a null key would join to every other one - the classic silent fan-out.
                    let k = l.get(j.on.0);
                    if k.is_null() {
                        continue;
                    }
                    for r in index.get(k).into_iter().flatten() {
                        self.accumulate(&mut groups, &Self::joined(l, Some(r)))?;
                    }
                }
            }
        }

        groups
            .into_iter()
            .map(|(k, acc)| Ok((k, self.finish(&acc)?)))
            .collect()
    }

    fn accumulate(&self, groups: &mut BTreeMap<Row, Acc>, row: &Row) -> Result<()> {
        let mut key = Vec::with_capacity(self.key.len());
        for e in &self.key {
            key.push(e.eval(row)?);
        }
        let acc = groups.entry(Row(key)).or_default();
        acc.count += 1;
        for a in &self.aggregates {
            match a {
                Agg::Count => {}
                Agg::Sum(e) | Agg::Avg(e) => {
                    let v = e.eval(row)?;
                    // SQL: SUM ignores NULLs, and SUM over no non-null rows is NULL, not 0.
                    if let Some(i) = v.as_int() {
                        if matches!(a, Agg::Sum(_)) {
                            let base = acc.sum.unwrap_or(0);
                            acc.sum = Some(checked(base.checked_add(i), base, i)?);
                        } else {
                            acc.avg_sum = checked(acc.avg_sum.checked_add(i), acc.avg_sum, i)?;
                            acc.avg_count += 1;
                        }
                    } else if !v.is_null() {
                        bail!("sum/avg over a non-integer value {v:?}")
                    }
                }
                Agg::Min(e) => {
                    let v = e.eval(row)?;
                    if !v.is_null() && acc.min.as_ref().is_none_or(|m| &v < m) {
                        acc.min = Some(v);
                    }
                }
                Agg::Max(e) => {
                    let v = e.eval(row)?;
                    if !v.is_null() && acc.max.as_ref().is_none_or(|m| &v > m) {
                        acc.max = Some(v);
                    }
                }
            }
        }
        Ok(())
    }

    fn finish(&self, acc: &Acc) -> Result<Row> {
        let mut out = Vec::with_capacity(self.aggregates.len());
        for a in &self.aggregates {
            out.push(match a {
                Agg::Count => Scalar::Int(acc.count),
                Agg::Sum(_) => acc.sum.map_or(Scalar::Null, Scalar::Int),
                Agg::Min(_) => acc.min.clone().unwrap_or(Scalar::Null),
                Agg::Max(_) => acc.max.clone().unwrap_or(Scalar::Null),
                // Integer division truncating toward zero: the entity's contract (RFC-0041 §3.3), not
                // the SQL engine's, whose `avg` over integers is fractional. Carrying
                // sum and count to the end is what makes this exact rather than a drifting mean.
                Agg::Avg(_) => {
                    if acc.avg_count == 0 {
                        Scalar::Null
                    } else {
                        Scalar::Int(acc.avg_sum / acc.avg_count)
                    }
                }
            });
        }
        Ok(Row(out))
    }
}

/// §3.3.1 at the aggregate boundary: a running total that leaves `i128` faults at the row that
/// carried it past, rather than resuming from the other end of the number line.
fn checked(r: Option<i128>, base: i128, add: i128) -> Result<i128> {
    match r {
        Some(v) => Ok(v),
        None => bail!(
            "aggregate overflow: {base} + {add} does not fit i128. An entity faults rather than \
             wrapping (RFC-0041 §3.3.1)."
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_expr::Cmp;

    fn col(i: usize) -> Expr {
        Expr::Column(i)
    }
    fn int(i: i128) -> Expr {
        Expr::Literal(Scalar::Int(i))
    }
    fn s(v: &str) -> Scalar {
        Scalar::Str(v.into())
    }
    fn src(t: &str, cols: &[&str]) -> Source {
        Source {
            table: t.into(),
            columns: cols.iter().map(|c| c.to_string()).collect(),
        }
    }

    /// The Lodestar shape slice 0 hardcoded, expressed as a plan instead:
    /// `SELECT d.indexer, d.delegator, SUM(d.amount) FROM delegations d JOIN indexers i
    ///  ON d.indexer = i.indexer WHERE d.amount > 0 AND i.active GROUP BY 1, 2`
    fn delegation_plan() -> Plan {
        Plan {
            left: src("delegations", &["indexer", "delegator", "amount"]),
            left_filter: Some(Expr::Compare(Cmp::Gt, col(2).into(), int(0).into())),
            join: Some(Join {
                right: src("indexers", &["indexer", "active"]),
                right_filter: Some(col(1)),
                on: (0, 0),
            }),
            key: vec![col(0), col(1)],
            aggregates: vec![Agg::Sum(col(2))],
        }
    }

    fn d(indexer: &str, delegator: &str, amount: i128) -> Row {
        Row(vec![s(indexer), s(delegator), Scalar::Int(amount)])
    }
    fn i_row(indexer: &str, active: bool) -> Row {
        Row(vec![s(indexer), Scalar::Bool(active)])
    }

    #[test]
    fn the_lodestar_shape_lowers_to_a_plan_and_evaluates() {
        // The same corpus slice 0's unit test uses: i1/a has two rows to sum, i1/b is filtered by
        // the negative amount, i2 is dropped for being inactive.
        let got = delegation_plan()
            .evaluate(
                &[
                    d("i1", "a", 7),
                    d("i1", "a", 5),
                    d("i1", "b", -3),
                    d("i2", "c", 11),
                ],
                &[i_row("i1", true), i_row("i2", false)],
            )
            .unwrap();

        assert_eq!(got.len(), 1, "one surviving group: {got:?}");
        assert_eq!(
            got.get(&Row(vec![s("i1"), s("a")])),
            Some(&Row(vec![Scalar::Int(12)]))
        );
    }

    #[test]
    fn each_operator_is_load_bearing_on_that_corpus() {
        // Guards against the slice-0 trap (#835): a corpus on which the filter, join and aggregate
        // are all inert still "passes". Remove each and the answer must change.
        let base = delegation_plan();
        let left = [
            d("i1", "a", 7),
            d("i1", "a", 5),
            d("i1", "b", -3),
            d("i2", "c", 11),
        ];
        let right = [i_row("i1", true), i_row("i2", false)];

        let no_filter = Plan {
            left_filter: None,
            ..base.clone()
        };
        assert!(
            no_filter.evaluate(&left, &right).unwrap().len()
                > base.evaluate(&left, &right).unwrap().len(),
            "the filter must exclude something"
        );

        let no_join = Plan {
            join: None,
            ..base.clone()
        };
        assert_ne!(
            no_join.evaluate(&left, &right).unwrap().len(),
            base.evaluate(&left, &right).unwrap().len(),
            "the join must exclude something"
        );

        // The aggregate: two rows collapse to one group, so a group holds more than it received.
        let one = base.evaluate(&[d("i1", "a", 7)], &right).unwrap();
        assert_eq!(one.values().next(), Some(&Row(vec![Scalar::Int(7)])));
    }

    #[test]
    fn a_null_join_key_matches_nothing() {
        // Without the guard, every null-keyed row joins every other - a silent fan-out.
        let plan = delegation_plan();
        let left = [Row(vec![Scalar::Null, s("a"), Scalar::Int(5)])];
        let right = [Row(vec![Scalar::Null, Scalar::Bool(true)])];
        assert!(plan.evaluate(&left, &right).unwrap().is_empty());
    }

    #[test]
    fn sum_ignores_nulls_and_is_null_over_no_non_null_rows() {
        let plan = Plan {
            left: src("t", &["k", "v"]),
            left_filter: None,
            join: None,
            key: vec![col(0)],
            aggregates: vec![Agg::Sum(col(1)), Agg::Count],
        };
        let got = plan
            .evaluate(
                &[
                    Row(vec![s("k1"), Scalar::Int(3)]),
                    Row(vec![s("k1"), Scalar::Null]),
                    Row(vec![s("k2"), Scalar::Null]),
                ],
                &[],
            )
            .unwrap();
        // k1: SUM skips the null, COUNT counts the row.
        assert_eq!(
            got[&Row(vec![s("k1")])],
            Row(vec![Scalar::Int(3), Scalar::Int(2)])
        );
        // k2: SUM over no non-null rows is NULL, not 0. COUNT is still 1.
        assert_eq!(
            got[&Row(vec![s("k2")])],
            Row(vec![Scalar::Null, Scalar::Int(1)])
        );
    }

    #[test]
    fn min_max_and_avg_over_a_group() {
        let plan = Plan {
            left: src("t", &["k", "v"]),
            left_filter: None,
            join: None,
            key: vec![col(0)],
            aggregates: vec![Agg::Min(col(1)), Agg::Max(col(1)), Agg::Avg(col(1))],
        };
        let got = plan
            .evaluate(
                &[
                    Row(vec![s("k"), Scalar::Int(1)]),
                    Row(vec![s("k"), Scalar::Int(4)]),
                ],
                &[],
            )
            .unwrap();
        // avg = 5/2 truncated toward zero = 2, the entity's integer AVG. Deliberately a pair that
        // *rounds differently*: round-half-up would give 3. 1/2/4 (sum 7, count 3) gives 2 either
        // way and so cannot tell the two apart - a mutation caught that the first time round.
        assert_eq!(
            got[&Row(vec![s("k")])],
            Row(vec![Scalar::Int(1), Scalar::Int(4), Scalar::Int(2)])
        );
    }

    #[test]
    fn an_aggregate_that_overflows_faults_rather_than_wrapping() {
        let plan = Plan {
            left: src("t", &["k", "v"]),
            left_filter: None,
            join: None,
            key: vec![col(0)],
            aggregates: vec![Agg::Sum(col(1))],
        };
        let err = plan
            .evaluate(
                &[
                    Row(vec![s("k"), Scalar::Int(i128::MAX)]),
                    Row(vec![s("k"), Scalar::Int(1)]),
                ],
                &[],
            )
            .unwrap_err()
            .to_string();
        assert!(err.contains("aggregate overflow"), "{err}");
        assert!(err.contains("§3.3.1"), "{err}");
    }

    #[test]
    fn a_group_with_one_row_still_groups() {
        // The tell from #835: a GROUP BY emitting as many rows as it consumed has grouped nothing.
        // This asserts the opposite direction explicitly - four rows, two groups.
        let plan = Plan {
            left: src("t", &["k", "v"]),
            left_filter: None,
            join: None,
            key: vec![col(0)],
            aggregates: vec![Agg::Count],
        };
        let rows: Vec<Row> = ["a", "a", "b", "b"]
            .iter()
            .map(|k| Row(vec![s(k), Scalar::Int(1)]))
            .collect();
        let got = plan.evaluate(&rows, &[]).unwrap();
        assert_eq!(got.len(), 2, "four rows must collapse to two groups");
        assert_eq!(got[&Row(vec![s("a")])], Row(vec![Scalar::Int(2)]));
    }
}
