//! RFC-0041 §4 step 4: the DBSP circuit a [`Plan`] lowers to (#870).
//!
//! §5.1 says each decoded window is *"converted to the circuit's input relations and applied at
//! weight `+1`"*. Slice 0 could not do that for any plan but one, because its circuit was built over
//! concrete Rust structs. [`entity_row`](crate::entity_row) answered whether a dynamic row can carry
//! DBSP's operator bounds at all (it can); [`entity_plan`](crate::entity_plan) settled the lowered
//! shape and built the batch oracle. This module is the join between them: **a circuit constructed
//! from a `Plan` at nest load, with no generated Rust, no `cargo`, no JVM and no external
//! compiler** - the plan drives the operators, and the operators are parameterised by column index
//! rather than by Rust field.
//!
//! ## How an entity faults
//!
//! DBSP operators are infallible by signature: a `filter` closure returns `bool`, not
//! `Result<bool>`. Every fault this module can raise - a type error in an expression, division by
//! zero, an aggregate that leaves `i128` - therefore leaves through a **panic inside the circuit
//! worker**, which DBSP catches and returns from [`DBSPHandle::transaction`] as
//! `RuntimeError::WorkerPanic`. [`EntityCircuit::apply`] turns that back into an `Err`.
//!
//! This is dbsp's own idiom rather than an invention: `dbsp::algebra::CheckedInt` is documented as
//! *"Ring on numeric values that panics on overflow"* and exists for exactly this. It also gives the
//! isolation the runtime needs - the panic is caught by *this* circuit's runtime, so one entity
//! faulting cannot take down another nest's cursor, nor the process.
//!
//! A faulted circuit is dead, deliberately: §3.3.1 wants the entity to stop and be visibly stopped
//! (#866), not to carry on with a wrapped number in it.
//!
//! ## Which aggregate gets which operator
//!
//! - `count`, `sum` and `avg` are **linear**: one [`LinAcc`] carries every linear slot the plan
//!   needs, so a plan with four aggregates still builds one `aggregate_linear` rather than four.
//! - `min`/`max` are **not linear under retraction** - retracting the current maximum does not tell
//!   you the next one - so each uses dbsp's [`Max`]/[`Min`] aggregator, which walks the group's
//!   value cursor from the extreme end and takes the first value whose accumulated weight is
//!   non-zero. Correct under retraction, at the cost of a scan per changed group.
//! - `avg` is carried as sum and count to the end and divided once (§3.3), never as a running mean.
//!
//! The min/max results are grafted onto the linear row by an outer join on the group key, so a group
//! whose min-input is entirely NULL still emits with a NULL there rather than vanishing.

use crate::entity_expr::{admits, Expr};
use crate::entity_plan::{Agg, Plan, Relation};
use crate::entity_row::{Row, Scalar};
use anyhow::{anyhow, Result};
use dbsp::algebra::{AddAssignByRef, AddByRef, HasZero, MulByRef};
use dbsp::operator::{Max, Min};
use dbsp::utils::Tup2;
use dbsp::{
    DBSPHandle, IndexedZSetReader, OrdIndexedZSet, OrdZSet, OutputHandle, RootCircuit, Runtime,
    Stream, ZSetHandle, ZWeight,
};
use rkyv::{Archive, Deserialize as RkyvDeserialize, Serialize as RkyvSerialize};
use size_of::SizeOf;

/// The linear slots of every aggregate in a plan, in one value.
///
/// Slot `0` is the group's row count, and aggregate `i` owns slots `1 + 2i` and `2 + 2i`:
///
/// | aggregate   | first slot | second slot   |
/// |-------------|------------|---------------|
/// | `count`     | unused     | unused        |
/// | `sum`       | total      | non-NULL rows |
/// | `avg`       | total      | non-NULL rows |
/// | `min`/`max` | unused     | unused        |
///
/// The uniform stride costs a few unused `i128`s on a `min` and buys an indexing rule with no table
/// to consult and no second place to get it wrong.
///
/// **Slot 0 is load-bearing and not merely `count`'s answer.** DBSP drops a group whose accumulator
/// is [`HasZero::is_zero`], which is the right rule - it is how a fully retracted group disappears -
/// but it means an entity of nothing but `min`/`max` has an all-zero accumulator and would lose
/// every group it ever had. Counting rows makes a group's *existence* the thing being accumulated,
/// so it survives however uninteresting its aggregates are.
///
/// The non-NULL count is not decoration either: SQL's `SUM` over no non-NULL rows is `NULL`, not
/// `0`, and nothing else in a linear accumulator can tell those two apart.
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
pub struct LinAcc(Vec<i128>);

dbsp::never_none!(LinAcc);
dbsp::never_roaring_filter!(LinAcc);

impl LinAcc {
    /// A missing slot reads as zero, so [`HasZero::zero`]'s empty vector adds correctly against a
    /// full-width accumulator without a width negotiation.
    fn at(&self, i: usize) -> i128 {
        self.0.get(i).copied().unwrap_or(0)
    }

    fn zip(&self, other: &Self, f: impl Fn(i128, i128) -> i128) -> Self {
        let n = self.0.len().max(other.0.len());
        LinAcc((0..n).map(|i| f(self.at(i), other.at(i))).collect())
    }
}

/// §3.3.1 at the point it bites. An aggregate that cannot be represented stops the entity; it does
/// not wrap, and it does not saturate. See the module docs for why this is a panic.
fn or_fault(v: Option<i128>, what: &str) -> i128 {
    v.unwrap_or_else(|| {
        panic!(
            "entity circuit fault: {what} does not fit i128. An entity faults rather than wrapping \
             (RFC-0041 §3.3.1)."
        )
    })
}

impl HasZero for LinAcc {
    fn zero() -> Self {
        LinAcc(Vec::new())
    }

    fn is_zero(&self) -> bool {
        self.0.iter().all(|v| *v == 0)
    }
}

impl AddByRef for LinAcc {
    fn add_by_ref(&self, other: &Self) -> Self {
        self.zip(other, |a, b| {
            or_fault(a.checked_add(b), "an aggregate total")
        })
    }
}

impl AddAssignByRef for LinAcc {
    fn add_assign_by_ref(&mut self, other: &Self) {
        *self = self.add_by_ref(other);
    }
}

/// Weighting is how a retraction reaches a linear aggregate: DBSP multiplies the row's contribution
/// by `-1` rather than calling a separate undo. Checked here too, because a `-1 * i128::MIN` is the
/// one multiplication that has no answer.
impl MulByRef<ZWeight> for LinAcc {
    type Output = Self;

    fn mul_by_ref(&self, w: &ZWeight) -> Self {
        let w = i128::from(*w);
        LinAcc(
            self.0
                .iter()
                .map(|v| or_fault(v.checked_mul(w), "a weighted aggregate contribution"))
                .collect(),
        )
    }
}

/// An entity's circuit, built from its plan.
///
/// Owns the DBSP runtime, the input handles for the plan's one or two sources, and the current
/// value of the derived relation. Deltas go in; the relation is what comes out.
pub struct EntityCircuit {
    plan: Plan,
    handle: DBSPHandle,
    left: ZSetHandle<Row>,
    right: Option<ZSetHandle<Row>>,
    out: OutputHandle<OrdZSet<Tup2<Row, Row>>>,
    /// How many rows the derived relation currently holds. The relation itself belongs to the
    /// caller (see [`Self::apply`]); only its size is kept here, for the fault message.
    len: usize,
    /// Why this entity stopped, if it has.
    ///
    /// The slice-zero spike does not have this, and #864 names the consequence: when a transaction
    /// errors after the rows have been appended, the rows are inside the circuit, the bookkeeping
    /// outside it says they are not, and the next window is applied on top of that disagreement.
    /// Once the two can disagree, every later answer is a guess.
    ///
    /// So a fault is terminal and sticky. The relation keeps its last consistent value - it was
    /// correct for the windows that did apply - and no further window is accepted, which is the
    /// signal #866 turns into a visibly dead entity rather than a silently current one.
    ///
    /// **Not redundant with DBSP's own refusal.** A worker panic terminates the runtime, so a later
    /// `transaction()` fails anyway - with *"circuit has been terminated"*, which says nothing about
    /// why the entity stopped. And that only holds for faults that panic. A transaction failing for
    /// any other reason leaves the circuit alive and holding rows this struct does not know about,
    /// which is exactly the divergence #864 describes and the one nothing else here would catch.
    faulted: Option<String>,
}

type Built = (
    ZSetHandle<Row>,
    Option<ZSetHandle<Row>>,
    OutputHandle<OrdZSet<Tup2<Row, Row>>>,
);

impl EntityCircuit {
    /// Build the circuit for `plan`. Nothing is compiled and nothing is spawned beyond the DBSP
    /// worker: §4 step 4's "no generated native code, `cargo` invocation, JVM, network fetch or
    /// external compiler appears at nest load" is a property of this function.
    pub fn build(plan: Plan) -> Result<Self> {
        let spec = plan.clone();
        let (handle, (left, right, out)) =
            Runtime::init_circuit(1, move |circuit| Ok(Self::lower(&spec, circuit)))
                .map_err(|e| anyhow!("building the entity circuit: {e}"))?;

        Ok(Self {
            plan,
            handle,
            left,
            right,
            out,
            len: 0,
            faulted: None,
        })
    }

    /// Lower a plan onto operators. Every closure owns its slice of the plan, because a DBSP
    /// constructor must be `'static` and is cloned once per worker.
    fn lower(plan: &Plan, circuit: &mut RootCircuit) -> Built {
        let (left_in, left_handle) = circuit.add_input_zset::<Row>();

        let mut left = left_in;
        if let Some(f) = plan.left_filter.clone() {
            left = left.filter(move |r: &Row| fault_on_err(admits(&f, r)));
        }

        let keys = plan.key.clone();
        let (grouped, right_handle) = match plan.join.clone() {
            // No join: the row the aggregates see is the left row itself.
            None => (
                left.map_index(move |r: &Row| (eval_key(&keys, r), r.clone())),
                None,
            ),
            Some(join) => {
                let (right_in, right_handle) = circuit.add_input_zset::<Row>();
                let mut right = right_in;
                if let Some(f) = join.right_filter.clone() {
                    right = right.filter(move |r: &Row| fault_on_err(admits(&f, r)));
                }

                // A NULL join key matches nothing, in SQL and here. Dropping both sides' NULL keys
                // is the same relation as dropping one - equality never holds against NULL - and it
                // keeps a nest with a mostly-NULL dimension column out of the join's index entirely.
                let (lk, rk) = join.on;
                let left_ix = left
                    .filter(move |r: &Row| !r.get(lk).is_null())
                    .map_index(move |r: &Row| (Row(vec![r.get(lk).clone()]), r.clone()));
                let right_ix = right
                    .filter(move |r: &Row| !r.get(rk).is_null())
                    .map_index(move |r: &Row| (Row(vec![r.get(rk).clone()]), r.clone()));

                let joined = left_ix.join_index(&right_ix, move |_k: &Row, l: &Row, r: &Row| {
                    let row = concat(l, r);
                    std::iter::once((eval_key(&keys, &row), row))
                });
                (joined, Some(right_handle))
            }
        };

        // One linear pass for every count/sum/avg the plan asks for.
        let aggs = plan.aggregates.clone();
        let linear = grouped.aggregate_linear(move |row: &Row| linear_slots(&aggs, row));

        let aggs = plan.aggregates.clone();
        let mut combined: Stream<RootCircuit, OrdIndexedZSet<Row, Row>> = linear
            .map_index(move |(k, acc): (&Row, &LinAcc)| (k.clone(), finish_linear(&aggs, acc)));

        // min/max are grafted on one at a time. Each is its own aggregate over the group's values,
        // because neither is a linear function of them.
        for (slot, agg) in plan.aggregates.iter().enumerate() {
            let (expr, wants_max) = match agg {
                Agg::Min(e) => (e.clone(), false),
                Agg::Max(e) => (e.clone(), true),
                _ => continue,
            };

            // NULLs are excluded before the aggregate rather than after: SQL's MIN and MAX ignore
            // them, and `Scalar::Null` sorts first, so leaving them in would make every MIN a NULL.
            let values = grouped
                .map_index(move |(k, row): (&Row, &Row)| (k.clone(), fault_on_err(expr.eval(row))))
                .filter(|(_, v): (&Row, &Scalar)| !v.is_null());

            let extreme = if wants_max {
                values.aggregate(Max)
            } else {
                values.aggregate(Min)
            };

            // Outer, not inner: a group all of whose min-inputs are NULL has no row in `extreme`,
            // and must still appear with a NULL in that column rather than disappearing from the
            // entity. The reverse case cannot happen - `extreme`'s groups are a subset of the
            // linear side's - and `graft` says so rather than papering over it.
            combined = combined
                .outer_join_default(&extreme, move |k: &Row, row: &Row, v: &Scalar| {
                    Tup2(k.clone(), graft(row, slot, v))
                })
                .map_index(|Tup2(k, v): &Tup2<Row, Row>| (k.clone(), v.clone()));
        }

        let out = combined
            .map(|(k, v): (&Row, &Row)| Tup2(k.clone(), v.clone()))
            .output();

        (left_handle, right_handle, out)
    }

    /// Apply one window of weighted facts and step the circuit.
    ///
    /// `+1` is an inserted fact and `-1` a retracted one, which is all a reorg is at this layer
    /// (§5.2): the same rows fed back with their weights negated. There is no rollback interface
    /// because there is nothing to roll back.
    /// `into` is the derived relation, and it belongs to the caller.
    ///
    /// **It used to belong to this struct, and the view thread published a full clone of it after
    /// every batch.** That clone is `O(relation)` where an incremental update is `O(delta)`, and on
    /// a real nest it was the whole cost: `relation().clone()` of 309,548 groups measured 27,255 µs
    /// on the ThinkPad, against a fold whose actual output delta was two rows (#897). Freeing the
    /// previous snapshot cost about as much again. Handing the relation in means the deltas land
    /// where readers already look, and nothing is copied per block.
    pub fn apply(
        &mut self,
        left: &[(Row, ZWeight)],
        right: &[(Row, ZWeight)],
        into: &mut Relation,
    ) -> Result<()> {
        if let Some(why) = &self.faulted {
            return Err(anyhow!(
                "this entity faulted and does not accept further windows: {why}"
            ));
        }
        if !right.is_empty() && self.right.is_none() {
            return Err(anyhow!(
                "the plan has no right input, but {} right-hand facts were supplied",
                right.len()
            ));
        }

        let mut batch: Vec<Tup2<Row, ZWeight>> =
            left.iter().map(|(r, w)| Tup2(r.clone(), *w)).collect();
        self.left.append(&mut batch);

        if let Some(handle) = &self.right {
            let mut batch: Vec<Tup2<Row, ZWeight>> =
                right.iter().map(|(r, w)| Tup2(r.clone(), *w)).collect();
            handle.append(&mut batch);
        }

        // Past this point the rows are inside the circuit. If the transaction fails, the circuit's
        // state and this struct's are no longer the same story, so the entity stops here rather
        // than carrying on over a disagreement it cannot see.
        //
        // **A transaction is a sequence of steps, not one step.** DBSP splits a large input across
        // several internal steps of its own choosing, and each step writes to the output handle
        // independently: reading `out` once, after the transaction, sees only the final step's
        // output and silently discards every earlier one. `DBSPHandle::transaction()` and
        // `commit_transaction()` both hide that loop, so the loop is driven here instead and the
        // output drained after **every** step.
        //
        // The failure this prevents is not subtle in its effect and is invisible in its symptoms.
        // Measured against a real Horizon nest (2026-08-26): a `GROUP BY delegator` over 346,288
        // sealed rows with 309,549 distinct delegators produced exactly `309,549 mod 10,000` =
        // 9,549 groups when fed as one window - dbsp's internal step is 10,000 rows, so all but the
        // last step's worth of the relation was thrown away. Nothing faulted, nothing logged, and
        // the relation looked like a smaller but perfectly well-formed answer. The reproducer is
        // `one_large_window_folds_every_group`, which fails without this loop.
        if let Err(e) = self.step_to_commit(into) {
            let why = format!("the entity circuit faulted: {e}");
            self.faulted = Some(why.clone());
            return Err(anyhow!(why));
        }

        Ok(())
    }

    /// Run one transaction to completion, integrating the output after each of its steps.
    fn step_to_commit(&mut self, into: &mut Relation) -> Result<()> {
        self.handle.start_transaction()?;
        self.handle.start_commit_transaction()?;
        loop {
            let complete = self.handle.step()?;
            // The output is the change to the derived relation, so integrating it here is what
            // keeps `relation()` a point-read rather than a recomputation.
            self.out.consolidate().iter().for_each(
                |(Tup2(key, value), (), weight): (Tup2<Row, Row>, (), ZWeight)| {
                    if weight > 0 {
                        into.insert(key, value);
                    } else if into.get(&key) == Some(&value) {
                        into.remove(&key);
                    }
                },
            );
            if complete {
                self.len = into.len();
                return Ok(());
            }
        }
    }

    /// How many rows the derived relation held after the last completed transaction.
    ///
    /// Clippy wants an `is_empty` beside a `len`; there is deliberately none. This is a *recorded
    /// count*, not a container, and "the relation is empty" is a question for whoever owns it.
    ///
    /// The relation itself is the caller's - see [`Self::apply`]. A fault leaves it holding whatever
    /// the transaction had integrated before the failing step, which is *not* necessarily a
    /// consistent value; `fault()` being `Some` is what says so, and a faulted entity is terminal.
    #[allow(clippy::len_without_is_empty)]
    pub fn len(&self) -> usize {
        self.len
    }

    /// Bytes the circuit's operators hold in memory, from DBSP's profile (#1834). The profile sizes
    /// every trace deep, so this costs time in proportion to the state: call it rarely.
    pub fn state_bytes(&mut self) -> Result<u64> {
        let profile = self.handle.retrieve_profile()?;
        let bytes = profile.total_used_bytes().map_err(|item| {
            anyhow!("DBSP profile carried a non-byte used_memory_bytes: {item:?}")
        })?;
        Ok(bytes.bytes)
    }

    /// Why this entity stopped, if it has. `None` is a live entity.
    pub fn fault(&self) -> Option<&str> {
        self.faulted.as_deref()
    }

    /// The plan this circuit was built from.
    pub fn plan(&self) -> &Plan {
        &self.plan
    }
}

/// The joined row: left columns then right columns. The plan's column indices are into this
/// concatenation, which is the same convention [`Plan::evaluate`] uses and the reason the binder
/// must know both widths.
fn concat(left: &Row, right: &Row) -> Row {
    Row(left.0.iter().chain(right.0.iter()).cloned().collect())
}

fn eval_key(key: &[Expr], row: &Row) -> Row {
    Row(key.iter().map(|e| fault_on_err(e.eval(row))).collect())
}

/// Put a min/max result into the column its aggregate owns. The row always has that column - it came
/// from `finish_linear`, which emits one per aggregate - and an empty row means the outer join found
/// a group on the extreme side that the linear side does not have, which the lowering rules out.
fn graft(row: &Row, slot: usize, value: &Scalar) -> Row {
    assert!(
        slot < row.len(),
        "entity circuit fault: a group reached the min/max join without a linear row. Slot {slot} \
         of a {}-column row.",
        row.len()
    );
    let mut cols = row.0.clone();
    cols[slot] = value.clone();
    Row(cols)
}

fn linear_slots(aggs: &[Agg], row: &Row) -> LinAcc {
    let mut slots = vec![0i128; 1 + aggs.len() * 2];
    // Slot 0: this row exists. See `LinAcc` for why that is not the same statement as `count`'s.
    slots[0] = 1;
    for (i, agg) in aggs.iter().enumerate() {
        match agg {
            // COUNT(*) is the row count, so it reads slot 0 and needs none of its own.
            Agg::Count | Agg::Min(_) | Agg::Max(_) => {}
            Agg::Sum(e) | Agg::Avg(e) => {
                let v = fault_on_err(e.eval(row));
                if let Some(n) = v.as_int() {
                    slots[1 + 2 * i] = n;
                    slots[2 + 2 * i] = 1;
                } else if !v.is_null() {
                    panic!("entity circuit fault: sum/avg over a non-integer value {v:?}");
                }
            }
        }
    }
    LinAcc(slots)
}

/// The linear half of the output row. min/max columns are NULL here and filled by the outer join.
fn finish_linear(aggs: &[Agg], acc: &LinAcc) -> Row {
    Row(aggs
        .iter()
        .enumerate()
        .map(|(i, agg)| match agg {
            Agg::Count => Scalar::Int(acc.at(0)),
            // SUM over no non-NULL rows is NULL, not zero.
            Agg::Sum(_) => {
                if acc.at(2 + 2 * i) == 0 {
                    Scalar::Null
                } else {
                    Scalar::Int(acc.at(1 + 2 * i))
                }
            }
            // Integer division truncating toward zero, once, at the end - matching `Plan::finish`.
            // The entity's contract, not the SQL engine's: Burrmill's `avg` over integers is fractional.
            Agg::Avg(_) => {
                let n = acc.at(2 + 2 * i);
                if n == 0 {
                    Scalar::Null
                } else {
                    Scalar::Int(acc.at(1 + 2 * i) / n)
                }
            }
            Agg::Min(_) | Agg::Max(_) => Scalar::Null,
        })
        .collect())
}

/// Carry an expression fault out of an infallible operator. See the module docs: the panic is the
/// fault channel, and DBSP hands it back from `transaction()`.
fn fault_on_err<T>(r: Result<T>) -> T {
    r.unwrap_or_else(|e| panic!("entity circuit fault: {e:#}"))
}

/// Fold a set of facts through a fresh circuit, one transaction, and read the relation out. The
/// shape §8's invariant is stated in, and the shape most tests want.
pub fn evaluate_incrementally(plan: &Plan, left: &[Row], right: &[Row]) -> Result<Relation> {
    let mut circuit = EntityCircuit::build(plan.clone())?;
    let l: Vec<(Row, ZWeight)> = left.iter().map(|r| (r.clone(), 1)).collect();
    let r: Vec<(Row, ZWeight)> = right.iter().map(|r| (r.clone(), 1)).collect();
    let mut relation = Relation::new();
    circuit.apply(&l, &r, &mut relation)?;
    Ok(relation)
}

/// The relation as a sorted vector, for assertions that want an order.
pub fn sorted(relation: &Relation) -> Vec<(Row, Row)> {
    relation
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entity_expr::Cmp;
    use crate::entity_plan::{Join, Source};
    use proptest::prelude::*;

    /// A circuit and the relation it folds into, which is the pairing every caller keeps now that
    /// the relation belongs to the caller (see [`EntityCircuit::apply`]) - in production it is what
    /// `EntityView` holds under its published lock.
    struct Folding {
        circuit: EntityCircuit,
        relation: Relation,
    }

    impl Folding {
        fn build(plan: Plan) -> Result<Self> {
            Ok(Folding {
                circuit: EntityCircuit::build(plan)?,
                relation: Relation::new(),
            })
        }
        fn apply(&mut self, left: &[(Row, ZWeight)], right: &[(Row, ZWeight)]) -> Result<()> {
            self.circuit.apply(left, right, &mut self.relation)
        }
        fn relation(&self) -> &Relation {
            &self.relation
        }
        fn fault(&self) -> Option<&str> {
            self.circuit.fault()
        }
    }

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
    fn d(indexer: &str, delegator: &str, amount: i128) -> Row {
        Row(vec![s(indexer), s(delegator), Scalar::Int(amount)])
    }
    fn i_row(indexer: &str, active: bool) -> Row {
        Row(vec![s(indexer), Scalar::Bool(active)])
    }

    /// The same plan `entity_plan`'s tests use, so the two implementations are compared on the shape
    /// slice 0 hardcoded rather than on something invented to be easy.
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

    fn plus(rows: &[Row]) -> Vec<(Row, ZWeight)> {
        rows.iter().map(|r| (r.clone(), 1)).collect()
    }
    fn minus(rows: &[Row]) -> Vec<(Row, ZWeight)> {
        rows.iter().map(|r| (r.clone(), -1)).collect()
    }

    /// **One window, many groups.** A restart seed feeds a nest's whole sealed history as a single
    /// window, and that is the one shape no fixture here exercised: every other test feeds a
    /// handful of rows.
    ///
    /// Found against a real Horizon nest (2026-08-26): a `GROUP BY delegator` over 346,288 sealed
    /// rows with 309,549 distinct delegators returned 309,549 groups when fed in 1,000-row windows
    /// and 9,549 - three percent - when the whole history went in at once. Nothing faulted and
    /// nothing logged. The batch evaluator is the oracle, so it says which of the two is right.
    #[test]
    fn one_large_window_folds_every_group() {
        let plan = delegation_plan();
        // Across dbsp's internal step boundary, which was 10,000 rows when this was written. The
        // sizes either side of it are the point: 4,096 always passed, 16,384 returned 6,384.
        for n in [4usize, 4_096, 16_384, 50_000] {
            let left: Vec<Row> = (0..n)
                .map(|i| d("i1", &format!("d{i:06}"), 1 + (i as i128 % 7)))
                .collect();
            let right = [i_row("i1", true)];

            let got = evaluate_incrementally(&plan, &left, &right).unwrap();
            assert_eq!(
                got.len(),
                n,
                "one window of {n} distinct groups must fold to {n} groups"
            );
            assert_eq!(
                got,
                plan.evaluate(&left, &right).unwrap(),
                "§8: the circuit and the batch oracle are the same relation, at {n} rows"
            );
        }
    }

    /// A window that carries the same fact twice carries it twice. Two identical delegations are
    /// one `Row` at weight 2, and the aggregate must see both - a window is a Z-set, not a set.
    #[test]
    fn a_repeated_row_in_one_window_counts_twice() {
        let plan = delegation_plan();
        let left = [d("i1", "a", 5), d("i1", "a", 5)];
        let right = [i_row("i1", true)];

        let got = evaluate_incrementally(&plan, &left, &right).unwrap();

        assert_eq!(
            got.get(&Row(vec![s("i1"), s("a")])),
            Some(&Row(vec![Scalar::Int(10)])),
            "the same delegation twice sums to 10, not 5"
        );
        assert_eq!(got, plan.evaluate(&left, &right).unwrap());
    }

    /// **§4 step 4, end to end.** A plan goes in, a circuit comes out, and the answer is the one the
    /// batch evaluator gives. No hand-built operators and no fixed Rust struct anywhere in it.
    #[test]
    fn the_lodestar_shape_runs_as_a_circuit_built_from_its_plan() {
        let plan = delegation_plan();
        let left = [
            d("i1", "a", 7),
            d("i1", "a", 5),
            d("i1", "b", -3),
            d("i2", "c", 11),
        ];
        let right = [i_row("i1", true), i_row("i2", false)];

        let got = evaluate_incrementally(&plan, &left, &right).unwrap();

        assert_eq!(
            got.get(&Row(vec![s("i1"), s("a")])),
            Some(&Row(vec![Scalar::Int(12)])),
            "i1/a sums 7+5"
        );
        assert_eq!(got.len(), 1, "the filter drops i1/b and the join drops i2");
        assert_eq!(
            got,
            plan.evaluate(&left, &right).unwrap(),
            "§8: the circuit and the batch oracle are the same relation"
        );
    }

    /// **The reason `min` and `max` are not linear.** Retracting the current maximum does not tell a
    /// running accumulator what the next one is; only something that can look at the group again
    /// does. If this passes with `aggregate_linear`, the aggregate is wrong.
    #[test]
    fn retracting_the_extreme_value_moves_min_and_max_to_the_next_one() {
        let plan = Plan {
            left: src("amounts", &["who", "amount"]),
            left_filter: None,
            join: None,
            key: vec![col(0)],
            aggregates: vec![Agg::Min(col(1)), Agg::Max(col(1)), Agg::Count],
        };
        let rows = |v: &[i128]| -> Vec<Row> {
            v.iter()
                .map(|a| Row(vec![s("a"), Scalar::Int(*a)]))
                .collect()
        };

        let mut circuit = Folding::build(plan.clone()).unwrap();
        circuit.apply(&plus(&rows(&[5, 9, 7])), &[]).unwrap();
        assert_eq!(
            circuit.relation().get(&Row(vec![s("a")])),
            Some(&Row(vec![Scalar::Int(5), Scalar::Int(9), Scalar::Int(3)]))
        );

        // Retract both ends at once, which is the case a "keep the second largest around" shortcut
        // gets wrong.
        circuit.apply(&minus(&rows(&[9, 5])), &[]).unwrap();
        assert_eq!(
            circuit.relation().get(&Row(vec![s("a")])),
            Some(&Row(vec![Scalar::Int(7), Scalar::Int(7), Scalar::Int(1)])),
            "7 is now both ends"
        );
        assert_eq!(
            circuit.relation(),
            &plan.evaluate(&rows(&[7]), &[]).unwrap(),
            "§8 holds after a retraction, not just after an insert"
        );
    }

    /// A group whose min/max input is entirely NULL still exists - it has rows, they just have
    /// nothing to compare. An inner join between the linear and the extreme streams would delete it.
    #[test]
    fn a_group_whose_min_input_is_all_null_still_appears_with_a_null() {
        let plan = Plan {
            left: src("amounts", &["who", "amount"]),
            left_filter: None,
            join: None,
            key: vec![col(0)],
            aggregates: vec![Agg::Count, Agg::Min(col(1)), Agg::Sum(col(1))],
        };
        let left = [
            Row(vec![s("a"), Scalar::Null]),
            Row(vec![s("a"), Scalar::Null]),
            Row(vec![s("b"), Scalar::Int(4)]),
        ];

        let got = evaluate_incrementally(&plan, &left, &[]).unwrap();
        assert_eq!(
            got.get(&Row(vec![s("a")])),
            Some(&Row(vec![Scalar::Int(2), Scalar::Null, Scalar::Null])),
            "two rows, no minimum, and SUM over no non-NULL rows is NULL rather than 0"
        );
        assert_eq!(got, plan.evaluate(&left, &[]).unwrap());
    }

    /// The joined row is left columns then right columns, and a plan may index into either half.
    /// Every test above happens to read only left columns, which leaves the concatenation itself
    /// unexercised - and the binder (§4 step 2) exists precisely to produce plans that do index the
    /// right-hand side.
    #[test]
    fn a_key_and_an_aggregate_can_both_read_right_hand_columns() {
        let plan = Plan {
            left: src("stakes", &["indexer", "amount"]),
            left_filter: None,
            join: Some(Join {
                right: src("indexers", &["indexer", "region", "fee"]),
                right_filter: None,
                // Column 2 of the joined row is the right side's `indexer`, 3 its `region`, 4 its
                // `fee` - the left row being two columns wide.
                on: (0, 0),
            }),
            key: vec![col(3)],
            aggregates: vec![Agg::Sum(col(1)), Agg::Max(col(4))],
        };
        let left = [
            Row(vec![s("i1"), Scalar::Int(5)]),
            Row(vec![s("i2"), Scalar::Int(7)]),
            Row(vec![s("i3"), Scalar::Int(2)]),
        ];
        let right = [
            Row(vec![s("i1"), s("eu"), Scalar::Int(10)]),
            Row(vec![s("i2"), s("eu"), Scalar::Int(30)]),
            Row(vec![s("i3"), s("us"), Scalar::Int(20)]),
        ];

        let got = evaluate_incrementally(&plan, &left, &right).unwrap();
        assert_eq!(
            got.get(&Row(vec![s("eu")])),
            Some(&Row(vec![Scalar::Int(12), Scalar::Int(30)])),
            "grouped by the right side's region, summing the left side's amount"
        );
        assert_eq!(
            got.get(&Row(vec![s("us")])),
            Some(&Row(vec![Scalar::Int(2), Scalar::Int(20)]))
        );
        assert_eq!(got, plan.evaluate(&left, &right).unwrap());
    }

    /// Both sides' join columns are at index 0 in every other test here, which is exactly the case
    /// that cannot tell `(left, right)` from `(right, left)`. A real nest's decoded tables put the
    /// key wherever the ABI put it.
    #[test]
    fn the_join_columns_are_asymmetric_and_each_side_uses_its_own() {
        let plan = Plan {
            left: src("stakes", &["epoch", "indexer", "amount"]),
            left_filter: None,
            join: Some(Join {
                right: src("indexers", &["indexer", "active"]),
                right_filter: Some(col(1)),
                // Left column 1 against right column 0. Swapped, this joins epochs to indexer names
                // and matches nothing.
                on: (1, 0),
            }),
            key: vec![col(1)],
            aggregates: vec![Agg::Sum(col(2))],
        };
        let left = [
            Row(vec![Scalar::Int(1), s("i1"), Scalar::Int(5)]),
            Row(vec![Scalar::Int(2), s("i1"), Scalar::Int(6)]),
            Row(vec![Scalar::Int(1), s("i2"), Scalar::Int(9)]),
        ];
        let right = [i_row("i1", true), i_row("i2", false)];

        let got = evaluate_incrementally(&plan, &left, &right).unwrap();
        assert_eq!(
            got.get(&Row(vec![s("i1")])),
            Some(&Row(vec![Scalar::Int(11)]))
        );
        assert_eq!(got.len(), 1, "i2 is inactive: {got:?}");
        assert_eq!(got, plan.evaluate(&left, &right).unwrap());
    }

    /// The zero-accumulator trap, as its own test rather than as a side effect of another.
    ///
    /// DBSP drops a group whose linear accumulator is zero. An entity of nothing but `min`/`max`
    /// contributes nothing linear at all, so before slot 0 existed this plan lost every group it
    /// had - and lost it *after* the aggregate had computed the right answer, which is the shape of
    /// bug that reads as "the entity is empty" rather than as "the entity is wrong".
    #[test]
    fn a_plan_of_nothing_but_min_and_max_still_has_groups() {
        let plan = Plan {
            left: src("amounts", &["who", "amount"]),
            left_filter: None,
            join: None,
            key: vec![col(0)],
            aggregates: vec![Agg::Min(col(1)), Agg::Max(col(1))],
        };
        let left = [
            Row(vec![s("a"), Scalar::Int(4)]),
            Row(vec![s("a"), Scalar::Int(9)]),
        ];

        let got = evaluate_incrementally(&plan, &left, &[]).unwrap();
        assert_eq!(
            got.get(&Row(vec![s("a")])),
            Some(&Row(vec![Scalar::Int(4), Scalar::Int(9)])),
            "no linear aggregate does not mean no group"
        );
        assert_eq!(got, plan.evaluate(&left, &[]).unwrap());
    }

    /// SQL's MIN and MAX ignore NULLs. `Scalar::Null` sorts *first* by design, so a MIN that does
    /// not exclude them reads NULL for every group that has one - the failure that looks like a
    /// missing value rather than like a bug.
    #[test]
    fn min_and_max_ignore_nulls_rather_than_being_dragged_to_one() {
        let plan = Plan {
            left: src("amounts", &["who", "amount"]),
            left_filter: None,
            join: None,
            key: vec![col(0)],
            aggregates: vec![Agg::Min(col(1)), Agg::Max(col(1))],
        };
        let left = [
            Row(vec![s("a"), Scalar::Null]),
            Row(vec![s("a"), Scalar::Int(4)]),
            Row(vec![s("a"), Scalar::Int(9)]),
        ];

        let got = evaluate_incrementally(&plan, &left, &[]).unwrap();
        assert_eq!(
            got.get(&Row(vec![s("a")])),
            Some(&Row(vec![Scalar::Int(4), Scalar::Int(9)])),
            "the NULL is ignored at both ends, not treated as the smallest value"
        );
        assert_eq!(got, plan.evaluate(&left, &[]).unwrap());
    }

    /// §3.3: `avg` is sum and count carried to the end, so it is exact and reversible. A running
    /// mean would drift, and would give a different answer for the same set applied in two windows.
    #[test]
    fn avg_is_carried_as_sum_and_count_and_survives_being_split_across_windows() {
        let plan = Plan {
            left: src("amounts", &["who", "amount"]),
            left_filter: None,
            join: None,
            key: vec![col(0)],
            aggregates: vec![Agg::Avg(col(1))],
        };
        let row = |a: i128| Row(vec![s("a"), Scalar::Int(a)]);

        let mut split = Folding::build(plan.clone()).unwrap();
        split.apply(&plus(&[row(1), row(2)]), &[]).unwrap();
        split.apply(&plus(&[row(10)]), &[]).unwrap();

        assert_eq!(
            split.relation().get(&Row(vec![s("a")])),
            Some(&Row(vec![Scalar::Int(4)])),
            "13/3 truncates to 4, computed once at the end"
        );
        assert_eq!(
            split.relation(),
            &plan.evaluate(&[row(1), row(2), row(10)], &[]).unwrap(),
            "two windows and one batch agree"
        );
    }

    /// §3.3.1. The contract is on the entity, not on whatever the engine happens to do: an aggregate
    /// that cannot be represented faults the circuit rather than wrapping round to a negative total.
    #[test]
    fn an_aggregate_that_leaves_i128_faults_the_circuit_rather_than_wrapping() {
        let plan = Plan {
            left: src("amounts", &["who", "amount"]),
            left_filter: None,
            join: None,
            key: vec![col(0)],
            aggregates: vec![Agg::Sum(col(1))],
        };
        let huge = Row(vec![s("a"), Scalar::Int(i128::MAX)]);

        let mut circuit = Folding::build(plan).unwrap();
        let err = circuit
            .apply(&plus(&[huge.clone(), huge]), &[])
            .expect_err("two i128::MAX rows cannot sum");

        let msg = format!("{err:#}");
        assert!(
            msg.contains("faulted"),
            "the fault must reach the caller as an error, not a wrapped number: {msg}"
        );
    }

    /// **A fault is terminal, and the last good answer survives it.**
    ///
    /// #864 names the shape this avoids: the spike appends its rows, the transaction errors, and the
    /// rows are then inside the circuit while the bookkeeping outside says they are not. The next
    /// window lands on top of that disagreement and every answer after it is a guess. Refusing
    /// further windows is what makes the entity *stopped* rather than *wrong*.
    #[test]
    fn a_faulted_entity_refuses_later_windows_and_keeps_its_last_good_answer() {
        let plan = Plan {
            left: src("amounts", &["who", "amount"]),
            left_filter: None,
            join: None,
            key: vec![col(0)],
            aggregates: vec![Agg::Sum(col(1))],
        };
        let row = |a: i128| Row(vec![s("a"), Scalar::Int(a)]);

        let mut circuit = Folding::build(plan).unwrap();
        circuit.apply(&plus(&[row(3), row(4)]), &[]).unwrap();
        let good = circuit.relation().clone();
        assert_eq!(
            good.get(&Row(vec![s("a")])),
            Some(&Row(vec![Scalar::Int(7)]))
        );
        assert!(circuit.fault().is_none(), "still live");

        circuit
            .apply(&plus(&[row(i128::MAX)]), &[])
            .expect_err("that total cannot be represented");

        assert!(
            circuit.fault().is_some_and(|w| w.contains("faulted")),
            "the entity must know it stopped: {:?}",
            circuit.fault()
        );
        assert_eq!(
            circuit.relation(),
            &good,
            "the relation keeps the value it had when it was last consistent"
        );

        let err = circuit
            .apply(&plus(&[row(1)]), &[])
            .expect_err("a stopped entity accepts nothing further");
        assert!(
            format!("{err:#}").contains("does not accept further windows"),
            "{err:#}"
        );
        assert_eq!(
            circuit.relation(),
            &good,
            "and a refused window does not move it either"
        );
    }

    /// A NULL join key matches nothing, in SQL and here. Without the guard every NULL-keyed row on
    /// one side would join every NULL-keyed row on the other - the silent fan-out that turns a
    /// missing ABI field into a plausible wrong number.
    #[test]
    fn a_null_join_key_matches_nothing() {
        let plan = Plan {
            left: src("l", &["k", "amount"]),
            left_filter: None,
            join: Some(Join {
                right: src("r", &["k"]),
                right_filter: None,
                on: (0, 0),
            }),
            key: vec![col(0)],
            aggregates: vec![Agg::Count],
        };
        let left = [
            Row(vec![Scalar::Null, Scalar::Int(1)]),
            Row(vec![s("k"), Scalar::Int(1)]),
        ];
        let right = [Row(vec![Scalar::Null]), Row(vec![s("k")])];

        let got = evaluate_incrementally(&plan, &left, &right).unwrap();
        assert_eq!(got.len(), 1, "only the non-NULL key joins: {got:?}");
        assert_eq!(got, plan.evaluate(&left, &right).unwrap());
    }

    /// Feeding right-hand facts to a plan that has no right input is a wiring bug, and one that
    /// would otherwise be invisible: the facts would simply never appear in any answer.
    #[test]
    fn right_hand_facts_without_a_join_are_refused_rather_than_dropped() {
        let plan = Plan {
            left: src("l", &["k"]),
            left_filter: None,
            join: None,
            key: vec![col(0)],
            aggregates: vec![Agg::Count],
        };
        let mut circuit = Folding::build(plan).unwrap();
        let err = circuit
            .apply(&[], &plus(&[Row(vec![s("x")])]))
            .expect_err("there is nowhere for these to go");
        assert!(format!("{err:#}").contains("no right input"), "{err:#}");
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]

        /// **RFC-0041 §8, as a property.** Insert a window, retract part of it, and the circuit's
        /// relation must equal the batch recomputation over exactly the facts that survive. This is
        /// the invariant that let the batch evaluator replace the DuckDB oracle, and the one a
        /// reorg exercises for real (§5.2).
        #[test]
        fn incremental_equals_batch_recomputation_under_retraction(
            facts in prop::collection::vec(
                (0usize..3, 0usize..3, -20i128..20i128, any::<bool>()),
                0..12,
            ),
            actives in prop::collection::vec(any::<bool>(), 3),
        ) {
            let names = ["i0", "i1", "i2"];
            let delegators = ["a", "b", "c"];
            let plan = delegation_plan();

            let right: Vec<Row> = actives
                .iter()
                .enumerate()
                .map(|(i, on)| i_row(names[i], *on))
                .collect();
            let inserted: Vec<Row> = facts
                .iter()
                .map(|(i, dg, amount, _)| d(names[*i], delegators[*dg], *amount))
                .collect();
            // The second window retracts the rows the generator flagged. What is left is what the
            // batch evaluator must be given.
            let retracted: Vec<Row> = facts
                .iter()
                .zip(&inserted)
                .filter(|((_, _, _, drop), _)| *drop)
                .map(|(_, row)| row.clone())
                .collect();
            let survivors: Vec<Row> = facts
                .iter()
                .zip(&inserted)
                .filter(|((_, _, _, drop), _)| !*drop)
                .map(|(_, row)| row.clone())
                .collect();

            let mut circuit = Folding::build(plan.clone()).unwrap();
            circuit.apply(&plus(&inserted), &plus(&right)).unwrap();
            circuit.apply(&minus(&retracted), &[]).unwrap();

            prop_assert_eq!(
                circuit.relation(),
                &plan.evaluate(&survivors, &right).unwrap(),
                "incremental result must equal batch recomputation over the surviving facts"
            );
        }
    }
}
