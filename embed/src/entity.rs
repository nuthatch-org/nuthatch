//! RFC-0075 S0, the remainder (#2476): nuthatch's own entity circuit stepped on wasm32.
//!
//! `entity_circuit.rs`, `entity_plan.rs`, `entity_expr.rs` and `entity_row.rs` are the binary's
//! files, compiled into this crate by path (see `lib.rs`), so the plan lowered here is the one a
//! nest lowers: `map_index` → `join_index` → `aggregate_linear`, with `aggregate(Min)`,
//! `aggregate(Max)` and the `outer_join_default` graft, over the rkyv-archived `Row`.
//!
//! The harness feeds `windows` windows of `rows` transfers through a plan that joins them to a
//! labels table, keyed by recipient, with every aggregate kind the plan admits; every fourth window
//! retracts half as many earlier transfers, and every fiftieth retracts and re-inserts a label with
//! a new tier, so both inputs see weight `-1`. The oracle is `Plan::evaluate`, the batch fold the
//! binary's own tests hold the circuit to (RFC-0041 §8).

use crate::entity_circuit::EntityCircuit;
use crate::entity_expr::{Cmp, Expr};
use crate::entity_plan::{Agg, Join, Plan, Relation, Source};
use crate::entity_row::{Row, Scalar};
use std::hash::Hasher;

/// How many distinct recipients, and so groups, the generator draws from.
const RECIPIENTS: u64 = 512;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Report {
    pub windows: u64,
    pub left_inserted: u64,
    pub left_retracted: u64,
    pub right_inserted: u64,
    pub right_retracted: u64,
    pub groups: usize,
    /// SipHash over the relation rendered as text, so a 32-bit and a 64-bit build hash the same bytes.
    pub digest: u64,
    pub first_rows: Vec<String>,
    /// Whether the circuit's relation equalled `Plan::evaluate` at every checkpoint.
    pub oracle_agrees: bool,
    /// Bytes dbsp's profile says the circuit holds, if the profiler answered.
    pub state_bytes: Option<u64>,
}

/// transfers(from, to, value, block) ⋈ labels(address, tier) on to = address, grouped by
/// (to, tier): count, sum(value), avg(value), min(value), max(value). Transfers of value 0 are
/// filtered out on the left, labels of tier 0 on the right. A label's tier change therefore moves
/// its recipient's whole group.
///
/// One aggregate of each kind, because `Plan::evaluate`'s accumulator (`entity_plan.rs`, `Acc`)
/// keeps one `sum`, one `min` and one `max` per group: a plan with two `Sum`s folds both into the
/// same field there, while the circuit keeps them apart (`LinAcc` has a slot pair per aggregate).
/// Found here on 2026-10-28 with `Sum(value), Sum(tier)`; the circuit was right and the oracle
/// wrong, and no binary test declares two aggregates of one kind.
pub fn plan() -> Plan {
    let value = || Expr::Column(2);
    Plan {
        left: Source {
            table: "transfers".into(),
            columns: vec!["from".into(), "to".into(), "value".into(), "block".into()],
        },
        left_filter: Some(Expr::Compare(
            Cmp::Gt,
            Box::new(value()),
            Box::new(Expr::Literal(Scalar::Int(0))),
        )),
        join: Some(Join {
            right: Source {
                table: "labels".into(),
                columns: vec!["address".into(), "tier".into()],
            },
            right_filter: Some(Expr::Compare(
                Cmp::Ne,
                Box::new(Expr::Column(1)),
                Box::new(Expr::Literal(Scalar::Int(0))),
            )),
            on: (1, 0),
        }),
        key: vec![Expr::Column(1), Expr::Column(5)],
        aggregates: vec![
            Agg::Count,
            Agg::Sum(value()),
            Agg::Avg(value()),
            Agg::Min(value()),
            Agg::Max(value()),
        ],
    }
}

struct Gen(u64);

impl Gen {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    /// Reduced in u64, so a 32-bit target draws the same index as a 64-bit one.
    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }
}

fn address(n: u64) -> Scalar {
    Scalar::Str(format!("0x{:04x}", n % RECIPIENTS))
}

fn transfer(g: &mut Gen, block: u64) -> Row {
    let from = address(g.next());
    let to = address(g.next());
    // One in sixteen is a zero-value transfer the left filter must drop; one in sixteen is a NULL
    // value SUM and MIN must ignore.
    let value = match g.next() % 16 {
        0 => Scalar::Int(0),
        1 => Scalar::Null,
        _ => Scalar::Int((g.next() % 1_000_000) as i128),
    };
    Row(vec![from, to, value, Scalar::Int(block as i128)])
}

fn label(n: u64, tier: i128) -> Row {
    Row(vec![address(n), Scalar::Int(tier)])
}

fn render(relation: &Relation) -> Vec<String> {
    relation
        .iter()
        .map(|(k, v)| {
            let cols = |r: &Row| {
                r.0.iter()
                    .map(|s| s.to_string())
                    .collect::<Vec<_>>()
                    .join(",")
            };
            format!("{} => {}", cols(k), cols(v))
        })
        .collect()
}

fn digest(lines: &[String]) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    for l in lines {
        h.write(l.as_bytes());
        h.write_u8(b'\n');
    }
    h.finish()
}

/// Step `windows` windows of `rows` transfers each, checking the relation against the oracle every
/// `check_every` windows and at the end. `progress` is called after every hundredth window with
/// the window index, so a host can sample memory between windows.
pub fn run_entity(
    windows: u64,
    rows: u64,
    check_every: u64,
    mut progress: impl FnMut(u64),
) -> Result<Report, String> {
    let plan = plan();
    let mut circuit = EntityCircuit::build(plan.clone()).map_err(|e| format!("build: {e:#}"))?;
    let mut relation = Relation::new();
    let mut g = Gen(0x9e3779b97f4a7c15);
    let mut live_left: Vec<Row> = Vec::new();
    let mut live_right: Vec<Row> = Vec::new();
    let mut report = Report {
        windows,
        left_inserted: 0,
        left_retracted: 0,
        right_inserted: 0,
        right_retracted: 0,
        groups: 0,
        digest: 0,
        first_rows: Vec::new(),
        oracle_agrees: true,
        state_bytes: None,
    };

    for w in 0..windows {
        let mut left = Vec::with_capacity(rows as usize);
        let mut right = Vec::new();
        if w == 0 {
            // Every label once, one in sixty-four at tier 0 so the right filter has work.
            for n in 0..RECIPIENTS {
                let tier = if n % 64 == 0 { 0 } else { 1 + (n % 5) as i128 };
                let row = label(n, tier);
                live_right.push(row.clone());
                right.push((row, 1));
                report.right_inserted += 1;
            }
        } else if w % 50 == 0 {
            // A label changes tier: retract and re-insert, so the join sees the right side move.
            let i = g.below(live_right.len());
            let old = live_right.swap_remove(i);
            let tier = old.get(1).as_int().unwrap_or(0) % 5 + 1;
            let new = Row(vec![old.get(0).clone(), Scalar::Int(tier)]);
            live_right.push(new.clone());
            right.push((old, -1));
            right.push((new, 1));
            report.right_retracted += 1;
            report.right_inserted += 1;
        }
        if w % 4 == 3 {
            for _ in 0..rows / 2 {
                if live_left.is_empty() {
                    break;
                }
                let i = g.below(live_left.len());
                left.push((live_left.swap_remove(i), -1));
                report.left_retracted += 1;
            }
        } else {
            for _ in 0..rows {
                let row = transfer(&mut g, w);
                live_left.push(row.clone());
                left.push((row, 1));
                report.left_inserted += 1;
            }
        }
        circuit
            .apply(&left, &right, &mut relation)
            .map_err(|e| format!("window {w}: {e:#}"))?;

        if (w + 1) % check_every == 0 || w + 1 == windows {
            let want = plan
                .evaluate(&live_left, &live_right)
                .map_err(|e| format!("oracle at window {w}: {e:#}"))?;
            if want != relation {
                report.oracle_agrees = false;
                let mut diff = Vec::new();
                for (k, v) in &want {
                    if relation.get(k) != Some(v) {
                        diff.push(format!("{k:?}: want {v:?}, got {:?}", relation.get(k)));
                    }
                }
                for k in relation.keys() {
                    if !want.contains_key(k) {
                        diff.push(format!("{k:?}: extra {:?}", relation.get(k)));
                    }
                }
                return Err(format!(
                    "window {w}: the circuit's relation ({} groups) differs from Plan::evaluate ({} groups) in {} groups; first: {}",
                    relation.len(),
                    want.len(),
                    diff.len(),
                    diff.iter().take(3).cloned().collect::<Vec<_>>().join("; ")
                ));
            }
        }
        if (w + 1) % 100 == 0 {
            progress(w + 1);
        }
    }

    let lines = render(&relation);
    report.groups = relation.len();
    report.digest = digest(&lines);
    report.first_rows = lines.iter().take(3).cloned().collect();
    report.state_bytes = circuit.state_bytes().ok();
    Ok(report)
}
