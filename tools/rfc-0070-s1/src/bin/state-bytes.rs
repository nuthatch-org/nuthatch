//! Usage: state-bytes <keys>
//!
//! Heap bytes per live key held by the fold, counted by the allocator rather than read off RSS: the
//! S1 fold with 20-byte address keys, and S0's form (two `Rc<str>` of the 42-character text) beside
//! it. Every key stays live.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cmp::Reverse;
use std::collections::{BTreeSet, HashMap};
use std::rc::Rc;
use std::sync::atomic::{AtomicUsize, Ordering};

use nuthatch_timelapse::alloy_primitives::I256;
use nuthatch_timelapse::{Fold, FoldKind, Key};

struct Counting;

static HELD: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, l: Layout) -> *mut u8 {
        HELD.fetch_add(l.size(), Ordering::Relaxed);
        System.alloc(l)
    }
    unsafe fn dealloc(&self, p: *mut u8, l: Layout) {
        HELD.fetch_sub(l.size(), Ordering::Relaxed);
        System.dealloc(p, l)
    }
}

#[global_allocator]
static A: Counting = Counting;

/// Spread, distinct and positive, so the index fills as real balances fill it rather than as a run.
fn value(i: usize) -> I256 {
    I256::try_from(((i as u64).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 2) as i64 + 1).unwrap()
}

fn address(i: u64) -> String {
    format!("0x{:040x}", (i as u128).wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

fn main() {
    let n: u64 = std::env::args()
        .nth(1)
        .and_then(|a| a.parse().ok())
        .unwrap_or(300_000);
    let texts: Vec<String> = (0..n).map(address).collect();

    let before = HELD.load(Ordering::Relaxed);
    let mut fold = Fold::new(FoldKind::Sum);
    for (i, t) in texts.iter().enumerate() {
        fold.apply(Key::parse(t), value(i)).unwrap();
    }
    let s1 = HELD.load(Ordering::Relaxed) - before;
    drop(fold);

    let before = HELD.load(Ordering::Relaxed);
    let mut held: HashMap<Rc<str>, I256> = HashMap::new();
    let mut rank: BTreeSet<(Reverse<I256>, Rc<str>)> = BTreeSet::new();
    for (i, t) in texts.iter().enumerate() {
        let k: Rc<str> = Rc::from(t.as_str());
        let v = value(i);
        held.insert(Rc::clone(&k), v);
        rank.insert((Reverse(v), k));
    }
    let s0 = HELD.load(Ordering::Relaxed) - before;
    println!(
        "{{\"keys\":{n},\"s1_bytes_per_key\":{:.1},\"s0_form_bytes_per_key\":{:.1}}}",
        s1 as f64 / n as f64,
        s0 as f64 / n as f64
    );
    drop((held, rank));
}
