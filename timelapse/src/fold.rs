//! The state a replay carries (§4.1): a value per live key and an ordered index over them.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeSet, HashMap};

use alloy_primitives::I256;
use anyhow::{Context, Result};

use crate::spec::FoldKind;

/// A key as the query returned it. A lower-case `0x` address is held as its 20 bytes and every other
/// key as text; either way it orders, and is written back, exactly as its text.
#[derive(Clone, PartialEq, Eq, Hash)]
pub enum Key {
    Address([u8; 20]),
    Text(Box<str>),
}

impl Key {
    pub fn parse(text: &str) -> Key {
        match address(text) {
            Some(a) => Key::Address(a),
            None => Key::Text(text.into()),
        }
    }

    pub fn text(&self) -> String {
        match self {
            Key::Address(a) => format!("0x{}", hex::encode(a)),
            Key::Text(t) => t.to_string(),
        }
    }
}

/// Only the lower-case spelling, so that the text written back is the text read.
fn address(text: &str) -> Option<[u8; 20]> {
    let hex = text.strip_prefix("0x")?;
    if hex.len() != 40 || !hex.bytes().all(|b| matches!(b, b'0'..=b'9' | b'a'..=b'f')) {
        return None;
    }
    let mut out = [0u8; 20];
    hex::decode_to_slice(hex, &mut out).ok()?;
    Some(out)
}

impl Ord for Key {
    fn cmp(&self, other: &Self) -> Ordering {
        match (self, other) {
            // Fixed-width lower-case hex sorts as its bytes do.
            (Key::Address(a), Key::Address(b)) => a.cmp(b),
            (Key::Text(a), Key::Text(b)) => a.as_bytes().cmp(b.as_bytes()),
            _ => self.text().as_bytes().cmp(other.text().as_bytes()),
        }
    }
}

impl PartialOrd for Key {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// A key leaves the state when its value reaches zero, so the state is bounded by live keys, not by
/// keys ever seen.
pub struct Fold {
    kind: FoldKind,
    value: HashMap<Key, I256>,
    rank: BTreeSet<(Reverse<I256>, Key)>,
    total: I256,
    pub events: u64,
    pub peak_keys: usize,
}

impl Fold {
    pub fn new(kind: FoldKind) -> Fold {
        Fold {
            kind,
            value: HashMap::new(),
            rank: BTreeSet::new(),
            total: I256::ZERO,
            events: 0,
            peak_keys: 0,
        }
    }

    pub fn apply(&mut self, key: Key, amount: I256) -> Result<()> {
        self.events += 1;
        let old = self.value.get(&key).copied().unwrap_or(I256::ZERO);
        let new = match self.kind {
            FoldKind::Sum => old
                .checked_add(amount)
                .with_context(|| format!("key {} overflows a signed 256-bit sum", key.text()))?,
            FoldKind::Last => amount,
        };
        if new == old {
            return Ok(());
        }
        let delta = match self.kind {
            FoldKind::Sum => Some(amount),
            FoldKind::Last => new.checked_sub(old),
        };
        self.total = delta
            .and_then(|d| self.total.checked_add(d))
            .context("the total overflows a signed 256-bit sum")?;
        if !old.is_zero() {
            self.rank.remove(&(Reverse(old), key.clone()));
        }
        if new.is_zero() {
            self.value.remove(&key);
        } else {
            self.value.insert(key.clone(), new);
            self.rank.insert((Reverse(new), key));
            self.peak_keys = self.peak_keys.max(self.value.len());
        }
        Ok(())
    }

    pub fn live(&self) -> usize {
        self.value.len()
    }

    pub fn total(&self) -> I256 {
        self.total
    }

    /// The `n` largest values, ties broken by key ascending as bytes.
    pub fn top(&self, n: usize) -> Vec<(String, String)> {
        self.rank
            .iter()
            .take(n)
            .map(|(Reverse(v), k)| (k.text(), v.to_string()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn i(n: i64) -> I256 {
        I256::try_from(n).unwrap()
    }

    #[test]
    fn an_address_round_trips_and_other_text_is_kept() {
        let a = "0x9623063377ad1b27544c965ccd7342f7ea7e88c7";
        assert!(matches!(Key::parse(a), Key::Address(_)));
        assert_eq!(Key::parse(a).text(), a);
        let mixed = "0x9623063377AD1b27544c965ccd7342f7ea7e88c7";
        assert!(matches!(Key::parse(mixed), Key::Text(_)));
        assert_eq!(Key::parse(mixed).text(), mixed);
    }

    #[test]
    fn keys_order_as_their_text_across_both_forms() {
        let texts = [
            "0x00000000000000000000000000000000000000ff",
            "0x0a",
            "0xab00000000000000000000000000000000000000",
            "0xB",
            "abc",
        ];
        let mut keys: Vec<Key> = texts.iter().rev().map(|t| Key::parse(t)).collect();
        keys.sort();
        let mut want: Vec<&str> = texts.to_vec();
        want.sort_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        assert_eq!(keys.iter().map(Key::text).collect::<Vec<_>>(), want);
    }

    #[test]
    fn a_sum_that_returns_to_zero_leaves_the_state() {
        let mut f = Fold::new(FoldKind::Sum);
        f.apply(Key::parse("a"), i(5)).unwrap();
        f.apply(Key::parse("b"), i(5)).unwrap();
        f.apply(Key::parse("a"), i(-5)).unwrap();
        assert_eq!((f.live(), f.total()), (1, i(5)));
        assert_eq!(f.top(10), vec![("b".to_string(), "5".to_string())]);
    }

    #[test]
    fn last_keeps_the_latest_value_and_ties_break_by_key() {
        let mut f = Fold::new(FoldKind::Last);
        f.apply(Key::parse("b"), i(7)).unwrap();
        f.apply(Key::parse("a"), i(3)).unwrap();
        f.apply(Key::parse("a"), i(7)).unwrap();
        assert_eq!(f.total(), i(14));
        assert_eq!(
            f.top(10),
            vec![
                ("a".to_string(), "7".to_string()),
                ("b".to_string(), "7".to_string())
            ]
        );
        f.apply(Key::parse("b"), i(0)).unwrap();
        assert_eq!((f.live(), f.total()), (1, i(7)));
    }
}
