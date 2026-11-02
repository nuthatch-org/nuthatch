//! RFC-0080 S0: the value-term shape and its per-delta filter, as a spike. Nothing here is served.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, HashMap};

/// One table's rows, every column in `terms` equal to one of its listed values (RFC-0080 §5.1).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Shape {
    pub table: String,
    #[serde(rename = "where", default)]
    pub terms: BTreeMap<String, BTreeSet<String>>,
}

impl Shape {
    /// Identity: the canonical JSON, hashed. The maps are ordered, so the bytes are canonical.
    pub fn hash(&self) -> String {
        let canonical = serde_json::to_vec(self).expect("a shape serialises");
        format!("{:x}", Sha256::digest(canonical))
    }

    pub fn matches(&self, row: &Value) -> bool {
        self.terms.iter().all(|(column, values)| {
            row.get(column)
                .is_some_and(|v| values.contains(text(v).as_ref()))
        })
    }
}

/// A column's value as the text a term names it by.
fn text(v: &Value) -> Cow<'_, str> {
    match v {
        Value::String(s) => Cow::Borrowed(s),
        other => Cow::Owned(other.to_string()),
    }
}

/// What reading one column off a row's JSON text came to.
enum Read<'a> {
    Absent,
    Scalar(&'a str),
    /// Not a plain string or integer, or a string with an escape: only a parse reads it.
    Complex,
}

/// The table a stored row is in, from its `table` field; `None` for a row with none, or one a
/// plain read cannot take it from, which no shape selects.
pub fn table_of(row: &str) -> Option<&str> {
    match scalar_in(row, "\"table\":") {
        Read::Scalar(table) => Some(table),
        _ => None,
    }
}

/// `needle` is the column's key as the writer spells it, `"name":`. Rows come from `serde_json` and
/// plain column names are never escaped there, so a key found is the row's own.
fn scalar_in<'a>(row: &'a str, needle: &str) -> Read<'a> {
    let Some(at) = row.find(needle) else {
        return Read::Absent;
    };
    let rest = &row[at + needle.len()..];
    match rest.as_bytes().first() {
        Some(b'"') => {
            let body = &rest[1..];
            match body.find(['"', '\\']) {
                Some(end) if body.as_bytes()[end] == b'"' => Read::Scalar(&body[..end]),
                _ => Read::Complex,
            }
        }
        Some(_) => {
            let end = rest.find([',', '}']).unwrap_or(rest.len());
            let word = &rest[..end];
            let plain = word.bytes().all(|b| b.is_ascii_digit() || b == b'-')
                || matches!(word, "true" | "false" | "null");
            if plain {
                Read::Scalar(word)
            } else {
                Read::Complex
            }
        }
        None => Read::Complex,
    }
}

struct Column {
    needle: String,
    by_value: HashMap<String, Candidates>,
}

/// The shapes that name one value of one column.
#[derive(Default)]
struct Candidates {
    /// Shapes whose only term is this column's: the value is the whole answer. Ascending.
    solo: Vec<usize>,
    /// Shapes with other terms, which must be checked as well. Ascending.
    multi: Vec<usize>,
}

/// Shapes indexed by their value terms: a row probes once per shapeable column, and each candidate
/// is then checked in full.
#[derive(Default)]
pub struct ShapeIndex {
    shapes: Vec<Shape>,
    by_column: HashMap<String, Column>,
    /// Shapes with no terms: every row is in them.
    open: Vec<usize>,
}

impl ShapeIndex {
    pub fn insert(&mut self, shape: Shape) -> usize {
        let id = self.shapes.len();
        if shape.terms.is_empty() {
            self.open.push(id);
        }
        for (column, values) in &shape.terms {
            let indexed = self
                .by_column
                .entry(column.clone())
                .or_insert_with(|| Column {
                    needle: format!("{}:", Value::String(column.clone())),
                    by_value: HashMap::new(),
                });
            for value in values {
                let found = indexed.by_value.entry(value.clone()).or_default();
                if shape.terms.len() == 1 {
                    found.solo.push(id);
                } else {
                    found.multi.push(id);
                }
            }
        }
        self.shapes.push(shape);
        id
    }

    pub fn shape(&self, id: usize) -> &Shape {
        &self.shapes[id]
    }

    /// The shapes `row` is in, in id order.
    pub fn matching(&self, row: &Value) -> Vec<usize> {
        let mut out = self.open.clone();
        for (column, indexed) in &self.by_column {
            let Some(v) = row.get(column) else { continue };
            if let Some(found) = indexed.by_value.get(text(v).as_ref()) {
                out.extend(
                    found
                        .solo
                        .iter()
                        .chain(&found.multi)
                        .copied()
                        .filter(|&id| self.shapes[id].matches(row)),
                );
            }
        }
        out.sort_unstable();
        out.dedup();
        out
    }

    /// [`ShapeIndex::matching`] for a row's JSON text, into `out`. Only the indexed columns are read
    /// off it, with no parse of the rest; a row that cannot be read that way is parsed whole.
    pub fn matching_text(&self, row: &str, out: &mut Vec<usize>) {
        if self.read_text(row, out).is_none() {
            out.clear();
            if let Ok(parsed) = serde_json::from_str::<Value>(row) {
                out.extend(self.matching(&parsed));
            }
        }
    }

    fn read_text(&self, row: &str, out: &mut Vec<usize>) -> Option<()> {
        out.clear();
        out.extend_from_slice(&self.open);
        // Ids within one list are ascending and unique, so a single list needs no sort.
        let mut sources = usize::from(!self.open.is_empty());
        for (column, indexed) in &self.by_column {
            let value = match scalar_in(row, &indexed.needle) {
                Read::Absent => continue,
                Read::Scalar(value) => value,
                Read::Complex => return None,
            };
            let Some(found) = indexed.by_value.get(value) else {
                continue;
            };
            let held = out.len();
            out.extend_from_slice(&found.solo);
            sources += usize::from(out.len() > held);
            let held = out.len();
            for &id in &found.multi {
                // The probed term holds; the rest of the shape's terms are read the same way.
                let mut all = true;
                for (other, values) in &self.shapes[id].terms {
                    if other == column {
                        continue;
                    }
                    let needle = &self.by_column[other].needle;
                    match scalar_in(row, needle) {
                        Read::Scalar(v) => all &= values.contains(v),
                        Read::Absent => all = false,
                        Read::Complex => return None,
                    }
                }
                if all {
                    out.push(id);
                }
            }
            sources += usize::from(out.len() > held);
        }
        if sources > 1 {
            out.sort_unstable();
            out.dedup();
        }
        Some(())
    }
}

/// A table's canonical rows, and the copy each subscriber holds of the rows its shape selects.
pub struct Table {
    name: String,
    rows: BTreeMap<String, Value>,
    index: ShapeIndex,
    held: Vec<BTreeMap<String, Value>>,
}

impl Table {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            rows: BTreeMap::new(),
            index: ShapeIndex::default(),
            held: Vec::new(),
        }
    }

    /// Adds a subscriber and returns its id. Its copy starts as the matching canonical rows.
    pub fn subscribe(&mut self, shape: Shape) -> usize {
        assert_eq!(shape.table, self.name, "a shape names another table");
        let held = self
            .rows
            .iter()
            .filter(|(_, row)| shape.matches(row))
            .map(|(key, row)| (key.clone(), row.clone()))
            .collect();
        self.held.push(held);
        self.index.insert(shape)
    }

    pub fn copy(&self, subscriber: usize) -> &BTreeMap<String, Value> {
        &self.held[subscriber]
    }

    pub fn canonical(&self) -> &BTreeMap<String, Value> {
        &self.rows
    }

    /// A commit that puts `row` under `key`, or replaces the row there.
    pub fn put(&mut self, key: &str, row: Value) {
        let mut touched = self
            .rows
            .get(key)
            .map(|old| self.index.matching(old))
            .unwrap_or_default();
        touched.extend(self.index.matching(&row));
        self.rows.insert(key.to_string(), row);
        self.refresh(key, touched);
    }

    /// A commit that removes the row under `key`, or a rollback of one.
    pub fn retract(&mut self, key: &str) {
        let Some(old) = self.rows.remove(key) else {
            return;
        };
        let touched = self.index.matching(&old);
        self.refresh(key, touched);
    }

    /// Brings each subscriber in `touched` to what its shape selects under `key` now.
    fn refresh(&mut self, key: &str, touched: Vec<usize>) {
        let row = self.rows.get(key);
        for id in touched {
            let selected = row.filter(|row| self.index.shape(id).matches(row));
            match selected {
                Some(row) => {
                    self.held[id].insert(key.to_string(), row.clone());
                }
                None => {
                    self.held[id].remove(key);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn swaps_in(pools: &[&str]) -> Shape {
        Shape {
            table: "swaps".into(),
            terms: BTreeMap::from([(
                "pool".to_string(),
                pools.iter().map(|p| p.to_string()).collect(),
            )]),
        }
    }

    fn swap(pool: &str, block: u64) -> Value {
        json!({ "pool": pool, "block": block, "amount0": "7" })
    }

    #[test]
    fn a_copy_holds_exactly_the_rows_its_terms_select() {
        let mut t = Table::new("swaps");
        let a = t.subscribe(swaps_in(&["0xa"]));
        t.put("1:0", swap("0xa", 1));
        t.put("1:1", swap("0xb", 1));
        t.put("2:0", swap("0xa", 2));
        assert_eq!(t.copy(a).keys().collect::<Vec<_>>(), ["1:0", "2:0"]);
    }

    #[test]
    fn a_replaced_row_leaves_the_copies_it_no_longer_matches() {
        let mut t = Table::new("swaps");
        let a = t.subscribe(swaps_in(&["0xa"]));
        let b = t.subscribe(swaps_in(&["0xb"]));
        t.put("1:0", swap("0xa", 1));
        t.put("1:0", swap("0xb", 1));
        assert!(t.copy(a).is_empty());
        assert_eq!(t.copy(b).len(), 1);
    }

    #[test]
    fn a_shape_with_no_terms_holds_every_row() {
        let mut t = Table::new("swaps");
        let all = t.subscribe(Shape {
            table: "swaps".into(),
            terms: BTreeMap::new(),
        });
        t.put("1:0", swap("0xa", 1));
        t.put("1:1", swap("0xb", 1));
        t.retract("1:0");
        assert_eq!(t.copy(all).len(), 1);
    }

    #[test]
    fn a_shape_is_identified_by_its_canonical_form() {
        let a = swaps_in(&["0xb", "0xa"]);
        let b = swaps_in(&["0xa", "0xb"]);
        assert_eq!(a.hash(), b.hash());
        assert_ne!(a.hash(), swaps_in(&["0xa"]).hash());
    }

    /// The property RFC-0080 S0 gates on: after any mix of overwrites, retractions and rollbacks,
    /// every copy equals the canonical rows its shape selects.
    #[test]
    fn overwrites_and_rollbacks_keep_every_copy_equal_to_the_filtered_canonical_rows() {
        let pools = ["0xa", "0xb", "0xc", "0xd"];
        let mut seed: u64 = 0x9e37_79b9_7f4a_7c15;
        let mut next = move |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let mut t = Table::new("swaps");
        for k in 0..12 {
            let chosen: Vec<&str> = pools.iter().copied().filter(|_| next(2) == 0).collect();
            let shape = if k == 0 {
                swaps_in(&pools)
            } else {
                swaps_in(&chosen)
            };
            t.subscribe(shape);
        }
        // Each step is undone by a rollback, so the log is the history to rewind.
        let mut log: Vec<(String, Option<Value>)> = Vec::new();
        for _ in 0..3000 {
            let key = format!("{}:{}", next(40), next(3));
            let before = t.canonical().get(&key).cloned();
            match next(10) {
                0..=6 => {
                    let row = swap(pools[next(4) as usize], next(40));
                    t.put(&key, row);
                    log.push((key, before));
                }
                7..=8 => {
                    t.retract(&key);
                    log.push((key, before));
                }
                _ => {
                    for _ in 0..next(5) {
                        let Some((key, before)) = log.pop() else {
                            break;
                        };
                        match before {
                            Some(row) => t.put(&key, row),
                            None => t.retract(&key),
                        }
                    }
                }
            }
            for id in 0..12 {
                let expected: BTreeMap<String, Value> = t
                    .canonical()
                    .iter()
                    .filter(|(_, row)| t.index.shape(id).matches(row))
                    .map(|(k, r)| (k.clone(), r.clone()))
                    .collect();
                assert_eq!(t.copy(id), &expected, "subscriber {id} diverged");
            }
        }
    }

    /// The text path is an optimisation of the parsed one and must agree with it on every row,
    /// including the ones it cannot read and hands to a parse.
    #[test]
    fn reading_columns_off_the_text_agrees_with_the_parsed_row() {
        let mut index = ShapeIndex::default();
        index.insert(swaps_in(&["0xa", "0xb"]));
        index.insert(Shape {
            table: "swaps".into(),
            terms: BTreeMap::from([
                ("pool".to_string(), BTreeSet::from(["0xa".to_string()])),
                ("block".to_string(), BTreeSet::from(["7".to_string()])),
            ]),
        });
        index.insert(Shape {
            table: "swaps".into(),
            terms: BTreeMap::from([("memo".to_string(), BTreeSet::from(["a\"b".to_string()]))]),
        });
        index.insert(Shape {
            table: "swaps".into(),
            terms: BTreeMap::new(),
        });
        let rows = [
            json!({ "pool": "0xa", "block": 7 }),
            json!({ "pool": "0xa", "block": 8 }),
            json!({ "pool": "0xb" }),
            json!({ "block": 7 }),
            json!({ "pool": "0xc", "block": 7 }),
            json!({ "memo": "a\"b", "pool": "0xb" }),
            json!({ "memo": "x\"pool\":\"0xa", "pool": "0xc" }),
            json!({ "pool": "0\u{78}a" }),
            json!({ "pool": null, "block": 7.0 }),
            json!({ "pool": { "inner": "0xa" } }),
            json!({ "other": 1 }),
        ];
        let mut out = Vec::new();
        for row in rows {
            index.matching_text(&row.to_string(), &mut out);
            assert_eq!(out, index.matching(&row), "row {row}");
        }
        index.matching_text("not json", &mut out);
        assert_eq!(out, [3], "only the open shape holds a row nothing reads");
    }
}
