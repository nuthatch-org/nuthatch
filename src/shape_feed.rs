//! RFC-0080 S1: the subscribers to shapes, and what a follower tells each when it applies a delta.
//!
//! A subscriber holds the rows of one table its shape selects. Each applied delta becomes at most one
//! [`Frame`] per subscriber: the rows to put and the keys to retract, found by looking each changed
//! row up in an index of the shapes (RFC-0080 §5.4). A queue that fills is closed, never waited on.

use crate::feed::Touch;
use crate::shape::{Shape, ShapeIndex};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

/// A change to a subscriber's copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ShapeOp {
    /// The row stored under `key`, new or replaced.
    Put { key: Arc<str>, row: Arc<str> },
    /// The row under `key` is no longer in the shape: a rollback dropped it, or a put replaced it
    /// with one that does not match. Retracting a key a copy does not hold changes nothing.
    Retract { key: Arc<str> },
}

/// What one applied delta did to a subscriber's copy, as of the `(epoch, generation)` it left.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    pub epoch: u64,
    pub generation: u64,
    /// Shared, so a frame sent to every subscriber of a shape is one copy of it.
    pub ops: Arc<[ShapeOp]>,
}

#[derive(serde::Serialize)]
#[serde(rename_all = "snake_case")]
enum WireOp<'a> {
    Put {
        key: &'a str,
        row: &'a serde_json::value::RawValue,
    },
    Retract {
        key: &'a str,
    },
}

#[derive(serde::Serialize)]
struct WireLine<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    offset: Option<(u64, u64)>,
    ops: Vec<WireOp<'a>>,
}

/// One line of a `/sync` stream: the ops, as `{"offset":[e,g],"ops":[{"put":{"key","row"}},
/// {"retract":{"key"}}]}`, newline-ended. A row goes out as the JSON it was stored as.
pub fn ops_line(offset: Option<(u64, u64)>, ops: &[ShapeOp]) -> Vec<u8> {
    use serde_json::value::{to_raw_value, RawValue};
    // Held beside the line so the borrowed ops outlive it; a stored row that is not JSON goes as text.
    let rows: Vec<Option<Box<RawValue>>> = ops
        .iter()
        .map(|op| match op {
            ShapeOp::Put { row, .. } => Some(
                serde_json::from_str::<Box<RawValue>>(row)
                    .or_else(|_| to_raw_value(&**row))
                    .expect("a string is JSON"),
            ),
            ShapeOp::Retract { .. } => None,
        })
        .collect();
    let wire = ops.iter().zip(&rows).map(|(op, row)| match (op, row) {
        (ShapeOp::Put { key, .. }, Some(row)) => WireOp::Put { key, row },
        (ShapeOp::Retract { key }, _) | (ShapeOp::Put { key, .. }, None) => WireOp::Retract { key },
    });
    let mut line = serde_json::to_vec(&WireLine {
        offset,
        ops: wire.collect(),
    })
    .expect("ops serialise");
    line.push(b'\n');
    line
}

/// A subscriber's end. When it ends, because its queue filled or the hub went, the copy is stale
/// and must be fetched again.
pub type Frames = mpsc::Receiver<Frame>;

/// A stored row as `/sql` shows it: the text a subscriber is sent, and the row its membership is
/// decided on, so a live put is in a shape exactly when the first answer would hold it (#2588).
#[derive(Debug, Clone)]
pub struct Shown {
    pub text: Arc<str>,
    pub row: serde_json::Value,
}

/// A stored row's table, from its own top-level `table` field, and the row parsed.
fn stored(row: &str) -> Option<(String, serde_json::Value)> {
    let row: serde_json::Value = serde_json::from_str(row).ok()?;
    let table = row.get("table")?.as_str()?.to_string();
    Some((table, row))
}

struct Entry {
    shape: Shape,
    senders: Vec<mpsc::Sender<Frame>>,
}

/// One table's shapes: subscribers to the same shape share an entry, and so an index slot.
#[derive(Default)]
struct Table {
    index: ShapeIndex,
    by_hash: HashMap<String, usize>,
    /// Parallel to the index's ids.
    entries: Vec<Entry>,
    /// Entries with no live subscriber, which the index still lists until it is rebuilt.
    empty: usize,
}

impl Table {
    fn sweep(&mut self) {
        for entry in &mut self.entries {
            if entry.senders.is_empty() {
                continue;
            }
            entry.senders.retain(|s| !s.is_closed());
            self.empty += usize::from(entry.senders.is_empty());
        }
        if self.empty > 16 && self.empty * 2 > self.entries.len() {
            self.rebuild();
        }
    }

    fn rebuild(&mut self) {
        let live: Vec<Entry> = std::mem::take(&mut self.entries)
            .into_iter()
            .filter(|e| !e.senders.is_empty())
            .collect();
        self.index = ShapeIndex::default();
        self.by_hash.clear();
        for entry in &live {
            let id = self.index.insert(entry.shape.clone());
            self.by_hash.insert(entry.shape.hash(), id);
        }
        self.entries = live;
        self.empty = 0;
    }
}

/// Every table's shapes with live subscribers.
#[derive(Default)]
pub struct ShapeHub {
    tables: HashMap<String, Table>,
}

impl ShapeHub {
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Drops the subscribers whose stream has ended, and every table left with none, so a table
    /// nobody follows is not projected on each delta (#2601).
    pub fn prune(&mut self) {
        for table in self.tables.values_mut() {
            table.sweep();
        }
        self.tables
            .retain(|_, t| t.entries.iter().any(|e| !e.senders.is_empty()));
    }

    /// Subscribes to `shape` with a queue of `queue` frames. The caller makes the copy it starts from
    /// and subscribes at the same point, so no delta falls between them; frames up to the copy's own
    /// generation are then already in it.
    pub fn subscribe(&mut self, shape: Shape, queue: usize) -> Frames {
        let (tx, rx) = mpsc::channel(queue.max(1));
        let table = self.tables.entry(shape.table.clone()).or_default();
        let hash = shape.hash();
        match table.by_hash.get(&hash) {
            Some(&id) => {
                let entry = &mut table.entries[id];
                table.empty -= usize::from(entry.senders.is_empty());
                entry.senders.push(tx);
            }
            None => {
                let id = table.index.insert(shape.clone());
                table.by_hash.insert(hash, id);
                table.entries.push(Entry {
                    shape,
                    senders: vec![tx],
                });
            }
        }
        rx
    }

    /// The tables some subscriber's shape names.
    pub fn tables(&self) -> impl Iterator<Item = &str> {
        self.tables.keys().map(String::as_str)
    }

    /// Subscribers now, counting each of those that share a shape.
    pub fn subscribers(&self) -> usize {
        self.tables
            .values()
            .flat_map(|t| &t.entries)
            .map(|e| e.senders.len())
            .sum()
    }

    /// Tells every subscriber what the delta at `(epoch, generation)` did to its copy. `log` is the
    /// delta's [`crate::feed::Changes::log`]. Never waits: a subscriber whose queue is full is closed.
    ///
    /// `shown` is each put's row as subscribers are to see it, by the key it is stored under, worked
    /// out once for the delta; `None` sends the stored rows as they are. A put is in a shape by its
    /// shown row, and a row it replaced or a rollback dropped by that row as stored. A put with no
    /// entry in `shown` is not sent.
    pub fn apply(
        &mut self,
        epoch: u64,
        generation: u64,
        log: &[Touch],
        shown: Option<&HashMap<String, Shown>>,
    ) {
        if self.tables.is_empty() || log.is_empty() {
            return;
        }
        // A key's last touch is where it ends up, and every row it had on the way is a copy that
        // may hold it, so each key is settled once however often the delta touched it.
        let mut order: Vec<&str> = Vec::new();
        let mut by_key: HashMap<&str, Vec<&Touch>> = HashMap::new();
        for touch in log {
            by_key
                .entry(touch.key.as_str())
                .or_insert_with(|| {
                    order.push(touch.key.as_str());
                    Vec::new()
                })
                .push(touch);
        }
        let mut pending: HashMap<(String, usize), Vec<ShapeOp>> = HashMap::new();
        for key in order {
            let touches = &by_key[key];
            // Each held once however many subscribers are told of it.
            let mut key_text: Option<Arc<str>> = None;
            let mut holds: Vec<(String, usize)> = Vec::new();
            if let Some(row) = touches.last().and_then(|t| t.new.as_deref()) {
                if let Some((name, parsed)) =
                    stored(row).filter(|(n, _)| self.tables.contains_key(n))
                {
                    let shown_row = match shown {
                        None => Some((Arc::from(row), &parsed)),
                        Some(shown) => shown.get(key).map(|s| (s.text.clone(), &s.row)),
                    };
                    match shown_row {
                        Some((text, row)) => {
                            let found = self.tables[&name].index.matching(row);
                            if !found.is_empty() {
                                let key = key_text.get_or_insert_with(|| Arc::from(key)).clone();
                                for &id in &found {
                                    pending.entry((name.clone(), id)).or_default().push(
                                        ShapeOp::Put {
                                            key: key.clone(),
                                            row: text.clone(),
                                        },
                                    );
                                }
                            }
                            holds.extend(found.into_iter().map(|id| (name.clone(), id)));
                        }
                        None => {
                            tracing::error!("a row of {name} changed and was not shown: not sent")
                        }
                    }
                }
            }
            let mut retracted: Vec<(String, usize)> = Vec::new();
            for old in touches.iter().filter_map(|t| t.old.as_deref()) {
                let Some((name, parsed)) = stored(old) else {
                    continue;
                };
                let Some(table) = self.tables.get(&name) else {
                    continue;
                };
                for id in table.index.matching(&parsed) {
                    let slot = (name.clone(), id);
                    if holds.contains(&slot) || retracted.contains(&slot) {
                        continue;
                    }
                    retracted.push(slot.clone());
                    let key = key_text.get_or_insert_with(|| Arc::from(key)).clone();
                    pending
                        .entry(slot)
                        .or_default()
                        .push(ShapeOp::Retract { key });
                }
            }
        }
        for ((name, id), ops) in pending {
            let table = self.tables.get_mut(&name).expect("a table with shapes");
            let entry = &mut table.entries[id];
            let frame = Frame {
                epoch,
                generation,
                ops: ops.into(),
            };
            let before = entry.senders.len();
            // A full queue is a client that is not reading: it is closed, and fetches again.
            entry.senders.retain(|s| s.try_send(frame.clone()).is_ok());
            if before > 0 && entry.senders.is_empty() {
                table.empty += 1;
            }
        }
        for table in self.tables.values_mut() {
            table.sweep();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::{Delta, MemStore, Op, Snapshot};
    use crate::shape::Range;
    use crate::store::Store;
    use serde_json::json;
    use std::collections::{BTreeMap, BTreeSet};

    /// #2601: a table whose last subscriber has gone, or whose subscriber was refused before reading
    /// a frame, is dropped, so nothing projects it on later deltas.
    #[test]
    fn a_table_with_no_live_subscriber_is_dropped() {
        let mut hub = ShapeHub::default();
        let kept = hub.subscribe(shape_of(&["0xa"]), 4);
        drop(hub.subscribe(shape_of(&["0xb"]), 4));
        hub.prune();
        assert_eq!(hub.tables().collect::<Vec<_>>(), ["swaps"]);
        assert_eq!(hub.subscribers(), 1);
        drop(kept);
        hub.prune();
        assert!(hub.is_empty(), "no subscriber is left");
    }

    fn shape_of(pools: &[&str]) -> Shape {
        Shape {
            table: "swaps".into(),
            terms: BTreeMap::from([(
                "pool".to_string(),
                pools.iter().map(|p| p.to_string()).collect::<BTreeSet<_>>(),
            )]),
            ..Default::default()
        }
    }

    fn row(pool: &str, block: u64, log: u64) -> String {
        json!({ "table": "swaps", "pool": pool, "block_number": block, "log_index": log })
            .to_string()
    }

    fn put(block: u64, log: u64, pool: &str) -> Op {
        Op::Put {
            key: Store::entity_key(block, log),
            row: row(pool, block, log),
        }
    }

    fn rollback_to(block: u64) -> Op {
        Op::Drop {
            from: Store::entity_key(block + 1, 0),
            to: None,
        }
    }

    /// A follower's copy and the hub fed from it, applying deltas as the follower does.
    struct Follower {
        _dir: tempfile::TempDir,
        store: MemStore,
        hub: ShapeHub,
        generation: u64,
    }

    impl Follower {
        fn new() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let store = MemStore::from_snapshot(
                dir.path(),
                Snapshot {
                    epoch: 1,
                    ..Default::default()
                },
            );
            store.track_changes();
            Follower {
                _dir: dir,
                store,
                hub: ShapeHub::default(),
                generation: 0,
            }
        }

        fn apply(&mut self, ops: Vec<Op>) {
            self.generation += 1;
            let delta = Delta {
                epoch: 1,
                generation: self.generation,
                ops,
                ..Default::default()
            };
            let changes = self.store.apply(&delta).unwrap();
            self.hub.apply(1, self.generation, &changes.log, None);
        }
    }

    fn drain(frames: &mut Frames) -> (Vec<Frame>, bool) {
        let mut out = Vec::new();
        loop {
            match frames.try_recv() {
                Ok(frame) => out.push(frame),
                Err(mpsc::error::TryRecvError::Empty) => return (out, false),
                Err(mpsc::error::TryRecvError::Disconnected) => return (out, true),
            }
        }
    }

    #[test]
    fn a_line_carries_the_offset_and_rows_as_the_json_they_were_stored_as() {
        let ops = [
            ShapeOp::Put {
                key: "k1".into(),
                row: r#"{"pool":"0xa","n":1}"#.into(),
            },
            ShapeOp::Retract { key: "k2".into() },
        ];
        let line = String::from_utf8(ops_line(Some((1, 7)), &ops)).unwrap();
        assert_eq!(
            line,
            "{\"offset\":[1,7],\"ops\":[{\"put\":{\"key\":\"k1\",\"row\":{\"pool\":\"0xa\",\"n\":1}}},\
             {\"retract\":{\"key\":\"k2\"}}]}\n"
        );
        let bare = String::from_utf8(ops_line(None, &ops[1..])).unwrap();
        assert_eq!(bare, "{\"ops\":[{\"retract\":{\"key\":\"k2\"}}]}\n");
    }

    #[test]
    fn a_put_reaches_only_the_subscribers_whose_shape_selects_it() {
        let mut f = Follower::new();
        let mut a = f.hub.subscribe(shape_of(&["0xa"]), 8);
        let mut b = f.hub.subscribe(shape_of(&["0xb"]), 8);
        f.apply(vec![put(1, 0, "0xa"), put(1, 1, "0xc")]);
        let (got, _) = drain(&mut a);
        assert_eq!(
            got,
            [Frame {
                epoch: 1,
                generation: 1,
                ops: vec![ShapeOp::Put {
                    key: Store::entity_key(1, 0).into(),
                    row: row("0xa", 1, 0).into()
                }]
                .into()
            }]
        );
        assert!(drain(&mut b).0.is_empty(), "nothing it selects changed");
    }

    #[test]
    fn a_replacement_that_leaves_the_shape_is_a_retraction() {
        let mut f = Follower::new();
        f.apply(vec![put(1, 0, "0xa")]);
        let mut a = f.hub.subscribe(shape_of(&["0xa"]), 8);
        f.apply(vec![put(1, 0, "0xb")]);
        assert_eq!(
            &*drain(&mut a).0[0].ops,
            [ShapeOp::Retract {
                key: Store::entity_key(1, 0).into()
            }]
        );
    }

    #[test]
    fn a_rollback_retracts_what_it_drops() {
        let mut f = Follower::new();
        f.apply(vec![put(1, 0, "0xa"), put(2, 0, "0xa"), put(2, 1, "0xb")]);
        let mut a = f.hub.subscribe(shape_of(&["0xa"]), 8);
        f.apply(vec![rollback_to(1)]);
        assert_eq!(
            &*drain(&mut a).0[0].ops,
            [ShapeOp::Retract {
                key: Store::entity_key(2, 0).into()
            }]
        );
    }

    #[test]
    fn a_row_put_and_rolled_back_in_one_delta_leaves_no_put() {
        let mut f = Follower::new();
        let mut a = f.hub.subscribe(shape_of(&["0xa"]), 8);
        f.apply(vec![put(3, 0, "0xa"), rollback_to(2)]);
        let ops = drain(&mut a)
            .0
            .pop()
            .map(|f| f.ops)
            .unwrap_or_else(|| Arc::from([]));
        assert!(
            ops.iter().all(|op| matches!(op, ShapeOp::Retract { .. })),
            "{ops:?}"
        );
    }

    /// A subscriber that does not read is closed after its queue fills, and the follower and every
    /// other subscriber, even one on the same shape, carry on as if it had never been there.
    #[test]
    fn a_stalled_subscriber_is_closed_and_delays_no_one() {
        let mut f = Follower::new();
        let mut slow = f.hub.subscribe(shape_of(&["0xa"]), 2);
        let mut fast = f.hub.subscribe(shape_of(&["0xa"]), 100);
        for block in 1..=10 {
            f.apply(vec![put(block, 0, "0xa")]);
        }
        let (fast_frames, fast_closed) = drain(&mut fast);
        assert_eq!(fast_frames.len(), 10);
        assert!(!fast_closed);
        let (slow_frames, slow_closed) = drain(&mut slow);
        assert_eq!(slow_frames.len(), 2, "what fitted before it was closed");
        assert!(slow_closed, "and then it must fetch again");
        assert_eq!(f.hub.subscribers(), 1);
    }

    #[test]
    fn subscribers_that_left_free_their_slots() {
        let mut f = Follower::new();
        let mut kept = f.hub.subscribe(shape_of(&["0xkept"]), 100);
        let gone: Vec<Frames> = (0..64)
            .map(|i| f.hub.subscribe(shape_of(&[format!("0x{i}").as_str()]), 8))
            .collect();
        drop(gone);
        f.apply(vec![put(1, 0, "0xkept")]);
        assert_eq!(f.hub.subscribers(), 1);
        let table = &f.hub.tables["swaps"];
        assert_eq!(table.entries.len(), 1, "the index was rebuilt without them");
        f.apply(vec![put(2, 0, "0xkept")]);
        assert_eq!(drain(&mut kept).0.len(), 2, "and still finds the one left");
    }

    /// RFC-0080 S1's gate: after any mix of new blocks, overwrites and reorgs, every subscriber's
    /// copy, built from its first answer and the frames since, equals the canonical rows its shape
    /// selects, for subscribers that joined at different points and ones that share a shape.
    #[test]
    fn every_copy_equals_the_filtered_canonical_rows_after_overwrites_and_reorgs() {
        let pools = ["0xa", "0xb", "0xc", "0xd"];
        let mut seed: u64 = 0x2545_f491_4f6c_dd1d;
        let mut next = move |n: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % n
        };
        let mut f = Follower::new();
        // The canonical rows, kept apart from the store the follower holds.
        let mut model: BTreeMap<String, String> = BTreeMap::new();
        struct Client {
            shape: Shape,
            frames: Frames,
            copy: BTreeMap<String, String>,
        }
        let mut clients: Vec<Client> = Vec::new();
        let mut head = 0u64;
        for step in 0..1500 {
            // Clients arrive through the run, each starting from the matching rows as they are then.
            if step % 100 == 0 && clients.len() < 12 {
                let chosen: Vec<&str> = pools.iter().copied().filter(|_| next(2) == 0).collect();
                let shape = match clients.len() % 4 {
                    3 => clients[0].shape.clone(),
                    // A window of blocks, with and without a choice of pools.
                    2 => Shape {
                        ranges: BTreeMap::from([(
                            "block_number".to_string(),
                            Range {
                                gte: Some(10),
                                lte: Some(60),
                            },
                        )]),
                        ..shape_of(&[pools[step / 100 % 4]])
                    },
                    1 => Shape {
                        ranges: BTreeMap::from([(
                            "block_number".to_string(),
                            Range {
                                gte: Some(5),
                                lte: None,
                            },
                        )]),
                        table: "swaps".into(),
                        ..Default::default()
                    },
                    _ => shape_of(&chosen),
                };
                let copy = model
                    .iter()
                    .filter(|(_, r)| {
                        shape.matches(&serde_json::from_str::<serde_json::Value>(r).unwrap())
                    })
                    .map(|(k, r)| (k.clone(), r.clone()))
                    .collect();
                let frames = f.hub.subscribe(shape.clone(), 100_000);
                clients.push(Client {
                    shape,
                    frames,
                    copy,
                });
            }
            let mut ops = Vec::new();
            match next(10) {
                0..=5 => {
                    head += 1;
                    for log in 0..=next(3) {
                        ops.push(put(head, log, pools[next(4) as usize]));
                    }
                }
                6..=7 if head > 0 => {
                    let block = 1 + next(head);
                    ops.push(put(block, 0, pools[next(4) as usize]));
                }
                _ if head > 0 => {
                    // A reorg: back to an earlier block, and a different chain from there.
                    let to = next(head);
                    ops.push(rollback_to(to));
                    head = to;
                    for _ in 0..next(3) {
                        head += 1;
                        ops.push(put(head, 0, pools[next(4) as usize]));
                    }
                }
                _ => {}
            }
            for op in &ops {
                match op {
                    Op::Put { key, row } => {
                        model.insert(key.clone(), row.clone());
                    }
                    Op::Drop { from, .. } => {
                        model.split_off(from);
                    }
                    _ => unreachable!(),
                }
            }
            if ops.is_empty() {
                continue;
            }
            f.apply(ops);
            for client in &mut clients {
                let (frames, closed) = drain(&mut client.frames);
                assert!(!closed);
                for frame in frames {
                    for op in frame.ops.iter() {
                        match op {
                            ShapeOp::Put { key, row } => {
                                client.copy.insert(key.to_string(), row.to_string());
                            }
                            ShapeOp::Retract { key } => {
                                client.copy.remove(&**key);
                            }
                        }
                    }
                }
                let expected: BTreeMap<String, String> = model
                    .iter()
                    .filter(|(_, r)| {
                        client
                            .shape
                            .matches(&serde_json::from_str::<serde_json::Value>(r).unwrap())
                    })
                    .map(|(k, r)| (k.clone(), r.clone()))
                    .collect();
                assert_eq!(client.copy, expected, "a copy diverged at step {step}");
            }
        }
        assert!(clients.len() > 4 && !model.is_empty());
        let holding = clients.iter().filter(|c| !c.copy.is_empty()).count();
        assert!(holding > 4, "the shapes select something: {holding}");
    }

    /// What the hub adds to a follower's work on deltas it has just decoded and applied, at 10,000
    /// single-term shapes over 2,000 pools, so each row goes to five subscribers. Run in release
    /// with `--ignored --nocapture`.
    #[test]
    #[ignore]
    fn the_hub_costs_what_it_costs() {
        use std::time::{Duration, Instant};
        const POOLS: u64 = 2_000;
        const BLOCKS: u64 = 100;
        const PER_BLOCK: u64 = 200;
        let address = |n: u64| format!("0x{n:040x}");
        let bodies: Vec<Vec<u8>> = (1..=BLOCKS)
            .map(|block| {
                let ops = (0..PER_BLOCK)
                    .map(|log| {
                        let i = block * PER_BLOCK + log;
                        Op::Put {
                            key: Store::entity_key(block, log),
                            row: json!({
                                "table": "swaps",
                                "pool": address(i * 7919 % POOLS),
                                "block_number": block,
                                "log_index": log,
                                "tx": format!("0x{i:064x}"),
                                "sender": address(i),
                                "amount0": "123456789",
                                "amount1": "987654321",
                            })
                            .to_string(),
                        }
                    })
                    .collect();
                let delta = Delta {
                    epoch: 1,
                    generation: block,
                    ops,
                    ..Default::default()
                };
                crate::feed_wire::encode_delta(&delta).unwrap()
            })
            .collect();
        let dir = tempfile::tempdir().unwrap();
        let fresh = || {
            MemStore::from_snapshot(
                dir.path(),
                Snapshot {
                    epoch: 1,
                    ..Default::default()
                },
            )
        };
        let best = |a: &mut Duration, b: Duration| *a = (*a).min(b);
        let (mut decode, mut plain, mut tracked, mut hub_time) =
            (Duration::MAX, Duration::MAX, Duration::MAX, Duration::MAX);
        for _ in 0..5 {
            let (store, untracked) = (fresh(), fresh());
            store.track_changes();
            let mut hub = ShapeHub::default();
            let _frames: Vec<Frames> = (0..10_000u64)
                .map(|s| hub.subscribe(shape_of(&[address(s % POOLS).as_str()]), 1000))
                .collect();
            let (mut d, mut p, mut t, mut h) = (
                Duration::ZERO,
                Duration::ZERO,
                Duration::ZERO,
                Duration::ZERO,
            );
            for body in &bodies {
                let started = Instant::now();
                let delta = crate::feed_wire::decode_delta(body, None).unwrap();
                d += started.elapsed();
                let started = Instant::now();
                untracked.apply(&delta).unwrap();
                p += started.elapsed();
                let started = Instant::now();
                let changes = store.apply(&delta).unwrap();
                t += started.elapsed();
                let started = Instant::now();
                hub.apply(delta.epoch, delta.generation, &changes.log, None);
                h += started.elapsed();
            }
            best(&mut decode, d);
            best(&mut plain, p);
            best(&mut tracked, t);
            best(&mut hub_time, h);
        }
        let follower = (decode + plain).as_secs_f64();
        println!(
            "{} rows: decode {decode:?}, apply {plain:?}, apply while tracking {tracked:?} ({:+.3} of \
             decode and apply), hub {hub_time:?} ({:.3})",
            BLOCKS * PER_BLOCK,
            (tracked.as_secs_f64() - plain.as_secs_f64()) / follower,
            hub_time.as_secs_f64() / follower,
        );
    }
}
