//! Immutable versions of derived state and hot copies, published once per window (#2467).
//!
//! A request answers from one published version of everything it reads, so it never waits on a
//! commit and never sees a window in part. A version must therefore be cheap to take from state the
//! writer keeps changing. [`Chunked`] is an ordered map held as copy-on-write chunks of at most
//! `2 * CHUNK` entries: taking a version clones a vector of `Arc`s, and the next change copies only
//! the chunks it touches. Measured in #2467's S0 spike at 309,548 entity groups, a two-row window
//! costs about 70 µs over an in-place update.
//!
//! A version a long request holds keeps its chunks alive after the writer has moved on. [`Versions`]
//! remembers what was published, so what old versions retain can be charged to the budget.

use std::borrow::Borrow;
use std::collections::{BTreeMap, HashSet};
use std::ops::{Bound, RangeBounds};
use std::sync::{Arc, Mutex, Weak};

use arc_swap::ArcSwap;

/// Entries per chunk at which a chunk is split in two. S0 settled on about 16.
const CHUNK: usize = 16;

/// What one entry is charged, in bytes.
pub type Weigh<K, V> = fn(&K, &V) -> u64;

#[derive(Clone)]
struct Chunk<K, V> {
    map: BTreeMap<K, V>,
    bytes: u64,
}

/// An ordered map whose clone shares every chunk with the original.
pub struct Chunked<K, V> {
    /// In key order, never overlapping, and never empty unless it is the only one.
    chunks: Vec<Arc<Chunk<K, V>>>,
    len: usize,
    bytes: u64,
    weigh: Weigh<K, V>,
}

impl<K, V> Clone for Chunked<K, V> {
    fn clone(&self) -> Self {
        Self {
            chunks: self.chunks.clone(),
            len: self.len,
            bytes: self.bytes,
            weigh: self.weigh,
        }
    }
}

impl<K: Ord + Clone + std::fmt::Debug, V: Clone + std::fmt::Debug> std::fmt::Debug
    for Chunked<K, V> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_map().entries(self.iter()).finish()
    }
}

impl<K: Ord + Clone, V: Clone + PartialEq> PartialEq for Chunked<K, V> {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.iter().eq(other.iter())
    }
}

impl<K: Ord + Clone, V: Clone> Chunked<K, V> {
    pub fn new(weigh: Weigh<K, V>) -> Self {
        Self {
            chunks: vec![Arc::new(Chunk {
                map: BTreeMap::new(),
                bytes: 0,
            })],
            len: 0,
            bytes: 0,
            weigh,
        }
    }

    pub fn from_map(map: BTreeMap<K, V>, weigh: Weigh<K, V>) -> Self {
        let mut out = Self::new(weigh);
        if map.is_empty() {
            return out;
        }
        out.len = map.len();
        out.chunks.clear();
        let mut chunk = BTreeMap::new();
        for (k, v) in map {
            chunk.insert(k, v);
            if chunk.len() == CHUNK {
                out.push_chunk(std::mem::take(&mut chunk));
            }
        }
        if !chunk.is_empty() {
            out.push_chunk(chunk);
        }
        out
    }

    fn push_chunk(&mut self, map: BTreeMap<K, V>) {
        let bytes = map.iter().map(|(k, v)| (self.weigh)(k, v)).sum();
        self.bytes += bytes;
        self.chunks.push(Arc::new(Chunk { map, bytes }));
    }

    /// The chunk that holds, or would hold, `k`.
    fn locate<Q: Ord + ?Sized>(&self, k: &Q) -> usize
    where
        K: Borrow<Q>,
    {
        self.chunks
            .partition_point(|c| c.map.first_key_value().is_some_and(|(f, _)| f.borrow() <= k))
            .saturating_sub(1)
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// What every entry is charged, by the map's weigher.
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    pub fn get<Q: Ord + ?Sized>(&self, k: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
    {
        self.chunks[self.locate(k)].map.get(k)
    }

    pub fn contains_key<Q: Ord + ?Sized>(&self, k: &Q) -> bool
    where
        K: Borrow<Q>,
    {
        self.get(k).is_some()
    }

    pub fn insert(&mut self, k: K, v: V) -> Option<V> {
        let i = self.locate(&k);
        let weigh = self.weigh;
        let chunk = Arc::make_mut(&mut self.chunks[i]);
        let added = weigh(&k, &v);
        let removed = chunk.map.get(&k).map_or(0, |old| weigh(&k, old));
        let old = chunk.map.insert(k, v);
        if old.is_none() {
            self.len += 1;
        }
        chunk.bytes = chunk.bytes + added - removed;
        self.bytes = self.bytes + added - removed;
        if chunk.map.len() > 2 * CHUNK {
            let mid = chunk.map.keys().nth(chunk.map.len() / 2).cloned().expect("non-empty");
            let upper = chunk.map.split_off(&mid);
            let moved: u64 = upper.iter().map(|(k, v)| weigh(k, v)).sum();
            chunk.bytes -= moved;
            self.chunks.insert(
                i + 1,
                Arc::new(Chunk {
                    map: upper,
                    bytes: moved,
                }),
            );
        }
        old
    }

    pub fn remove<Q: Ord + ?Sized>(&mut self, k: &Q) -> Option<V>
    where
        K: Borrow<Q>,
    {
        let i = self.locate(k);
        if !self.chunks[i].map.contains_key(k) {
            return None;
        }
        let weigh = self.weigh;
        let chunk = Arc::make_mut(&mut self.chunks[i]);
        let (key, old) = chunk.map.remove_entry(k).expect("checked above");
        let w = weigh(&key, &old);
        chunk.bytes -= w;
        self.bytes -= w;
        self.len -= 1;
        if chunk.map.is_empty() && self.chunks.len() > 1 {
            self.chunks.remove(i);
        }
        Some(old)
    }

    /// Remove every entry at or above `k`, returning them in key order.
    pub fn split_off<Q: Ord + ?Sized>(&mut self, k: &Q) -> Vec<(K, V)>
    where
        K: Borrow<Q>,
    {
        let i = self.locate(k);
        let weigh = self.weigh;
        let first = Arc::make_mut(&mut self.chunks[i]).map.split_off(k);
        let mut gone: Vec<(K, V)> = first.into_iter().collect();
        for c in self.chunks.drain(i + 1..) {
            gone.extend(c.map.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        let w: u64 = gone.iter().map(|(k, v)| weigh(k, v)).sum();
        let chunk = Arc::make_mut(&mut self.chunks[i]);
        chunk.bytes = chunk.map.iter().map(|(k, v)| weigh(k, v)).sum();
        self.bytes -= w;
        self.len -= gone.len();
        if chunk.map.is_empty() && self.chunks.len() > 1 {
            self.chunks.remove(i);
        }
        gone
    }

    /// Keep only the entries `keep` accepts. Rebuilds every chunk, so it is for rare sweeps.
    pub fn retain(&mut self, mut keep: impl FnMut(&K, &V) -> bool) {
        let kept: BTreeMap<K, V> = self
            .iter()
            .filter(|(k, v)| keep(k, v))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        *self = Self::from_map(kept, self.weigh);
    }

    pub fn iter(&self) -> impl DoubleEndedIterator<Item = (&K, &V)> + '_ {
        self.chunks.iter().flat_map(|c| c.map.iter())
    }

    pub fn keys(&self) -> impl DoubleEndedIterator<Item = &K> + '_ {
        self.iter().map(|(k, _)| k)
    }

    pub fn values(&self) -> impl DoubleEndedIterator<Item = &V> + '_ {
        self.iter().map(|(_, v)| v)
    }

    pub fn first_key_value(&self) -> Option<(&K, &V)> {
        self.iter().next()
    }

    pub fn range<Q, R>(&self, range: R) -> impl DoubleEndedIterator<Item = (&K, &V)> + '_
    where
        Q: Ord + ?Sized,
        K: Borrow<Q>,
        R: RangeBounds<Q> + Clone + 'static,
    {
        let from = match range.start_bound() {
            Bound::Included(k) | Bound::Excluded(k) => self.locate(k),
            Bound::Unbounded => 0,
        };
        let to = match range.end_bound() {
            Bound::Included(k) | Bound::Excluded(k) => self.locate(k),
            Bound::Unbounded => self.chunks.len() - 1,
        }
        .max(from);
        self.chunks[from..=to]
            .iter()
            .flat_map(move |c| c.map.range::<Q, R>(range.clone()))
    }

    /// Each chunk's address and bytes, which [`retained`] compares across versions.
    pub fn footprint(&self, out: &mut Vec<(usize, u64)>) {
        out.extend(
            self.chunks
                .iter()
                .map(|c| (Arc::as_ptr(c) as *const () as usize, c.bytes)),
        );
    }
}

/// Where a circuit folds its output: the published [`Chunked`] map, or a plain map in a test.
pub trait Sink<K, V> {
    fn put(&mut self, k: K, v: V);
    fn take(&mut self, k: &K) -> Option<V>;
    fn peek(&self, k: &K) -> Option<&V>;
    fn count(&self) -> usize;
}

impl<K: Ord + Clone, V: Clone> Sink<K, V> for Chunked<K, V> {
    fn put(&mut self, k: K, v: V) {
        self.insert(k, v);
    }
    fn take(&mut self, k: &K) -> Option<V> {
        self.remove(k)
    }
    fn peek(&self, k: &K) -> Option<&V> {
        self.get(k)
    }
    fn count(&self) -> usize {
        self.len()
    }
}

impl<K: Ord, V> Sink<K, V> for BTreeMap<K, V> {
    fn put(&mut self, k: K, v: V) {
        self.insert(k, v);
    }
    fn take(&mut self, k: &K) -> Option<V> {
        self.remove(k)
    }
    fn peek(&self, k: &K) -> Option<&V> {
        self.get(k)
    }
    fn count(&self) -> usize {
        self.len()
    }
}

impl<K: Eq + std::hash::Hash, V> Sink<K, V> for std::collections::HashMap<K, V> {
    fn put(&mut self, k: K, v: V) {
        self.insert(k, v);
    }
    fn take(&mut self, k: &K) -> Option<V> {
        self.remove(k)
    }
    fn peek(&self, k: &K) -> Option<&V> {
        self.get(k)
    }
    fn count(&self) -> usize {
        self.len()
    }
}

/// A version that can say what it holds, chunk by chunk, so chunks two versions share are charged
/// once.
pub trait Footprint {
    fn footprint(&self, out: &mut Vec<(usize, u64)>);
}

impl<K: Ord + Clone, V: Clone> Footprint for Chunked<K, V> {
    fn footprint(&self, out: &mut Vec<(usize, u64)>) {
        Chunked::footprint(self, out)
    }
}

/// The bytes `old` versions hold that `latest` does not.
pub fn retained<T: Footprint>(latest: &T, old: &[Arc<T>]) -> u64 {
    let mut now = Vec::new();
    latest.footprint(&mut now);
    let mut seen: HashSet<usize> = now.into_iter().map(|(p, _)| p).collect();
    let mut chunks = Vec::new();
    for v in old {
        v.footprint(&mut chunks);
    }
    chunks
        .into_iter()
        .filter(|(p, _)| seen.insert(*p))
        .map(|(_, b)| b)
        .sum()
}

/// The latest published version of one piece of state, and every earlier one still referenced.
pub struct Versions<T> {
    latest: ArcSwap<T>,
    old: Mutex<Vec<Weak<T>>>,
}

impl<T: Footprint> Versions<T> {
    pub fn new(first: T) -> Self {
        Self {
            latest: ArcSwap::from_pointee(first),
            old: Mutex::new(Vec::new()),
        }
    }

    pub fn load(&self) -> Arc<T> {
        self.latest.load_full()
    }

    pub fn publish(&self, next: T) {
        let prev = self.latest.swap(Arc::new(next));
        let mut old = self.old.lock().unwrap_or_else(|e| e.into_inner());
        old.retain(|w| w.strong_count() > 0);
        if Arc::strong_count(&prev) > 1 {
            old.push(Arc::downgrade(&prev));
        }
    }

    /// What versions older than the latest hold that the latest does not, in bytes.
    pub fn retained(&self) -> u64 {
        let alive: Vec<Arc<T>> = {
            let mut old = self.old.lock().unwrap_or_else(|e| e.into_inner());
            old.retain(|w| w.strong_count() > 0);
            old.iter().filter_map(Weak::upgrade).collect()
        };
        if alive.is_empty() {
            return 0;
        }
        retained(&*self.load(), &alive)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(_: &u64, v: &String) -> u64 {
        8 + v.len() as u64
    }

    fn filled(n: u64) -> Chunked<u64, String> {
        let mut c = Chunked::new(w);
        for i in 0..n {
            c.insert(i * 2, format!("v{i}"));
        }
        c
    }

    #[test]
    fn it_reads_as_the_btreemap_it_replaces() {
        let mut c = filled(1000);
        let mut m: BTreeMap<u64, String> = (0..1000).map(|i| (i * 2, format!("v{i}"))).collect();
        for i in (0..2000).step_by(7) {
            assert_eq!(c.remove(&i), m.remove(&i));
            assert_eq!(c.insert(i + 1, "x".into()), m.insert(i + 1, "x".into()));
        }
        assert_eq!(c.len(), m.len());
        assert!(c.iter().eq(m.iter()));
        assert!(c.iter().rev().eq(m.iter().rev()));
        assert!(c.range(100..=400).eq(m.range(100..=400)));
        assert!(c.range(1990..).eq(m.range(1990..)));
        assert!(c.range(5000..).eq(m.range(5000..)));
        assert_eq!(c.get(&8), m.get(&8));
        let want: u64 = m.iter().map(|(k, v)| w(k, v)).sum();
        assert_eq!(c.bytes(), want);
        let tail = c.split_off(&1500);
        let mtail = m.split_off(&1500);
        assert!(tail.iter().map(|(k, v)| (k, v)).eq(mtail.iter()));
        assert!(c.iter().eq(m.iter()));
        assert_eq!(c.bytes(), m.iter().map(|(k, v)| w(k, v)).sum::<u64>());
        c.retain(|k, _| k % 3 == 0);
        m.retain(|k, _| k % 3 == 0);
        assert!(c.iter().eq(m.iter()));
        assert_eq!(c.bytes(), m.iter().map(|(k, v)| w(k, v)).sum::<u64>());
    }

    #[test]
    fn a_version_does_not_move_with_the_map() {
        let mut c = filled(500);
        let v = c.clone();
        c.insert(3, "new".into());
        c.remove(&0);
        assert_eq!(v.get(&0).map(String::as_str), Some("v0"));
        assert_eq!(v.get(&3), None);
        assert_eq!(c.get(&3).map(String::as_str), Some("new"));
    }

    #[test]
    fn an_old_version_is_charged_only_for_what_it_holds_alone_and_only_while_held() {
        let mut c = filled(5000);
        let versions = Versions::new(c.clone());
        let held = versions.load();
        // One key changed: the held version alone holds one chunk.
        c.insert(10, "changed".into());
        versions.publish(c.clone());
        let one = versions.retained();
        let chunk_of_10 = held
            .chunks
            .iter()
            .find(|ch| ch.map.contains_key(&10))
            .unwrap()
            .bytes;
        assert_eq!(one, chunk_of_10);
        // Two versions holding the same chunks are charged them once.
        let again = versions.load();
        c.insert(9_000, "far".into());
        versions.publish(c.clone());
        assert!(versions.retained() >= one);
        assert!(versions.retained() < c.bytes());
        drop((held, again));
        assert_eq!(versions.retained(), 0);
    }
}
