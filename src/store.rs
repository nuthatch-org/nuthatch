//! The node's hot store: `nuthatch-core`'s redb store (RFC-0075 S2) with the feed's tap behind it,
//! plus the feed's snapshot reads (RFC-0073 FR1), whose receivers are tokio's and so stay out of a tab.

use anyhow::Result;
use redb::ReadableTable;
use redb::TableDefinition;
use std::sync::Arc;

pub use nuthatch_core::redb_store::*;
pub use nuthatch_core::store::{HotRowsSnapshot, HotStore as CoreHotStore, Lease};

/// The redb store as the node opens it, its commits published through the feed.
pub type Store = nuthatch_core::redb_store::Store<crate::feed::Tap>;

/// Every commit after a feed snapshot.
pub type FeedDeltas = tokio::sync::broadcast::Receiver<Arc<crate::feed::Delta>>;

/// The tables a feed snapshot carries, in its field order, and which of their keys it keeps.
type FeedTable = (
    &'static str,
    TableDefinition<'static, &'static str, &'static str>,
    fn(&str) -> bool,
);
const FEED_TABLES: [FeedTable; 4] = [
    ("entities", ENTITIES, |_| true),
    ("meta", META, crate::feed::mirrored_meta),
    ("blocks", BLOCKS, |_| true),
    ("outbox", OUTBOX, |_| true),
];

/// The hot store as the node serves it: `nuthatch-core`'s contract, which the cursor writes through,
/// plus the feed (RFC-0073 FR1), whose receivers are tokio's and so stay out of a tab.
pub trait HotStore: nuthatch_core::store::HotStore {
    /// A read-only handle that answers every read as of now, which a window's snapshot holds so a
    /// request reads rows and derived state of one window (#2467). `None` from a backend that cannot
    /// pin, whose requests read at the latest commit.
    fn pinned(&self) -> Result<Option<Arc<dyn HotStore>>> {
        Ok(None)
    }

    /// What versions taken by [`HotStore::pinned`] and still read hold beside the latest, in bytes.
    /// Zero where nothing is kept in memory per version.
    fn retained_bytes(&self) -> u64 {
        0
    }

    /// RFC-0073 FR1: a snapshot of the mirrored tables and a receiver for every commit after it, or
    /// `None` from a backend with no feed.
    fn feed_subscribe(&self) -> Result<Option<crate::feed::Subscription>> {
        Ok(None)
    }

    /// [`HotStore::feed_subscribe`] with the snapshot written to `out` as JSON as it is read.
    fn feed_stream(&self, _out: &mut dyn std::io::Write) -> Result<Option<FeedDeltas>> {
        Ok(None)
    }
}

impl HotStore for Store {
    fn pinned(&self) -> Result<Option<Arc<dyn HotStore>>> {
        Ok(Some(Arc::new(Store::pinned(self)?)))
    }

    fn feed_subscribe(&self) -> Result<Option<crate::feed::Subscription>> {
        feed_subscribe(self).map(Some)
    }

    fn feed_stream(&self, out: &mut dyn std::io::Write) -> Result<Option<FeedDeltas>> {
        feed_stream(self, out).map(Some)
    }
}

/// Every shared handle on a node store is one, as `nuthatch-core` makes it one of the core trait.
impl<T: HotStore + ?Sized> HotStore for Arc<T> {
    fn pinned(&self) -> Result<Option<Arc<dyn HotStore>>> {
        (**self).pinned()
    }
    fn retained_bytes(&self) -> u64 {
        (**self).retained_bytes()
    }
    fn feed_subscribe(&self) -> Result<Option<crate::feed::Subscription>> {
        (**self).feed_subscribe()
    }
    fn feed_stream(&self, out: &mut dyn std::io::Write) -> Result<Option<FeedDeltas>> {
        (**self).feed_stream(out)
    }
}

/// The feed's snapshot of the mirrored tables and a receiver for every commit after it.
pub fn feed_subscribe(store: &Store) -> Result<crate::feed::Subscription> {
    let (deltas, rtx, mut snapshot) = feed_begin(store)?;
    let [entities, meta, blocks, outbox] = FEED_TABLES;
    let tables = [
        (entities, &mut snapshot.entities),
        (meta, &mut snapshot.meta),
        (blocks, &mut snapshot.blocks),
        (outbox, &mut snapshot.outbox),
    ];
    for ((_, table, keep), rows) in tables {
        for row in rtx.open_table(table)?.iter()? {
            let (k, v) = row?;
            if keep(k.value()) {
                rows.map
                    .insert(k.value().to_string(), v.value().to_string());
            }
        }
    }
    Ok(crate::feed::Subscription { snapshot, deltas })
}

/// RFC-0078 §5.4: write a follower's copy into a store that holds nothing, in one transaction, so a
/// promoted standby's store holds exactly the rows the writer had committed when the copy was taken.
pub fn import_mirrored(store: &Store, tables: &crate::feed::MirroredTables) -> Result<()> {
    let wtx = store.database().begin_write()?;
    {
        let [entities, meta, blocks, outbox] = FEED_TABLES;
        for ((_, table, _), rows) in [
            (entities, &tables.entities),
            (meta, &tables.meta),
            (blocks, &tables.blocks),
            (outbox, &tables.outbox),
        ] {
            let mut t = wtx.open_table(table)?;
            for (k, v) in rows {
                t.insert(k.as_str(), v.as_str())?;
            }
        }
        // The writer's counter is its own and not mirrored; deliveries after promotion number on
        // from the last one imported, not over it.
        let next = tables
            .outbox
            .keys()
            .filter_map(|k| k.parse::<u64>().ok())
            .max()
            .map_or(0, |last| last + 1);
        wtx.open_table(META)?
            .insert("outbox_next_seq", next.to_string().as_str())?;
    }
    wtx.commit()?;
    Ok(())
}

/// [`feed_subscribe`] writing the snapshot to `out` as JSON while its read transaction iterates, so
/// no copy of it, decoded or encoded, is ever whole. The same bytes as serialising
/// [`feed_subscribe`]'s snapshot.
pub fn feed_stream(store: &Store, out: &mut dyn std::io::Write) -> Result<FeedDeltas> {
    let (deltas, rtx, head) = feed_begin(store)?;
    write!(
        out,
        "{{\"epoch\":{},\"generation\":{}",
        head.epoch, head.generation
    )?;
    for (name, table, keep) in FEED_TABLES {
        write!(out, ",\"{name}\":{{")?;
        let mut first = true;
        for row in rtx.open_table(table)?.iter()? {
            let (k, v) = row?;
            if !keep(k.value()) {
                continue;
            }
            if !std::mem::take(&mut first) {
                out.write_all(b",")?;
            }
            serde_json::to_writer(&mut *out, k.value())?;
            out.write_all(b":")?;
            serde_json::to_writer(&mut *out, v.value())?;
        }
        out.write_all(b"}")?;
    }
    out.write_all(b",\"catalogue\":")?;
    serde_json::to_writer(&mut *out, &head.catalogue)?;
    out.write_all(b"}")?;
    Ok(deltas)
}

/// Subscribe, and open the read transaction a snapshot is taken in: its position and catalogue, and
/// no rows.
fn feed_begin(store: &Store) -> Result<(FeedDeltas, redb::ReadTransaction, crate::feed::Snapshot)> {
    let mut attempts = 0;
    loop {
        let (deltas, generation, rtx) = store
            .feed()
            .subscribe(|| Ok(store.database().begin_read()?))?;
        let sealed = rtx
            .open_table(META)?
            .get("sealed_through")?
            .and_then(|v| v.value().parse().ok())
            .unwrap_or(0);
        let catalogue = store.feed().catalogue();
        // A seal between its catalogue and its commit: take the snapshot again after the commit.
        if crate::feed::catalogue_ahead(catalogue.as_deref(), sealed) && attempts < 400 {
            attempts += 1;
            drop(rtx);
            std::thread::sleep(std::time::Duration::from_millis(5));
            continue;
        }
        let snapshot = crate::feed::Snapshot {
            epoch: store.feed().epoch(),
            generation,
            catalogue,
            ..Default::default()
        };
        return Ok((deltas, rtx, snapshot));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// RFC-0078 §5.4: a promoted standby's store holds the same mirrored rows as the writer's.
    #[test]
    fn importing_a_followed_copy_reproduces_the_writers_tables() {
        let a = tempfile::tempdir().unwrap();
        let writer = Store::open(&a.path().join("w.redb")).unwrap();
        let rows: Vec<(String, String)> = (1..=5)
            .map(|n| {
                (
                    Store::entity_key(n, 0),
                    format!(r#"{{"table":"t","block_number":{n}}}"#),
                )
            })
            .collect();
        writer.commit_window(&rows, Some((5, "0xaa")), 5).unwrap();
        writer.set_meta("sealed_through", "0").unwrap();
        writer.claim("old-writer").unwrap();
        let copy = crate::feed::MirroredTables::of(&feed_subscribe(&writer).unwrap().snapshot);

        let b = tempfile::tempdir().unwrap();
        let promoted = Store::open(&b.path().join("p.redb")).unwrap();
        import_mirrored(&promoted, &copy).unwrap();
        let again = crate::feed::MirroredTables::of(&feed_subscribe(&promoted).unwrap().snapshot);
        assert_eq!(again, copy);
        assert_eq!(promoted.count().unwrap(), 5);
        assert_eq!(
            promoted.current_fence().unwrap(),
            0,
            "the old writer's fence is not carried"
        );
    }

    /// RFC-0078 §5.5: deliveries the writer had not made reach a follower, by snapshot and by delta,
    /// and a store promoted from it delivers them and numbers the next one after them.
    #[test]
    fn the_outbox_is_followed_and_promoted() {
        let a = tempfile::tempdir().unwrap();
        let writer = Store::open(&a.path().join("w.redb")).unwrap();
        writer.outbox_push(r#"{"alert":1}"#).unwrap();
        let sub = feed_subscribe(&writer).unwrap();
        let copy = crate::feed::MemStore::from_snapshot(a.path(), sub.snapshot);
        let mut deltas = sub.deltas;
        writer.outbox_push(r#"{"alert":2}"#).unwrap();
        writer.outbox_push(r#"{"alert":3}"#).unwrap();
        writer.outbox_remove(0).unwrap();
        while let Ok(delta) = deltas.try_recv() {
            copy.apply(&delta).unwrap();
        }
        let followed = copy.tables();
        assert_eq!(
            followed,
            crate::feed::MirroredTables::of(&feed_subscribe(&writer).unwrap().snapshot)
        );
        assert_eq!(followed.outbox.len(), 2, "{:?}", followed.outbox);

        let b = tempfile::tempdir().unwrap();
        let promoted = Store::open(&b.path().join("p.redb")).unwrap();
        import_mirrored(&promoted, &followed).unwrap();
        let pending: Vec<u64> = promoted
            .outbox_pending(10)
            .unwrap()
            .into_iter()
            .map(|(seq, _)| seq)
            .collect();
        assert_eq!(pending, vec![1, 2]);
        assert_eq!(
            promoted.outbox_push(r#"{"alert":4}"#).unwrap(),
            3,
            "numbered after the imported deliveries"
        );
    }
}
