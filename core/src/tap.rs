//! What a hot-store commit tells whoever mirrors it (RFC-0073 FR1). The node's tap broadcasts each
//! commit's ops to its followers; a tab has none, so its tap only numbers commits.

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

/// One change to the mirrored tables, in the order the commit made it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    Put {
        key: String,
        row: String,
    },
    /// Entity keys in `from..=to`: a seal's prune. Absent `to`, from `from` to the end: a reorg's
    /// rollback, the only open-ended drop the store makes, which a follower's derived state retracts.
    Drop {
        from: String,
        to: Option<String>,
    },
    Meta {
        key: String,
        value: String,
    },
    Unmeta {
        key: String,
    },
    Block {
        key: String,
        record: String,
    },
    Unblock {
        key: String,
    },
    /// Every checkpoint at or above `key`.
    UnblockFrom {
        key: String,
    },
}

/// Meta keys that describe the writer's own process rather than the data: its outbox counter and its
/// ownership lease. Neither is mirrored, so a reader cannot be mistaken for the owner.
pub fn mirrored_meta(key: &str) -> bool {
    !matches!(
        key,
        crate::redb_store::OWNER_FENCE
            | crate::redb_store::LEASE_OWNER
            | crate::redb_store::LEASE_EXPIRES_AT
            | "owner"
            | "outbox_next_seq"
    )
}

/// Ops gathered inside one write transaction, or `None` when nothing is subscribed and gathering
/// them would only copy rows nobody reads.
pub struct Recorder(Option<Vec<Op>>, bool);

impl Recorder {
    pub fn new(gather: bool) -> Recorder {
        Recorder(gather.then(Vec::new), false)
    }

    /// A meta write. One to `sealed_through` is noted whether or not anyone is subscribed, since
    /// segments kept for followers wait on the commit that carries it.
    pub fn meta(&mut self, key: &str, value: &str) {
        self.1 |= key == "sealed_through";
        self.push(|| Op::Meta {
            key: key.to_string(),
            value: value.to_string(),
        });
    }

    pub fn seals(&self) -> bool {
        self.1
    }

    pub fn into_ops(self) -> Option<Vec<Op>> {
        self.0
    }

    pub fn push(&mut self, op: impl FnOnce() -> Op) {
        if let Some(ops) = &mut self.0 {
            let op = op();
            if let Op::Meta { key, .. } | Op::Unmeta { key } = &op {
                if !mirrored_meta(key) {
                    return;
                }
            }
            ops.push(op);
        }
    }
}

/// The store's side of a feed: a recorder per write transaction, and each commit run through it so
/// it can be numbered and published.
pub trait Feed: Send + Sync + 'static {
    /// The tap for a store whose dataset directory is `dir`, where it has one.
    fn open(dir: Option<&Path>) -> Self
    where
        Self: Sized;
    fn recorder(&self) -> Recorder;
    fn commit(&self, rec: Recorder, commit: &mut dyn FnMut() -> Result<()>) -> Result<()>;
    fn epoch(&self) -> u64;
    fn generation(&self) -> u64;
}

/// A store nobody follows: commits are numbered and nothing is gathered.
#[derive(Default)]
pub struct NoFeed(AtomicU64);

impl Feed for NoFeed {
    fn open(_: Option<&Path>) -> NoFeed {
        NoFeed::default()
    }
    fn recorder(&self) -> Recorder {
        Recorder::new(false)
    }
    fn commit(&self, _: Recorder, commit: &mut dyn FnMut() -> Result<()>) -> Result<()> {
        commit()?;
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    fn epoch(&self) -> u64 {
        0
    }
    fn generation(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
