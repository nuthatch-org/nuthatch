//! RFC-0075 S2: a recorded chain a tab plays to `nuthatch-core` as its [`Source`], with reorgs
//! scripted onto it. The page hands it over as JSON; the reorg gate builds it in code.

use anyhow::Result;
use nuthatch_core::source::{Log, LogFilter, Source};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;

/// One block as recorded: its hash, header timestamp and every log it emitted.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Block {
    pub hash: String,
    pub timestamp: u64,
    pub logs: Vec<Log>,
}

/// The chain as the tape holds it now: blocks by number, the tip it reports and its finalized tag.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Chain {
    pub blocks: BTreeMap<u64, Block>,
    pub tip: u64,
    #[serde(default)]
    pub finalized: Option<u64>,
}

#[derive(Default)]
pub struct TapeSource(Mutex<Chain>);

impl TapeSource {
    pub fn new(chain: Chain) -> TapeSource {
        TapeSource(Mutex::new(chain))
    }

    pub fn from_json(json: &str) -> Result<TapeSource> {
        Ok(TapeSource::new(serde_json::from_str(json)?))
    }

    fn chain(&self) -> std::sync::MutexGuard<'_, Chain> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub fn insert(&self, number: u64, block: Block) {
        let mut c = self.chain();
        c.blocks.insert(number, block);
        c.tip = c.tip.max(number);
    }

    /// Replace every block above `fork` with `replacement`, numbered from `fork + 1`; the tip follows.
    pub fn reorg(&self, fork: u64, replacement: Vec<Block>) {
        let mut c = self.chain();
        c.blocks.split_off(&(fork + 1));
        c.tip = fork;
        for (i, block) in replacement.into_iter().enumerate() {
            let number = fork + 1 + i as u64;
            c.blocks.insert(number, block);
            c.tip = number;
        }
    }
}

fn wanted(want: &[String], got: &str) -> bool {
    want.is_empty() || want.iter().any(|w| w.eq_ignore_ascii_case(got))
}

#[async_trait::async_trait]
impl Source for TapeSource {
    async fn tip(&self) -> Result<u64> {
        Ok(self.chain().tip)
    }

    async fn block_hash(&self, number: u64) -> Result<Option<String>> {
        Ok(self.chain().blocks.get(&number).map(|b| b.hash.clone()))
    }

    async fn finalized(&self) -> Result<Option<u64>> {
        Ok(self.chain().finalized)
    }

    async fn block_timestamps(&self, blocks: &[u64]) -> Result<HashMap<u64, u64>> {
        let c = self.chain();
        Ok(blocks
            .iter()
            .filter_map(|b| c.blocks.get(b).map(|f| (*b, f.timestamp)))
            .collect())
    }

    async fn logs(&self, filter: &LogFilter, from: u64, to: u64) -> Result<Vec<Log>> {
        let c = self.chain();
        Ok(c.blocks
            .range(from..=to)
            .flat_map(|(_, b)| &b.logs)
            .filter(|l| wanted(filter.addresses(), &l.address))
            .filter(|l| {
                l.topics
                    .first()
                    .is_some_and(|t| wanted(filter.topic0s(), t))
            })
            .cloned()
            .collect())
    }
}
