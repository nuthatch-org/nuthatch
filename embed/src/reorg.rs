//! RFC-0075 S2's gate: `tests/e2e_reorg.rs`'s property, run through `nuthatch-core` on the redb hot
//! store over a byte buffer, so it runs in Node as it does natively. A chain of `CHAIN_LEN` blocks is
//! indexed, reorged above a fork drawn from the same three depth bands, and followed until the tip
//! row carries the replacement hash; the hot rows and balances must then equal a clean run over the
//! post-reorg chain, and the rollback must have resumed where the node's would.

use crate::tab::TabNest;
use crate::tape::{Block, Chain, TapeSource};
use crate::window::{block_on, token};
use anyhow::{ensure, Result};
use nuthatch_core::backend::Bytes;
use nuthatch_core::source::Log;
use proptest::strategy::{Strategy, ValueTree};
use proptest::test_runner::{Config, RngAlgorithm, TestRng, TestRunner};

/// As `tests/e2e_reorg.rs`.
pub const CHAIN_LEN: u64 = 24;
/// Blocks per window, so a checkpoint falls every fourth block and a rollback can undo more blocks
/// than the fork replaced, as the node's does.
const WINDOW: u64 = 4;
const TOKEN: &str = "0x3333333333333333333333333333333333333333";
const TRANSFER: &str = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef";

/// `tests/e2e_reorg.rs`'s `reorg_depth`, unchanged: three equally weighted bands.
pub fn reorg_depth() -> impl Strategy<Value = u64> {
    proptest::prop_oneof![
        1u64..=2,
        3u64..=(CHAIN_LEN / 2),
        (CHAIN_LEN / 2 + 1)..CHAIN_LEN,
    ]
}

pub fn band(depth: u64) -> &'static str {
    match depth {
        1..=2 => "shallow",
        d if d <= CHAIN_LEN / 2 => "mid",
        _ => "deep",
    }
}

fn account(id: u64) -> String {
    format!("0x{id:040x}")
}

fn block_hash(number: u64, variant: u64) -> String {
    format!("0x{variant:032x}{number:032x}")
}

fn transfer_block(number: u64, variant: u64, ts: u64, from: u64, to: u64, value: u64) -> Block {
    let hash = block_hash(number, variant);
    let topic = |a: &str| format!("0x{:0>64}", &a[2..]);
    Block {
        logs: vec![Log {
            address: TOKEN.into(),
            topics: vec![TRANSFER.into(), topic(&account(from)), topic(&account(to))],
            data: format!("0x{value:064x}"),
            block_number: number,
            block_hash: hash.clone(),
            tx_hash: format!("0x{:064x}", number << 20),
            log_index: 0,
        }],
        hash,
        timestamp: ts,
    }
}

fn canonical_block(b: u64) -> Block {
    transfer_block(b, 0, 1_700_000_000 + b, 1, 2, 100 * b)
}

fn replacement_block(b: u64) -> Block {
    transfer_block(b, 1, 1_700_000_500 + b, 3, 4, 7_000 + b)
}

fn chain(fork: Option<u64>) -> Chain {
    let mut c = Chain::default();
    for b in 1..=CHAIN_LEN {
        let block = match fork {
            Some(f) if b > f => replacement_block(b),
            _ => canonical_block(b),
        };
        c.blocks.insert(b, block);
    }
    c.tip = CHAIN_LEN;
    c
}

fn nest() -> Result<TabNest> {
    let db = redb::Builder::new().create_with_backend(Bytes::default())?;
    TabNest::open(
        db,
        token(),
        vec![TOKEN.into()],
        vec![TRANSFER.into()],
        1,
        WINDOW,
    )
}

/// The hot rows and balances, hashed, with the rows themselves for a failure to show.
fn state(nest: &TabNest) -> Result<(String, Vec<String>)> {
    let rows = nest.store.entities_in_range(1, CHAIN_LEN)?;
    let text = format!("{rows:?}\n{:?}", nest.balances.top(16));
    Ok((nuthatch_seal::content_hash(text.as_bytes()), rows))
}

/// `converge_after_reorg(fork)`: one case of the property, returning its line for the transcript.
pub async fn converge_after_reorg(fork: u64) -> Result<String> {
    ensure!((1..CHAIN_LEN).contains(&fork));
    let tape = TapeSource::new(chain(None));
    let reorged = nest()?;
    ensure!(
        reorged.follow(&tape).await?.is_none(),
        "rolled back before any reorg"
    );
    let ancestor = reorged
        .store
        .checkpoints_desc()?
        .into_iter()
        .map(|(b, _)| b)
        .find(|b| *b <= fork)
        .unwrap_or(0);

    tape.reorg(
        fork,
        ((fork + 1)..=CHAIN_LEN).map(replacement_block).collect(),
    );
    let resumed = reorged.follow(&tape).await?;
    ensure!(
        resumed == Some(ancestor + 1),
        "fork={fork}: resumed from {resumed:?}, want the block after checkpoint {ancestor}"
    );
    let tip_row = reorged
        .store
        .get_entity(&nuthatch_core::store::entity_key(CHAIN_LEN, 0))?
        .unwrap_or_default();
    let tip_hash = serde_json::from_str::<serde_json::Value>(&tip_row)?["block_hash"].clone();
    ensure!(
        tip_hash == serde_json::json!(block_hash(CHAIN_LEN, 1)),
        "fork={fork}: the tip row is {tip_row}, not the replacement's"
    );
    let (got, got_rows) = state(&reorged)?;

    let clean = nest()?;
    clean.follow(&TapeSource::new(chain(Some(fork)))).await?;
    let (want, want_rows) = state(&clean)?;
    ensure!(
        got_rows == want_rows && got == want,
        "fork={fork}: reorged hot state must equal a clean run over the post-reorg chain\n\
         reorged: {got_rows:?}\nclean:   {want_rows:?}"
    );
    Ok(format!(
        "depth={} band={} fork={fork} rolled_back={} digest={got}",
        CHAIN_LEN - fork,
        band(CHAIN_LEN - fork),
        CHAIN_LEN - ancestor,
    ))
}

/// `cases` depths drawn from [`reorg_depth`] by proptest's deterministic ChaCha runner, so Node and
/// native draw the same ones; every band must be among them or the run proves nothing about it.
pub fn run_property(cases: usize) -> Result<String> {
    let mut runner = TestRunner::new_with_rng(
        Config::default(),
        TestRng::deterministic_rng(RngAlgorithm::ChaCha),
    );
    let mut lines = Vec::new();
    let mut bands = std::collections::BTreeSet::new();
    let mut depths = Vec::new();
    for _ in 0..cases {
        let tree = reorg_depth().new_tree(&mut runner);
        depths.push(tree.map_err(|e| anyhow::anyhow!("{e}"))?.current());
    }
    // Each band's edges as well, so the deepest, below every checkpoint, is always among them.
    depths.extend([1, 2, 3, CHAIN_LEN / 2, CHAIN_LEN / 2 + 1, CHAIN_LEN - 1]);
    for depth in depths {
        bands.insert(band(depth));
        lines.push(block_on(converge_after_reorg(CHAIN_LEN - depth))?);
    }
    ensure!(
        bands.len() == 3,
        "only {bands:?} drawn in {cases} cases; every band must be exercised"
    );
    Ok(lines.join("\n"))
}
