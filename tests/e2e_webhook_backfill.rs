//! A `since = "genesis"` webhook on a plain backfill delivers every row that seals, in order, once.
//!
//! The chain's finality is far ahead of the backfill from the first window, so a delivery bounded by
//! finality rather than by what the nest has sealed walks its cursor over blocks that are not
//! sealed yet and never comes back for them. Drives the real `spawn_nest` loop over a tape, with
//! the finalized block at the tip from the start and sealing cutting at padded blocks.

mod common;

use std::sync::Arc;

use nuthatch::config::Webhook;
use nuthatch::{analytics, indexer};

use common::tape::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_genesis_webhook_on_a_plain_backfill_misses_no_sealed_row() {
    let dir = tempfile::tempdir().unwrap();
    let mut cfg = scaffold_nest(dir.path(), "usdc", USDC);
    cfg.webhooks.push(Webhook {
        name: "all".into(),
        table: transfer_table("usdc"),
        where_clause: None,
        url: "http://127.0.0.1:9/hook".into(),
        batch_max: Some(1_000),
        finality: None,
        since: Some("genesis".into()),
        secret: None,
    });
    let tape = Arc::new(TapeSource::new());
    let (a1, a2) = (account(1), account(2));
    for b in 1..=20u64 {
        let mut fx = transfers_block(
            b,
            0,
            1_700_000_000 + b,
            USDC,
            &[(a1.as_str(), a2.as_str(), (100 * b) as u128)],
        );
        // Two cuts, so rows 6..=15 seal after the first window already saw finality at 20.
        if b == 5 {
            pad_address_to_seal(&mut fx, USDC, 4);
        }
        if b == 15 {
            pad_address_to_seal(&mut fx, USDC, 9);
        }
        tape.insert_block(b, fx);
    }
    tape.advance_tip_to(20);
    tape.advance_finalized_to(19);

    // Windows of five blocks: finality is 19 while the nest is still at block 5.
    let rt = indexer::spawn_nest(
        tape.clone(),
        dir.path().to_path_buf(),
        cfg,
        None,
        false,
        1,
        Some(5),
        false,
        None,
    )
    .await
    .expect("spawn_nest");
    let store = rt.state.store.clone();
    assert!(
        wait_until(std::time::Duration::from_secs(40), || store
            .sealed_through()
            >= 15)
        .await,
        "sealed only through {} (last_block {:?})",
        store.sealed_through(),
        store.get_meta("last_block")
    );

    let table = transfer_table("usdc");
    let sealed = analytics::query(
        dir.path(),
        &format!(
            "SELECT block_number, log_index FROM \"{table}\" ORDER BY block_number, log_index"
        ),
    )
    .unwrap();
    let want: Vec<(u64, u64)> = sealed
        .iter()
        .map(|r| {
            (
                r["block_number"].as_u64().unwrap(),
                r["log_index"].as_u64().unwrap(),
            )
        })
        .collect();
    assert!(
        want.iter().any(|(b, _)| *b > 5),
        "the second cut sealed rows past block 5"
    );

    let delivered = || -> Vec<(u64, u64)> {
        store
            .outbox_pending(usize::MAX)
            .unwrap()
            .iter()
            .flat_map(|(_, p)| {
                let v: serde_json::Value = serde_json::from_str(p).unwrap();
                v["rows"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .map(|r| {
                        (
                            r["block_number"].as_u64().unwrap(),
                            r["log_index"].as_u64().unwrap(),
                        )
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    };
    let caught_up = wait_until(POLL_TIMEOUT, || delivered().len() >= want.len()).await;
    let got = delivered();
    assert!(
        caught_up && got == want,
        "{} of {} sealed rows queued, first missing block: {:?}",
        got.len(),
        want.len(),
        want.iter().find(|w| !got.contains(w)).map(|w| w.0)
    );
}
