//! User webhooks (RFC-0010 Part B): POST rows of an event table matching a predicate to a URL as they
//! seal. This is the *second producer* of the one shared delivery engine (RFC-0010 §Reconciliation) -
//! the compliance alerts (RFC-0008 C5) are the first. Both just push a `{..., url}` payload onto the
//! durable outbox in the hot store; the same background worker ([`crate::alerts::run_delivery_worker`])
//! drains it, at-least-once, and never blocks indexing. No new engine - webhooks are a producer.
//!
//! Delivery is on the **sealed** path (finality-gated: a sealed webhook never lies). Each webhook
//! carries a cursor - the highest block it has delivered - persisted in the hot store. On first
//! registration the cursor initialises per `since`: `"registration"` (default) starts it at the tip,
//! so a `--seal-direct` backfill sealing the entire history does *not* fire millions of historical
//! rows; only rows sealed after registration deliver. `"genesis"` or a block number replay history
//! deliberately.

use crate::config::Webhook;
use crate::store::HotStore;
use anyhow::Result;
use serde_json::json;

/// Default rows per delivery POST when a webhook doesn't set `batch_max`.
pub const DEFAULT_BATCH_MAX: usize = 50;

/// HMAC-SHA256 of `msg` under `key`, hex-encoded (RFC 2104). Hand-rolled over `sha2` (already a
/// dependency) rather than pulling the `hmac` crate - it's a small, well-defined construction. The
/// delivery worker signs each webhook body so the receiver can verify it came from this nest.
pub fn hmac_sha256_hex(key: &[u8], msg: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    const BLOCK: usize = 64;
    let mut k = [0u8; BLOCK];
    if key.len() > BLOCK {
        k[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        k[..key.len()].copy_from_slice(key);
    }
    let mut inner = Sha256::new();
    inner.update(k.iter().map(|b| b ^ 0x36).collect::<Vec<u8>>());
    inner.update(msg);
    let inner = inner.finalize();
    let mut outer = Sha256::new();
    outer.update(k.iter().map(|b| b ^ 0x5c).collect::<Vec<u8>>());
    outer.update(inner);
    hex::encode(outer.finalize())
}

/// The per-webhook HMAC secrets, by webhook name - the delivery worker uses this to sign a payload
/// whose `webhook` field names a secret-carrying webhook.
pub fn secrets(webhooks: &[Webhook]) -> std::collections::HashMap<String, String> {
    webhooks
        .iter()
        .filter_map(|w| w.secret.clone().map(|s| (w.name.clone(), s)))
        .collect()
}

fn cursor_key(name: &str) -> String {
    format!("webhook_cursor_{name}")
}

/// Whether a webhook delivers on the sealed path (the default and only path in this step; `"tip"`
/// finality lands in a later step with retraction deliveries).
fn is_sealed(w: &Webhook) -> bool {
    w.finality.as_deref().unwrap_or("sealed") == "sealed"
}

/// Initialise each webhook's cursor on first registration (no stored cursor), per its `since`. Called
/// once at startup with the current chain `tip`. `"registration"` → tip (suppresses backfill history);
/// `"genesis"` → 0; a number → that block. Idempotent: a webhook with a stored cursor is left alone.
pub fn init_cursors(store: &dyn HotStore, webhooks: &[Webhook], tip: u64) -> Result<()> {
    for w in webhooks {
        if store.get_meta(&cursor_key(&w.name))?.is_some() {
            continue;
        }
        let start = match w.since.as_deref() {
            Some("genesis") => 0,
            Some(other) if other != "registration" => other.parse::<u64>().unwrap_or(tip),
            _ => tip, // "registration" (default)
        };
        store.set_meta(&cursor_key(&w.name), &start.to_string())?;
        tracing::info!(
            "webhook '{}' registered at block {start} → {}",
            w.name,
            crate::rpc::redact_url(&w.url)
        );
    }
    Ok(())
}

/// Deliver newly-sealed rows for every sealed webhook up to `sealed_to`: for each, query its table's
/// rows in `(cursor, sealed_to]` matching its `where`, enqueue them (batched) onto the outbox, and
/// advance the cursor. Returns the number of deliveries enqueued. Best-effort per webhook - a table
/// with no sealed segment yet just yields nothing.
pub fn deliver_sealed(
    store: &dyn HotStore,
    dir: &std::path::Path,
    webhooks: &[Webhook],
    sealed_to: u64,
) -> Result<usize> {
    let mut enqueued = 0usize;
    for w in webhooks.iter().filter(|w| is_sealed(w)) {
        let cursor: u64 = store
            .get_meta(&cursor_key(&w.name))?
            .and_then(|s| s.parse().ok())
            .unwrap_or(sealed_to);
        if cursor >= sealed_to {
            continue;
        }
        // `table`/`where` are operator-authored (trusted, like nest views); block bounds are ours.
        let predicate = w
            .where_clause
            .as_deref()
            .map(|p| format!(" AND ({p})"))
            .unwrap_or_default();
        let sql = format!(
            "SELECT * FROM \"{}\" WHERE block_number > {cursor} AND block_number <= {sealed_to}{predicate} \
             ORDER BY block_number, log_index",
            w.table
        );
        let rows = match crate::analytics::query(dir, &sql) {
            Ok(r) => r,
            Err(e) => {
                tracing::debug!("webhook '{}' query skipped: {e:#}", w.name);
                continue;
            }
        };
        let batch_max = w.batch_max.unwrap_or(DEFAULT_BATCH_MAX).max(1);
        for chunk in rows.chunks(batch_max) {
            let payload = json!({
                "event": "rows",
                "webhook": w.name,
                "url": w.url,
                "finality": "sealed",
                "table": w.table,
                "count": chunk.len(),
                "rows": chunk,
            });
            store.outbox_push(&payload.to_string())?;
            enqueued += 1;
        }
        store.set_meta(&cursor_key(&w.name), &sealed_to.to_string())?;
        if !rows.is_empty() {
            tracing::info!(
                "webhook '{}': {} row(s) sealed in ({cursor}, {sealed_to}] → queued",
                w.name,
                rows.len()
            );
        }
    }
    let _ = store.outbox_trim(crate::alerts::OUTBOX_MAX);
    Ok(enqueued)
}

/// Refuse a webhook whose `table` or `where` does not bind against the nest's declared schema (#2365).
/// The delivery query would fail the same way on every seal, and the webhook would never fire.
pub fn refuse_unbound(
    dir: &std::path::Path,
    declared: &[crate::registry::TableSchema],
    webhooks: &[Webhook],
) -> Result<()> {
    let plan = |sql: &str| {
        let guard = crate::analytics::QueryGuard {
            timeout: std::time::Duration::from_secs(10),
            max_rows: 1,
        };
        crate::analytics::plan_hot_cold(
            dir,
            sql,
            guard,
            &crate::analytics::HotRows::new(),
            u64::MAX,
            declared,
        )
    };
    for w in webhooks {
        let base = format!(
            "SELECT * FROM \"{}\" WHERE block_number > 0 ORDER BY block_number, log_index",
            w.table
        );
        if let Err(e) = plan(&base) {
            anyhow::bail!(
                "webhook '{}': `table = \"{}\"` is not a table this nest can deliver from: {e:#}",
                w.name,
                w.table
            );
        }
        if let Some(p) = &w.where_clause {
            let sql = format!(
                "SELECT * FROM \"{}\" WHERE block_number > 0 AND ({p}) ORDER BY block_number, log_index",
                w.table
            );
            if let Err(e) = plan(&sql) {
                anyhow::bail!(
                    "webhook '{}': `where = \"{p}\"` does not bind against `{}`: {e:#}",
                    w.name,
                    w.table
                );
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;

    /// HMAC-SHA256 known-answer (the standard RFC test vector), so the hand-rolled construction is
    /// provably correct.
    #[test]
    fn hmac_matches_the_reference_vector() {
        let got = hmac_sha256_hex(b"key", b"The quick brown fox jumps over the lazy dog");
        assert_eq!(
            got,
            "f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"
        );
    }

    fn wh(name: &str, table: &str, since: &str) -> Webhook {
        Webhook {
            name: name.into(),
            table: table.into(),
            where_clause: None,
            url: "http://127.0.0.1:9/hook".into(),
            batch_max: None,
            finality: None,
            since: Some(since.into()),
            secret: None,
        }
    }

    /// The RFC-0010 correctness catch: a `since = "registration"` webhook registered at the tip does
    /// NOT fire for a `--seal-direct` backfill (history sealed below the cursor), but DOES fire for a
    /// row sealed after registration.
    #[test]
    fn registration_since_suppresses_backfill_history() {
        let dir = tempfile::tempdir().unwrap();
        // The two seals below fold into one segment under the table floor (#1150), which is the
        // shape that aborted the dev-profile DuckDB before `analytics` switched off compressed
        // materialisation in debug builds (#1152). This test runs on one folded file on purpose:
        // it is the regression test for that guard as much as for the cursor.
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let webhooks = vec![wh("w", "t__transfer", "registration")];
        // Registered at tip 100.
        init_cursors(&store, &webhooks, 100).unwrap();

        // Seal a t__transfer segment covering historical blocks 10..=50 (below the cursor).
        let rows: Vec<String> = (10..=50)
            .map(|b| format!(r#"{{"table":"t__transfer","from":"0xa","to":"0xb","value":"1","block_number":{b},"log_index":0,"tx_hash":"0xt"}}"#))
            .collect();
        crate::seal::seal_range(dir.path(), &rows, 10, 50).unwrap();

        // A backfill that sealed up to block 50 delivers nothing (all history < the registration tip).
        let n = deliver_sealed(&store, dir.path(), &webhooks, 50).unwrap();
        assert_eq!(
            n, 0,
            "registration cursor suppresses historical backfill rows"
        );
        assert_eq!(store.outbox_len(), 0);

        // Now a row seals at block 105 (after registration) → it delivers.
        let post = vec![r#"{"table":"t__transfer","from":"0xa","to":"0xb","value":"1","block_number":105,"log_index":0,"tx_hash":"0xt"}"#.to_string()];
        crate::seal::seal_range(dir.path(), &post, 101, 105).unwrap();
        let n = deliver_sealed(&store, dir.path(), &webhooks, 105).unwrap();
        assert_eq!(n, 1, "a row sealed after registration delivers");
        assert_eq!(store.outbox_len(), 1);
    }

    /// `since = "genesis"` replays history; `where` filters; `batch_max` chunks the deliveries.
    #[test]
    fn genesis_replays_history_with_where_and_batching() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(&dir.path().join("t.redb")).unwrap();
        let mut w = wh("big", "t__transfer", "genesis");
        w.where_clause = Some("CAST(value AS HUGEINT) >= 100".into());
        w.batch_max = Some(2);
        let webhooks = vec![w];
        init_cursors(&store, &webhooks, 1000).unwrap();

        // Five rows; three match the predicate (value ≥ 100).
        let rows: Vec<String> = [("10", "5"), ("11", "200"), ("12", "50"), ("13", "300"), ("14", "150")]
            .iter()
            .map(|(b, v)| format!(r#"{{"table":"t__transfer","from":"0xa","to":"0xb","value":"{v}","block_number":{b},"log_index":0,"tx_hash":"0xt"}}"#))
            .collect();
        crate::seal::seal_range(dir.path(), &rows, 10, 14).unwrap();

        let n = deliver_sealed(&store, dir.path(), &webhooks, 14).unwrap();
        // 3 matching rows, batched by 2 → 2 deliveries (a batch of 2 and a batch of 1).
        assert_eq!(n, 2, "3 matching rows batched by 2 → 2 outbox deliveries");
        assert_eq!(store.outbox_len(), 2);
        // Cursor advanced, so a re-run delivers nothing more.
        assert_eq!(
            deliver_sealed(&store, dir.path(), &webhooks, 14).unwrap(),
            0
        );
    }

    /// #2365: a webhook whose `table` or `where` cannot bind is refused at registration, by field,
    /// before any segment exists, instead of failing at debug on every seal.
    #[test]
    fn an_unbound_table_or_where_is_refused_by_field() {
        let dir = tempfile::tempdir().unwrap();
        let mut columns = crate::registry::implicit_columns(false);
        columns.push(crate::registry::ColumnSchema {
            name: "value".into(),
            sol_type: "uint256".into(),
            storage: "varchar".into(),
            indexed: false,
            components: Vec::new(),
        });
        let declared = vec![crate::registry::TableSchema {
            table: "t__transfer".into(),
            alias: "t".into(),
            kind: crate::registry::TableKind::Event,
            event: "Transfer".into(),
            topic0: "0x".into(),
            function: String::new(),
            selector: String::new(),
            columns,
        }];
        let mut good = wh("w", "t__transfer", "registration");
        good.where_clause = Some("CAST(value AS HUGEINT) > 100".into());
        refuse_unbound(dir.path(), &declared, &[good]).expect("a webhook that binds is accepted");

        let err = format!(
            "{:#}",
            refuse_unbound(
                dir.path(),
                &declared,
                &[wh("w", "t__transfr", "registration")]
            )
            .unwrap_err()
        );
        assert!(err.contains("`table = \"t__transfr\"`"), "{err}");

        let mut bad = wh("w", "t__transfer", "registration");
        bad.where_clause = Some("vlaue > 100".into());
        let err = format!(
            "{:#}",
            refuse_unbound(dir.path(), &declared, &[bad]).unwrap_err()
        );
        assert!(err.contains("`where = \"vlaue > 100\"`"), "{err}");
    }
}
