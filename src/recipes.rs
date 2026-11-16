//! RFC-0023 tier 1 - **derive-first recipes.** The Foundation says >70% of subgraphs use `eth_call`,
//! but most of those reads are *derivable* from the events a nest already indexes - subgraphs fetch
//! them only because they have no incremental-view engine. Nuthatch does (the DBSP/IVM core, and
//! authored SQL views over the tip ∪ sealed surface), so a recipe here **replaces an eth_call with a
//! derivation**: deterministic, free, no archive node, and a capability subgraphs structurally lack.
//!
//! A recipe is an authored `CREATE VIEW` (RFC-0018 §1) over a nest's decoded tables. `nuthatch recipe
//! add <name>` drops one into the nest's `views/`. The flagship is `total_supply` - the ERC-20
//! `totalSupply()` computed as Σ minted − Σ burned (Transfers from / to the zero address).

use crate::config::Config;
use anyhow::{bail, Result};
use std::path::Path;

/// The zero address - an ERC-20 mint's `from` and a burn's `to`.
pub const ZERO_ADDRESS: &str = "0x0000000000000000000000000000000000000000";

/// One derive-first recipe: a name and what eth_call it replaces.
pub struct Recipe {
    pub name: &'static str,
    pub about: &'static str,
}

/// The recipe library. Grows as more derivable reads are covered (reserves, …).
pub const RECIPES: &[Recipe] = &[
    Recipe {
        name: "total_supply",
        about: "ERC-20 totalSupply(), derived from Transfer mints/burns - no eth_call",
    },
    Recipe {
        name: "balances",
        about: "per-address token balance (current holders), derived from Transfers - no eth_call",
    },
    Recipe {
        name: "holder_count",
        about: "number of non-zero holders, derived from Transfers - no eth_call",
    },
    Recipe {
        name: "frozen",
        about: "addresses a FiatToken-style issuer has blacklisted now, with their balances (#2943)",
    },
    Recipe {
        name: "outstanding",
        about: "supply less frozen and treasury-labelled balances, as an attestation counts it (#2943)",
    },
    Recipe {
        name: "mint_burn_daily",
        about: "per UTC day: minted, burned, the Mint and Burn events beside them, and supply (#2943)",
    },
    Recipe {
        name: "reserves",
        about: "Uniswap-V2 getReserves(): the latest Sync per pair (needs a `Sync` event) - no eth_call",
    },
];

/// The `SELECT` body of the `reserves` recipe: each pair's current reserves - the most recent `Sync`
/// event per contract address. This is exactly what Uniswap-V2 `getReserves()` returns, derived from
/// the `Sync(uint112,uint112)` events already indexed. Needs the nest to index the `Sync` event
/// (`{alias}__sync`).
pub fn reserves_select(alias: &str) -> String {
    format!(
        "SELECT address, \
           TRY_CAST(reserve0 AS HUGEINT) AS reserve0, \
           TRY_CAST(reserve1 AS HUGEINT) AS reserve1 \
         FROM (SELECT address, reserve0, reserve1, \
                 ROW_NUMBER() OVER (PARTITION BY address ORDER BY block_number DESC, log_index DESC) AS rn \
               FROM \"{alias}__sync\") \
         WHERE rn = 1"
    )
}

/// A per-address net-balance subquery over `{alias}__transfer`: +value to `to`, −value from `from`
/// (the same Σ(in) − Σ(out) the IVM balance view maintains). The building block for `balances` and
/// `holder_count`.
fn net_balance_subquery(alias: &str) -> String {
    let t = format!("{alias}__transfer");
    let d = crate::analytics::exact_or_null("\"value\"", "HUGEINT");
    format!(
        "SELECT addr, SUM(d) AS balance FROM (\
           SELECT lower(\"to\") AS addr, {d} AS d FROM \"{t}\" \
           UNION ALL \
           SELECT lower(\"from\") AS addr, -{d} AS d FROM \"{t}\"\
         ) GROUP BY addr"
    )
}

/// The `SELECT` body of the `balances` recipe: each non-zero holder and its current balance.
pub fn balances_select(alias: &str) -> String {
    format!(
        "SELECT addr, balance FROM ({}) WHERE addr <> '{ZERO_ADDRESS}' AND balance <> 0",
        net_balance_subquery(alias)
    )
}

/// The `SELECT` body of the `holder_count` recipe: how many non-zero holders there are.
pub fn holder_count_select(alias: &str) -> String {
    format!(
        "SELECT COUNT(*) AS holders FROM ({}) WHERE addr <> '{ZERO_ADDRESS}' AND balance <> 0",
        net_balance_subquery(alias)
    )
}

/// The `SELECT` body of the `total_supply` recipe over a contract's `{alias}__transfer` table:
/// Σ(value where `from` = 0x0) − Σ(value where `to` = 0x0). Exposed so it can be queried directly (and
/// tested) as well as wrapped in a `CREATE VIEW`.
pub fn total_supply_select(alias: &str) -> String {
    let d = crate::analytics::exact_or_null("\"value\"", "HUGEINT");
    format!(
        "SELECT \
           COALESCE(SUM(CASE WHEN lower(\"from\") = '{ZERO_ADDRESS}' \
             THEN {d} ELSE 0 END), 0) \
         - COALESCE(SUM(CASE WHEN lower(\"to\") = '{ZERO_ADDRESS}' \
             THEN {d} ELSE 0 END), 0) \
         AS total_supply FROM \"{alias}__transfer\""
    )
}

/// The `SELECT` body of the `frozen` recipe: every address whose latest blacklist event is
/// `Blacklisted`, with its balance and the block it was frozen at. Needs `{alias}__blacklisted` and
/// `{alias}__un_blacklisted` (Circle's FiatToken and its copies).
pub fn frozen_select(alias: &str) -> String {
    format!(
        "SELECT f.account, COALESCE(b.balance, 0) AS balance, f.block_number AS frozen_at FROM (\
           SELECT account, kind, block_number, \
             ROW_NUMBER() OVER (PARTITION BY account ORDER BY block_number DESC, log_index DESC) AS rn \
           FROM (\
             SELECT lower(_account) AS account, 'blacklisted' AS kind, block_number, log_index \
             FROM \"{alias}__blacklisted\" \
             UNION ALL \
             SELECT lower(_account) AS account, 'unblacklisted' AS kind, block_number, log_index \
             FROM \"{alias}__un_blacklisted\"\
           )\
         ) f LEFT JOIN ({}) b ON b.addr = f.account \
         WHERE f.rn = 1 AND f.kind = 'blacklisted'",
        net_balance_subquery(alias)
    )
}

/// The label that marks an issuer's own unissued or treasury holdings for the `outstanding` recipe.
pub const TREASURY_LABEL: &str = "treasury";

/// The `SELECT` body of the `outstanding` recipe: total supply, less frozen balances, less what
/// addresses labelled `treasury` hold that is not frozen. The attestation definition, e.g. Circle's
/// "USDC in circulation" net of tokens allowed but not issued and access-denied tokens.
pub fn outstanding_select(alias: &str) -> String {
    let frozen = frozen_select(alias);
    format!(
        "SELECT s.total_supply, z.frozen, t.treasury, \
           s.total_supply - z.frozen - t.treasury AS outstanding \
         FROM ({}) s \
         CROSS JOIN (SELECT COALESCE(SUM(balance), 0) AS frozen FROM ({frozen})) z \
         CROSS JOIN (SELECT COALESCE(SUM(b.balance), 0) AS treasury FROM ({}) b \
           WHERE b.addr IN (SELECT lower(address) FROM labels WHERE label = '{TREASURY_LABEL}') \
             AND b.addr NOT IN (SELECT account FROM ({frozen}))) t",
        total_supply_select(alias),
        net_balance_subquery(alias)
    )
}

/// The `SELECT` body of the `mint_burn_daily` recipe: per UTC day, what was minted and burned by
/// zero-address transfers, the `Mint` and `Burn` event amounts beside them (a difference is worth
/// explaining), and supply at the day's end.
pub fn mint_burn_daily_select(alias: &str) -> String {
    let d = crate::analytics::exact_or_null("\"value\"", "HUGEINT");
    let amount = crate::analytics::exact_or_null("amount", "HUGEINT");
    // Epoch arithmetic, so the day is UTC whatever the session time zone.
    let day = "DATE '1970-01-01' + CAST(block_timestamp // 86400 AS INTEGER)";
    format!(
        "SELECT day, minted, burned, mint_events, burn_events, \
           SUM(minted - burned) OVER (ORDER BY day) AS supply \
         FROM (\
           SELECT day, SUM(minted) AS minted, SUM(burned) AS burned, \
             SUM(mint_events) AS mint_events, SUM(burn_events) AS burn_events \
           FROM (\
             SELECT {day} AS day, \
               CASE WHEN lower(\"from\") = '{ZERO_ADDRESS}' THEN {d} ELSE 0 END AS minted, \
               CASE WHEN lower(\"to\") = '{ZERO_ADDRESS}' THEN {d} ELSE 0 END AS burned, \
               CAST(0 AS HUGEINT) AS mint_events, CAST(0 AS HUGEINT) AS burn_events \
             FROM \"{alias}__transfer\" \
             WHERE lower(\"from\") = '{ZERO_ADDRESS}' OR lower(\"to\") = '{ZERO_ADDRESS}' \
             UNION ALL \
             SELECT {day}, 0, 0, {amount}, 0 FROM \"{alias}__mint\" \
             UNION ALL \
             SELECT {day}, 0, 0, 0, {amount} FROM \"{alias}__burn\"\
           ) GROUP BY day\
         ) ORDER BY day"
    )
}

/// The authored `CREATE VIEW` SQL for a recipe over the given contract alias.
pub fn view_sql(name: &str, alias: &str) -> Result<String> {
    match name {
        "total_supply" => Ok(format!(
            "-- Recipe: total_supply (RFC-0023 tier 1) - the ERC-20 `totalSupply()` a subgraph fetches\n\
             -- via eth_call, DERIVED here from the Transfer events already indexed: Σ minted − Σ burned\n\
             -- (transfers from / to the zero address). Deterministic and free - no eth_call, no archive\n\
             -- node. Query it: SELECT total_supply FROM {alias}_total_supply\n\
             CREATE VIEW {alias}_total_supply AS\n{};\n",
            total_supply_select(alias)
        )),
        "balances" => Ok(format!(
            "-- Recipe: balances (RFC-0023 tier 1) - each address's current token balance, DERIVED from\n\
             -- the Transfer events already indexed as Σ(in) − Σ(out). No eth_call `balanceOf` per address.\n\
             -- Query it: SELECT * FROM {alias}_balances ORDER BY balance DESC\n\
             CREATE VIEW {alias}_balances AS\n{};\n",
            balances_select(alias)
        )),
        "holder_count" => Ok(format!(
            "-- Recipe: holder_count (RFC-0023 tier 1) - the number of non-zero holders, DERIVED from the\n\
             -- Transfer events already indexed. No eth_call. Query it: SELECT * FROM {alias}_holder_count\n\
             CREATE VIEW {alias}_holder_count AS\n{};\n",
            holder_count_select(alias)
        )),
        "frozen" => Ok(format!(
            "-- Recipe: frozen (#2943) - every address the issuer has blacklisted now, with its balance and\n\
             -- the block it was frozen at, from the Blacklisted and UnBlacklisted events. Query it:\n\
             -- SELECT * FROM {alias}_frozen ORDER BY balance DESC\n\
             CREATE VIEW {alias}_frozen AS\n{};\n",
            frozen_select(alias)
        )),
        "outstanding" => Ok(format!(
            "-- Recipe: outstanding (#2943) - supply as an attestation counts it: total supply, less frozen\n\
             -- balances, less what addresses labelled `{TREASURY_LABEL}` hold (minted but not issued). The\n\
             -- treasury addresses are yours to supply: `nuthatch labels import` a set labelling them.\n\
             -- `at_time` or `at_block` gives supply and freezes at a report date; the label set is the one\n\
             -- imported now, so import the treasury addresses as they were then. Query it:\n\
             -- SELECT * FROM {alias}_outstanding\n\
             CREATE VIEW {alias}_outstanding AS\n{};\n",
            outstanding_select(alias)
        )),
        "mint_burn_daily" => Ok(format!(
            "-- Recipe: mint_burn_daily (#2943) - per UTC day, what zero-address transfers minted and burned,\n\
             -- the Mint and Burn event amounts beside them, and supply at the day's end. A day where the\n\
             -- two disagree is worth explaining. Query it: SELECT * FROM {alias}_mint_burn_daily\n\
             CREATE VIEW {alias}_mint_burn_daily AS\n{};\n",
            mint_burn_daily_select(alias)
        )),
        "reserves" => Ok(format!(
            "-- Recipe: reserves (RFC-0023 tier 1) - Uniswap-V2 `getReserves()`, DERIVED as the latest\n\
             -- `Sync(uint112,uint112)` event per pair. No eth_call, no archive node. Requires the nest to\n\
             -- index the `Sync` event (`{alias}__sync`). Query it: SELECT * FROM {alias}_reserves\n\
             CREATE VIEW {alias}_reserves AS\n{};\n",
            reserves_select(alias)
        )),
        _ => bail!(
            "unknown recipe '{name}' - available: {}",
            RECIPES
                .iter()
                .map(|r| r.name)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// `nuthatch recipe list` - the available derive-first recipes.
pub fn list_cli() {
    println!("Derive-first recipes (RFC-0023) - each replaces an eth_call with a derivation:");
    for r in RECIPES {
        println!("  {:<14} {}", r.name, r.about);
    }
    println!("\nadd one with:  nuthatch recipe add <name> [--alias <contract>]");
}

/// `nuthatch recipe add <name> [--alias <a>] [--dir <d>]` - write a recipe's view into the nest's
/// `views/`. `alias` defaults to the nest's first contract. Never clobbers an existing file.
pub fn add_cli(dir: &Path, name: &str, alias: Option<&str>) -> Result<()> {
    let alias = match alias {
        Some(a) => a.to_string(),
        None => Config::load(dir)?.primary()?.alias.clone(),
    };
    let sql = view_sql(name, &alias)?;
    if let Some(have) = nest_tables(dir) {
        let missing: Vec<String> = required_tables(name)
            .iter()
            .map(|t| format!("{alias}__{t}"))
            .filter(|t| !have.contains(t))
            .collect();
        if !missing.is_empty() {
            bail!(
                "the {name} recipe reads {}, which this nest does not index",
                missing.join(", ")
            );
        }
    }
    if name == "outstanding" && !has_label_snapshot(dir) {
        bail!(
            "the outstanding recipe nets off addresses labelled `{TREASURY_LABEL}`, and this nest has no \
             label set: `nuthatch labels import <file>` one first (it may be empty of treasury rows)"
        );
    }
    let views = dir.join("views");
    std::fs::create_dir_all(&views)?;
    let path = views.join(format!("{name}.sql"));
    if path.exists() {
        bail!(
            "{} already exists - remove it first, or edit it in place",
            path.display()
        );
    }
    std::fs::write(&path, sql)?;
    println!(
        "✓ wrote {} - a derived `{name}` view (no eth_call). Query it: \
         nuthatch sql \"SELECT * FROM {alias}_{name}\"",
        path.display()
    );
    Ok(())
}

/// The event tables a recipe reads, by suffix after `{alias}__`.
fn required_tables(name: &str) -> &'static [&'static str] {
    match name {
        "frozen" | "outstanding" => &["transfer", "blacklisted", "un_blacklisted"],
        "mint_burn_daily" => &["transfer", "mint", "burn"],
        "reserves" => &["sync"],
        _ => &["transfer"],
    }
}

/// The tables `schema.json` declares, or `None` where there is none to check against.
fn nest_tables(dir: &Path) -> Option<std::collections::BTreeSet<String>> {
    let raw = std::fs::read(dir.join("schema.json")).ok()?;
    let v: serde_json::Value = serde_json::from_slice(&raw).ok()?;
    Some(
        v["tables"]
            .as_array()?
            .iter()
            .filter_map(|t| t["table"].as_str().map(String::from))
            .collect(),
    )
}

/// Whether `labels import` has written a snapshot, without which a view naming `labels` cannot bind.
fn has_label_snapshot(dir: &Path) -> bool {
    std::fs::read_dir(dir.join(crate::labels::LABELS_DIR)).is_ok_and(|rd| {
        rd.flatten()
            .any(|e| e.path().extension().is_some_and(|x| x == "json"))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn total_supply_select_targets_the_transfer_table_and_zero_address() {
        let sql = total_supply_select("usdc");
        assert!(sql.contains("\"usdc__transfer\""));
        assert!(sql.contains(ZERO_ADDRESS));
        assert!(sql.contains("total_supply"));
    }

    /// #2943: fixed rows in, exact figures out. Two mints, a transfer, a burn, two freezes of which one
    /// is lifted, and a treasury the issuer labels.
    #[test]
    fn issuer_recipes_reconcile_supply_freezes_and_treasury() {
        let dir = tempfile::tempdir().unwrap();
        crate::seal::test_set_table_floor(dir.path(), 0);
        let z = ZERO_ADDRESS;
        let (a, b, t) = (
            "0x00000000000000000000000000000000000000a1",
            "0x00000000000000000000000000000000000000b2",
            "0x00000000000000000000000000000000000000e3",
        );
        let day = 1_700_006_400u64;
        let row = |table: &str, block: u64, li: u64, ts: u64, mut v: serde_json::Value| {
            v["table"] = format!("usdc__{table}").into();
            v["block_number"] = block.into();
            v["log_index"] = li.into();
            v["block_timestamp"] = ts.into();
            v.to_string()
        };
        let transfer = |block, li, ts, from: &str, to: &str, value: u64| {
            let v = serde_json::json!({"from": from, "to": to, "value": value.to_string()});
            row("transfer", block, li, ts, v)
        };
        let mint = |li, to: &str, amount: &str| {
            let v = serde_json::json!({"minter": t, "to": to, "amount": amount});
            row("mint", 1, li, day, v)
        };
        let rows = vec![
            transfer(1, 0, day, z, a, 1000),
            mint(1, a, "1000"),
            transfer(1, 2, day, z, t, 500),
            mint(3, t, "500"),
            transfer(2, 0, day + 60, a, b, 300),
            transfer(3, 0, day + 86_400, b, z, 100),
            row(
                "burn",
                3,
                1,
                day + 86_400,
                serde_json::json!({"burner": b, "amount": "100"}),
            ),
            row(
                "blacklisted",
                3,
                2,
                day + 86_400,
                serde_json::json!({"_account": b}),
            ),
            row(
                "blacklisted",
                4,
                0,
                day + 86_460,
                serde_json::json!({"_account": a}),
            ),
            row(
                "un_blacklisted",
                5,
                0,
                day + 86_520,
                serde_json::json!({"_account": a}),
            ),
        ];
        crate::seal::seal_range(dir.path(), &rows, 1, 5).unwrap();
        let labels = dir.path().join("treasury.csv");
        let set = format!("address,label\n{t},{TREASURY_LABEL}\n{a},customer\n");
        std::fs::write(&labels, set).unwrap();
        crate::labels::import(dir.path(), &labels).unwrap();
        let q = |sql: String| crate::analytics::query(dir.path(), &sql).unwrap();
        let n = |v: &serde_json::Value| {
            v.as_str()
                .map(String::from)
                .unwrap_or_else(|| v.to_string())
        };

        let frozen = q(frozen_select("usdc"));
        assert_eq!(frozen.len(), 1, "a lifted freeze is not frozen: {frozen:?}");
        assert_eq!(frozen[0]["account"].as_str(), Some(b));
        assert_eq!(n(&frozen[0]["balance"]), "200");

        let o = &q(outstanding_select("usdc"))[0];
        assert_eq!(
            [
                &o["total_supply"],
                &o["frozen"],
                &o["treasury"],
                &o["outstanding"]
            ]
            .map(n),
            ["1400", "200", "500", "700"]
        );

        let days: Vec<[String; 5]> = q(mint_burn_daily_select("usdc"))
            .iter()
            .map(|d| {
                [
                    &d["minted"],
                    &d["burned"],
                    &d["mint_events"],
                    &d["burn_events"],
                    &d["supply"],
                ]
                .map(n)
            })
            .collect();
        assert_eq!(
            days,
            [
                ["1500", "0", "1500", "0", "1500"].map(String::from),
                ["0", "100", "0", "100", "1400"].map(String::from),
            ]
        );
    }

    #[test]
    fn a_recipe_refuses_a_nest_without_the_events_it_reads() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"usdc__transfer"}]}"#,
        )
        .unwrap();
        let err = add_cli(dir.path(), "frozen", Some("usdc"))
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("usdc__blacklisted, usdc__un_blacklisted"),
            "{err}"
        );
        add_cli(dir.path(), "total_supply", Some("usdc")).unwrap();
    }

    #[test]
    fn the_outstanding_recipe_waits_for_a_label_set() {
        let dir = tempfile::tempdir().unwrap();
        let err = add_cli(dir.path(), "outstanding", Some("usdc"))
            .unwrap_err()
            .to_string();
        assert!(err.contains("labels import"), "{err}");
        assert!(!dir.path().join("views").join("outstanding.sql").exists());
    }

    #[test]
    fn view_sql_wraps_a_create_view_and_rejects_unknown() {
        let v = view_sql("total_supply", "weth").unwrap();
        assert!(v.contains("CREATE VIEW weth_total_supply AS"));
        let err = view_sql("nope", "weth").unwrap_err().to_string();
        assert!(err.contains("unknown recipe"), "got: {err}");
    }
}
