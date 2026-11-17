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
        name: "fiat_threshold",
        about: "transfers above a fiat amount at the day's offchain rate, to unlabelled recipients (#2945)",
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

/// What the `fiat_threshold` recipe needs that the nest does not say (#2945).
#[derive(Clone)]
pub struct Fiat {
    /// The threshold in the rate table's currency, as a decimal. A transfer above it is flagged.
    pub amount: String,
    /// The offchain table (`offchain drop` or `pull`) with `day` (`YYYY-MM-DD`, UTC) and `rate`
    /// (currency per whole token) columns.
    pub rates: String,
    pub decimals: u32,
    /// Labels whose addresses are already classified. A transfer to one is not flagged.
    pub unless: Vec<String>,
}

impl Fiat {
    fn check(&self) -> Result<()> {
        let (whole, frac) = self.amount.split_once('.').unwrap_or((&self.amount, ""));
        let digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
        if whole.is_empty()
            || whole.len() > 20
            || !digits(whole)
            || !digits(frac)
            || frac.len() > 10
        {
            bail!(
                "--amount must be a decimal like 1000 or 1000.50, under 10^20 and with at most 10 \
                 decimal places"
            );
        }
        if self.decimals > 30 {
            bail!(
                "--decimals {} is more than this recipe supports (30)",
                self.decimals
            );
        }
        crate::offchain::validate_table(&self.rates)?;
        for l in &self.unless {
            if l.is_empty() || l.chars().any(|c| c == '\'' || c.is_control()) {
                bail!("--unless-label {l:?} is not a label name");
            }
        }
        Ok(())
    }
}

/// The `SELECT` body of the `fiat_threshold` recipe: each transfer whose value, at its UTC day's rate,
/// is above `amount`, and whose recipient carries none of the `unless` labels. Mints and burns are not
/// transfers between parties and are left out. A transfer the rate table cannot price, because its day
/// has no rate or two different ones, or because it moves 10^17 tokens or more, is returned with
/// `priced = false` rather than passed over, and a rate that will not parse refuses the query.
pub fn fiat_threshold_select(alias: &str, f: &Fiat) -> String {
    let d = crate::analytics::exact_or_null("\"value\"", "HUGEINT");
    let day = "DATE '1970-01-01' + CAST(block_timestamp // 86400 AS INTEGER)";
    let scale = format!("CAST('1{}' AS HUGEINT)", "0".repeat(f.decimals as usize));
    // Checked arithmetic refuses the whole query on an overflow, so no product here may overflow: whole
    // tokens below 10^17 times the rate, plus the fraction cut to 10 places of a token.
    let tenth = |n: u32| format!("CAST('1{}' AS HUGEINT)", "0".repeat(n as usize));
    let frac10 = if f.decimals >= 10 {
        format!("(t.value % {scale}) // {}", tenth(f.decimals - 10))
    } else {
        format!("(t.value % {scale}) * {}", tenth(10 - f.decimals))
    };
    let unless = if f.unless.is_empty() {
        String::new()
    } else {
        let set = f
            .unless
            .iter()
            .map(|l| format!("'{l}'"))
            .collect::<Vec<_>>()
            .join(", ");
        // NOT IN over a NULL would hold for no row at all, so the NULLs are kept out of the set.
        format!(
            " AND t.recipient NOT IN (SELECT lower(address) FROM labels \
               WHERE label IN ({set}) AND address IS NOT NULL)"
        )
    };
    format!(
        "SELECT block_number, log_index, block_timestamp, day, sender, recipient, value, rate, \
           fiat_value, priced \
         FROM (\
           SELECT t.*, r.rate, \
             CASE WHEN t.value // {scale} < 100000000000000000 THEN \
               CAST(t.value // {scale} AS DECIMAL(38,0)) * r.rate \
               + CAST({frac10} AS DECIMAL(38,0)) * r.rate / CAST('10000000000' AS DECIMAL(38,0)) \
             END AS fiat_value, \
             r.rate IS NOT NULL AND COALESCE(t.value // {scale} < 100000000000000000, false) AS priced \
           FROM (SELECT block_number, log_index, block_timestamp, {day} AS day, \
                   lower(\"from\") AS sender, lower(\"to\") AS recipient, {d} AS value \
                 FROM \"{alias}__transfer\") t \
           LEFT JOIN (SELECT day, CASE WHEN MIN(rate) = MAX(rate) AND COUNT(*) = COUNT(rate) \
                        THEN MIN(rate) END AS rate \
                      FROM (SELECT CAST(day AS DATE) AS day, \
                              CAST(rate AS DECIMAL(20,10)) AS rate \
                            FROM \"offchain__{rates}\") \
                      GROUP BY day) r ON r.day = t.day \
           WHERE t.sender <> '{ZERO_ADDRESS}' AND t.recipient <> '{ZERO_ADDRESS}'{unless}\
         ) WHERE NOT priced OR fiat_value > CAST('{amount}' AS DECIMAL(38,10)) \
         ORDER BY block_number, log_index",
        rates = f.rates,
        amount = f.amount,
    )
}

fn fiat_threshold_view(alias: &str, f: &Fiat) -> String {
    let unless = if f.unless.is_empty() {
        "any recipient".to_string()
    } else {
        format!("a recipient labelled none of {}", f.unless.join(", "))
    };
    format!(
        "-- Recipe: fiat_threshold (#2945) - transfers above {amount} at their UTC day's rate in\n\
         -- offchain__{rates}, to {unless}. A transfer the table cannot price is listed with priced =\n\
         -- false. Drop or pull the rates before querying; without them the view refuses by name.\n\
         -- Query it: SELECT * FROM {alias}_fiat_threshold\n\
         CREATE VIEW {alias}_fiat_threshold AS\n{};\n",
        fiat_threshold_select(alias, f),
        amount = f.amount,
        rates = f.rates,
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
        "fiat_threshold" => bail!(
            "the fiat_threshold recipe needs --amount, --rates and --decimals (and any --unless-label)"
        ),
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
pub fn add_cli(dir: &Path, name: &str, alias: Option<&str>, fiat: Option<&Fiat>) -> Result<()> {
    let alias = match alias {
        Some(a) => a.to_string(),
        None => Config::load(dir)?.primary()?.alias.clone(),
    };
    let sql = match (name, fiat) {
        ("fiat_threshold", Some(f)) => {
            f.check()?;
            if !crate::offchain::load(dir).is_ok_and(|c| c.tables.contains_key(&f.rates)) {
                bail!(
                    "there is no offchain table `{}`: `nuthatch offchain drop <file> --table {}` the \
                     rates first",
                    f.rates,
                    f.rates
                );
            }
            if !f.unless.is_empty() && !has_label_snapshot(dir) {
                bail!(
                    "--unless-label needs a label set: `nuthatch labels import <file>` one first"
                );
            }
            fiat_threshold_view(&alias, f)
        }
        (_, Some(_)) => {
            bail!("--amount, --rates, --decimals and --unless-label are for fiat_threshold")
        }
        _ => view_sql(name, &alias)?,
    };
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

    /// #2947: the MiCA Art 22 example's own views, over one transfer per RTS deduction, give each
    /// deduction, (c) and (d) per single currency area exactly.
    #[test]
    fn the_mica_art22_example_deducts_each_rts_case_once() {
        let dir = tempfile::tempdir().unwrap();
        crate::seal::test_set_table_floor(dir.path(), 0);
        let views = Path::new(env!("CARGO_MANIFEST_DIR")).join("examples/mica-art22/views");
        std::fs::create_dir(dir.path().join("views")).unwrap();
        for e in std::fs::read_dir(&views).unwrap().flatten() {
            let name = e.file_name().to_string_lossy().into_owned();
            if name.contains("art22") {
                std::fs::copy(e.path(), dir.path().join("views").join(name)).unwrap();
            }
        }
        let a = |n: u8| format!("0x{:040x}", n);
        let (z, issuer, c1, c2, c3, n1, n2) = (a(0), a(1), a(2), a(3), a(4), a(5), a(6));
        let (k, d, x, p1, p2) = (a(7), a(8), a(9), a(10), a(11));
        let q3 = 1_782_864_000u64;
        let q4 = 1_790_812_800u64;
        let eur = 1_000_000u64;
        let transfers = [
            (q3, &z, &issuer, 1000),
            (q3, &issuer, &c1, 500),
            (q3, &c1, &c2, 100),
            (q3, &c1, &n1, 50),
            (q3, &n1, &n2, 40),
            (q3, &c1, &c3, 30),
            (q3, &c2, &k, 20),
            (q3, &c2, &d, 10),
            (q3, &c2, &x, 5),
            (q3, &p1, &p2, 7),
            (q3 + 86_400, &c1, &c1, 3),
            (q3 + 86_400, &c2, &z, 2),
            (q4, &c1, &c2, 1000),
        ];
        let rows: Vec<String> = transfers
            .iter()
            .enumerate()
            .map(|(i, (ts, from, to, v))| {
                serde_json::json!({
                    "table": "fiat_token_v2_2__transfer", "block_number": i as u64 + 1,
                    "log_index": 0, "block_timestamp": ts,
                    "from": from, "to": to, "value": (v * eur).to_string(),
                })
                .to_string()
            })
            .collect();
        crate::seal::seal_range(dir.path(), &rows, 1, transfers.len() as u64).unwrap();
        let set = format!(
            "address,label\n{issuer},issuer\n{c1},custodial\n{c1},area:EUR\n{c2},custodial\n\
             {c2},area:EUR\n{c3},custodial\n{c3},area:PLN\n{k},collateral\n{d},derivative\n\
             {x},not_payment\n{p1},custodial\n{p1},person:alice\n{p2},custodial\n{p2},person:alice\n"
        );
        let labels = dir.path().join("set.csv");
        std::fs::write(&labels, set).unwrap();
        crate::labels::import(dir.path(), &labels).unwrap();
        let q = |sql: &str| crate::analytics::query(dir.path(), sql).unwrap();
        let s = |v: &serde_json::Value| v.as_str().map(String::from).unwrap_or(v.to_string());

        let c: Vec<_> = q("SELECT * FROM art22_quarter_transactions ORDER BY quarter")
            .iter()
            .map(|r| {
                [
                    &r["quarter"],
                    &r["days"],
                    &r["transactions"],
                    &r["avg_per_day"],
                ]
                .map(s)
            })
            .collect();
        assert_eq!(c, [["3", "92", "12", "0.13"], ["4", "92", "1", "0.01"]]);
        let c3 = &q("SELECT avg_eur_per_day FROM art22_quarter_transactions WHERE quarter = 3")[0];
        assert_eq!(
            s(&c3["avg_eur_per_day"]),
            "19.206521",
            "1767 EUR over 92 days"
        );

        let d: Vec<_> =
            q("SELECT * FROM art22_quarter_means_of_exchange WHERE quarter = 3 ORDER BY area")
                .iter()
                .map(|r| {
                    [
                        &r["area"],
                        &r["transactions"],
                        &r["value_base_units"],
                        &r["avg_eur_per_day"],
                    ]
                    .map(s)
                })
                .collect();
        assert_eq!(
            d,
            [
                ["EUR", "1", "100000000", "1.086956"],
                ["unassigned", "2", "80000000", "0.869565"]
            ],
            "cross-area and an unlabelled party are unassigned"
        );

        let lines: Vec<_> = q(
            "SELECT deduction, transactions FROM art22_quarter_deductions WHERE quarter = 3 \
             ORDER BY deduction",
        )
        .iter()
        .map(|r| [&r["deduction"], &r["transactions"]].map(s))
        .collect();
        assert_eq!(
            lines,
            [
                ["collateral", "1"],
                ["derivative", "1"],
                ["exchange", "3"],
                ["non_custodial", "2"],
                ["not_payment", "1"],
                ["same_person", "2"]
            ]
        );
    }

    /// #2945: EUR 1,000 at each transfer's UTC day rate, to recipients no `vasp` label covers. The
    /// first day's rate is 0.8, so 1,250 tokens is exactly EUR 1,000 and is not above it.
    #[test]
    fn the_fiat_threshold_flags_transfers_above_it_to_unlabelled_recipients() {
        let dir = tempfile::tempdir().unwrap();
        crate::seal::test_set_table_floor(dir.path(), 0);
        let (a, b, v) = (
            "0x00000000000000000000000000000000000000a1",
            "0x00000000000000000000000000000000000000b2",
            "0x00000000000000000000000000000000000000c3",
        );
        let day = 1_700_006_400u64;
        let transfer = |block: u64, ts: u64, from: &str, to: &str, value: &str| {
            serde_json::json!({
                "table": "usdc__transfer", "block_number": block, "log_index": 0,
                "block_timestamp": ts, "from": from, "to": to, "value": value,
            })
            .to_string()
        };
        let rows = vec![
            transfer(1, day, ZERO_ADDRESS, a, "9000000000"),
            transfer(2, day + 10, a, b, "1250000000"),
            transfer(3, day + 20, a, b, "1250000001"),
            transfer(4, day + 30, a, v, "5000000000"),
            transfer(5, day + 86_400, a, b, "1250000001"),
            transfer(6, day + 86_410, a, b, "2000000001"),
            transfer(7, day + 172_800, a, b, "10"),
            transfer(8, day + 40, a, b, "1000000000000000000000000000000"),
        ];
        crate::seal::seal_range(dir.path(), &rows, 1, 8).unwrap();
        let rates = dir.path().join("eur.csv");
        std::fs::write(&rates, "day,rate\n2023-11-15,0.8\n2023-11-16,0.5\n").unwrap();
        crate::offchain::drop_file(dir.path(), &rates, "eur").unwrap();
        let labels = dir.path().join("vasps.csv");
        std::fs::write(&labels, format!("address,label\n{v},vasp\n")).unwrap();
        crate::labels::import(dir.path(), &labels).unwrap();
        let fiat = Fiat {
            amount: "1000".into(),
            rates: "eur".into(),
            decimals: 6,
            unless: vec!["vasp".into()],
        };

        let got: Vec<(u64, bool)> =
            crate::analytics::query(dir.path(), &fiat_threshold_select("usdc", &fiat))
                .unwrap()
                .iter()
                .map(|r| (r["block_number"].as_u64().unwrap(), r["priced"] == true))
                .collect();
        assert_eq!(got, [(3, true), (6, true), (7, false), (8, false)]);
        for decimals in [18, 30] {
            let wide = Fiat {
                decimals,
                ..fiat.clone()
            };
            crate::analytics::query(dir.path(), &fiat_threshold_select("usdc", &wide))
                .unwrap_or_else(|e| panic!("decimals {decimals}: {e:#}"));
        }

        let again = dir.path().join("eur2.csv");
        std::fs::write(&again, "day,rate\n2023-11-16,0.6\n").unwrap();
        crate::offchain::drop_file(dir.path(), &again, "eur").unwrap();
        let unpriced: Vec<u64> =
            crate::analytics::query(dir.path(), &fiat_threshold_select("usdc", &fiat))
                .unwrap()
                .iter()
                .filter(|r| r["priced"] == false)
                .map(|r| r["block_number"].as_u64().unwrap())
                .collect();
        assert_eq!(
            unpriced,
            [5, 6, 7, 8],
            "two rates for one day price none of its transfers"
        );

        add_cli(dir.path(), "fiat_threshold", Some("usdc"), Some(&fiat)).unwrap();
        std::fs::remove_dir_all(dir.path().join(crate::offchain::DIR)).unwrap();
        let err =
            crate::analytics::query(dir.path(), &fiat_threshold_select("usdc", &fiat)).unwrap_err();
        assert!(format!("{err:#}").contains("offchain__eur"), "{err:#}");
        let err = add_cli(dir.path(), "fiat_threshold", Some("usdc"), Some(&fiat)).unwrap_err();
        assert!(err.to_string().contains("no offchain table `eur`"), "{err}");
    }

    #[test]
    fn a_recipe_refuses_a_nest_without_the_events_it_reads() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("schema.json"),
            r#"{"tables":[{"table":"usdc__transfer"}]}"#,
        )
        .unwrap();
        let err = add_cli(dir.path(), "frozen", Some("usdc"), None)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("usdc__blacklisted, usdc__un_blacklisted"),
            "{err}"
        );
        add_cli(dir.path(), "total_supply", Some("usdc"), None).unwrap();
    }

    #[test]
    fn the_outstanding_recipe_waits_for_a_label_set() {
        let dir = tempfile::tempdir().unwrap();
        let err = add_cli(dir.path(), "outstanding", Some("usdc"), None)
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
