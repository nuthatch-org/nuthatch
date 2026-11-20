//! The tools offered on an address-history nest (RFC-0063): its watched accounts, what it covers of
//! them, and one account's history over a window. They read `/api`, `/api/coverage` and `/ready`, the
//! same routes rotki reads, and add only arithmetic: decimals applied to the integers served.

use super::{get, get_query, Link};
use anyhow::{anyhow, bail, Result};
use serde_json::{json, Value};

/// Rows per kind when the caller names no limit; `/api` itself allows up to 10,000.
const DEFAULT_LIMIT: u64 = 200;
const MAX_LIMIT: u64 = 10_000;

/// Each kind of row `wallet_history` reads, the `/api` action serving it, and how its `value` scales.
const KINDS: [(&str, &str, Scale); 5] = [
    ("normal", "txlist", Scale::Native),
    ("internal", "txlistinternal", Scale::Native),
    ("token", "tokentx", Scale::Token),
    ("nft", "tokennfttx", Scale::None),
    ("erc1155", "token1155tx", Scale::None),
];

const DEFAULT_KINDS: [&str; 3] = ["normal", "internal", "token"];

#[derive(Clone, Copy)]
enum Scale {
    /// Wei, at the 18 decimals of every chain this nest serves.
    Native,
    /// The row's own `tokenDecimal`.
    Token,
    None,
}

/// What the nest cannot see, said on every `wallet_history` answer.
const NOT_SEEN: [&str; 5] = [
    "An incoming transaction that moves no value, a zero-value call or a failed transfer to the account, is not listed.",
    "A kind whose window is not wholly covered is refused whole, never shortened; `coverage` says which blocks are missing and why.",
    "Token names, symbols and decimals are the token contract's answers at the latest block, read when this answer was built, not at the transfer. A token that answered none of them has `value_formatted` null.",
    "On chains where transactions are proved from the account's balance, a contract or an account with delegated code has its normal and internal transactions refused from the block it had code.",
    "No prices, fiat values or balances: the nest holds the account's transactions and transfers only.",
];

pub(super) fn tool_specs() -> Vec<Value> {
    vec![
        json!({ "name": "wallet_status", "title": "Address-history nest status", "description": "Status of this address-history nest (watched EVM accounts' history, RFC-0063): chain, finalized head, tip, how many accounts are watched, and `served_through`, the block each family of history is answered through without waiting. Answers an HTTP 503 with the reason while the nest is not ready.",
          "inputSchema": { "type": "object", "properties": {} } }),
        json!({ "name": "coverage", "title": "Watched accounts and coverage", "description": "The EVM accounts this nest watches and, per account and per action (txlist, txlistinternal, tokentx, tokennfttx, token1155tx), the block ranges it holds (`covered`) and the ranges between `start_block` and the finalized head it does not (`missing`), each with a status: `pending` (watched, owed, backfill running), `stalled` (owed, but the account's backfill windows are failing or the cursor is stalled, with the error), `not_watched` (watching it would backfill it) or `never` (this nest will not cover it, with why). Before the first finalized head `missing` is absent and `missing_unknown` says why. Read from one store transaction with its `generation`, finalized head and tip. Give `address` for one account, watched or not.",
          "inputSchema": { "type": "object", "properties": { "address": { "type": "string", "description": "One 0x address; omitted, every watched account." }, "chainid": { "type": "integer", "description": "The chain, where one URL serves a directory of chains." } } } }),
        json!({ "name": "wallet_history", "title": "One account's history", "description": "One EVM account's normal transactions, internal transactions and token transfers over a block or time window, as this address-history nest serves them to rotki at `/api`, with decimals applied: `value_native` on normal and internal rows (wei at 18 decimals), `value_formatted` on token rows (by the row's `tokenDecimal`). Each kind is answered whole for the window or refused with the nest's reason (not indexed, pending, not watched), never shortened. A window above the finalized head, up to the chain head, is covered on demand through the nest's RPC within its request deadline. The answer carries a provenance stamp (generation, finalized head, tip) and `not_seen`. Give `from_block` or `from_time`, and `to_block` or `to_time`.",
          "inputSchema": { "type": "object", "properties": {
            "address": { "type": "string", "description": "The 0x address." },
            "chainid": { "type": "integer", "description": "The chain, where one URL serves a directory of chains." },
            "from_block": { "type": "integer", "description": "First block, inclusive." },
            "to_block": { "type": "integer", "description": "Last block, inclusive." },
            "from_time": { "type": "integer", "description": "Start, unix seconds UTC: the first block at or after it." },
            "to_time": { "type": "integer", "description": "End, unix seconds UTC: the last block at or before it." },
            "kinds": { "type": "array", "items": { "type": "string", "enum": ["normal", "internal", "token", "nft", "erc1155"] }, "description": "Which kinds to read (default normal, internal, token)." },
            "limit": { "type": "integer", "description": "Rows per kind per page (default 200, at most 10000).", "default": DEFAULT_LIMIT },
            "page": { "type": "integer", "description": "Page of each kind, from 1.", "default": 1 } },
            "required": ["address"] } }),
    ]
}

pub(super) fn prompt_specs() -> Vec<Value> {
    vec![
        json!({ "name": "wallet-history", "description": "What one watched account did over a window, from the nest's own rows.",
          "arguments": [
            { "name": "address", "description": "The 0x address.", "required": true },
            { "name": "window", "description": "The window, as blocks or dates (default: everything covered).", "required": false } ] }),
        json!({ "name": "coverage-gaps", "description": "Which blocks of the watched accounts' history this nest does not hold, and why.",
          "arguments": [] }),
    ]
}

pub(super) fn render_prompt(name: &str, args: &Value) -> Option<String> {
    Some(match name {
        "wallet-history" => {
            let a = args.get("address").and_then(Value::as_str).unwrap_or("<address>");
            let w = args
                .get("window")
                .and_then(Value::as_str)
                .unwrap_or("everything the nest covers");
            format!(
                "Summarise what the account {a} did over {w}. Call `coverage` with the address first and \
                 pick a window inside its covered blocks; dates go to `wallet_history` as `from_time` and \
                 `to_time`. Then call `wallet_history`, paging while a kind says `truncated`. Report \
                 amounts as `value_native` and `value_formatted` with the token symbol, cite the \
                 provenance stamp, list any kind that was refused with its reason, and repeat the \
                 answer's `not_seen`. Do not fill a refused or missing range from anywhere else."
            )
        }
        "coverage-gaps" => "Tell me which parts of my accounts' history this nest does not hold. Call \
            `coverage`, then for each account and action list the `missing` ranges with their status \
            and why, grouped as pending (coming), stalled (owed but failing, with the error), not watched \
            (watching would fetch it) and never (this nest will not hold it). Say the finalized head and `start_block` the ranges are counted \
            between, and the generation the report was read at. Infer nothing beyond what it says."
            .to_string(),
        _ => return None,
    })
}

const TOOLS: [&str; 3] = ["wallet_status", "coverage", "wallet_history"];

pub(super) fn is_tool(name: &str) -> bool {
    TOOLS.contains(&name)
}

/// A wallet tool's answer.
pub(super) async fn call(name: &str, args: &Value, link: Link<'_>, base: &str) -> Result<String> {
    match name {
        "wallet_status" => get(link, &format!("{base}/ready")).await,
        "coverage" => {
            let chain = args["chainid"].as_u64().map(|c| c.to_string());
            let q: Vec<(&str, &str)> = [
                ("address", args["address"].as_str()),
                ("chainid", chain.as_deref()),
            ]
            .into_iter()
            .filter_map(|(k, v)| Some((k, v?)))
            .collect();
            get_query(link, &format!("{base}/api/coverage"), &q).await
        }
        "wallet_history" => history(args, link, base).await.map(|v| v.to_string()),
        other => bail!("unknown tool `{other}`"),
    }
}

async fn history(args: &Value, link: Link<'_>, base: &str) -> Result<Value> {
    let address = args["address"]
        .as_str()
        .ok_or_else(|| anyhow!("`address` is required"))?;
    let chain = args["chainid"].as_u64().map(|c| c.to_string());
    let chain = chain.as_deref();
    let from = match (args["from_block"].as_u64(), args["from_time"].as_u64()) {
        (Some(b), _) => b,
        (None, Some(t)) => block_by_time(link, base, chain, t, "after").await?,
        (None, None) => bail!("give `from_block` or `from_time`"),
    };
    let to = match (args["to_block"].as_u64(), args["to_time"].as_u64()) {
        (Some(b), _) => b,
        (None, Some(t)) => block_by_time(link, base, chain, t, "before").await?,
        (None, None) => bail!("give `to_block` or `to_time`"),
    };
    if to < from {
        bail!("the window ends at block {to}, before it starts at block {from}");
    }
    let limit = args["limit"].as_u64().unwrap_or(DEFAULT_LIMIT);
    if !(1..=MAX_LIMIT).contains(&limit) {
        bail!("`limit` must be 1 to {MAX_LIMIT}");
    }
    let page = args["page"].as_u64().unwrap_or(1).max(1);
    let kinds: Vec<&str> = match args["kinds"].as_array() {
        Some(k) => k.iter().filter_map(Value::as_str).collect(),
        None => DEFAULT_KINDS.to_vec(),
    };
    let mut out = serde_json::Map::new();
    out.insert("address".into(), json!(address));
    out.insert("from_block".into(), json!(from));
    out.insert("to_block".into(), json!(to));
    for kind in &kinds {
        let Some(&(_, action, scale)) = KINDS.iter().find(|(k, _, _)| k == kind) else {
            bail!("unknown kind `{kind}` (want: normal, internal, token, nft, erc1155)");
        };
        let (from_s, to_s, page_s, limit_s) = (
            from.to_string(),
            to.to_string(),
            page.to_string(),
            limit.to_string(),
        );
        let mut q = vec![
            ("module", "account"),
            ("action", action),
            ("address", address),
            ("startblock", from_s.as_str()),
            ("endblock", to_s.as_str()),
            ("page", page_s.as_str()),
            ("offset", limit_s.as_str()),
            ("sort", "asc"),
        ];
        q.extend(chain.map(|c| ("chainid", c)));
        let body: Value =
            serde_json::from_str(&get_query(link, &format!("{base}/api"), &q).await?)?;
        out.insert(kind.to_string(), section(body, scale, limit, page));
    }
    out.insert("provenance".into(), provenance(link, base, chain).await);
    out.insert("not_seen".into(), json!(NOT_SEEN));
    Ok(Value::Object(out))
}

/// One kind's `/api` answer: its rows with decimals applied, or the nest's refusal as it was given.
fn section(body: Value, scale: Scale, limit: u64, page: u64) -> Value {
    // Etherscan answers a window with no rows `status: "0"`, "No transactions found", `result: []`;
    // a refusal's `result` is a string.
    let Some(Value::Array(mut rows)) = body.get("result").cloned() else {
        return json!({ "refused": body["result"] });
    };
    for row in rows.iter_mut().filter_map(Value::as_object_mut) {
        let raw = row.get("value").and_then(Value::as_str).unwrap_or("");
        match scale {
            Scale::Native => {
                let v = scaled(raw, 18);
                row.insert("value_native".into(), json!(v));
            }
            Scale::Token => {
                let field = |k: &str| row.get(k).and_then(Value::as_str).unwrap_or("");
                let answered = !(field("tokenName").is_empty()
                    && field("tokenSymbol").is_empty()
                    && field("tokenDecimal") == "0");
                let v = field("tokenDecimal")
                    .parse::<u32>()
                    .ok()
                    .filter(|_| answered)
                    .and_then(|d| scaled(raw, d));
                row.insert("value_formatted".into(), json!(v));
            }
            Scale::None => {}
        }
    }
    let truncated = rows.len() as u64 >= limit;
    let mut s = json!({
        "count": rows.len(),
        "generation": body["generation"],
        "rows": rows,
        "truncated": truncated,
    });
    if truncated {
        s["next"] = json!(format!(
            "more rows may follow: ask again with page {}",
            page + 1
        ));
    }
    s
}

/// The integer `raw` divided by 10^`decimals`, exactly, with no trailing zeros; `None` when `raw` is
/// not a decimal integer.
pub(super) fn scaled(raw: &str, decimals: u32) -> Option<String> {
    if raw.is_empty() || !raw.bytes().all(|b| b.is_ascii_digit()) || decimals > 255 {
        return None;
    }
    let digits = raw.trim_start_matches('0');
    let d = decimals as usize;
    let padded = format!("{digits:0>width$}", width = d + 1);
    let (whole, frac) = padded.split_at(padded.len() - d);
    let frac = frac.trim_end_matches('0');
    Some(if frac.is_empty() {
        whole.to_string()
    } else {
        format!("{whole}.{frac}")
    })
}

/// `getblocknobytime`, which the nest answers from verified headers or, on a loopback bind, by a
/// search of its own RPC proven against the neighbouring block.
async fn block_by_time(
    link: Link<'_>,
    base: &str,
    chain: Option<&str>,
    t: u64,
    closest: &str,
) -> Result<u64> {
    let ts = t.to_string();
    let mut q = vec![
        ("module", "block"),
        ("action", "getblocknobytime"),
        ("timestamp", ts.as_str()),
        ("closest", closest),
    ];
    q.extend(chain.map(|c| ("chainid", c)));
    let body: Value = serde_json::from_str(&get_query(link, &format!("{base}/api"), &q).await?)?;
    match (&body["status"], body["result"].as_str()) {
        (s, Some(b)) if s == "1" => b
            .parse()
            .map_err(|_| anyhow!("the nest answered block `{b}` for time {t}")),
        _ => bail!(
            "the nest cannot place time {t} ({}); give the window in blocks",
            body["result"]
        ),
    }
}

/// What the answer stands on: the nest's finalized head, tip and `served_through` from `/ready`, the
/// asked chain's on a directory of chains.
async fn provenance(link: Link<'_>, base: &str, chain: Option<&str>) -> Value {
    let ready = get(link, &format!("{base}/ready"))
        .await
        .map_err(|e| e.to_string())
        .and_then(|b| serde_json::from_str::<Value>(&b).map_err(|e| e.to_string()));
    match ready {
        Ok(mut r) => {
            if let Some(c) = chain.filter(|_| r.get("chains").is_some()) {
                r = r["chains"][c].clone();
            }
            json!({
            "chain_id": r["chain_id"],
            "finalized_head": r["head"],
            "tip": r["tip"],
            "served_through": r["served_through"],
            "generation": "per kind, under each kind's `generation`: a reorg or an unwatch moves it",
            })
        }
        Err(e) => json!({ "unread": format!("the nest's /ready could not be read: {e}") }),
    }
}
