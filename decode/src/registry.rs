//! The decode registry (RFC-0001): ABI-driven, deterministic event decode for N contracts.
//!
//! Replaces the hardcoded `Transfer` path. Given each contract's resolved ABI, we build one
//! immutable registry mapping topic0 → decoders (filtered by emitting address), and decode any log
//! into a typed row keyed to a per-(alias, event) table. No LLM ever sits here - it is deterministic
//! Rust, and the registry's content hash is recorded so re-execution is verifiable.

use alloy_dyn_abi::{DynSolValue, EventExt};
use alloy_json_abi::{Event, JsonAbi};
use alloy_primitives::{Address, B256, I256, U256};
use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};

use crate::rpc::Log;

/// How a Solidity value is stored canonically (exact form; SQL convenience forms are derived).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageKind {
    Address,
    U64,
    I64,
    Word16, // 65..=128-bit int/uint, big-endian
    Word32, // >128-bit int/uint, big-endian
    Bool,
    FixedBytes,
    Bytes,
    Str,
    Json,   // arrays / tuples
    Hash32, // indexed dynamic type: the topic holds keccak(value), not the value
}

impl StorageKind {
    /// Map a Solidity type string (+ indexed flag) to its canonical storage kind.
    pub fn from_sol(ty: &str, indexed: bool) -> StorageKind {
        if indexed && is_hashed_when_indexed(ty) {
            return StorageKind::Hash32;
        }
        if ty == "address" {
            StorageKind::Address
        } else if ty == "bool" {
            StorageKind::Bool
        } else if ty == "string" {
            StorageKind::Str
        } else if ty == "bytes" {
            StorageKind::Bytes
        } else if let Some(bits) = ty.strip_prefix("uint").and_then(parse_bits) {
            uint_kind(bits)
        } else if let Some(bits) = ty.strip_prefix("int").and_then(parse_bits) {
            int_kind(bits)
        } else if ty.starts_with("bytes") {
            StorageKind::FixedBytes // bytes1..=bytes32
        } else {
            StorageKind::Json // arrays, tuples, and anything unrecognized
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            StorageKind::Address => "address",
            StorageKind::U64 => "u64",
            StorageKind::I64 => "i64",
            StorageKind::Word16 => "word16",
            StorageKind::Word32 => "word32",
            StorageKind::Bool => "bool",
            StorageKind::FixedBytes => "fixed_bytes",
            StorageKind::Bytes => "bytes",
            StorageKind::Str => "string",
            StorageKind::Json => "json",
            StorageKind::Hash32 => "hash32",
        }
    }
}

fn uint_kind(bits: usize) -> StorageKind {
    if bits <= 64 {
        StorageKind::U64
    } else if bits <= 128 {
        StorageKind::Word16
    } else {
        StorageKind::Word32
    }
}

fn int_kind(bits: usize) -> StorageKind {
    if bits <= 64 {
        StorageKind::I64
    } else if bits <= 128 {
        StorageKind::Word16
    } else {
        StorageKind::Word32
    }
}

/// `intN`/`uintN` default to 256 when N is omitted.
fn parse_bits(rest: &str) -> Option<usize> {
    if rest.is_empty() {
        Some(256)
    } else {
        rest.parse().ok()
    }
}

/// A dynamic (non-value) type whose indexed form is a keccak hash in the topic.
fn is_hashed_when_indexed(ty: &str) -> bool {
    ty == "string"
        || ty == "bytes"
        || ty.ends_with(']')
        || ty.starts_with('(')
        || ty.starts_with("tuple")
}

/// A canonically-encoded decoded value.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Address([u8; 20]),
    U64(u64),
    I64(i64),
    Word16([u8; 16]),
    Word32([u8; 32]),
    /// Signed big integers (int65..=128 and int129..=256), kept distinct from the unsigned `Word*` so
    /// they render as *signed* decimals - negatives are two's-complement bytes, not a huge positive.
    IWord16([u8; 16]),
    IWord32([u8; 32]),
    Bool(bool),
    Bytes(Vec<u8>),
    Str(String),
    Json(String),
    Hash32([u8; 32]),
}

impl Value {
    /// LLM/HTTP-facing JSON. Big integers are hex (lossless); SQL views derive decimals.
    pub fn to_json(&self) -> Json {
        match self {
            Value::Address(a) => json!(format!("0x{}", hex::encode(a))),
            Value::U64(n) => json!(n),
            Value::I64(n) => json!(n),
            // Big integers as their full decimal string - always queryable, and `SUM(c_dec)` works
            // (the derived `_dec` view column TRY_CASTs to DECIMAL(38,0), NULL past 38 digits). Signed
            // types render as *signed* decimals (int256 amounts, e.g. a Uniswap swap's negative leg).
            Value::Word16(b) => json!(u128::from_be_bytes(*b).to_string()),
            Value::Word32(b) => json!(U256::from_be_bytes::<32>(*b).to_string()),
            Value::IWord16(b) => json!(i128::from_be_bytes(*b).to_string()),
            Value::IWord32(b) => json!(I256::from_be_bytes::<32>(*b).to_string()),
            Value::Bool(b) => json!(b),
            Value::Bytes(b) => json!(format!("0x{}", hex::encode(b))),
            Value::Str(s) => json!(s),
            Value::Json(s) => serde_json::from_str(s).unwrap_or_else(|_| json!(s)),
            Value::Hash32(b) => json!(format!("0x{}", hex::encode(b))),
        }
    }
}

/// One output column in a table's schema.
#[derive(Debug, Clone)]
pub struct Column {
    pub name: String,
    pub sol_type: String,
    pub kind: StorageKind,
    pub indexed: bool,
    /// A tuple's components, in ABI order, when the ABI names them.
    ///
    /// A fully-named tuple is stored as a JSON object keyed by those names (#1304). A tuple the ABI
    /// leaves unnamed stays a positional array. Sealed history is not re-decoded: an ABI whose
    /// components change is a new nest.
    ///
    /// Empty for everything that is not a tuple, and for a tuple whose components the ABI leaves
    /// unnamed: no name is invented.
    pub components: Vec<Component>,
}

/// One component of a tuple column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Component {
    pub name: String,
    pub sol_type: String,
    pub storage: String,
}

/// What kind of chain data a table holds. Events are the only kind that existed before RFC-0014, so
/// this defaults to `Event` and is omitted from `schema.json` in that case - an existing nest's
/// artifact is byte-identical after the field was added.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TableKind {
    #[default]
    Event,
    /// One row per block, from the header (RFC-0036). Has no topic0, no selector and no ABI: it is
    /// the first table whose source is the chain itself rather than something a contract emitted.
    Block,
    /// A decoded contract call, keyed by 4-byte function selector (RFC-0014).
    Call,
    /// Raw storage writes (RFC-0014).
    State,
}

impl TableKind {
    fn is_event(&self) -> bool {
        matches!(self, TableKind::Event)
    }
}

/// The `blocks` table (RFC-0036 §4.2). Declared once, here, and used by both the schema and the row
/// builder - two places that must agree on every name and order, and would drift if each held its own
/// list. `number`/`hash`/`timestamp` are deliberately **not** repeated as columns: they arrive as the
/// implicit `block_number`/`block_hash`/`block_timestamp` every row already carries.
pub const BLOCK_COLUMNS: &[(&str, &str, StorageKind)] = &[
    ("parent_hash", "bytes32", StorageKind::FixedBytes),
    ("miner", "address", StorageKind::Address),
    ("gas_used", "uint64", StorageKind::U64),
    ("gas_limit", "uint64", StorageKind::U64),
    ("base_fee_per_gas", "uint64", StorageKind::U64),
    ("size", "uint64", StorageKind::U64),
    ("transaction_count", "uint64", StorageKind::U64),
    // **The L1 block an L2 block was sequenced against**, on chains that report one. Arbitrum puts
    // `l1BlockNumber` in the header it already returns, so this costs nothing extra to keep - the
    // same reasoning as every other column here.
    //
    // It is the only bridge between the two block spaces, and its absence is load-bearing:
    // `EpochManager` counts epochs in **L1** blocks while an indexed log carries an **L2** block, so
    // an epoch boundary has to be guessed from the first *observed* event instead, leaving a window
    // of several thousand blocks whose contents are filed against the wrong epoch
    // (nuthatch-org/nuthatch#1116).
    //
    // **0 on a chain that does not report it**, which is most of them, following `base_fee_per_gas`
    // above: 0 is the honest value for "the field did not exist".
    ("l1_block_number", "uint64", StorageKind::U64),
];

/// The table name for [`BLOCK_COLUMNS`]. Unprefixed by an alias, because a block belongs to the chain
/// rather than to any one contract in the nest.
pub const BLOCKS_TABLE: &str = "blocks";

/// The `[extract] l1_blocks` table (#1839): one row per block that carries this nest's logs, holding
/// that block's `l1_block_number`. A side table rather than a column on every event table, so a nest
/// that leaves the setting off keeps its event schemas and segment bytes exactly as they were.
pub const L1_BLOCKS_TABLE: &str = "l1_blocks";

/// The `log_index` a **block row** is stored under (#642).
///
/// Rows are keyed `(block, log_index)` by `Store::entity_key`, which assumes every row descends from
/// a log. A block row descends from none, and using `0` made it indistinguishable from the *first log
/// in the block* - so the block row, written second, silently overwrote it. That is the first event of
/// every block, gone, with no warning and no gap in the cursor.
///
/// `500_000..=999_999` is therefore **reserved for rows that descend from no log**. Real logs cannot
/// reach it: `entity_key` already asserts `log_index < 1_000_000`, and a block's gas limit caps logs
/// around 80k, well below the reserve. Block rows take the very top so they sort after the logs they
/// summarise; RFC-0023 tier-3 call results take the rest.
///
/// The band is half the range rather than a thousand slots because a **row-driven** call fires once
/// per source row (RFC-0038 §3), so one block can want thousands of results. A narrow band would have
/// turned that into a silent key collision - the very bug (#642) this constant exists to fix.
pub const BLOCK_ROW_LOG_INDEX: u64 = 999_999;

/// The base `log_index` for **pinned call results** (RFC-0023 tier 3), inside the same reserved band
/// as [`BLOCK_ROW_LOG_INDEX`].
///
/// A call result descends from no log either, and there may be many in one block: a sampled
/// declaration contributes one, a row-driven one contributes a call per source row. They are laid out
/// from this base in a deterministic order - declarations in config order, and within a row-driven
/// declaration its source rows in `log_index` order - so two operators running the same nest produce
/// the same keys, not merely the same content addresses.
pub const CALL_ROW_LOG_INDEX_BASE: u64 = 500_000;

/// The base `log_index` for **decoded top-level calls** (RFC-0038 §5), the upper half of the reserved
/// band.
///
/// A top-level call is a transaction, so its ordinal is the transaction index - which lives in the
/// same numeric space as `log_index` and would otherwise collide with both a log and a tier-3 result.
/// `CallContext::call_index` recorded this as a known gap "deliberately left for the extraction
/// slice"; this is that slice, and the band is the answer.
///
/// A block's gas limit caps transactions near 1,500, so the quarter-million slots here are enormous
/// headroom. The band is partitioned:
///
/// | Range | Rows |
/// |---|---|
/// | `500_000..=624_999` | pinned `eth_call` results ([`CALL_ROW_LOG_INDEX_BASE`]) |
/// | `625_000..=749_999` | resolved IPFS documents ([`IPFS_ROW_LOG_INDEX_BASE`]), and when a declaration explodes them into typed rows, documents in `625_000..=625_999` and their rows in `626_000..=749_999` ([`IPFS_DOCUMENT_ROW_LOG_INDEX_BASE`]) |
/// | `750_000..=999_998` | decoded top-level calls (here) |
/// | `999_999` | the block row ([`BLOCK_ROW_LOG_INDEX`]) |
///
/// Every one of these is bounded by something smaller than its quarter: reads and resolutions by the
/// source rows in a block (itself bounded by the ~80k log ceiling), calls by the transaction count.
pub const TX_CALL_ROW_LOG_INDEX_BASE: u64 = 750_000;

/// The base `log_index` for **resolved IPFS documents** (RFC-0037), between pinned reads and calls.
///
/// A resolved document descends from no log: it is the content behind a CID some row referenced. Same
/// reasoning as its neighbours, and the same band.
pub const IPFS_ROW_LOG_INDEX_BASE: u64 = 625_000;

/// Documents a block may hold once any declaration explodes documents into typed rows (RFC-0037 slice
/// 8). Their rows need the rest of the IPFS band, so the documents give up all but its first thousand.
pub const IPFS_DOCUMENT_SLOTS: u64 = 1_000;

/// The first `log_index` of a document's typed rows. Each document is allotted its declaration's
/// `max_rows` from here, in slot order, so a row's key follows from the plan and never from which
/// documents happened to resolve first.
pub const IPFS_DOCUMENT_ROW_LOG_INDEX_BASE: u64 = IPFS_ROW_LOG_INDEX_BASE + IPFS_DOCUMENT_SLOTS;

/// The last `log_index` typed rows may take: the call band starts at the next one.
pub const IPFS_DOCUMENT_ROW_LOG_INDEX_END: u64 = TX_CALL_ROW_LOG_INDEX_BASE - 1;

/// A serializable table schema (per-event table + its columns).
///
/// `event`/`topic0` and `function`/`selector` are the same idea for different [`TableKind`]s, and
/// exactly one pair is populated. They are kept as distinct fields rather than one reused pair
/// because `schema.json` is read by humans and by agents, and a 4-byte selector sitting in a field
/// called `topic0` would be a lie that costs more than the two extra keys.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub table: String,
    pub alias: String,
    #[serde(default, skip_serializing_if = "TableKind::is_event")]
    pub kind: TableKind,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub event: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub topic0: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub function: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub selector: String,
    pub columns: Vec<ColumnSchema>,
}

impl TableSchema {
    /// Whether this table is ERC-20/721 `Transfer`-shaped: named `*__transfer`, whose three *event*
    /// params (the non-implicit columns) are `address, address, uint`. The schema-level mirror of
    /// [`EventDecoder::transfer_columns`], so the balance-derived MCP tools are gated (RFC-0025) on the
    /// same signal the balance view itself is built from.
    pub fn is_transfer_shaped(&self) -> bool {
        if !self.table.ends_with("__transfer") {
            return false;
        }
        let params: Vec<&ColumnSchema> = self
            .columns
            .iter()
            .filter(|c| c.sol_type != "implicit")
            .collect();
        params.len() == 3 && params[0].sol_type == "address" && params[1].sol_type == "address"
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnSchema {
    pub name: String,
    pub sol_type: String,
    pub storage: String,
    pub indexed: bool,
    /// A tuple's named components, in ABI order. See `Column::components`.
    ///
    /// `skip_serializing_if` so a nest with no tuple column writes a byte-identical `schema.json`, and
    /// `default` so one written before this field existed still parses - the same contract `TableKind`
    /// established.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub components: Vec<Component>,
}

/// The implicit columns every table carries (before the event's own params). `_seq` is a single
/// monotonic per-row ordering key, derived deterministically from (block, log_index) - not a mutable
/// insertion counter, so it stays re-executable and reorg-stable per the determinism rule.
pub fn implicit_columns(timestamps: bool) -> Vec<ColumnSchema> {
    [
        "block_number",
        "block_hash",
        "block_timestamp",
        "tx_hash",
        "log_index",
        "address",
        "_seq",
    ]
    .iter()
    // A timestamp-free nest (RFC-0029 §6b) must not *advertise* the column it doesn't produce.
    // `/tables`, `schema.json`, the MCP schema tool and `llms.txt` all read this, and the whole point
    // of declaring it at init is that consumers can see the shape before they write a query against
    // a column that will never arrive.
    .filter(|n| timestamps || **n != "block_timestamp")
    .map(|n| ColumnSchema {
        name: (*n).to_string(),
        sol_type: "implicit".to_string(),
        components: Vec::new(),
        storage: match *n {
            "block_number" | "log_index" | "_seq" | "block_timestamp" => "u64",
            "address" => "address",
            "block_hash" | "tx_hash" => "bytes32",
            _ => "string",
        }
        .to_string(),
        indexed: false,
    })
    .collect()
}

/// Decodes one event of one contract into rows of one table.
pub struct EventDecoder {
    pub alias: String,
    pub contract: Address,
    pub table: String,
    pub columns: Vec<Column>,
    pub topic0: B256,
    pub signature: String,
    event: Event,
}

/// A tuple's named components, in ABI order, or empty when the ABI names none of them.
///
/// All-or-nothing on purpose: a partially-named tuple would give some components a name and others an
/// index, and a view mixing the two is harder to read than one that is honestly positional. Duplicate
/// names are the same refusal: an object cannot carry two values on one key. No name is ever invented.
///
/// One level only. A component that is itself a tuple keeps its positional JSON, and says so by carrying
/// `sol_type: "tuple"` with no components of its own.
pub fn tuple_components(components: &[alloy_json_abi::Param]) -> Vec<Component> {
    if components.is_empty() || components.iter().any(|c| c.name.is_empty()) {
        return Vec::new();
    }
    let mut seen = HashSet::with_capacity(components.len());
    if !components.iter().all(|c| seen.insert(c.name.as_str())) {
        return Vec::new();
    }
    components
        .iter()
        .map(|c| Component {
            name: c.name.clone(),
            sol_type: c.ty.clone(),
            storage: StorageKind::from_sol(&c.ty, false).as_str().to_string(),
        })
        .collect()
}

impl EventDecoder {
    fn new(alias: &str, contract: Address, event: Event) -> EventDecoder {
        let columns: Vec<Column> = event
            .inputs
            .iter()
            .enumerate()
            .map(|(i, p)| {
                let kind = StorageKind::from_sol(&p.ty, p.indexed);
                let base = if p.name.is_empty() {
                    format!("arg{i}")
                } else {
                    p.name.clone()
                };
                // Indexed dynamic types (string/bytes/arrays) arrive as keccak(value), not the value
                // (RFC-0001 §Design): suffix `_hash` so the column can't be mistaken for the value.
                let name = if kind == StorageKind::Hash32 {
                    format!("{base}_hash")
                } else {
                    base
                };
                Column {
                    name,
                    sol_type: p.ty.clone(),
                    kind,
                    indexed: p.indexed,
                    components: tuple_components(&p.components),
                }
            })
            .collect();
        EventDecoder {
            alias: alias.to_string(),
            contract,
            table: format!("{alias}__{}", snake_case(&event.name)),
            columns,
            topic0: event.selector(),
            signature: event.signature(),
            event,
        }
    }

    /// If this decoder is ERC-20/721 `Transfer(address, address, uint)`-shaped, the (from, to, value)
    /// column *names* - which vary by token (USDC: from/to/value; WETH: src/dst/wad). Mirrors
    /// `DecodedRow::is_erc20_transfer` at the schema level so the balance-view rebuild reads the same
    /// columns the live path feeds positionally.
    pub fn transfer_columns(&self) -> Option<(&str, &str, &str)> {
        if self.table.ends_with("__transfer")
            && self.columns.len() == 3
            && matches!(self.columns[0].kind, StorageKind::Address)
            && matches!(self.columns[1].kind, StorageKind::Address)
        {
            Some((
                &self.columns[0].name,
                &self.columns[1].name,
                &self.columns[2].name,
            ))
        } else {
            None
        }
    }
}

/// Where [`count_header_transactions`] leaves a header's transaction count.
const HEADER_TX_COUNT: &str = "transactionCount";

/// Replace a hashes-only header's `transactions` with their count, which is all [`block_row`] reads.
/// A dense block carries thousands of hashes, and a window holds a header for every block (#2309).
pub fn count_header_transactions(header: &mut Json) {
    let Some(obj) = header.as_object_mut() else {
        return;
    };
    if let Some(n) = obj
        .get("transactions")
        .and_then(Json::as_array)
        .map(Vec::len)
    {
        obj.remove("transactions");
        obj.insert(HEADER_TX_COUNT.to_string(), json!(n));
    }
}

/// Build the `blocks` row for one header (RFC-0036 §4.2). `None` when the header is missing the
/// fields that identify it, which the caller turns into a refused window rather than a gap.
///
/// **Row identity, stated because a silent convention in the key encoding is how COR-6 happened.**
/// The hot store keys on `(block, log_index)` and every row carries `tx_hash`/`address`. A block has
/// none of the three, so: `log_index` is 0 (one row per block makes it unique by construction),
/// `tx_hash` is the block's own hash, and `address` is empty. The alternative - reshaping the key for
/// one table - would touch the storage layer this design exists to leave alone.
pub fn block_row(number: u64, header: &Json, timestamps: bool) -> Option<DecodedRow> {
    let hex = |k: &str| -> u64 {
        header
            .get(k)
            .and_then(Json::as_str)
            .and_then(|v| u64::from_str_radix(v.trim_start_matches("0x"), 16).ok())
            .unwrap_or(0)
    };
    // A 20-byte address arrives as `0x`-prefixed hex; anything else (a `null` `miner` on some
    // endpoints) becomes the zero address rather than failing the whole window.
    let addr = |k: &str| -> Value {
        let raw = header.get(k).and_then(Json::as_str).unwrap_or_default();
        let mut out = [0u8; 20];
        if let Ok(bytes) = hex::decode(raw.trim_start_matches("0x")) {
            if bytes.len() == 20 {
                out.copy_from_slice(&bytes);
            }
        }
        Value::Address(out)
    };
    // `bytes32` is `StorageKind::FixedBytes`, whose value is `Value::Bytes` - matching what an
    // ordinary bytes32 event param produces, so `parent_hash` is not a special case downstream.
    let fixed_bytes = |k: &str| -> Value {
        let raw = header.get(k).and_then(Json::as_str).unwrap_or_default();
        Value::Bytes(hex::decode(raw.trim_start_matches("0x")).unwrap_or_default())
    };
    let hash = header.get("hash").and_then(Json::as_str)?.to_string();
    let tx_count = header
        .get("transactions")
        .and_then(Json::as_array)
        .map(|a| a.len() as u64)
        .or_else(|| header.get(HEADER_TX_COUNT).and_then(Json::as_u64))
        // `[block, false]` returns transaction *hashes*; an endpoint that omits the array entirely
        // yields 0 rather than a wrong count, and the column says `transaction_count` not `has_txs`.
        .unwrap_or(0);
    let params = vec![
        ("parent_hash".to_string(), fixed_bytes("parentHash")),
        ("miner".to_string(), addr("miner")),
        ("gas_used".to_string(), Value::U64(hex("gasUsed"))),
        ("gas_limit".to_string(), Value::U64(hex("gasLimit"))),
        // Pre-EIP-1559 blocks have no base fee at all; 0 is the honest value for "the field did not
        // exist yet", and OBIB case 3 covers blocks 0-100,000, which are all pre-London.
        (
            "base_fee_per_gas".to_string(),
            Value::U64(hex("baseFeePerGas")),
        ),
        ("size".to_string(), Value::U64(hex("size"))),
        ("transaction_count".to_string(), Value::U64(tx_count)),
        // `hex` already yields 0 for a key the endpoint does not send, which is the whole handling a
        // non-Arbitrum chain needs.
        (
            "l1_block_number".to_string(),
            Value::U64(hex("l1BlockNumber")),
        ),
    ];
    Some(DecodedRow {
        table: BLOCKS_TABLE.to_string(),
        params,
        block_number: number,
        block_hash: hash.clone(),
        block_timestamp: hex("timestamp"),
        timestamps,
        log_index: BLOCK_ROW_LOG_INDEX,
        tx_hash: hash,
        address: String::new(),
    })
}

/// Build the `l1_blocks` row for one header, keyed like a block row. `None` when the header has no
/// hash or no `l1BlockNumber`: unlike `blocks`, a 0 here would be sealed as a wrong answer rather than
/// an absent field, so the caller refuses the window instead.
pub fn l1_block_row(number: u64, header: &Json, timestamps: bool) -> Option<DecodedRow> {
    let hex = |k: &str| -> Option<u64> {
        header
            .get(k)
            .and_then(Json::as_str)
            .and_then(|v| u64::from_str_radix(v.trim_start_matches("0x"), 16).ok())
    };
    let hash = header.get("hash").and_then(Json::as_str)?.to_string();
    let l1 = hex("l1BlockNumber")?;
    Some(DecodedRow {
        table: L1_BLOCKS_TABLE.to_string(),
        params: vec![("l1_block_number".to_string(), Value::U64(l1))],
        block_number: number,
        block_hash: hash.clone(),
        block_timestamp: hex("timestamp").unwrap_or(0),
        timestamps,
        log_index: BLOCK_ROW_LOG_INDEX,
        tx_hash: hash,
        address: String::new(),
    })
}

/// One decoded log row.
///
/// `PartialEq` so a reconstruction can be compared to the original **field by field, with its values
/// still typed**. Comparing `to_json` output would not do: `Value::Str("7")` and `Value::Word16(7)`
/// render identically, and telling those two apart is the entire job of `from_stored`.
#[derive(Debug, Clone, PartialEq)]
pub struct DecodedRow {
    pub table: String,
    pub params: Vec<(String, Value)>,
    pub block_number: u64,
    pub block_hash: String,
    /// Unix seconds from the block header. Set by the indexer after decode (the log doesn't carry
    /// it); 0 until then, and 0 if the source couldn't supply it.
    pub block_timestamp: u64,
    /// Whether this row's table has a `block_timestamp` column at all (RFC-0029 §6b), copied from the
    /// registry that decoded it. When false, [`DecodedRow::to_json`] omits the key entirely - the
    /// column is *absent*, not null: a null would keep the schema stable but invite an `ORDER BY
    /// block_timestamp` that silently returns arbitrary order, which is worse than an error.
    pub timestamps: bool,
    pub log_index: u64,
    pub tx_hash: String,
    pub address: String,
}

impl DecodedRow {
    /// A single monotonic ordering key for this row within its table, derived deterministically from
    /// (block, log_index): `block << 20 | log_index`. Deterministic (re-executable) by construction -
    /// no mutable insertion counter - and total/stable since log_index is unique within a block.
    pub fn seq(&self) -> u64 {
        // The 20-bit log_index field holds up to 1,048,575 - orders of magnitude above any real block's
        // log count (~80k at current gas limits). A pathological block beyond that would collide/wrap;
        // catch it in tests/CI rather than let it silently mis-order.
        debug_assert!(
            self.log_index < (1 << 20),
            "log_index {} exceeds the 20-bit _seq field",
            self.log_index
        );
        (self.block_number << 20) | (self.log_index & 0xF_FFFF)
    }

    pub fn to_json(&self) -> Json {
        let mut obj = serde_json::Map::new();
        obj.insert("table".into(), json!(self.table));
        obj.insert("block_number".into(), json!(self.block_number));
        obj.insert("block_hash".into(), json!(self.block_hash));
        if self.timestamps {
            obj.insert("block_timestamp".into(), json!(self.block_timestamp));
        }
        obj.insert("tx_hash".into(), json!(self.tx_hash));
        obj.insert("log_index".into(), json!(self.log_index));
        obj.insert("address".into(), json!(self.address));
        obj.insert("_seq".into(), json!(self.seq()));
        for (name, v) in &self.params {
            obj.insert(name.clone(), v.to_json());
        }
        Json::Object(obj)
    }

    /// Rebuild a row from the form it was stored in, against the table's schema.
    ///
    /// The counterpart to [`DecodedRow::to_json`], which was one-way until now, so every consumer
    /// that needed typed values back out of the hot store hand-rolled its own (nuthatch#864). The
    /// reorg path is the one that cannot afford a second opinion: it feeds rolled-back rows to a
    /// circuit at weight `-1`, and DBSP cancels by key, so a retraction built by a different
    /// converter than the insertion does not cancel it - it lands beside it and stays forever.
    ///
    /// **The schema decides the column order, not the JSON.** A stored row is a map, and a plan
    /// indexes its columns by position; taking the order from the map would make it depend on
    /// whatever the serialiser felt like, which for `serde_json` is insertion order and for a
    /// Parquet reader is the file's.
    pub fn from_stored(stored: &Json, schema: &TableSchema) -> Result<DecodedRow> {
        let obj = stored
            .as_object()
            .ok_or_else(|| anyhow!("a stored row is an object, not {stored}"))?;
        let get = |key: &str| -> Result<&Json> {
            obj.get(key)
                .ok_or_else(|| anyhow!("stored row of {} has no {key}", schema.table))
        };
        let text = |key: &str| -> Result<String> {
            Ok(get(key)?
                .as_str()
                .map(str::to_string)
                .unwrap_or_else(|| get(key).map(ToString::to_string).unwrap_or_default()))
        };
        // Numbers arrive as JSON numbers from the hot store and as text from a sealed segment's
        // `Utf8` column, so both have to parse or the reorg path and the restart path disagree.
        let number = |key: &str| -> Result<u64> {
            let v = get(key)?;
            match v {
                Json::Number(n) => n
                    .as_u64()
                    .ok_or_else(|| anyhow!("{key} does not fit u64: {v}")),
                Json::String(s) => s
                    .parse()
                    .with_context(|| format!("{key} is not a number: {s}")),
                other => bail!("{key} is not a number: {other}"),
            }
        };

        if let Some(table) = obj.get("table").and_then(Json::as_str) {
            if table != schema.table {
                bail!(
                    "stored row says table {table}, schema says {}; reconstructing it against the \
                     wrong schema would read its columns as the wrong types",
                    schema.table
                )
            }
        }

        let timestamps = schema.columns.iter().any(|c| c.name == "block_timestamp");
        let block_number = number("block_number")?;
        let log_index = number("log_index")?;

        let mut params = Vec::new();
        for column in schema.columns.iter().filter(|c| c.sol_type != "implicit") {
            let v = obj.get(&column.name).ok_or_else(|| {
                anyhow!(
                    "stored row of {} has no {} - the schema and the stored row disagree about this \
                     table's shape",
                    schema.table,
                    column.name
                )
            })?;
            params.push((column.name.clone(), value_from_stored(v, column)?));
        }

        let row = DecodedRow {
            table: schema.table.clone(),
            params,
            block_number,
            block_hash: text("block_hash")?,
            block_timestamp: if timestamps {
                number("block_timestamp")?
            } else {
                0
            },
            timestamps,
            log_index,
            tx_hash: text("tx_hash")?,
            address: text("address")?,
        };

        // `_seq` is derived rather than stored authoritatively, so it is a checksum on the two fields
        // it is derived from. A row whose `_seq` disagrees with its own block and log index has been
        // rewritten by something, and ordering built on it would be wrong in a way nothing else here
        // would notice.
        if let Some(stored_seq) = obj.get("_seq") {
            let stored_seq = number("_seq").with_context(|| format!("{stored_seq}"))?;
            if stored_seq != row.seq() {
                bail!(
                    "stored row of {} carries _seq {stored_seq} but block {} log {} derives {}",
                    schema.table,
                    row.block_number,
                    row.log_index,
                    row.seq()
                )
            }
        }
        Ok(row)
    }

    /// True if this row looks like an ERC-20/721 `Transfer(address, address, uint)` - the shape the
    /// hardcoded balance view + transfer sealing understand.
    pub fn is_erc20_transfer(&self) -> bool {
        self.table.ends_with("__transfer")
            && self.params.len() == 3
            && matches!(self.params[0].1, Value::Address(_))
            && matches!(self.params[1].1, Value::Address(_))
    }

    /// (from, to, value-decimal-if-it-fits-u128, value-hex) for a transfer row, else None.
    pub fn erc20_transfer_fields(&self) -> Option<(String, String, Option<String>, String)> {
        if !self.is_erc20_transfer() {
            return None;
        }
        let addr = |v: &Value| match v {
            Value::Address(a) => Some(format!("0x{}", hex::encode(a))),
            _ => None,
        };
        let from = addr(&self.params[0].1)?;
        let to = addr(&self.params[1].1)?;
        let (value, value_hex) = match &self.params[2].1 {
            Value::U64(n) => (Some(n.to_string()), format!("0x{:064x}", n)),
            Value::Word16(b) => {
                let mut full = [0u8; 32];
                full[16..].copy_from_slice(b);
                (
                    Some(u128::from_be_bytes(*b).to_string()),
                    format!("0x{}", hex::encode(full)),
                )
            }
            Value::Word32(b) => {
                let hex = format!("0x{}", hex::encode(b));
                let value = b[..16]
                    .iter()
                    .all(|&x| x == 0)
                    .then(|| u128::from_be_bytes(b[16..].try_into().unwrap()).to_string());
                (value, hex)
            }
            _ => return None,
        };
        Some((from, to, value, value_hex))
    }
}

/// One contract to index: an alias, its address, and its resolved ABI.
pub struct ContractSpec {
    pub alias: String,
    pub address: Address,
    pub abi: JsonAbi,
    /// Event allowlist (RFC-0011): only events whose ABI name is listed are decoded. Empty = all.
    pub events: Vec<String>,
}

/// A template (RFC-0009): an ABI applied to contracts discovered at runtime rather than a fixed
/// address. Its decoders are keyed by topic0 but matched against the child registry, not an address.
pub struct TemplateSpec {
    pub name: String,
    pub abi: JsonAbi,
    /// Event allowlist for this template's children, empty = decode every event the ABI defines.
    /// Mirrors `[[contracts]].events`; see `config::Template::events`.
    pub events: Vec<String>,
}

/// The immutable per-nest decode registry.
pub struct DecodeRegistry {
    /// Contract decoders - matched by (topic0, emitting address ∈ the contract's fixed address).
    by_topic0: HashMap<B256, Vec<EventDecoder>>,
    /// Template decoders (RFC-0009) - matched by (topic0, template name) against the runtime child
    /// registry. Address-agnostic: one set of tables shared across every discovered child.
    templates_by_topic0: HashMap<B256, Vec<EventDecoder>>,
    hash: [u8; 32],
    skipped_anonymous: usize,
    /// Whether decoded rows carry `block_timestamp` (RFC-0029 §6b) - from `[nest] block_timestamps`.
    ///
    /// The registry is already the single source of truth for a nest's table schema, so the policy
    /// that *changes* that schema belongs here rather than being threaded separately to decode, to
    /// serialisation and to `/tables` - three places that must never disagree about whether a column
    /// exists. Deliberately **not** folded into [`DecodeRegistry::hash`]: that hash versions decode
    /// behaviour and is stamped into every segment's `registry_snapshot`, so mixing a schema flag
    /// into it would invalidate the snapshots of every existing nest to describe something the
    /// content-addressed segment bytes already distinguish.
    timestamps: bool,
    /// Whether this nest declares `[extract] blocks` (RFC-0036). Lives here for the same reason
    /// `timestamps` does: the registry is the single source of truth for a nest's table set, and a
    /// second place that decides which tables exist is a second place that can disagree.
    ///
    /// Also **not** folded into [`DecodeRegistry::hash`] - for the same reason as `timestamps`. The
    /// hash versions *decode behaviour* and is stamped into every segment's `registry_snapshot`;
    /// mixing a table-set flag into it would invalidate every existing nest's snapshots to describe
    /// something the content-addressed segment bytes already distinguish.
    blocks: bool,
    /// Whether this nest declares `[extract] l1_blocks` (#1839). Not in the hash, as above.
    l1_blocks: bool,
}

/// Event parameter names that would collide with a column nuthatch adds itself.
///
/// Every table carries the implicit columns before the event's own parameters (see
/// [`implicit_columns`]). An ABI is free to name a parameter any of them, and until #814 nothing
/// refused it - with two consequences, both silent:
///
///  * **The parameter won the data.** `DecodedRow::to_json` inserts the implicit columns first and
///    then loops over params; `serde_json::Map::insert` *replaces*. A row whose true block was
///    4,000,000 serialised with the event's number instead, and the chain's block number was not in
///    the row at all - while `_seq`, computed before the overwrite, still encoded the true block.
///  * **The schema advertised the name twice**, and `/tables`, `schema.json`, the MCP schema tool
///    and `llms.txt` all published it.
///
/// `DecodedRow::from_stored` reads `block_number` back out on the reorg path, where a retraction
/// built from a different value than its insertion does not cancel it in DBSP - it lands beside it
/// and stays. That is why the board chose refusal over renaming (2026-08-31): it is the only option
/// that cannot produce a wrong number.
///
/// **Reserved regardless of the `block_timestamps` flag.** A timestamp-free nest does not carry that
/// column today, but flipping the flag is supported (RFC-0029 §6b) and would otherwise turn a
/// working nest into a colliding one at the worst possible moment.
pub fn reserved_column_names() -> &'static [&'static str] {
    &[
        "block_number",
        "block_hash",
        "block_timestamp",
        "tx_hash",
        "log_index",
        "address",
        "_seq",
        "table",
    ]
}

/// Where offchain snapshots are served (RFC-0045 §6), and how an entity tells an offchain source
/// from a decoded one (#1437).
pub const OFFCHAIN_NAMESPACE: &str = "offchain__";

/// Refuse a contract or template whose `{alias}__{event}` tables would land in the offchain
/// namespace. View names are lowercased when defined and resolved case-insensitively, so the two
/// views would replace each other.
fn refuse_offchain_namespace(what: &str, who: &str) -> Result<()> {
    if format!("{who}__")
        .to_ascii_lowercase()
        .starts_with(OFFCHAIN_NAMESPACE)
    {
        bail!(
            "{what} '{who}' would name its tables `{who}__<event>`, inside the `{OFFCHAIN_NAMESPACE}` \
             namespace offchain snapshots are served from (RFC-0045 §6). On the SQL surface one \
             table would silently answer with the other's rows. Rename the {what}."
        );
    }
    Ok(())
}

/// Refuse an ABI whose decoded events name a parameter after an implicit column (#814).
fn refuse_reserved_parameter_names(
    what: &str,
    who: &str,
    abi: &JsonAbi,
    allowlist: &[String],
) -> Result<()> {
    for ev in abi.events() {
        // Only events that will actually be decoded: an allowlisted nest is not broken by a
        // colliding parameter on an event it never reads.
        if !allowlist.is_empty() && !allowlist.iter().any(|a| a == &ev.name) {
            continue;
        }
        // The resolved column names, after `_hash` and `arg{i}`, are what `to_json` writes (#2159).
        let columns = EventDecoder::new(who, Address::ZERO, ev.clone()).columns;
        let mut seen = HashSet::with_capacity(columns.len());
        for c in &columns {
            if !seen.insert(c.name.as_str()) {
                bail!(
                    "{what} '{who}' declares event '{}' with two parameters that both become column \
                     '{}'. One would silently replace the other in every stored row.",
                    ev.name,
                    c.name,
                );
            }
            if reserved_column_names().contains(&c.name.as_str()) {
                bail!(
                    "{what} '{who}' declares event '{}' with a parameter stored as column '{}', which \
                     is a column nuthatch adds to every row. Indexing it would silently replace the \
                     chain's own value with the event's - the block number in the stored row would \
                     be the event's parameter, not the block it came from (#814). Reserved names: \
                     {}. There is no safe automatic rename: the column name is part of the query \
                     surface that views, `entities.toml` and any published schema already depend on.",
                    ev.name,
                    c.name,
                    reserved_column_names().join(", "),
                );
            }
        }
    }
    Ok(())
}

impl DecodeRegistry {
    /// Build from a nest's config: load each contract's vendored ABI and register its events.
    /// dozens of test and tool call sites that don't care keep the behaviour they had.
    /// Declare the `blocks` table (RFC-0036 §4.2). Builder-style beside `with_timestamps` because
    /// both answer "what tables does this nest have" and are set from the same config load.
    pub fn with_blocks(mut self, blocks: bool) -> DecodeRegistry {
        self.blocks = blocks;
        self
    }

    /// Does this nest declare a `blocks` table?
    pub fn blocks(&self) -> bool {
        self.blocks
    }

    pub fn with_l1_blocks(mut self, l1_blocks: bool) -> DecodeRegistry {
        self.l1_blocks = l1_blocks;
        self
    }

    /// Does this nest declare an `l1_blocks` table?
    pub fn l1_blocks(&self) -> bool {
        self.l1_blocks
    }

    pub fn with_timestamps(mut self, timestamps: bool) -> DecodeRegistry {
        self.timestamps = timestamps;
        self
    }

    /// Whether this nest indexes block timestamps. Callers that would otherwise fetch them (the four
    /// backfill/tip paths in `indexer.rs`) consult this before spending the round trips.
    pub fn timestamps(&self) -> bool {
        self.timestamps
    }

    pub fn build(contracts: Vec<ContractSpec>) -> Result<DecodeRegistry> {
        Self::build_with_templates(contracts, Vec::new())
    }

    pub fn build_with_templates(
        contracts: Vec<ContractSpec>,
        templates: Vec<TemplateSpec>,
    ) -> Result<DecodeRegistry> {
        let mut by_topic0: HashMap<B256, Vec<EventDecoder>> = HashMap::new();
        let mut skipped_anonymous = 0usize;

        for c in &contracts {
            // A typo in an allowlist would silently index nothing at scale - reject it loudly.
            if !c.events.is_empty() {
                let known: std::collections::HashSet<&str> =
                    c.abi.events().map(|e| e.name.as_str()).collect();
                for want in &c.events {
                    if !known.contains(want.as_str()) {
                        bail!(
                            "contract '{}' allowlists event '{}', which its ABI does not define \
                             (known events: {})",
                            c.alias,
                            want,
                            {
                                let mut ks: Vec<&str> = known.iter().copied().collect();
                                ks.sort();
                                ks.join(", ")
                            }
                        );
                    }
                }
            }
            refuse_offchain_namespace("contract", &c.alias)?;
            refuse_reserved_parameter_names("contract", &c.alias, &c.abi, &c.events)?;
            skipped_anonymous +=
                register_events(&mut by_topic0, &c.alias, c.address, &c.abi, &c.events);
        }

        // Template decoders share the same machinery; their contract address is unused (ZERO) - they
        // are matched by template name against the runtime child registry, not by address.
        let mut templates_by_topic0: HashMap<B256, Vec<EventDecoder>> = HashMap::new();
        for t in &templates {
            // Same allowlist rule as a contract: a typo would silently index nothing at scale.
            if !t.events.is_empty() {
                let known: std::collections::HashSet<&str> =
                    t.abi.events().map(|e| e.name.as_str()).collect();
                for want in &t.events {
                    if !known.contains(want.as_str()) {
                        bail!(
                            "template '{}' allowlists event '{}', which its ABI does not define \
                             (known events: {})",
                            t.name,
                            want,
                            {
                                let mut ks: Vec<&str> = known.iter().copied().collect();
                                ks.sort();
                                ks.join(", ")
                            }
                        );
                    }
                }
            }
            refuse_offchain_namespace("template", &t.name)?;
            refuse_reserved_parameter_names("template", &t.name, &t.abi, &t.events)?;
            // An empty allowlist still decodes everything, so a template that does not set `events`
            // behaves exactly as it did before RFC-0009 gained one.
            skipped_anonymous += register_events(
                &mut templates_by_topic0,
                &t.name,
                Address::ZERO,
                &t.abi,
                &t.events,
            );
        }

        let hash = registry_hash(&by_topic0, &templates_by_topic0);
        Ok(DecodeRegistry {
            by_topic0,
            templates_by_topic0,
            hash,
            skipped_anonymous,
            timestamps: true,
            blocks: false,
            l1_blocks: false,
        })
    }

    pub fn hash(&self) -> [u8; 32] {
        self.hash
    }

    pub fn skipped_anonymous(&self) -> usize {
        self.skipped_anonymous
    }

    /// All topic0s to request in a combined `eth_getLogs` filter - contract *and* template events,
    /// so a factory nest's topic0-only tip fetch (RFC-0009) captures children's logs too.
    pub fn topic0s(&self) -> Vec<B256> {
        let mut set: Vec<B256> = self
            .by_topic0
            .keys()
            .chain(self.templates_by_topic0.keys())
            .copied()
            .collect();
        set.sort();
        set.dedup();
        set
    }

    /// True if this nest declares any templates (i.e. is a factory nest).
    pub fn has_templates(&self) -> bool {
        !self.templates_by_topic0.is_empty()
    }

    /// All contract addresses to request in a combined filter.
    pub fn addresses(&self) -> Vec<Address> {
        let mut set: Vec<Address> = self
            .by_topic0
            .values()
            .flatten()
            .map(|d| d.contract)
            .collect();
        set.sort();
        set.dedup();
        set
    }

    /// Every table this registry produces (contract and template), with its columns.
    pub fn tables(&self) -> Vec<&EventDecoder> {
        let mut v: Vec<&EventDecoder> = self
            .by_topic0
            .values()
            .flatten()
            .chain(self.templates_by_topic0.values().flatten())
            .collect();
        v.sort_by(|a, b| a.table.cmp(&b.table));
        v
    }

    /// A serializable schema of every table - the single source of truth for `/tables`, the MCP
    /// `schema`/`tables` tools, `llms.txt`, and the nest's `schema.json`.
    pub fn schema(&self) -> Vec<TableSchema> {
        let mut out: Vec<TableSchema> = self
            .tables()
            .iter()
            .map(|d| {
                let mut columns = implicit_columns(self.timestamps);
                columns.extend(d.columns.iter().map(|c| ColumnSchema {
                    name: c.name.clone(),
                    sol_type: c.sol_type.clone(),
                    storage: c.kind.as_str().to_string(),
                    indexed: c.indexed,
                    components: c.components.clone(),
                }));
                TableSchema {
                    table: d.table.clone(),
                    alias: d.alias.clone(),
                    kind: TableKind::Event,
                    event: d.signature.clone(),
                    topic0: format!("0x{}", hex::encode(d.topic0)),
                    function: String::new(),
                    selector: String::new(),
                    columns,
                }
            })
            .collect();
        if self.blocks {
            let mut columns = implicit_columns(self.timestamps);
            columns.extend(BLOCK_COLUMNS.iter().map(|(name, sol, kind)| ColumnSchema {
                name: (*name).to_string(),
                sol_type: (*sol).to_string(),
                storage: kind.as_str().to_string(),
                indexed: false,
                components: Vec::new(),
            }));
            out.push(TableSchema {
                table: BLOCKS_TABLE.to_string(),
                // No contract owns it, and an empty alias would read as "unset" rather than
                // "deliberately none", so it names the chain layer it comes from.
                alias: "chain".to_string(),
                kind: TableKind::Block,
                event: String::new(),
                topic0: String::new(),
                function: String::new(),
                selector: String::new(),
                columns,
            });
            out.sort_by(|a, b| a.table.cmp(&b.table));
        }
        if self.l1_blocks {
            let mut columns = implicit_columns(self.timestamps);
            columns.push(ColumnSchema {
                name: "l1_block_number".to_string(),
                sol_type: "uint64".to_string(),
                storage: StorageKind::U64.as_str().to_string(),
                indexed: false,
                components: Vec::new(),
            });
            out.push(TableSchema {
                table: L1_BLOCKS_TABLE.to_string(),
                alias: "chain".to_string(),
                kind: TableKind::Block,
                event: String::new(),
                topic0: String::new(),
                function: String::new(),
                selector: String::new(),
                columns,
            });
            out.sort_by(|a, b| a.table.cmp(&b.table));
        }
        out
    }

    /// Decode a log against the contract decoders. Returns None if no decoder matches (topic0 +
    /// emitting address). Factory children are decoded separately via [`decode_child`].
    pub fn decode(&self, log: &Log) -> Result<Option<DecodedRow>> {
        let Some(t0_str) = log.topics.first() else {
            return Ok(None);
        };
        let topic0 = parse_b256(t0_str)?;
        let emitter = parse_address(&log.address)?;
        let Some(decoders) = self.by_topic0.get(&topic0) else {
            return Ok(None);
        };
        // Contract-specific decoders first (Allium ordering; a future generic fallback appends).
        let Some(dec) = decoders.iter().find(|d| d.contract == emitter) else {
            return Ok(None);
        };
        Ok(Some(build_row(dec, log, emitter, self.timestamps)?))
    }

    /// Decode a log emitted by a discovered child under `template` (RFC-0009). The caller has already
    /// confirmed the log's address is in the child registry for this template; here we match the
    /// event by topic0 within that template's decoders. Rows land in the shared `{template}__{event}`
    /// table, distinguished by the implicit `address` column.
    pub fn decode_child(&self, log: &Log, template: &str) -> Result<Option<DecodedRow>> {
        let Some(t0_str) = log.topics.first() else {
            return Ok(None);
        };
        let topic0 = parse_b256(t0_str)?;
        let Some(decoders) = self.templates_by_topic0.get(&topic0) else {
            return Ok(None);
        };
        let Some(dec) = decoders.iter().find(|d| d.alias == template) else {
            return Ok(None);
        };
        let emitter = parse_address(&log.address)?;
        Ok(Some(build_row(dec, log, emitter, self.timestamps)?))
    }
}

/// Decode a log's params against a matched decoder into a [`DecodedRow`]. Shared by contract and
/// template decode; the emitter address is recorded so template rows are per-child distinguishable.
fn build_row(
    dec: &EventDecoder,
    log: &Log,
    emitter: Address,
    timestamps: bool,
) -> Result<DecodedRow> {
    let topics: Vec<B256> = log
        .topics
        .iter()
        .map(|t| parse_b256(t))
        .collect::<Result<_>>()?;
    let data = parse_bytes(&log.data)?;
    let decoded = dec
        .event
        .decode_log_parts(topics.iter().copied(), &data)
        .map_err(|e| anyhow!("decode {}: {e}", dec.signature))?;

    let mut indexed = decoded.indexed.iter();
    let mut body = decoded.body.iter();
    let mut params = Vec::with_capacity(dec.columns.len());
    for col in &dec.columns {
        let dv = if col.indexed {
            indexed.next()
        } else {
            body.next()
        }
        .ok_or_else(|| anyhow!("param count mismatch decoding {}", dec.signature))?;
        params.push((col.name.clone(), value_from_dynsol(dv, col)));
    }

    Ok(DecodedRow {
        table: dec.table.clone(),
        params,
        block_number: log.block_number,
        block_hash: log.block_hash.clone(),
        block_timestamp: 0, // filled by the indexer from the block header (see index_loop)
        timestamps,
        log_index: log.log_index,
        tx_hash: log.tx_hash.clone(),
        address: format!("0x{}", hex::encode(emitter)),
    })
}

pub fn value_from_dynsol(dv: &DynSolValue, col: &Column) -> Value {
    // Indexed dynamic types arrive as the 32-byte topic hash.
    if col.kind == StorageKind::Hash32 {
        if let DynSolValue::FixedBytes(w, _) = dv {
            return Value::Hash32(w.0);
        }
    }
    if let DynSolValue::Tuple(items) = dv {
        if !col.components.is_empty() && col.components.len() == items.len() {
            let mut names = HashSet::with_capacity(col.components.len());
            // Duplicate keys would overwrite; fall back to the positional array.
            if col.components.iter().all(|c| names.insert(c.name.as_str())) {
                let mut obj = serde_json::Map::new();
                for (c, item) in col.components.iter().zip(items) {
                    obj.insert(c.name.clone(), dynsol_to_json(item));
                }
                return Value::Json(Json::Object(obj).to_string());
            }
        }
    }
    match dv {
        DynSolValue::Address(a) => Value::Address(a.into_array()),
        DynSolValue::Bool(b) => Value::Bool(*b),
        DynSolValue::Uint(u, bits) => {
            if *bits <= 64 {
                // A conformant uint≤64 fits u64; a maliciously-crafted log with dirty high bits above
                // the declared width would panic `to::<u64>()` and take down the ingestion task (COR-11).
                // Saturate instead - no panic, and a well-formed value is unaffected.
                Value::U64(u.saturating_to::<u64>())
            } else if *bits <= 128 {
                Value::Word16(u.to_be_bytes::<32>()[16..].try_into().unwrap())
            } else {
                Value::Word32(u.to_be_bytes::<32>())
            }
        }
        DynSolValue::Int(i, bits) => {
            if *bits <= 64 {
                // Saturate as the uint arm does (#2148): every value that fits `i64` is unchanged.
                Value::I64(i64::try_from(*i).unwrap_or(if i.is_negative() {
                    i64::MIN
                } else {
                    i64::MAX
                }))
            } else if *bits <= 128 {
                Value::IWord16(i.to_be_bytes::<32>()[16..].try_into().unwrap())
            } else {
                Value::IWord32(i.to_be_bytes::<32>())
            }
        }
        DynSolValue::FixedBytes(w, n) => Value::Bytes(w.0[..(*n).min(32)].to_vec()),
        DynSolValue::Bytes(b) => Value::Bytes(b.clone()),
        DynSolValue::String(s) => Value::Str(s.clone()),
        other => Value::Json(dynsol_to_json(other).to_string()),
    }
}

/// JSON rendering of compound / fallback values.
fn dynsol_to_json(dv: &DynSolValue) -> Json {
    match dv {
        DynSolValue::Address(a) => json!(format!("0x{}", hex::encode(a.into_array()))),
        DynSolValue::Bool(b) => json!(b),
        DynSolValue::Uint(u, _) => json!(u.to_string()),
        DynSolValue::Int(i, _) => json!(i.to_string()),
        DynSolValue::FixedBytes(w, n) => json!(format!("0x{}", hex::encode(&w.0[..(*n).min(32)]))),
        DynSolValue::Bytes(b) => json!(format!("0x{}", hex::encode(b))),
        DynSolValue::String(s) => json!(s),
        DynSolValue::Array(items) | DynSolValue::FixedArray(items) => {
            json!(items.iter().map(dynsol_to_json).collect::<Vec<_>>())
        }
        DynSolValue::Tuple(items) => json!(items.iter().map(dynsol_to_json).collect::<Vec<_>>()),
        _ => json!(null),
    }
}

/// Register every (non-anonymous) event of one ABI into `map` under `alias` (a contract alias or a
/// template name) with `address` (a fixed contract address, or `Address::ZERO` for a template).
/// Overloaded event names get a 4-hex topic0 table suffix. Returns the anonymous-event skip count.
fn register_events(
    map: &mut HashMap<B256, Vec<EventDecoder>>,
    alias: &str,
    address: Address,
    abi: &JsonAbi,
    events_allow: &[String],
) -> usize {
    // An event is indexed when the allowlist is empty (index all) or names it (by ABI event name).
    let allowed =
        |ev: &Event| events_allow.is_empty() || events_allow.iter().any(|a| a == &ev.name);
    let mut skipped = 0usize;
    let mut name_counts: HashMap<String, usize> = HashMap::new();
    for ev in abi.events() {
        if ev.anonymous || !allowed(ev) {
            continue;
        }
        *name_counts.entry(snake_case(&ev.name)).or_default() += 1;
    }
    for ev in abi.events() {
        if ev.anonymous {
            skipped += 1;
            continue;
        }
        if !allowed(ev) {
            continue; // filtered out by the per-contract allowlist (RFC-0011)
        }
        let mut dec = EventDecoder::new(alias, address, ev.clone());
        if name_counts.get(&snake_case(&ev.name)).copied().unwrap_or(0) > 1 {
            let t0 = hex::encode(dec.topic0);
            dec.table = format!("{}_{}", dec.table, &t0[..4]);
        }
        map.entry(dec.topic0).or_default().push(dec);
    }
    skipped
}

/// What a column appends to its registry-hash line: `:object(name:type;...)` when
/// [`value_from_dynsol`] stores it as a JSON object, and nothing otherwise, so every other line is
/// the v3.6.1 line (#1364).
///
/// That is a non-indexed `tuple` whose component names are all non-empty and unique, checked here
/// rather than trusted from `tuple_components`: an indexed tuple is a topic hash, and a `tuple[]`
/// stays positional whatever its names.
pub fn object_shape(c: &Column) -> String {
    if c.components.is_empty() || c.sol_type != "tuple" || c.kind == StorageKind::Hash32 {
        return String::new();
    }
    let mut names = HashSet::with_capacity(c.components.len());
    if !c
        .components
        .iter()
        .all(|f| !f.name.is_empty() && names.insert(f.name.as_str()))
    {
        return String::new();
    }
    // `(` and `;` cannot occur in an ABI identifier or type, and `;` leaves the `,` column split alone.
    let fields: Vec<String> = c
        .components
        .iter()
        .map(|f| format!("{}:{}", f.name, f.sol_type))
        .collect();
    format!(":object({})", fields.join(";"))
}

/// sha256 over a canonical serialization of the registry (deterministic, order-independent). Includes
/// template decoders (RFC-0009) so a factory nest's data model is content-addressed too.
fn registry_hash(
    by_topic0: &HashMap<B256, Vec<EventDecoder>>,
    templates_by_topic0: &HashMap<B256, Vec<EventDecoder>>,
) -> [u8; 32] {
    let line = |d: &EventDecoder, kind: &str| {
        let cols: Vec<String> = d
            .columns
            .iter()
            .map(|c| {
                format!(
                    "{}:{}:{}{}",
                    c.name,
                    c.sol_type,
                    c.kind.as_str(),
                    object_shape(c)
                )
            })
            .collect();
        format!(
            "{kind}|{}|0x{}|0x{}|{}|{}",
            d.alias,
            hex::encode(d.contract),
            hex::encode(d.topic0),
            d.signature,
            cols.join(",")
        )
    };
    let mut lines: Vec<String> = by_topic0.values().flatten().map(|d| line(d, "c")).collect();
    lines.extend(templates_by_topic0.values().flatten().map(|d| line(d, "t")));
    lines.sort();
    Sha256::digest(lines.join("\n").as_bytes()).into()
}

/// `EventName` → `event_name`. Crate-visible so the factory layer can compute a template's table
/// name (`{template}__{event_snake}`) identically to how the decode registry names tables.
pub fn snake_case(name: &str) -> String {
    let mut out = String::with_capacity(name.len() + 4);
    for (i, ch) in name.chars().enumerate() {
        if ch.is_ascii_uppercase() {
            if i != 0 {
                out.push('_');
            }
            out.push(ch.to_ascii_lowercase());
        } else {
            out.push(ch);
        }
    }
    out
}

fn parse_b256(s: &str) -> Result<B256> {
    let bytes = hex::decode(s.trim_start_matches("0x")).context("bad topic hex")?;
    if bytes.len() != 32 {
        return Err(anyhow!("topic is not 32 bytes"));
    }
    Ok(B256::from_slice(&bytes))
}

fn parse_address(s: &str) -> Result<Address> {
    let bytes = hex::decode(s.trim_start_matches("0x")).context("bad address hex")?;
    if bytes.len() != 20 {
        return Err(anyhow!("address is not 20 bytes"));
    }
    Ok(Address::from_slice(&bytes))
}

fn parse_bytes(s: &str) -> Result<Vec<u8>> {
    hex::decode(s.trim_start_matches("0x")).context("bad data hex")
}

#[cfg(test)]
mod stored_roundtrip {
    use super::*;

    fn col(name: &str, sol: &str, kind: StorageKind) -> ColumnSchema {
        ColumnSchema {
            name: name.into(),
            sol_type: sol.into(),
            storage: kind.as_str().to_string(),
            indexed: false,
            components: Vec::new(),
        }
    }

    /// The seal writer's spelling: `rows_to_batch` keeps JSON strings as-is and stringifies every
    /// other JSON value into a `Utf8` column. Reconstruction has to accept both, because the reorg
    /// path reads the JSON and the restart seed reads the text.
    fn as_sealed_text(v: &Json) -> Json {
        match v {
            Json::String(s) => Json::String(s.clone()),
            other => Json::String(other.to_string()),
        }
    }

    /// **The property the reorg path depends on.** A retraction cancels an insertion only if the two
    /// produce the same row, so `to_json` followed by `value_from_stored` must be the identity - in
    /// both spellings. Anything less and a rolled-back fact stays in an entity forever, alongside a
    /// phantom row at weight -1 that nothing will ever cancel (nuthatch#864).
    #[test]
    fn every_value_survives_the_round_trip_in_both_spellings() {
        let cases: Vec<(Value, ColumnSchema)> = vec![
            (
                Value::Address([0x11; 20]),
                col("who", "address", StorageKind::Address),
            ),
            (
                Value::Hash32([0xab; 32]),
                col("t", "string", StorageKind::Hash32),
            ),
            (Value::Bytes(vec![]), col("b", "bytes", StorageKind::Bytes)),
            (
                Value::Bytes(vec![1, 2, 3]),
                col("b", "bytes", StorageKind::Bytes),
            ),
            (
                Value::Bytes(vec![9; 32]),
                col("b", "bytes32", StorageKind::FixedBytes),
            ),
            (Value::Bool(true), col("f", "bool", StorageKind::Bool)),
            (Value::Bool(false), col("f", "bool", StorageKind::Bool)),
            (Value::U64(0), col("n", "uint64", StorageKind::U64)),
            (Value::U64(u64::MAX), col("n", "uint64", StorageKind::U64)),
            (Value::I64(i64::MIN), col("n", "int64", StorageKind::I64)),
            (Value::I64(-1), col("n", "int64", StorageKind::I64)),
            (Value::I64(i64::MAX), col("n", "int64", StorageKind::I64)),
            (
                Value::Word16(0u128.to_be_bytes()),
                col("v", "uint128", StorageKind::Word16),
            ),
            (
                Value::Word16(u128::MAX.to_be_bytes()),
                col("v", "uint128", StorageKind::Word16),
            ),
            (
                Value::IWord16(i128::MIN.to_be_bytes()),
                col("v", "int128", StorageKind::Word16),
            ),
            (
                Value::IWord16((-1i128).to_be_bytes()),
                col("v", "int128", StorageKind::Word16),
            ),
            (
                Value::IWord16(i128::MAX.to_be_bytes()),
                col("v", "int128", StorageKind::Word16),
            ),
            (
                Value::Word32(U256::ZERO.to_be_bytes::<32>()),
                col("v", "uint256", StorageKind::Word32),
            ),
            (
                Value::Word32(U256::MAX.to_be_bytes::<32>()),
                col("v", "uint256", StorageKind::Word32),
            ),
            (
                Value::IWord32(I256::MIN.to_be_bytes::<32>()),
                col("v", "int256", StorageKind::Word32),
            ),
            (
                Value::IWord32(I256::MINUS_ONE.to_be_bytes::<32>()),
                col("v", "int256", StorageKind::Word32),
            ),
            (
                Value::IWord32(I256::MAX.to_be_bytes::<32>()),
                col("v", "int256", StorageKind::Word32),
            ),
            (
                Value::Str(String::new()),
                col("s", "string", StorageKind::Str),
            ),
            (
                Value::Str("hello".into()),
                col("s", "string", StorageKind::Str),
            ),
        ];

        for (value, schema) in cases {
            let rendered = value.to_json();
            let back = value_from_stored(&rendered, &schema)
                .unwrap_or_else(|e| panic!("{value:?} as JSON: {e:#}"));
            assert_eq!(back, value, "{value:?} did not survive the JSON spelling");

            let sealed = as_sealed_text(&rendered);
            let back = value_from_stored(&sealed, &schema)
                .unwrap_or_else(|e| panic!("{value:?} as sealed text: {e:#}"));
            assert_eq!(back, value, "{value:?} did not survive the sealed spelling");
        }
    }

    /// `uint128` and `int128` share one storage kind, so `sol_type` is the only thing separating
    /// them. Reading a negative `int128` as unsigned yields a colossal positive number rather than an
    /// error, which is the failure that looks like data and not like a bug.
    #[test]
    fn the_sol_type_is_what_separates_signed_from_unsigned() {
        let negative = Value::IWord16((-5i128).to_be_bytes());
        let rendered = negative.to_json();
        assert_eq!(rendered, Json::String("-5".into()));

        let as_signed =
            value_from_stored(&rendered, &col("v", "int128", StorageKind::Word16)).unwrap();
        assert_eq!(as_signed, negative);

        // Same bytes, same storage kind, told it is unsigned: refused rather than silently enormous.
        let err = value_from_stored(&rendered, &col("v", "uint128", StorageKind::Word16))
            .expect_err("a negative decimal is not a uint128");
        assert!(format!("{err:#}").contains("unsigned"), "{err:#}");
    }

    fn schema(timestamps: bool, params: &[(&str, &str, StorageKind)]) -> TableSchema {
        let mut columns = implicit_columns(timestamps);
        columns.extend(params.iter().map(|(name, sol, kind)| ColumnSchema {
            name: (*name).to_string(),
            sol_type: (*sol).to_string(),
            storage: kind.as_str().to_string(),
            indexed: false,
            components: Vec::new(),
        }));
        TableSchema {
            table: "usdc__transfer".into(),
            alias: "usdc".into(),
            kind: TableKind::Event,
            event: String::new(),
            topic0: String::new(),
            function: String::new(),
            selector: String::new(),
            columns,
        }
    }

    /// COR-6 (#814): an ABI naming a parameter after an implicit column is **refused**.
    ///
    /// Board decision 2026-08-31. The probe that stood here demonstrated the alternative - the
    /// parameter replaced the chain's block number in the stored row while `_seq` kept the true one,
    /// and the schema published the name twice - and its own comment said to delete it rather than
    /// relax it if refusal was chosen. It was.
    ///
    /// Refusal rather than renaming because `from_stored` reads `block_number` back on the reorg
    /// path, and a retraction built from a different value than its insertion does not cancel it in
    /// DBSP; it lands beside it and stays. Nothing else on the table could rule that out.
    #[test]
    fn cor6_a_parameter_named_like_an_implicit_column_is_refused() {
        let odd = |param: &str| {
            format!(
                r#"[{{"type":"event","name":"Odd","anonymous":false,"inputs":[
                    {{"name":"{param}","type":"uint256","indexed":false,"internalType":"uint256"}}
                ]}}]"#
            )
        };
        let build = |abi_json: &str| {
            DecodeRegistry::build(vec![ContractSpec {
                alias: "x".into(),
                address: Address::from([0x11; 20]),
                abi: serde_json::from_str(abi_json).expect("parse ABI"),
                events: Vec::new(),
            }])
        };

        for name in [
            "block_number",
            "block_hash",
            "block_timestamp",
            "tx_hash",
            "log_index",
            "address",
            "_seq",
            "table",
        ] {
            // `DecodeRegistry` is not `Debug`, so match on the arm rather than `expect_err`.
            let msg = match build(&odd(name)) {
                Err(e) => format!("{e:#}"),
                Ok(_) => panic!(
                    "a parameter named '{name}' collides with a column nuthatch adds to every row \
                     and must be refused rather than indexed"
                ),
            };
            assert!(
                msg.contains(name) && msg.contains("Odd"),
                "the refusal must name the offending parameter and its event, or an operator with \
                 several ABIs cannot tell which to open; got: {msg}"
            );
        }
    }

    /// #2159: the refusal checks the column name a parameter resolves to, not the ABI's raw name. An
    /// indexed `string block` becomes `block_hash`, and an unnamed parameter becomes `arg{i}`.
    #[test]
    fn a_resolved_column_name_that_collides_is_refused() {
        let build = |inputs: &str| {
            let abi = format!(
                r#"[{{"type":"event","name":"Odd","anonymous":false,"inputs":[{inputs}]}}]"#
            );
            match DecodeRegistry::build(vec![ContractSpec {
                alias: "x".into(),
                address: Address::from([0x11; 20]),
                abi: serde_json::from_str(&abi).expect("parse ABI"),
                events: Vec::new(),
            }]) {
                Err(e) => format!("{e:#}"),
                Ok(_) => String::new(),
            }
        };
        let msg = build(r#"{"name":"block","type":"string","indexed":true}"#);
        assert!(
            msg.contains("block_hash") && msg.contains("Odd"),
            "got: {msg}"
        );
        let msg = build(
            r#"{"name":"","type":"uint256","indexed":false},
               {"name":"arg0","type":"uint256","indexed":false}"#,
        );
        assert!(msg.contains("arg0") && msg.contains("Odd"), "got: {msg}");
        let msg = build(
            r#"{"name":"label","type":"string","indexed":true},
               {"name":"label_hash","type":"bytes32","indexed":false}"#,
        );
        assert!(msg.contains("label_hash"), "got: {msg}");
    }

    /// #1437: a decoded table inside `offchain__` would be replaced by an offchain view of the same
    /// name on `/sql`, and an entity would bind it as the wrong kind of source.
    #[test]
    fn an_alias_inside_the_offchain_namespace_is_refused() {
        let abi: JsonAbi = serde_json::from_str(
            r#"[{"type":"event","name":"Price","anonymous":false,"inputs":[
                {"name":"v","type":"uint256","indexed":false,"internalType":"uint256"}]}]"#,
        )
        .unwrap();
        let contract = |alias: &str| {
            DecodeRegistry::build(vec![ContractSpec {
                alias: alias.into(),
                address: Address::from([0x11; 20]),
                abi: abi.clone(),
                events: Vec::new(),
            }])
        };
        let template = |name: &str| {
            DecodeRegistry::build_with_templates(
                Vec::new(),
                vec![TemplateSpec {
                    name: name.into(),
                    abi: abi.clone(),
                    events: Vec::new(),
                }],
            )
        };
        for alias in ["offchain", "Offchain", "offchain_", "offchain__feed"] {
            for (what, built) in [("contract", contract(alias)), ("template", template(alias))] {
                let msg = match built {
                    Err(e) => format!("{e:#}"),
                    Ok(_) => panic!("{what} `{alias}` decodes into the offchain namespace"),
                };
                assert!(msg.contains(OFFCHAIN_NAMESPACE), "{msg}");
            }
        }
        for alias in ["offchainfeed", "feed_offchain", "off_chain"] {
            assert!(
                contract(alias).is_ok(),
                "`{alias}` is outside the namespace"
            );
            assert!(
                template(alias).is_ok(),
                "`{alias}` is outside the namespace"
            );
        }
    }

    /// The false-positive direction: an ordinary ABI must still build.
    ///
    /// A guard that fires on normal input gets switched off, so this is the half that keeps the
    /// refusal deployable.
    #[test]
    fn cor6_an_ordinary_abi_is_unaffected() {
        let abi = r#"[{"type":"event","name":"Transfer","anonymous":false,"inputs":[
            {"name":"from","type":"address","indexed":true,"internalType":"address"},
            {"name":"to","type":"address","indexed":true,"internalType":"address"},
            {"name":"value","type":"uint256","indexed":false,"internalType":"uint256"}
        ]}]"#;
        DecodeRegistry::build(vec![ContractSpec {
            alias: "usdc".into(),
            address: Address::from([0x11; 20]),
            abi: serde_json::from_str(abi).unwrap(),
            events: Vec::new(),
        }])
        .unwrap_or_else(|e| panic!("an ordinary Transfer ABI must still build: {e:#}"));
    }

    /// A collision on an event the nest never decodes must not refuse it.
    ///
    /// RFC-0011 allowlists exist so a nest can read two events from a busy contract. Refusing on an
    /// event outside the allowlist would break nests that work today, for a shadowing that cannot
    /// happen because the row is never produced.
    #[test]
    fn cor6_a_collision_on_a_non_allowlisted_event_is_ignored() {
        let abi = r#"[
            {"type":"event","name":"Good","anonymous":false,"inputs":[
                {"name":"amount","type":"uint256","indexed":false,"internalType":"uint256"}]},
            {"type":"event","name":"Bad","anonymous":false,"inputs":[
                {"name":"block_number","type":"uint256","indexed":false,"internalType":"uint256"}]}
        ]"#;
        DecodeRegistry::build(vec![ContractSpec {
            alias: "x".into(),
            address: Address::from([0x11; 20]),
            abi: serde_json::from_str(abi).unwrap(),
            events: vec!["Good".into()],
        }])
        .unwrap_or_else(|e| {
            panic!("the colliding event is not allowlisted, so it is never decoded: {e:#}")
        });
    }

    fn a_row(timestamps: bool) -> DecodedRow {
        DecodedRow {
            table: "usdc__transfer".into(),
            params: vec![
                ("from".into(), Value::Address([0x11; 20])),
                ("to".into(), Value::Address([0x22; 20])),
                ("value".into(), Value::Word32(U256::MAX.to_be_bytes::<32>())),
                ("ok".into(), Value::Bool(true)),
                ("memo".into(), Value::Str("7".into())),
            ],
            block_number: 4_000_000,
            block_hash: "0xbh".into(),
            block_timestamp: if timestamps { 1_700_000_000 } else { 0 },
            timestamps,
            log_index: 12,
            tx_hash: "0xtx".into(),
            address: "0xaa".into(),
        }
    }

    fn transfer_schema(timestamps: bool) -> TableSchema {
        schema(
            timestamps,
            &[
                ("from", "address", StorageKind::Address),
                ("to", "address", StorageKind::Address),
                ("value", "uint256", StorageKind::Word32),
                ("ok", "bool", StorageKind::Bool),
                ("memo", "string", StorageKind::Str),
            ],
        )
    }

    /// **The whole point.** A row that goes to the store and comes back must be the *same row*, with
    /// its values still typed. `memo` is the string `"7"` and `value` is a `uint256`; both render as
    /// text and only the schema tells them apart, so comparing `to_json` output would pass on a
    /// reconstruction that confused them - and a `Scalar::Str("7")` retraction does not cancel a
    /// `Scalar::Int(7)` insertion.
    #[test]
    fn a_row_survives_the_store_and_comes_back_typed() {
        for timestamps in [true, false] {
            let original = a_row(timestamps);
            let schema = transfer_schema(timestamps);

            let back = DecodedRow::from_stored(&original.to_json(), &schema)
                .unwrap_or_else(|e| panic!("timestamps={timestamps}: {e:#}"));
            assert_eq!(back, original, "timestamps={timestamps}");

            // And in the sealed spelling, where everything but the four numeric implicit columns is
            // `Utf8` (`seal.rs::rows_to_batch`).
            let sealed = Json::Object(
                original
                    .to_json()
                    .as_object()
                    .unwrap()
                    .iter()
                    .map(|(k, v)| {
                        let numeric = matches!(
                            k.as_str(),
                            "block_number" | "log_index" | "_seq" | "block_timestamp"
                        );
                        let v = match v {
                            _ if numeric => v.clone(),
                            Json::String(s) => Json::String(s.clone()),
                            other => Json::String(other.to_string()),
                        };
                        (k.clone(), v)
                    })
                    .collect(),
            );
            let back = DecodedRow::from_stored(&sealed, &schema)
                .unwrap_or_else(|e| panic!("sealed, timestamps={timestamps}: {e:#}"));
            assert_eq!(back, original, "sealed spelling, timestamps={timestamps}");
        }
    }

    /// The column order comes from the schema, never from the stored map. A plan indexes columns by
    /// position, so taking the order from a JSON object would make an entity's answer depend on the
    /// serialiser's insertion order.
    #[test]
    fn the_schema_decides_the_column_order_not_the_stored_map() {
        let original = a_row(true);
        let mut shuffled: serde_json::Map<String, Json> = serde_json::Map::new();
        // Reverse the params relative to the schema, keeping the implicit columns where they are.
        let stored = original.to_json();
        let obj = stored.as_object().unwrap();
        for key in ["memo", "ok", "value", "to", "from"] {
            shuffled.insert(key.into(), obj[key].clone());
        }
        for (k, v) in obj {
            shuffled.entry(k.clone()).or_insert_with(|| v.clone());
        }

        let back =
            DecodedRow::from_stored(&Json::Object(shuffled), &transfer_schema(true)).unwrap();
        assert_eq!(
            back.params
                .iter()
                .map(|(n, _)| n.as_str())
                .collect::<Vec<_>>(),
            vec!["from", "to", "value", "ok", "memo"]
        );
        assert_eq!(back, original);
    }

    /// A schema column the stored row does not carry means the two disagree about the table's shape.
    /// Treating it as absent would put a hole in a positional row, and every column after it would
    /// shift by one.
    #[test]
    fn a_column_the_stored_row_lacks_is_refused_and_named() {
        let original = a_row(true);
        let mut obj = original.to_json().as_object().unwrap().clone();
        obj.remove("value");

        let err = DecodedRow::from_stored(&Json::Object(obj), &transfer_schema(true))
            .expect_err("the schema declares `value` and the row has none");
        let err = format!("{err:#}");
        assert!(err.contains("has no value"), "{err}");
        assert!(err.contains("disagree about this table"), "{err}");
    }

    /// The implicit numeric columns are `UInt64` in a sealed segment and JSON numbers in the hot
    /// store, and the comment on `number` claims both spellings work. This is that claim, asserted
    /// rather than assumed - a reader that hands them back as text would otherwise silently produce
    /// block 0, and `_seq` would then be the only thing that noticed.
    #[test]
    fn the_implicit_numeric_columns_parse_from_text_as_well_as_numbers() {
        let original = a_row(true);
        let mut obj = original.to_json().as_object().unwrap().clone();
        for key in ["block_number", "log_index", "_seq", "block_timestamp"] {
            let as_text = obj[key].to_string();
            obj.insert(key.into(), Json::String(as_text));
        }

        let back = DecodedRow::from_stored(&Json::Object(obj), &transfer_schema(true))
            .expect("text spellings of the numeric columns must parse");
        assert_eq!(back, original);
        assert_eq!(back.block_number, 4_000_000);
    }

    /// `_seq` is derived from block and log index, so it is a checksum on them. A row whose `_seq`
    /// disagrees has been rewritten by something, and ordering built on it would be wrong in a way
    /// nothing else here would notice.
    #[test]
    fn a_seq_that_disagrees_with_its_own_block_and_log_index_is_refused() {
        let original = a_row(true);
        let mut obj = original.to_json().as_object().unwrap().clone();
        obj.insert("_seq".into(), json!(1));

        let err = DecodedRow::from_stored(&Json::Object(obj), &transfer_schema(true))
            .expect_err("a rewritten _seq is a corrupt row");
        assert!(format!("{err:#}").contains("_seq 1"), "{err:#}");
    }

    /// Reconstructing against another table's schema would read the columns as the wrong types and
    /// succeed at it, which is worse than failing.
    #[test]
    fn a_row_from_another_table_is_refused() {
        let original = a_row(true);
        let mut schema = transfer_schema(true);
        schema.table = "usdc__approval".into();

        let err = DecodedRow::from_stored(&original.to_json(), &schema)
            .expect_err("the row says transfer and the schema says approval");
        assert!(format!("{err:#}").contains("wrong schema"), "{err:#}");
    }

    /// A stored address that is not twenty bytes is a corrupt row, and the guard is what keeps it an
    /// error. Without it the `try_into` below panics, which in the ingest path is not a refusal - it
    /// is the cursor going down.
    #[test]
    fn a_wrong_length_address_is_refused_rather_than_panicking() {
        let long = format!("0x{}", "11".repeat(21));
        for bad in ["0x1122", "0x", long.as_str()] {
            let err = value_from_stored(
                &Json::String(bad.to_string()),
                &col("who", "address", StorageKind::Address),
            )
            .expect_err("{bad} is not a 20-byte address");
            assert!(format!("{err:#}").contains("expected 20"), "{bad}: {err:#}");
        }
    }

    /// A null column has no `Value` variant to become. Handing back a zero would put a number where
    /// the chain had nothing.
    #[test]
    fn a_null_column_is_refused_rather_than_defaulted() {
        let err = value_from_stored(&Json::Null, &col("v", "uint256", StorageKind::Word32))
            .expect_err("null is not a value");
        // The guard's own words, not merely the substring "null" - a JSON null renders as `null`
        // inside "column v is not text: null" too, so the looser assertion passed with this guard
        // deleted.
        assert!(
            format!("{err:#}").contains("is null in the stored row"),
            "{err:#}"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// nuthatch-org/nuthatch#1116 - the L1 block an L2 block was sequenced against.
    ///
    /// The bridge between the two block spaces. `EpochManager` counts epochs in L1 blocks while an
    /// indexed log carries an L2 block, so without this an epoch boundary has to be guessed from the
    /// first *observed* event, which files several thousand blocks' worth of activity against the
    /// wrong epoch.
    #[test]
    fn a_block_row_keeps_the_l1_block_number_and_zeroes_it_where_absent() {
        let arbitrum = serde_json::json!({
            "hash": "0xaa",
            "parentHash": "0xbb",
            "miner": "0x0000000000000000000000000000000000000001",
            "gasUsed": "0x10",
            "gasLimit": "0x20",
            "baseFeePerGas": "0x1",
            "size": "0x30",
            "timestamp": "0x64",
            "l1BlockNumber": "0x18b23d3",
            "transactions": [],
        });
        let row =
            block_row(501_277_084, &arbitrum, true).expect("a header with a hash yields a row");
        let l1 = row
            .params
            .iter()
            .find(|(k, _)| k == "l1_block_number")
            .map(|(_, v)| v.clone())
            .expect("the column must be populated, not merely declared");
        assert_eq!(l1, Value::U64(0x018b_23d3), "0x18b23d3 is 25,896,915");

        // Mainnet does not report one. Zero, following `base_fee_per_gas` on a pre-London block:
        // the honest value for "the field did not exist", not a failure of the whole window.
        let mut mainnet = arbitrum.clone();
        mainnet.as_object_mut().unwrap().remove("l1BlockNumber");
        let row = block_row(1, &mainnet, true).expect("row");
        assert_eq!(
            row.params
                .iter()
                .find(|(k, _)| k == "l1_block_number")
                .map(|(_, v)| v.clone()),
            Some(Value::U64(0)),
        );
    }

    /// Counting a header's transactions at fetch must not change the row it seals (#2309).
    #[test]
    fn a_counted_header_builds_the_same_block_row() {
        let full = serde_json::json!({
            "hash": "0xaa",
            "timestamp": "0x64",
            "transactions": ["0x01", "0x02", "0x03"],
        });
        let mut counted = full.clone();
        count_header_transactions(&mut counted);
        assert!(counted.get("transactions").is_none());
        let row = |h: &Json| block_row(1, h, true).unwrap().to_json().to_string();
        assert_eq!(row(&counted), row(&full));
        assert!(
            row(&full).contains("\"transaction_count\":3"),
            "{}",
            row(&full)
        );
    }

    /// The schema and the row builder read the same `BLOCK_COLUMNS`, and that constant's own comment
    /// says the two must agree on every name and order. That only holds while every declared column
    /// is actually emitted, so assert it rather than trusting the comment.
    #[test]
    fn every_declared_block_column_is_emitted_by_the_row_builder() {
        let header = serde_json::json!({ "hash": "0xaa", "transactions": [] });
        let row = block_row(1, &header, false).expect("row");
        let emitted: Vec<&str> = row.params.iter().map(|(k, _)| k.as_str()).collect();
        let declared: Vec<&str> = BLOCK_COLUMNS.iter().map(|(n, _, _)| *n).collect();
        assert_eq!(
            emitted, declared,
            "a column declared and not emitted is a null in every row, and one emitted and not \
             declared is a row the schema cannot describe"
        );
    }

    #[test]
    fn is_transfer_shaped_gates_on_shape_not_name() {
        // Build a TableSchema from a name + its event-param Solidity types (implicit columns aside).
        fn ts(name: &str, params: &[&str]) -> TableSchema {
            TableSchema {
                table: name.to_string(),
                alias: "t".to_string(),
                kind: TableKind::Event,
                event: "E".to_string(),
                topic0: "0x00".to_string(),
                function: String::new(),
                selector: String::new(),
                columns: params
                    .iter()
                    .map(|s| ColumnSchema {
                        name: "c".to_string(),
                        sol_type: s.to_string(),
                        storage: "x".to_string(),
                        indexed: false,
                        components: Vec::new(),
                    })
                    .collect(),
            }
        }
        // Real ERC-20/721 Transfer shape (USDC from/to/value, WETH src/dst/wad - names don't matter).
        assert!(ts("usdc__transfer", &["address", "address", "uint256"]).is_transfer_shaped());
        assert!(ts("weth__transfer", &["address", "address", "uint256"]).is_transfer_shaped());
        // Not transfer-shaped:
        assert!(
            !ts("usdc__approval", &["address", "address", "uint256"]).is_transfer_shaped(),
            "wrong table name"
        );
        assert!(
            !ts("x__transfer", &["address", "address"]).is_transfer_shaped(),
            "only 2 params"
        );
        assert!(
            !ts("x__transfer", &["address", "address", "uint256", "uint256"]).is_transfer_shaped(),
            "4 params"
        );
        assert!(
            !ts("x__transfer", &["address", "uint256", "address"]).is_transfer_shaped(),
            "2nd param not an address"
        );
        assert!(
            !ts("x__transfer", &["uint256", "address", "uint256"]).is_transfer_shaped(),
            "1st param not an address"
        );
        // Implicit columns (block_number etc.) precede the params in a real schema and must be ignored.
        let mut with_implicit = ts("x__transfer", &["address", "address", "uint256"]);
        with_implicit.columns.insert(
            0,
            ColumnSchema {
                name: "block_number".to_string(),
                sol_type: "implicit".to_string(),
                storage: "u64".to_string(),
                indexed: false,
                components: Vec::new(),
            },
        );
        assert!(
            with_implicit.is_transfer_shaped(),
            "implicit columns must not affect the shape check"
        );
    }

    #[test]
    fn signed_and_large_bigints_render_as_decimals() {
        // int256 = -100000 → a *signed* decimal, not two's-complement hex (a Uniswap swap's out-leg).
        let neg = I256::try_from(-100_000i64).unwrap();
        assert_eq!(
            Value::IWord32(neg.to_be_bytes::<32>()).to_json(),
            json!("-100000")
        );
        // int128 negative → signed too.
        assert_eq!(
            Value::IWord16((-42i128).to_be_bytes()).to_json(),
            json!("-42")
        );
        // uint256 above u128 → full decimal (previously fell back to hex, breaking `SUM(_dec)`).
        let big = U256::from(u128::MAX) + U256::from(1u8);
        assert_eq!(
            Value::Word32(big.to_be_bytes::<32>()).to_json(),
            json!(big.to_string())
        );
        // Small positive still decimal.
        assert_eq!(
            Value::Word32(U256::from(1_000_000u64).to_be_bytes::<32>()).to_json(),
            json!("1000000")
        );
    }

    fn abi(json: &str) -> JsonAbi {
        // Accept a bare events array by wrapping it as a contract ABI.
        serde_json::from_str(json).unwrap()
    }

    fn spec(alias: &str, addr: &str, abi_json: &str) -> ContractSpec {
        ContractSpec {
            alias: alias.into(),
            address: parse_address(addr).unwrap(),
            abi: abi(abi_json),
            events: Vec::new(),
        }
    }

    /// A tuple column records the component names the ABI declares.
    ///
    /// A fully-named tuple is stored as a JSON object keyed by those names (#1304). On the pinned
    /// Uniswap-V4-shaped target that was 12 of 60 unanswered fields, every one a decoded event param.
    ///
    /// Two refusals are asserted alongside, because inventing a name is worse than having none:
    /// an unnamed component yields nothing, and a component that is itself a tuple keeps its positional
    /// JSON rather than being flattened.
    #[test]
    fn a_tuple_column_records_its_abi_component_names() {
        const TUPLES: &str = r#"[
          {"type":"event","name":"Named","anonymous":false,"inputs":[
            {"name":"id","type":"uint256","indexed":false},
            {"name":"order","type":"tuple","indexed":false,"components":[
              {"name":"y","type":"uint128"},
              {"name":"z","type":"uint128"},
              {"name":"A","type":"uint64"}
            ]}
          ]},
          {"type":"event","name":"Unnamed","anonymous":false,"inputs":[
            {"name":"pair","type":"tuple","indexed":false,"components":[
              {"name":"a","type":"address"},
              {"name":"","type":"uint256"}
            ]}
          ]},
          {"type":"event","name":"Duped","anonymous":false,"inputs":[
            {"name":"pair","type":"tuple","indexed":false,"components":[
              {"name":"x","type":"uint256"},
              {"name":"x","type":"uint256"}
            ]}
          ]},
          {"type":"event","name":"Nested","anonymous":false,"inputs":[
            {"name":"outer","type":"tuple","indexed":false,"components":[
              {"name":"inner","type":"tuple","components":[{"name":"leaf","type":"uint256"}]},
              {"name":"tail","type":"uint256"}
            ]}
          ]}
        ]"#;
        let reg = DecodeRegistry::build(vec![spec("t", USDC, TUPLES)]).unwrap();
        let col = |table: &str, name: &str| -> Column {
            reg.tables()
                .iter()
                .find(|d| d.table.ends_with(table))
                .unwrap_or_else(|| panic!("no table {table}"))
                .columns
                .iter()
                .find(|c| c.name == name)
                .unwrap_or_else(|| panic!("no column {name} in {table}"))
                .clone()
        };

        // Named: every component, in ABI order, with its own solidity type and storage kind.
        let order = col("named", "order");
        assert_eq!(order.sol_type, "tuple");
        assert_eq!(order.kind, StorageKind::Json);
        assert_eq!(
            order.components,
            vec![
                Component {
                    name: "y".into(),
                    sol_type: "uint128".into(),
                    storage: "word16".into()
                },
                Component {
                    name: "z".into(),
                    sol_type: "uint128".into(),
                    storage: "word16".into()
                },
                Component {
                    name: "A".into(),
                    sol_type: "uint64".into(),
                    storage: "u64".into()
                },
            ],
            "order is still ABI order: object keys come from this list, index 2 is `A`"
        );

        // A non-tuple carries none.
        assert!(col("named", "id").components.is_empty());

        // Partially named is all-or-nothing: one component without a name and the whole tuple stays
        // positional. Naming some and not others would produce a view mixing names and indices.
        assert!(
            col("unnamed", "pair").components.is_empty(),
            "a tuple with an unnamed component must not be half-named"
        );
        assert!(
            col("duped", "pair").components.is_empty(),
            "duplicate component names cannot be object keys"
        );

        // One level. A component that is itself a tuple is named but keeps its own positional JSON,
        // which it says by carrying `sol_type: "tuple"` and no components of its own.
        let outer = col("nested", "outer");
        assert_eq!(
            outer
                .components
                .iter()
                .map(|c| (c.name.as_str(), c.sol_type.as_str()))
                .collect::<Vec<_>>(),
            vec![("inner", "tuple"), ("tail", "uint256")]
        );
    }

    /// A fully-named tuple decodes as a JSON object, not a positional array (#1304).
    #[test]
    fn a_named_tuple_decodes_as_a_json_object() {
        const ABI: &str = r#"[
          {"type":"event","name":"Made","anonymous":false,"inputs":[
            {"name":"order","type":"tuple","indexed":false,"components":[
              {"name":"y","type":"uint256"},
              {"name":"z","type":"uint256"}
            ]}
          ]}
        ]"#;
        let reg = DecodeRegistry::build(vec![spec("t", USDC, ABI)]).unwrap();
        let dec = reg
            .tables()
            .into_iter()
            .find(|d| d.table.ends_with("made"))
            .expect("Made");
        let topic0 = format!("0x{}", hex::encode(dec.topic0));
        let y = "00".repeat(31) + "01";
        let z = "00".repeat(31) + "02";
        let data = format!("0x{y}{z}");
        let row = reg
            .decode(&log(USDC, &[&topic0], &data, 1, 0))
            .unwrap()
            .expect("topic0 and address match");
        let Value::Json(raw) = &row.params[0].1 else {
            panic!("order must be Json, got {:?}", row.params[0].1);
        };
        let obj: Json = serde_json::from_str(raw).unwrap();
        assert!(obj.is_object(), "named tuple must be an object, got {obj}");
        assert_eq!(obj["y"], json!("1"));
        assert_eq!(obj["z"], json!("2"));
    }

    /// The stored object keeps the ABI's component order in every build, this crate's alone
    /// included, so a node and a tab store the same string (#2224).
    #[test]
    fn a_named_tuple_keeps_its_abi_key_order() {
        let abi = made_abi(
            "tuple",
            false,
            r#"[{"name":"z","type":"uint256"},{"name":"a","type":"uint256"}]"#,
        );
        let reg = DecodeRegistry::build(vec![spec("t", USDC, &abi)]).unwrap();
        let topic0 = format!("0x{}", hex::encode(reg.tables()[0].topic0));
        let data = format!("0x{}01{}02", "00".repeat(31), "00".repeat(31));
        let row = reg
            .decode(&log(USDC, &[&topic0], &data, 1, 0))
            .unwrap()
            .expect("topic0 and address match");
        assert_eq!(row.params[0].1, Value::Json(r#"{"z":"1","a":"2"}"#.into()));
        assert_eq!(
            row.to_json()["order"].to_string(),
            r#"{"z":"1","a":"2"}"#,
            "the hot-store spelling the sealer re-serialises"
        );
    }

    /// Duplicate component names cannot be object keys; the value stays a positional array.
    #[test]
    fn a_tuple_with_duplicate_names_decodes_as_an_array() {
        const ABI: &str = r#"[
          {"type":"event","name":"Made","anonymous":false,"inputs":[
            {"name":"order","type":"tuple","indexed":false,"components":[
              {"name":"y","type":"uint256"},
              {"name":"y","type":"uint256"}
            ]}
          ]}
        ]"#;
        let reg = DecodeRegistry::build(vec![spec("t", USDC, ABI)]).unwrap();
        let dec = reg
            .tables()
            .into_iter()
            .find(|d| d.table.ends_with("made"))
            .expect("Made");
        let topic0 = format!("0x{}", hex::encode(dec.topic0));
        let y1 = "00".repeat(31) + "01";
        let y2 = "00".repeat(31) + "02";
        let row = reg
            .decode(&log(USDC, &[&topic0], &format!("0x{y1}{y2}"), 1, 0))
            .unwrap()
            .expect("topic0 and address match");
        let Value::Json(raw) = &row.params[0].1 else {
            panic!("order must be Json, got {:?}", row.params[0].1);
        };
        let arr: Json = serde_json::from_str(raw).unwrap();
        assert!(
            arr.is_array(),
            "duplicate names must stay an array, got {arr}"
        );
        assert_eq!(arr, json!(["1", "2"]));
    }

    /// `schema.json` is byte-identical for a nest with no tuple column.
    ///
    /// The field is `skip_serializing_if = "Vec::is_empty"` for the same reason `TableKind` is: an
    /// existing nest's artifact must not change shape because a field was added to the type.
    #[test]
    fn the_components_field_is_absent_from_a_tuple_free_schema() {
        let reg = DecodeRegistry::build(vec![spec("usdc", USDC, ERC20)]).unwrap();
        let json = serde_json::to_string(&reg.schema()).unwrap();
        assert!(
            !json.contains("components"),
            "ERC20 has no tuple, so `components` must not appear:\n{json}"
        );
    }

    /// Like [`spec`] but with an event allowlist (RFC-0011).
    fn spec_events(alias: &str, addr: &str, abi_json: &str, events: &[&str]) -> ContractSpec {
        ContractSpec {
            events: events.iter().map(|s| s.to_string()).collect(),
            ..spec(alias, addr, abi_json)
        }
    }

    const USDC: &str = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48";

    fn log(addr: &str, topics: &[&str], data: &str, block: u64, li: u64) -> Log {
        Log {
            address: addr.into(),
            topics: topics.iter().map(|s| s.to_string()).collect(),
            data: data.into(),
            block_number: block,
            block_hash: "0xbh".into(),
            tx_hash: "0xtx".into(),
            log_index: li,
        }
    }

    const ERC20: &str = r#"[
        {"type":"event","name":"Transfer","inputs":[
            {"name":"from","type":"address","indexed":true},
            {"name":"to","type":"address","indexed":true},
            {"name":"value","type":"uint256","indexed":false}],"anonymous":false},
        {"type":"event","name":"Approval","inputs":[
            {"name":"owner","type":"address","indexed":true},
            {"name":"spender","type":"address","indexed":true},
            {"name":"value","type":"uint256","indexed":false}],"anonymous":false}
    ]"#;

    #[test]
    fn decodes_real_usdc_transfer() {
        let reg = DecodeRegistry::build(vec![spec("usdc", USDC, ERC20)]).unwrap();
        let l = log(
            USDC,
            &[
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x000000000000000000000000943f303a8019652d3a14b29954b2d780dde42ca3",
                "0x000000000000000000000000db5985dbd132b9e5cc4bf0a18a8fb04a396ba0a0",
            ],
            "0x000000000000000000000000000000000000000000000000000000001cd4ad20",
            25529850,
            139,
        );
        let row = reg.decode(&l).unwrap().unwrap();
        assert_eq!(row.table, "usdc__transfer");
        assert_eq!(row.params[0].0, "from");
        assert_eq!(
            row.params[0].1,
            Value::Address(
                hex::decode("943f303a8019652d3a14b29954b2d780dde42ca3")
                    .unwrap()
                    .try_into()
                    .unwrap()
            )
        );
        assert_eq!(row.params[2].0, "value");
        assert_eq!(
            row.params[2].1,
            Value::Word32({
                let mut b = [0u8; 32];
                b[28..].copy_from_slice(&483_700_000u32.to_be_bytes());
                b
            })
        );
        // JSON shape for serving
        let j = row.to_json();
        assert_eq!(j["from"], "0x943f303a8019652d3a14b29954b2d780dde42ca3");
        assert_eq!(j["block_number"], 25529850);
    }

    #[test]
    fn wrong_address_does_not_decode() {
        let reg = DecodeRegistry::build(vec![spec("usdc", USDC, ERC20)]).unwrap();
        let l = log(
            "0x1111111111111111111111111111111111111111",
            &[
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x000000000000000000000000943f303a8019652d3a14b29954b2d780dde42ca3",
                "0x000000000000000000000000db5985dbd132b9e5cc4bf0a18a8fb04a396ba0a0",
            ],
            "0x000000000000000000000000000000000000000000000000000000001cd4ad20",
            1,
            0,
        );
        assert!(reg.decode(&l).unwrap().is_none());
    }

    #[test]
    fn same_signature_two_contracts_land_in_separate_tables() {
        let usdc = spec("usdc", USDC, ERC20);
        let weth = spec("weth", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", ERC20);
        let reg = DecodeRegistry::build(vec![usdc, weth]).unwrap();
        let tables: Vec<&str> = reg.tables().iter().map(|d| d.table.as_str()).collect();
        assert!(tables.contains(&"usdc__transfer"));
        assert!(tables.contains(&"weth__transfer"));
        // one topic0, two decoders keyed by address
        assert_eq!(reg.topic0s().len(), 2); // Transfer + Approval share across both → 2 distinct topic0s
        assert_eq!(reg.addresses().len(), 2);
    }

    /// RFC-0011: a per-contract `events` allowlist decodes only the listed events. The ERC20 ABI has
    /// Transfer + Approval; allowlisting Transfer drops Approval from the tables *and* the topic0 set,
    /// so the getLogs filter narrows too. An Approval log no longer decodes.
    #[test]
    fn event_allowlist_restricts_tables_and_topics() {
        let full = DecodeRegistry::build(vec![spec("t", USDC, ERC20)]).unwrap();
        assert_eq!(full.tables().len(), 2, "unfiltered: Transfer + Approval");

        let reg =
            DecodeRegistry::build(vec![spec_events("t", USDC, ERC20, &["Transfer"])]).unwrap();
        let tables: Vec<&str> = reg.tables().iter().map(|d| d.table.as_str()).collect();
        assert_eq!(tables, vec!["t__transfer"], "only the allowlisted event");
        assert_eq!(
            reg.topic0s().len(),
            1,
            "Approval's topic0 isn't even requested"
        );

        // A Transfer still decodes; an Approval on the same contract now doesn't.
        let transfer = log(
            USDC,
            &[
                "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
                "0x000000000000000000000000943f303a8019652d3a14b29954b2d780dde42ca3",
                "0x000000000000000000000000db5985dbd132b9e5cc4bf0a18a8fb04a396ba0a0",
            ],
            "0x000000000000000000000000000000000000000000000000000000001cd4ad20",
            1,
            0,
        );
        assert!(reg.decode(&transfer).unwrap().is_some());
        let approval = log(
            USDC,
            &[
                "0x8c5be1e5ebec7d5bd14f71427d1e84f3dd0314c0f7b2291e5b200ac8c7c3b925",
                "0x000000000000000000000000943f303a8019652d3a14b29954b2d780dde42ca3",
                "0x000000000000000000000000db5985dbd132b9e5cc4bf0a18a8fb04a396ba0a0",
            ],
            "0x000000000000000000000000000000000000000000000000000000001cd4ad20",
            1,
            1,
        );
        assert!(
            reg.decode(&approval).unwrap().is_none(),
            "Approval filtered out"
        );
    }

    /// The allowlist changes the registry's content hash - the data model is content-addressed, so a
    /// filtered nest is a different (smaller) model, not the same one.
    #[test]
    fn event_allowlist_changes_the_registry_hash() {
        let full = DecodeRegistry::build(vec![spec("t", USDC, ERC20)]).unwrap();
        let filtered =
            DecodeRegistry::build(vec![spec_events("t", USDC, ERC20, &["Transfer"])]).unwrap();
        assert_ne!(full.hash(), filtered.hash());
    }

    /// A typo in the allowlist (an event the ABI doesn't define) is a loud build error, never a silent
    /// "indexes nothing" - the whole point at GraphToken scale.
    #[test]
    fn unknown_allowlisted_event_is_a_build_error() {
        let err = match DecodeRegistry::build(vec![spec_events("t", USDC, ERC20, &["Transferr"])]) {
            Ok(_) => panic!("a typo'd allowlist should fail the build"),
            Err(e) => e.to_string(),
        };
        assert!(
            err.contains("Transferr"),
            "names the offending event: {err}"
        );
        assert!(
            err.contains("Approval, Transfer"),
            "lists the known events: {err}"
        );
    }

    #[test]
    fn type_mapping_covers_value_and_dynamic_kinds() {
        assert_eq!(
            StorageKind::from_sol("address", false),
            StorageKind::Address
        );
        assert_eq!(StorageKind::from_sol("uint256", false), StorageKind::Word32);
        assert_eq!(StorageKind::from_sol("uint128", false), StorageKind::Word16);
        assert_eq!(StorageKind::from_sol("uint64", false), StorageKind::U64);
        assert_eq!(StorageKind::from_sol("int24", false), StorageKind::I64);
        assert_eq!(StorageKind::from_sol("int256", false), StorageKind::Word32);
        assert_eq!(StorageKind::from_sol("bool", false), StorageKind::Bool);
        assert_eq!(
            StorageKind::from_sol("bytes32", false),
            StorageKind::FixedBytes
        );
        assert_eq!(StorageKind::from_sol("bytes", false), StorageKind::Bytes);
        assert_eq!(StorageKind::from_sol("string", false), StorageKind::Str);
        assert_eq!(StorageKind::from_sol("uint256[]", false), StorageKind::Json);
        // indexed dynamic → hash
        assert_eq!(StorageKind::from_sol("string", true), StorageKind::Hash32);
        assert_eq!(StorageKind::from_sol("uint256", true), StorageKind::Word32);
    }

    /// Golden: an address-heavy event with an indexed non-address type (Uniswap V3 `PoolCreated`):
    /// three indexed params (two addresses + a uint24) and two in the data (int24 + address).
    #[test]
    fn decodes_address_heavy_pool_created() {
        const FACTORY: &str = "0x1f98431c8ad98523631ae4a59f267346ea31f984";
        const POOL_ABI: &str = r#"[
            {"type":"event","name":"PoolCreated","inputs":[
                {"name":"token0","type":"address","indexed":true},
                {"name":"token1","type":"address","indexed":true},
                {"name":"fee","type":"uint24","indexed":true},
                {"name":"tickSpacing","type":"int24","indexed":false},
                {"name":"pool","type":"address","indexed":false}],"anonymous":false}
        ]"#;
        let reg = DecodeRegistry::build(vec![spec("uni", FACTORY, POOL_ABI)]).unwrap();
        let topic0 = format!("0x{}", hex::encode(reg.tables()[0].topic0));

        let t0 = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let t1 = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        let pool = "cccccccccccccccccccccccccccccccccccccccc";
        let topic_addr = |a: &str| format!("0x000000000000000000000000{a}");
        let fee = format!("0x{:064x}", 3000u64);
        let data = format!("{:064x}000000000000000000000000{pool}", 60u64); // tickSpacing=60, pool
        let l = log(
            FACTORY,
            &[&topic0, &topic_addr(t0), &topic_addr(t1), &fee],
            &format!("0x{data}"),
            100,
            5,
        );

        let row = reg.decode(&l).unwrap().unwrap();
        assert_eq!(row.table, "uni__pool_created");
        assert_eq!(row.params[0].0, "token0");
        assert_eq!(
            row.params[0].1,
            Value::Address(hex::decode(t0).unwrap().try_into().unwrap())
        );
        assert_eq!(row.params[2].0, "fee");
        assert_eq!(row.params[2].1, Value::U64(3000)); // uint24 indexed → not hashed, fits u64
        assert_eq!(row.params[3].0, "tickSpacing");
        assert_eq!(row.params[3].1, Value::I64(60));
        assert_eq!(
            row.params[4].1,
            Value::Address(hex::decode(pool).unwrap().try_into().unwrap())
        );
    }

    /// Golden: an indexed dynamic type (`string indexed`) - the topic holds keccak(value), not the
    /// value, so it's stored as a 32-byte hash under a `_hash`-suffixed column.
    #[test]
    fn decodes_indexed_string_as_hash() {
        const C: &str = "0x2222222222222222222222222222222222222222";
        const ABI: &str = r#"[
            {"type":"event","name":"Named","inputs":[
                {"name":"label","type":"string","indexed":true},
                {"name":"amount","type":"uint256","indexed":false}],"anonymous":false}
        ]"#;
        let reg = DecodeRegistry::build(vec![spec("c", C, ABI)]).unwrap();
        let dec = reg.tables()[0];
        assert_eq!(dec.columns[0].name, "label_hash", "indexed dynamic → _hash");
        assert_eq!(dec.columns[0].kind, StorageKind::Hash32);
        let topic0 = format!("0x{}", hex::encode(dec.topic0));

        let label_hash = "1234567890123456789012345678901234567890123456789012345678901234";
        let l = log(
            C,
            &[&topic0, &format!("0x{label_hash}")],
            &format!("0x{:064x}", 42u64),
            7,
            0,
        );
        let row = reg.decode(&l).unwrap().unwrap();
        assert_eq!(row.params[0].0, "label_hash");
        assert_eq!(
            row.params[0].1,
            Value::Hash32(hex::decode(label_hash).unwrap().try_into().unwrap())
        );
        assert_eq!(
            row.params[1].1,
            Value::Word32({
                let mut b = [0u8; 32];
                b[31] = 42;
                b
            })
        );
        // Serving JSON carries the implicit provenance columns.
        let mut row = row;
        row.block_timestamp = 1_700_000_000;
        let j = row.to_json();
        assert_eq!(j["block_hash"], "0xbh");
        assert_eq!(j["_seq"], json!(7u64 << 20));
        assert_eq!(j["block_timestamp"], json!(1_700_000_000u64));
    }

    fn row_at(block: u64, log_index: u64) -> DecodedRow {
        DecodedRow {
            table: "t".into(),
            params: vec![],
            block_number: block,
            block_hash: "0xbh".into(),
            block_timestamp: 0,
            timestamps: false,
            log_index,
            tx_hash: "0xtx".into(),
            address: "0xa".into(),
        }
    }

    /// COR-10: `_seq` is `block << 20 | log_index`. A log_index that fills the 20-bit field must
    /// still sort immediately before the next block's index 0, or the packing is not an order.
    #[test]
    fn seq_packs_block_and_log_index_in_the_20_bit_field() {
        assert_eq!(row_at(7, 0).seq(), 7u64 << 20);
        assert_eq!(
            row_at(7, (1 << 20) - 1).seq(),
            (7u64 << 20) | ((1 << 20) - 1)
        );
        assert_eq!(
            row_at(8, 0).seq(),
            row_at(7, (1 << 20) - 1).seq() + 1,
            "a full 20-bit log_index in block N must not collide with block N+1"
        );
    }

    /// COR-10: the 20-bit field is unreachable under current gas limits. If that changes, a debug
    /// build must not silently mask. Deleting the `debug_assert` in `DecodedRow::seq` fails this.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "exceeds the 20-bit _seq field")]
    fn a_log_index_past_the_20_bit_field_is_not_silent() {
        let _ = row_at(1, 1 << 20).seq();
    }

    #[test]
    fn registry_hash_is_stable_and_order_independent() {
        let a = DecodeRegistry::build(vec![
            spec("usdc", USDC, ERC20),
            spec("weth", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", ERC20),
        ])
        .unwrap();
        let b = DecodeRegistry::build(vec![
            spec("weth", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", ERC20),
            spec("usdc", USDC, ERC20),
        ])
        .unwrap();
        assert_eq!(
            a.hash(),
            b.hash(),
            "registry hash must not depend on input order"
        );
    }

    /// `object_shape` states the decoder's name predicate itself, so a column built without
    /// `tuple_components` cannot claim an object the decoder would not write.
    #[test]
    fn object_shape_refuses_duplicate_or_missing_names_on_its_own() {
        let column = |names: &[&str]| Column {
            name: "order".into(),
            sol_type: "tuple".into(),
            kind: StorageKind::Json,
            indexed: false,
            components: names
                .iter()
                .map(|n| Component {
                    name: (*n).into(),
                    sol_type: "uint256".into(),
                    storage: "word32".into(),
                })
                .collect(),
        };
        assert_eq!(object_shape(&column(&["x", "x"])), "");
        assert_eq!(object_shape(&column(&["a", ""])), "");
        assert_eq!(
            object_shape(&column(&["a", "b"])),
            ":object(a:uint256;b:uint256)"
        );
    }

    fn made_abi(ty: &str, indexed: bool, components: &str) -> String {
        format!(
            r#"[{{"type":"event","name":"Made","anonymous":false,"inputs":[{{"name":"order","type":"{ty}","indexed":{indexed},"components":{components}}}]}}]"#
        )
    }

    const YZ: &str = r#"[{"name":"y","type":"uint256"},{"name":"z","type":"uint256"}]"#;
    // What v3.6.1 hashed `made_abi("tuple", false, _)` to, whatever the component names.
    const V361_MADE_TUPLE: &str =
        "f126ed95e8b79c3d50f2c48597f8bcf9d9ad003e0f869cb3ba328d632a45ec88";

    fn made_hash(ty: &str, indexed: bool, components: &str) -> String {
        let abi = made_abi(ty, indexed, components);
        hex::encode(
            DecodeRegistry::build(vec![spec("t", USDC, &abi)])
                .unwrap()
                .hash(),
        )
    }

    /// A registry with no tuple keeps the hash v3.6.1 gave it, so #1364 re-indexes only nests that
    /// store an object. The literal was printed by this build on the `v3.6.1` tag.
    #[test]
    fn a_tuple_free_registry_keeps_its_v3_6_1_hash() {
        let reg = DecodeRegistry::build(vec![spec("usdc", USDC, ERC20)]).unwrap();
        assert_eq!(
            hex::encode(reg.hash()),
            "d3f4a2fd9c5e50015ad2e77de1e71ef1416c5759e8b350e0f99034718048a63c"
        );
    }

    /// The same event with named and with unnamed components stores an object and an array, so the
    /// two are different decodings (#1364). v3.6.1 hashed both to `V361_MADE_TUPLE`.
    #[test]
    fn named_and_unnamed_tuples_hash_differently() {
        let blank = r#"[{"name":"","type":"uint256"},{"name":"","type":"uint256"}]"#;
        let named = made_abi("tuple", false, YZ);
        let unnamed = made_abi("tuple", false, blank);
        let topic0 = |abi: &str| {
            DecodeRegistry::build(vec![spec("t", USDC, abi)])
                .unwrap()
                .topic0s()
        };
        assert_eq!(
            topic0(&named),
            topic0(&unnamed),
            "one signature, one topic0"
        );

        assert_ne!(
            made_hash("tuple", false, YZ),
            made_hash("tuple", false, blank)
        );
        assert_eq!(made_hash("tuple", false, blank), V361_MADE_TUPLE);

        // The outer tuple of a nested one is still an object; its inner tuple stays positional.
        let nested = r#"[{"name":"inner","type":"tuple","components":[{"name":"leaf","type":"uint256"}]},{"name":"tail","type":"uint256"}]"#;
        assert_ne!(
            made_hash("tuple", false, nested),
            "a91cafa0fa27b80ca2c76f63b13316385c31a3ce60f708f10e428d992f8e235a"
        );
    }

    /// A tuple the decoder stores as anything but an object keeps its v3.6.1 hash: the line must not
    /// claim a shape the stored bytes do not have.
    #[test]
    fn a_tuple_not_stored_as_an_object_keeps_its_v3_6_1_hash() {
        let duped = r#"[{"name":"y","type":"uint256"},{"name":"y","type":"uint256"}]"#;
        let partial = r#"[{"name":"y","type":"uint256"},{"name":"","type":"uint256"}]"#;
        for (what, ty, indexed, components, v361) in [
            ("duplicate names", "tuple", false, duped, V361_MADE_TUPLE),
            ("a missing name", "tuple", false, partial, V361_MADE_TUPLE),
            (
                "an indexed tuple",
                "tuple",
                true,
                YZ,
                "39c4e7d2b4e4963207faf599017cf569ea72f567a934c52c5620aa13293b9549",
            ),
            (
                "a named tuple[]",
                "tuple[]",
                false,
                YZ,
                "ebc7955973d7f13ccd2b80169933f7f5a504748b8ae236baead8932d80cbd980",
            ),
        ] {
            assert_eq!(made_hash(ty, indexed, components), v361, "{what}");
        }

        // A named tuple[] really is stored positionally, which is why it keeps the old line.
        let reg =
            DecodeRegistry::build(vec![spec("t", USDC, &made_abi("tuple[]", false, YZ))]).unwrap();
        let topic0 = format!("0x{}", hex::encode(reg.tables()[0].topic0));
        let word = |n: u8| format!("{}{n:02x}", "00".repeat(31));
        let data = format!("0x{}{}{}{}", word(0x20), word(1), word(1), word(2));
        let row = reg
            .decode(&log(USDC, &[&topic0], &data, 1, 0))
            .unwrap()
            .expect("topic0 and address match");
        let Value::Json(raw) = &row.params[0].1 else {
            panic!("orders must be Json, got {:?}", row.params[0].1);
        };
        assert_eq!(
            serde_json::from_str::<Json>(raw).unwrap(),
            json!([["1", "2"]])
        );
    }

    #[test]
    fn anonymous_events_are_skipped_and_counted() {
        let anon = r#"[
            {"type":"event","name":"Transfer","inputs":[
                {"name":"from","type":"address","indexed":true},
                {"name":"to","type":"address","indexed":true},
                {"name":"value","type":"uint256","indexed":false}],"anonymous":false},
            {"type":"event","name":"Secret","inputs":[
                {"name":"x","type":"uint256","indexed":false}],"anonymous":true}
        ]"#;
        let reg = DecodeRegistry::build(vec![spec("t", USDC, anon)]).unwrap();
        assert_eq!(reg.skipped_anonymous(), 1);
        assert_eq!(reg.tables().len(), 1); // only Transfer, Secret skipped
    }

    #[test]
    fn snake_case_events() {
        assert_eq!(snake_case("Transfer"), "transfer");
        assert_eq!(snake_case("PoolCreated"), "pool_created");
        assert_eq!(snake_case("OperatorSet"), "operator_set");
    }

    /// A `[[templates]]` allowlist scopes a child's decode without editing the vendored ABI
    /// (#311). Before this, the ABI was the only filter: a full `UniswapV2Pair` ABI decoded six
    /// events where the nest wanted one, which is a different workload with nothing to say so.
    #[test]
    fn a_template_allowlist_scopes_what_children_decode() {
        let abi: JsonAbi = serde_json::from_str(PAIR_ABI).unwrap();

        // Empty allowlist = decode everything, exactly as before the key existed.
        let all = DecodeRegistry::build_with_templates(
            vec![],
            vec![TemplateSpec {
                name: "pair".into(),
                abi: abi.clone(),
                events: Vec::new(),
            }],
        )
        .unwrap();
        let mut names: Vec<String> = all.tables().iter().map(|d| d.table.clone()).collect();
        names.sort();
        assert_eq!(names, vec!["pair__swap", "pair__sync", "pair__transfer"]);

        // Allowlisted = only that event, and the other decoders are gone rather than merely unused.
        let scoped = DecodeRegistry::build_with_templates(
            vec![],
            vec![TemplateSpec {
                name: "pair".into(),
                abi,
                events: vec!["Swap".into()],
            }],
        )
        .unwrap();
        let names: Vec<String> = scoped.tables().iter().map(|d| d.table.clone()).collect();
        assert_eq!(names, vec!["pair__swap"]);
    }

    /// The same loud rejection a contract allowlist gets. A typo that silently indexed nothing
    /// would only surface as an empty table long after the backfill.
    #[test]
    fn a_template_allowlist_typo_is_rejected_at_build() {
        let abi: JsonAbi = serde_json::from_str(PAIR_ABI).unwrap();
        let err = DecodeRegistry::build_with_templates(
            vec![],
            vec![TemplateSpec {
                name: "pair".into(),
                abi,
                events: vec!["Swop".into()],
            }],
        )
        .err()
        .expect("a template allowlisting an event its ABI lacks must be rejected")
        .to_string();
        assert!(err.contains("template 'pair'"), "names the template: {err}");
        assert!(err.contains("Swop"), "names the offending event: {err}");
        assert!(
            err.contains("Swap"),
            "lists what the ABI does define: {err}"
        );
    }

    const PAIR_ABI: &str = r#"[
      {"type":"event","name":"Swap","anonymous":false,"inputs":[{"name":"amount0In","type":"uint256","indexed":false}]},
      {"type":"event","name":"Sync","anonymous":false,"inputs":[{"name":"reserve0","type":"uint112","indexed":false}]},
      {"type":"event","name":"Transfer","anonymous":false,"inputs":[{"name":"from","type":"address","indexed":true}]}
    ]"#;

    /// COR-11 oracle (nuthatch#290 fuzz follow-up, GH#581). `value_from_dynsol` guards a
    /// declared-width-<=64 uint with `saturating_to::<u64>()` because alloy's dyn-abi decoder does
    /// not require the padding above a sub-256-bit declared width to be zero: a log emitted (or
    /// forged) with dirty high bits decodes to a `DynSolValue::Uint` whose `U256` does not fit in
    /// `u64`. An unchecked `.to::<u64>()` there panics and would take the ingestion task down on
    /// attacker-supplied log data. This is the "reds when the guard is removed" proof the cargo-fuzz
    /// harness in fuzz/ could not complete in this environment (rustc ICE compiling dbsp under
    /// sanitizer instrumentation, GH#581) - it needs no nightly toolchain and no sanitizer, so it
    /// runs the same crafted input through the real decode path on stable `cargo test`.
    #[test]
    fn dirty_high_bits_on_a_sub64_uint_saturate_instead_of_panicking() {
        const ODD: &str = r#"[
            {"type":"event","name":"Odd","anonymous":false,"inputs":[
                {"name":"v","type":"uint64","indexed":false}]}
        ]"#;
        let reg = DecodeRegistry::build(vec![spec("odd", USDC, ODD)]).unwrap();
        let topic0 = format!("0x{}", hex::encode(reg.tables()[0].topic0));
        // A full 32-byte word of 0xff: every bit above the declared 64-bit width is dirty.
        let l = log(USDC, &[&topic0], &format!("0x{}", "ff".repeat(32)), 1, 0);
        let row = reg
            .decode(&l)
            .expect("a dirty-high-bits uint64 must not panic decoding")
            .expect("topic0/address both match the fixture");
        assert_eq!(row.params[0].0, "v");
        // Saturated, not truncated/masked - the guard's documented behaviour.
        assert_eq!(row.params[0].1, Value::U64(u64::MAX));
    }

    /// The int arm of COR-11 (#2148): alloy detokenizes every `int<N>` as a full 256-bit word with no
    /// sign-extension check, so an `int64` word that does not fit `i64` panicked `as_i64()`.
    #[test]
    fn an_out_of_range_sub64_int_saturates_instead_of_panicking() {
        const ODD: &str = r#"[
            {"type":"event","name":"Odd","anonymous":false,"inputs":[
                {"name":"t","type":"int64","indexed":true},
                {"name":"v","type":"int64","indexed":false}]}
        ]"#;
        let reg = DecodeRegistry::build(vec![spec("odd", USDC, ODD)]).unwrap();
        let topic0 = format!("0x{}", hex::encode(reg.tables()[0].topic0));
        let decode = |topic: String, data: String| {
            let l = log(USDC, &[&topic0, &topic], &data, 1, 0);
            reg.decode(&l)
                .expect("an out-of-range int64 must not panic decoding")
                .expect("topic0/address both match the fixture")
                .params
        };
        let high = format!("0x01{}", "00".repeat(31));
        let low = format!("0x80{}", "00".repeat(31));
        let p = decode(high.clone(), low);
        assert_eq!(p[0].1, Value::I64(i64::MAX));
        assert_eq!(p[1].1, Value::I64(i64::MIN));
        // A conformant negative still decodes to itself.
        let minus_five = format!("0x{}fb", "ff".repeat(31));
        let p = decode(minus_five.clone(), minus_five);
        assert_eq!(p[0].1, Value::I64(-5));
        assert_eq!(p[1].1, Value::I64(-5));
    }

    /// Prove the fuzz/fuzz_targets/decode_log.rs fixture ABI parses and that `decode` is actually
    /// reached (nuthatch#231/#290). The ABI was previously invalid: `indexed` appeared on tuple
    /// *components*, which alloy-json-abi rejects. The harness panicked at `unwrap()` in
    /// `build_registry()` before libFuzzer ran a single iteration, so the decode path was never
    /// exercised. This test uses the same six events and a well-formed Transfer log to confirm:
    /// (1) `DecodeRegistry::build` succeeds, (2) `decode` returns `Ok(Some(..))` rather than
    /// panicking.
    #[test]
    fn decode_log_fuzz_fixture_abi_parses_and_decode_is_reached() {
        fn nested_tuple_param(depth: u16) -> serde_json::Value {
            let mut param = serde_json::json!({"name": "leaf", "type": "uint256"});
            for i in 0..depth {
                param = serde_json::json!({
                    "name": format!("t{i}"),
                    "type": "tuple",
                    "components": [param],
                });
            }
            param
        }
        let mut top = nested_tuple_param(2);
        top["indexed"] = serde_json::Value::Bool(false);
        let events = serde_json::json!([
            {"type":"event","name":"Transfer","anonymous":false,"inputs":[
                {"name":"from","type":"address","indexed":true},
                {"name":"to","type":"address","indexed":true},
                {"name":"value","type":"uint256","indexed":false},
            ]},
            {"type":"event","name":"Hashed","anonymous":false,"inputs":[
                {"name":"label","type":"string","indexed":true},
                {"name":"amount","type":"uint256","indexed":false},
            ]},
            {"type":"event","name":"Collection","anonymous":false,"inputs":[
                {"name":"amounts","type":"uint256[]","indexed":false},
                {"name":"pair","type":"tuple","indexed":false,"components":[
                    {"name":"a","type":"address"},
                    {"name":"b","type":"uint256"},
                ]},
            ]},
            {"type":"event","name":"HugeArray","anonymous":false,"inputs":[
                {"name":"data","type":"uint256[4000000000]","indexed":false},
            ]},
            {"type":"event","name":"Deep","anonymous":false,"inputs":[top]},
        ]);
        let abi: alloy_json_abi::JsonAbi = serde_json::from_value(events)
            .expect("fuzz fixture ABI must parse without indexed on components");
        let addr = "0x1111111111111111111111111111111111111111";
        let reg = DecodeRegistry::build(vec![ContractSpec {
            alias: "fuzz".into(),
            address: parse_address(addr).unwrap(),
            abi,
            events: Vec::new(),
        }])
        .expect("build must succeed");
        // Find the Transfer table and construct a valid log to confirm decode is reachable.
        let transfer_dec = reg
            .tables()
            .into_iter()
            .find(|d| d.table == "fuzz__transfer")
            .expect("Transfer fixture must be registered");
        let topic0 = format!("0x{}", hex::encode(transfer_dec.topic0));
        let from = format!("0x{}", "aa".repeat(32));
        let to = format!("0x{}", "bb".repeat(32));
        let value = format!("0x{}", "00".repeat(31) + "2a");
        let l = log(addr, &[&topic0, &from, &to], &value, 1, 0);
        let row = reg
            .decode(&l)
            .expect("well-formed Transfer log must not error")
            .expect("topic0 and address match - must return Some");
        // uint256 decodes as Word32 (a 32-byte big-endian word), not U64.
        let mut expected = [0u8; 32];
        expected[31] = 42;
        assert_eq!(
            row.params.iter().find(|(n, _)| n == "value").unwrap().1,
            Value::Word32(expected)
        );
    }
}

/// Rebuild one [`Value`] from the text a stored row carries, given the column it belongs to.
///
/// **The inverse of [`Value::to_json`], and it exists because nothing else is** (nuthatch#864). An
/// authored incremental entity's rows arrive from three places: the decode registry at `+1`, the hot
/// store's JSON when a reorg feeds them back at `-1`, and sealed Parquet text when a restart seeds
/// from finalized history. A retraction only cancels an insertion if the two produce the *same* row,
/// so a second, separately-written converter for the `-1` path is not a duplication of effort - it is
/// a way for the two to disagree and for a rolled-back fact to stay in an entity forever.
///
/// **The column is required, not a convenience.** The rendering is lossy about which variant a value
/// came from: `"0x11…"` could be an address, fixed bytes or a topic hash, and `"7"` could be a
/// `uint128`, an `int128` or a string. Worse, `storage` alone is not enough either - `word16` covers
/// both `uint128` and `int128`, and only [`ColumnSchema::sol_type`] separates them, because
/// [`value_from_dynsol`] chose the variant from the Solidity type in the first place.
pub fn value_from_stored(v: &Json, col: &ColumnSchema) -> Result<Value> {
    // A JSON null is an absent value, which no `Value` variant represents. Callers that can have one
    // must decide what it means for their column rather than being handed a silent zero.
    if v.is_null() {
        bail!("column {} is null in the stored row", col.name)
    }
    let signed = col.sol_type.starts_with("int");
    let text = || -> Result<&str> {
        v.as_str()
            .ok_or_else(|| anyhow!("column {} is not text: {v}", col.name))
    };
    let bytes = |want: Option<usize>| -> Result<Vec<u8>> {
        let s = text()?;
        let raw = hex::decode(s.trim_start_matches("0x"))
            .with_context(|| format!("column {} is not hex: {s}", col.name))?;
        if let Some(n) = want {
            if raw.len() != n {
                bail!("column {} is {} bytes, expected {n}", col.name, raw.len())
            }
        }
        Ok(raw)
    };
    // Numbers survive `to_json` as JSON numbers and the seal writer's Utf8 columns as text, so both
    // spellings have to work or the reorg path and the restart path disagree by construction.
    //
    // The four cases are spelled out rather than funnelled through one integer type on purpose: a
    // signed 256-bit value does not fit `i128`, and parsing it through one is the kind of narrowing
    // that works on every test amount and fails on the one that matters.
    let word = |wide: bool| -> Result<Vec<u8>> {
        let s = match v {
            Json::Number(n) => n.to_string(),
            _ => text()?.to_string(),
        };
        let bad = |what: &str| anyhow!("column {} is not {what}: {s}", col.name);
        Ok(match (signed, wide) {
            (false, false) => s
                .parse::<u128>()
                .map_err(|_| bad("a 128-bit unsigned integer"))?
                .to_be_bytes()
                .to_vec(),
            (true, false) => s
                .parse::<i128>()
                .map_err(|_| bad("a 128-bit signed integer"))?
                .to_be_bytes()
                .to_vec(),
            (false, true) => U256::from_str_radix(&s, 10)
                .map_err(|_| bad("a 256-bit unsigned integer"))?
                .to_be_bytes::<32>()
                .to_vec(),
            (true, true) => s
                .parse::<I256>()
                .map_err(|_| bad("a 256-bit signed integer"))?
                .to_be_bytes::<32>()
                .to_vec(),
        })
    };

    Ok(match col.storage.as_str() {
        "address" => {
            let raw = bytes(Some(20))?;
            Value::Address(raw.try_into().expect("checked to be 20 bytes"))
        }
        "hash32" => {
            let raw = bytes(Some(32))?;
            Value::Hash32(raw.try_into().expect("checked to be 32 bytes"))
        }
        "bytes" | "fixed_bytes" => Value::Bytes(bytes(None)?),
        "bool" => Value::Bool(match v {
            Json::Bool(b) => *b,
            _ => text()?
                .parse()
                .with_context(|| format!("column {} is not a boolean", col.name))?,
        }),
        "u64" => Value::U64(match v {
            Json::Number(n) => n
                .as_u64()
                .ok_or_else(|| anyhow!("column {} does not fit u64: {v}", col.name))?,
            _ => text()?
                .parse()
                .with_context(|| format!("column {} is not a u64", col.name))?,
        }),
        "i64" => Value::I64(match v {
            Json::Number(n) => n
                .as_i64()
                .ok_or_else(|| anyhow!("column {} does not fit i64: {v}", col.name))?,
            _ => text()?
                .parse()
                .with_context(|| format!("column {} is not an i64", col.name))?,
        }),
        "word16" => {
            let w: [u8; 16] = word(false)?.try_into().expect("16 bytes");
            if signed {
                Value::IWord16(w)
            } else {
                Value::Word16(w)
            }
        }
        "word32" => {
            let w: [u8; 32] = word(true)?.try_into().expect("32 bytes");
            if signed {
                Value::IWord32(w)
            } else {
                Value::Word32(w)
            }
        }
        "string" => Value::Str(text()?.to_string()),
        "json" => Value::Json(match v {
            Json::String(s) => s.clone(),
            other => other.to_string(),
        }),
        other => bail!("column {} has unknown storage kind {other}", col.name),
    })
}
