//! Calldata decode and the firehose-class table shapes (RFC-0014, node-independent slice).
//!
//! Event decode is keyed by `topic0`; call decode is keyed by the **4-byte function selector**, which
//! is the same idea one layer down. Everything here is a pure function of `(ABI, calldata)` - no RPC,
//! no node, no clock - so it is fully testable today even though the thing that will *feed* it (the
//! ExEx `ChainCommitted` notification, RFC-0003) does not exist yet.
//!
//! **Why build the decoder before the source.** The decode surface is where the correctness risk
//! lives - selector collisions across overloads, non-conformant calldata, the raw-fallback rule - and
//! none of that risk needs a node to exercise. What genuinely needs the node is extraction, so that
//! is the only part deferred. A nest that asks for extraction today is refused at startup by
//! [`crate::config::Extract::scope_check`] and the source check in `indexer`, rather than being served
//! tables that would silently never fill.
//!
//! ## What differs from event decode, deliberately
//!
//! - **A miss still produces a row.** An unrecognised topic0 is not our business - it belongs to some
//!   other contract. An unrecognised *selector on a contract we index* is our business and is
//!   information: it means someone called a function this ABI does not describe (a proxy whose
//!   implementation moved, an ABI that was fetched before an upgrade). Those land in
//!   [`RAW_CALLS_TABLE`] with the selector and raw input, so the gap is visible rather than absent.
//! - **No `indexed` distinction.** Function inputs are all in the body; there is no topics/data split
//!   and therefore no hashed-dynamic-type problem. Call columns are never `Hash32`.

use std::collections::{HashMap, HashSet};

use alloy_dyn_abi::JsonAbiExt;
use alloy_json_abi::{Function, JsonAbi};
use alloy_primitives::Address;
use anyhow::{anyhow, Result};
use sha2::{Digest, Sha256};

use crate::config::{Config, Extract};
use crate::registry::{
    implicit_columns, reserved_column_names, snake_case, value_from_dynsol, Column, ColumnSchema,
    DecodedRow, StorageKind, TableKind, TableSchema, Value,
};

/// Undecodable calls to contracts we index. One shared table rather than one per contract: the rows
/// have no ABI-derived shape to differ by, and the implicit `address` column already says who was
/// called. Keeping them together also makes "what are we failing to decode?" a single query.
pub const RAW_CALLS_TABLE: &str = "calls_raw";

/// Raw storage writes: one row per `SSTORE`, slots undecoded (RFC-0014 non-goal: layout-aware decode
/// is a later increment). One wide table, per the RFC's open question 1 - per-contract views over it
/// if anyone ever demands them.
pub const STATE_DIFFS_TABLE: &str = "state_diffs";

/// Decodes one function of one contract into rows of one table.
pub struct CallDecoder {
    pub alias: String,
    pub contract: Address,
    pub table: String,
    pub columns: Vec<Column>,
    pub selector: [u8; 4],
    pub signature: String,
    function: Function,
}

impl CallDecoder {
    fn new(alias: &str, contract: Address, function: Function) -> CallDecoder {
        let columns: Vec<Column> = function
            .inputs
            .iter()
            .enumerate()
            .map(|(i, p)| Column {
                name: if p.name.is_empty() {
                    format!("arg{i}")
                } else {
                    p.name.clone()
                },
                sol_type: p.ty.clone(),
                kind: StorageKind::from_sol(&p.ty, false),
                // Calldata has no topics, so nothing is ever indexed. Stated rather than implied,
                // because `value_from_dynsol` branches on it.
                indexed: false,
                // A function argument can be a tuple exactly as an event parameter can (#1304).
                components: nuthatch_decode::registry::tuple_components(&p.components),
            })
            .collect();
        CallDecoder {
            alias: alias.to_string(),
            contract,
            // `call_` rather than bare `{alias}__{name}`: a contract may legitimately have both a
            // `Transfer` event and a `transfer` function, and `usdc__transfer` must keep meaning the
            // event it has always meant. Renaming the event table to disambiguate would break every
            // existing query, so the new surface takes the qualified name.
            table: format!("{alias}__call_{}", snake_case(&function.name)),
            columns,
            selector: function.selector().0,
            signature: function.signature(),
            function,
        }
    }
}

/// The call-decode half of a nest's registry (RFC-0014). Separate from [`crate::registry::DecodeRegistry`]
/// rather than folded into it: it is opt-in, it keys off a different discriminator, and keeping it
/// separate means a nest that does not use it carries no extra state.
pub struct CallRegistry {
    by_selector: HashMap<[u8; 4], Vec<CallDecoder>>,
    /// Addresses extraction is scoped to. Empty means unscoped - which the volume guard only permits
    /// with an explicit `unbounded = true`.
    scope: Vec<Address>,
    /// Selector allowlist. Empty means every selector.
    allow: Vec<[u8; 4]>,
    hash: [u8; 32],
    /// Whether call rows carry `block_timestamp` (RFC-0029 §6b). Mirrors `DecodeRegistry::timestamps`:
    /// a nest declares the policy once and *every* table it produces obeys it, or `/tables` would
    /// advertise one shape for events and another for calls.
    timestamps: bool,
}

impl CallRegistry {
    /// Build from a nest's vendored ABIs, honouring `[extract]` scoping.
    pub fn from_nest(dir: &std::path::Path, config: &Config) -> Result<CallRegistry> {
        let mut specs = Vec::new();
        for c in &config.contracts {
            // Same path-traversal guard as event decode - an ABI path is user input.
            let path = crate::blob::checked_join(dir, &c.abi)?;
            let raw = std::fs::read_to_string(&path)
                .map_err(|e| anyhow!("cannot read ABI {}: {e}", path.display()))?;
            let abi: JsonAbi = serde_json::from_str(&raw)
                .map_err(|e| anyhow!("cannot parse ABI {}: {e}", path.display()))?;
            let address: Address = c
                .address
                .parse()
                .map_err(|e| anyhow!("contract {} has an unparseable address: {e}", c.alias))?;
            specs.push((c.alias.clone(), address, abi));
        }
        Ok(Self::build(specs, &config.extract)?.with_timestamps(config.nest.block_timestamps))
    }

    /// See [`crate::registry::DecodeRegistry::with_timestamps`].
    pub fn with_timestamps(mut self, timestamps: bool) -> CallRegistry {
        self.timestamps = timestamps;
        self
    }

    pub fn build(
        specs: Vec<(String, Address, JsonAbi)>,
        extract: &Extract,
    ) -> Result<CallRegistry> {
        let allow = extract.selector_keys()?;
        // Scoping names *aliases*, so an alias that matches no contract is a typo that would
        // otherwise widen the scope to nothing (or, read the other way, silently drop extraction).
        // Refuse it, exactly as the event allowlist refuses an unknown event name.
        let mut scope = Vec::new();
        for want in &extract.contracts {
            let found = specs
                .iter()
                .find(|(alias, _, _)| alias == want)
                .ok_or_else(|| {
                    anyhow!(
                        "[extract] contracts lists `{want}`, which is not a contract in this nest \
                         (have: {})",
                        specs
                            .iter()
                            .map(|(a, _, _)| a.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                })?;
            scope.push(found.1);
        }

        let mut by_selector: HashMap<[u8; 4], Vec<CallDecoder>> = HashMap::new();
        for (alias, address, abi) in &specs {
            if !scope.is_empty() && !scope.contains(address) {
                continue;
            }
            register_functions(&mut by_selector, alias, *address, abi, &allow);
        }
        let mut decoders: Vec<&CallDecoder> = by_selector.values().flatten().collect();
        decoders.sort_by(|a, b| a.table.cmp(&b.table));
        for d in decoders {
            let mut seen = HashSet::with_capacity(d.columns.len());
            for c in &d.columns {
                if c.name == TX_FROM_COLUMN || reserved_column_names().contains(&c.name.as_str()) {
                    return Err(anyhow!(
                        "`{}` has an argument stored as column `{}`, which every call table already \
                         carries for the chain's own value. Two columns of one name cannot both be \
                         stored (#814, #2159).",
                        d.signature,
                        c.name
                    ));
                }
                if !seen.insert(c.name.as_str()) {
                    return Err(anyhow!(
                        "`{}` has two arguments that both become column `{}`. One would silently \
                         replace the other in every stored row.",
                        d.signature,
                        c.name
                    ));
                }
            }
        }
        let hash = registry_hash(&by_selector);
        Ok(CallRegistry {
            by_selector,
            scope,
            allow,
            hash,
            timestamps: true,
        })
    }

    /// Content address of the call-decode surface. Distinct from the event registry's hash and mixed
    /// into it by the caller, so that turning extraction on changes the nest's decode identity -
    /// otherwise two nests with different extraction config would claim the same decode version.
    pub fn hash(&self) -> [u8; 32] {
        self.hash
    }

    pub fn is_empty(&self) -> bool {
        self.by_selector.is_empty()
    }

    /// Is this address in scope for extraction? Unscoped means every address.
    pub fn in_scope(&self, to: Address) -> bool {
        self.scope.is_empty() || self.scope.contains(&to)
    }

    /// Does this nest declare a function on `to`? The event registry cannot answer that for a contract
    /// whose ABI has no events, and a calldata-only contract (a DataEdge) is exactly that shape.
    pub fn declares(&self, to: Address) -> bool {
        self.by_selector
            .values()
            .flatten()
            .any(|d| d.contract == to)
    }

    pub fn tables(&self) -> Vec<&CallDecoder> {
        let mut all: Vec<&CallDecoder> = self.by_selector.values().flatten().collect();
        all.sort_by(|a, b| a.table.cmp(&b.table));
        all
    }

    /// Decode one call. `input` is the raw calldata: 4-byte selector then ABI-encoded arguments.
    ///
    /// Returns `Ok(None)` only when the call carries nothing to decode or is out of scope - never
    /// because decoding failed. A call to an in-scope contract whose selector we do not know, or
    /// whose arguments do not decode, yields a [`RAW_CALLS_TABLE`] row instead of vanishing.
    pub fn decode_call(&self, to: Address, input: &[u8], ctx: &CallContext) -> Option<DecodedRow> {
        if !self.in_scope(to) {
            return None;
        }
        // Under 4 bytes there is no selector: a plain value transfer, or a call into a contract's
        // receive/fallback with no data. Nothing to decode and nothing worth a raw row.
        if input.len() < 4 {
            return None;
        }
        let selector: [u8; 4] = input[..4].try_into().ok()?;
        if !self.allow.is_empty() && !self.allow.contains(&selector) {
            return None;
        }

        let matched = self
            .by_selector
            .get(&selector)
            .and_then(|ds| ds.iter().find(|d| d.contract == to));

        if let Some(dec) = matched {
            // A selector match is not a decode guarantee: the same 4 bytes can front arguments that
            // do not match this ABI (an upgraded implementation, or a deliberate collision - the
            // selector is only 4 bytes and collisions are cheap to grind). Fall back rather than
            // propagate an error, so one odd transaction cannot stall a block.
            match decode_params(dec, &input[4..]) {
                Ok(params) => return Some(ctx.row(dec.table.clone(), params, to)),
                Err(_) => return Some(self.raw_row(to, selector, input, ctx)),
            }
        }
        Some(self.raw_row(to, selector, input, ctx))
    }

    fn raw_row(
        &self,
        to: Address,
        selector: [u8; 4],
        input: &[u8],
        ctx: &CallContext,
    ) -> DecodedRow {
        ctx.row(
            RAW_CALLS_TABLE.to_string(),
            vec![
                (
                    "selector".to_string(),
                    Value::Str(format!("0x{}", hex::encode(selector))),
                ),
                (
                    "input".to_string(),
                    Value::Str(format!("0x{}", hex::encode(input))),
                ),
            ],
            to,
        )
    }

    /// The call tables this nest produces, in the same shape `/tables`, MCP and `schema.json` already
    /// consume for events.
    pub fn schema(&self, extract: &Extract) -> Vec<TableSchema> {
        let mut out: Vec<TableSchema> = self
            .tables()
            .iter()
            .map(|d| {
                let mut columns = implicit_columns(self.timestamps);
                columns.extend(d.columns.iter().map(|c| ColumnSchema {
                    name: c.name.clone(),
                    sol_type: c.sol_type.clone(),
                    storage: c.kind.as_str().to_string(),
                    indexed: false,
                    components: c.components.clone(),
                }));
                columns.push(tx_from_column());
                TableSchema {
                    table: d.table.clone(),
                    alias: d.alias.clone(),
                    kind: TableKind::Call,
                    event: String::new(),
                    topic0: String::new(),
                    function: d.signature.clone(),
                    selector: format!("0x{}", hex::encode(d.selector)),
                    columns,
                }
            })
            .collect();
        // The raw-calls catch-all belongs to either call surface: an unrecognised selector on a
        // contract we index is information (a proxy whose implementation moved, an ABI fetched before
        // an upgrade), and that is as true of a top-level call as of an internal one.
        if extract.traces || extract.top_level_calls {
            out.push(raw_calls_schema(self.timestamps));
        }
        if extract.state {
            out.push(state_diffs_schema(self.timestamps));
        }
        out
    }
}

/// The implicit column carrying a call row's transaction sender.
pub const TX_FROM_COLUMN: &str = "tx_from";

fn tx_from_column() -> ColumnSchema {
    ColumnSchema {
        name: TX_FROM_COLUMN.into(),
        // Not "implicit": the sender travels as a param, and a row rebuilt from storage keeps only the
        // columns that are not implicit.
        sol_type: "address".into(),
        storage: "address".into(),
        indexed: false,
        components: Vec::new(),
    }
}

/// Context every call row shares - the block/transaction it belongs to.
pub struct CallContext {
    pub block_number: u64,
    pub block_hash: String,
    pub block_timestamp: u64,
    pub tx_hash: String,
    /// The transaction's sender. A DataEdge accepts posts from anyone, so without it a nest cannot tell
    /// the publisher's rows from a stranger's.
    pub tx_from: Address,
    /// Position of this call in a deterministic depth-first walk of the block's call tree. Plays the
    /// part `log_index` plays for events: a stable per-block ordinal that makes the row addressable
    /// and re-executable.
    ///
    /// **The gap this once recorded is closed.** The hot store keys every row by `(block, log_index)`
    /// in one namespace, so call ordinal 5 and log index 5 in the same block used to collide - the
    /// same defect that turned out to be *live* for block rows (#642). Rows that descend from no log
    /// now live in a reserved band: [`crate::registry::TX_CALL_ROW_LOG_INDEX_BASE`] for calls,
    /// [`crate::registry::CALL_ROW_LOG_INDEX_BASE`] for pinned reads, and
    /// [`crate::registry::BLOCK_ROW_LOG_INDEX`] for the block row.
    pub call_index: u64,
    /// See [`crate::registry::DecodedRow::timestamps`] - carried on the context because every row
    /// built from one block shares it.
    pub timestamps: bool,
}

impl CallContext {
    fn row(&self, table: String, mut params: Vec<(String, Value)>, to: Address) -> DecodedRow {
        params.push((
            TX_FROM_COLUMN.to_string(),
            Value::Address(self.tx_from.into_array()),
        ));
        DecodedRow {
            table,
            params,
            block_number: self.block_number,
            block_hash: self.block_hash.clone(),
            block_timestamp: self.block_timestamp,
            timestamps: self.timestamps,
            log_index: self.call_index,
            tx_hash: self.tx_hash.clone(),
            address: format!("0x{}", hex::encode(to)),
        }
    }
}

fn decode_params(dec: &CallDecoder, args: &[u8]) -> Result<Vec<(String, Value)>> {
    let values = dec
        .function
        .abi_decode_input(args)
        .map_err(|e| anyhow!("decode {}: {e}", dec.signature))?;
    if values.len() != dec.columns.len() {
        return Err(anyhow!("param count mismatch decoding {}", dec.signature));
    }
    Ok(dec
        .columns
        .iter()
        .zip(values.iter())
        .map(|(col, dv)| (col.name.clone(), value_from_dynsol(dv, col)))
        .collect())
}

/// Register a contract's functions by selector, mirroring `registry::register_events`.
fn register_functions(
    map: &mut HashMap<[u8; 4], Vec<CallDecoder>>,
    alias: &str,
    address: Address,
    abi: &JsonAbi,
    allow: &[[u8; 4]],
) {
    // Overload disambiguation, as for events: Solidity lets `swap(uint)` and `swap(uint,uint)`
    // coexist, and they would otherwise share a table whose columns disagree with half its rows.
    let mut name_counts: HashMap<String, usize> = HashMap::new();
    for f in abi.functions() {
        *name_counts.entry(snake_case(&f.name)).or_default() += 1;
    }
    for f in abi.functions() {
        let selector = f.selector().0;
        if !allow.is_empty() && !allow.contains(&selector) {
            continue;
        }
        let mut dec = CallDecoder::new(alias, address, f.clone());
        if name_counts.get(&snake_case(&f.name)).copied().unwrap_or(0) > 1 {
            let s = hex::encode(dec.selector);
            dec.table = format!("{}_{}", dec.table, &s[..4]);
        }
        map.entry(selector).or_default().push(dec);
    }
}

fn registry_hash(by_selector: &HashMap<[u8; 4], Vec<CallDecoder>>) -> [u8; 32] {
    // Sorted so the hash is a property of the decode surface, not of HashMap iteration order.
    let mut lines: Vec<String> = by_selector
        .values()
        .flatten()
        .map(|d| {
            format!(
                "f|{}|{}|{}|{}|{}",
                d.alias,
                hex::encode(d.contract),
                hex::encode(d.selector),
                d.signature,
                d.columns
                    .iter()
                    .map(|c| {
                        format!(
                            "{}:{}{}",
                            c.name,
                            c.sol_type,
                            nuthatch_decode::registry::object_shape(c)
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(",")
            )
        })
        .collect();
    // The sender column changed every call table's shape, so it has to change the identity too.
    lines.push(format!("implicit|{TX_FROM_COLUMN}"));
    lines.sort();
    let mut h = Sha256::new();
    for l in &lines {
        h.update(l.as_bytes());
        h.update(b"\n");
    }
    h.finalize().into()
}

fn raw_calls_schema(timestamps: bool) -> TableSchema {
    let mut columns = implicit_columns(timestamps);
    columns.push(ColumnSchema {
        name: "selector".into(),
        sol_type: "bytes4".into(),
        storage: "str".into(),
        indexed: false,
        components: Vec::new(),
    });
    columns.push(ColumnSchema {
        name: "input".into(),
        sol_type: "bytes".into(),
        storage: "str".into(),
        indexed: false,
        components: Vec::new(),
    });
    columns.push(tx_from_column());
    TableSchema {
        table: RAW_CALLS_TABLE.into(),
        alias: String::new(),
        kind: TableKind::Call,
        event: String::new(),
        topic0: String::new(),
        function: String::new(),
        selector: String::new(),
        columns,
    }
}

fn state_diffs_schema(timestamps: bool) -> TableSchema {
    let mut columns = implicit_columns(timestamps);
    for (name, ty) in [("slot", "bytes32"), ("prev", "bytes32"), ("new", "bytes32")] {
        columns.push(ColumnSchema {
            name: name.into(),
            sol_type: ty.into(),
            storage: "str".into(),
            indexed: false,
            components: Vec::new(),
        });
    }
    TableSchema {
        table: STATE_DIFFS_TABLE.into(),
        alias: String::new(),
        kind: TableKind::State,
        event: String::new(),
        topic0: String::new(),
        function: String::new(),
        selector: String::new(),
        columns,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ERC20: &str = r#"[
      {"type":"function","name":"transfer","inputs":[
        {"name":"to","type":"address"},{"name":"value","type":"uint256"}],
       "outputs":[{"name":"","type":"bool"}],"stateMutability":"nonpayable"},
      {"type":"function","name":"approve","inputs":[
        {"name":"spender","type":"address"},{"name":"value","type":"uint256"}],
       "outputs":[{"name":"","type":"bool"}],"stateMutability":"nonpayable"}
    ]"#;

    /// Two `swap` overloads - the case that silently corrupts a table if names alone key it.
    const OVERLOADED: &str = r#"[
      {"type":"function","name":"swap","inputs":[{"name":"a","type":"uint256"}],
       "outputs":[],"stateMutability":"nonpayable"},
      {"type":"function","name":"swap","inputs":[
        {"name":"a","type":"uint256"},{"name":"b","type":"uint256"}],
       "outputs":[],"stateMutability":"nonpayable"}
    ]"#;

    fn addr(b: u8) -> Address {
        Address::from([b; 20])
    }

    fn reg(abi_json: &str, extract: &Extract) -> CallRegistry {
        let abi: JsonAbi = serde_json::from_str(abi_json).unwrap();
        CallRegistry::build(vec![("tok".into(), addr(1), abi)], extract).unwrap()
    }

    fn ctx() -> CallContext {
        CallContext {
            block_number: 100,
            block_hash: "0xbb".into(),
            block_timestamp: 1_700_000_000,
            timestamps: true,
            tx_hash: "0xtt".into(),
            tx_from: addr(7),
            call_index: 3,
        }
    }

    /// #2159: an argument named after an implicit column would replace the chain's own value in the
    /// stored row, exactly as an event parameter would (#814). Only `tx_from` used to be refused.
    #[test]
    fn an_argument_named_like_an_implicit_column_is_refused() {
        let build = |inputs: &str| {
            let abi: JsonAbi = serde_json::from_str(&format!(
                r#"[{{"type":"function","name":"post","inputs":[{inputs}],
                    "outputs":[],"stateMutability":"nonpayable"}}]"#
            ))
            .unwrap();
            match CallRegistry::build(vec![("tok".into(), addr(1), abi)], &Extract::default()) {
                Err(e) => format!("{e:#}"),
                Ok(_) => String::new(),
            }
        };
        for name in [
            "block_number",
            "tx_hash",
            "log_index",
            "table",
            TX_FROM_COLUMN,
        ] {
            let msg = build(&format!(r#"{{"name":"{name}","type":"uint256"}}"#));
            assert!(
                msg.contains(name) && msg.contains("post"),
                "{name}: got {msg}"
            );
        }
        let msg = build(r#"{"name":"","type":"uint256"},{"name":"arg0","type":"uint256"}"#);
        assert!(msg.contains("arg0"), "got: {msg}");
    }

    /// The sender is on every call row, decoded or raw, and the schema says so. A DataEdge takes posts
    /// from anyone; without this a nest cannot keep the publisher apart from whoever else writes to it.
    #[test]
    fn every_call_row_names_its_sender_and_the_schema_declares_it() {
        let r = reg(ERC20, &Extract::default());
        let sender = (
            TX_FROM_COLUMN.to_string(),
            Value::Address(addr(7).into_array()),
        );
        let decoded = r
            .decode_call(addr(1), &transfer_calldata(addr(9), 1), &ctx())
            .expect("a row");
        assert_eq!(decoded.params.last(), Some(&sender));
        let raw = r
            .decode_call(addr(1), &[0xde, 0xad, 0xbe, 0xef], &ctx())
            .expect("a raw row");
        assert_eq!(raw.table, RAW_CALLS_TABLE);
        assert_eq!(raw.params.last(), Some(&sender));

        let extract = Extract {
            top_level_calls: true,
            ..Extract::default()
        };
        for t in r.schema(&extract) {
            assert!(
                t.columns.iter().any(|c| c.name == TX_FROM_COLUMN),
                "{} must declare the column its rows carry",
                t.table
            );
        }
    }

    /// A call row rebuilt from storage (a restart, an entity seed, a sealed segment read back) must be
    /// the row that was stored. Declared implicit, the sender column was dropped on the way back in.
    #[test]
    fn the_sender_survives_a_row_rebuilt_from_storage() {
        let r = reg(ERC20, &Extract::default());
        let row = r
            .decode_call(addr(1), &transfer_calldata(addr(9), 1), &ctx())
            .expect("a row");
        let extract = Extract {
            top_level_calls: true,
            ..Extract::default()
        };
        let schema = r
            .schema(&extract)
            .into_iter()
            .find(|t| t.table == row.table)
            .expect("the call table's schema");
        let rebuilt = DecodedRow::from_stored(&row.to_json(), &schema).unwrap();
        assert_eq!(rebuilt.params, row.params);
    }

    #[test]
    fn an_argument_named_like_the_sender_column_is_refused() {
        let abi: JsonAbi = serde_json::from_str(
            r#"[{"type":"function","name":"post","inputs":[{"name":"tx_from","type":"address"}],"outputs":[],"stateMutability":"nonpayable"}]"#,
        )
        .unwrap();
        let err = CallRegistry::build(vec![("edge".into(), addr(1), abi)], &Extract::default())
            .err()
            .expect("a colliding argument must be refused, not silently shadowed");
        assert!(format!("{err:#}").contains("tx_from"), "{err:#}");
    }

    /// `transfer(address,uint256)` = 0xa9059cbb, the most-checked selector in Ethereum.
    fn transfer_calldata(to: Address, value: u64) -> Vec<u8> {
        let mut d = hex::decode("a9059cbb").unwrap();
        let mut word = [0u8; 32];
        word[12..].copy_from_slice(to.as_slice());
        d.extend_from_slice(&word);
        let mut v = [0u8; 32];
        v[24..].copy_from_slice(&value.to_be_bytes());
        d.extend_from_slice(&v);
        d
    }

    #[test]
    fn decodes_a_known_selector_into_its_own_table() {
        let r = reg(ERC20, &Extract::default());
        let row = r
            .decode_call(addr(1), &transfer_calldata(addr(9), 1234), &ctx())
            .expect("a row");
        assert_eq!(row.table, "tok__call_transfer");
        assert_eq!(row.params.len(), 3, "two arguments and the sender");
        assert_eq!(row.params[0].0, "to");
        assert_eq!(row.params[0].1, Value::Address(addr(9).into_array()));
        // uint256 is a `Word32`, exactly as it is for an event param - the two decode paths must
        // agree on storage kind or the same value would render differently depending on whether it
        // arrived as calldata or as a log.
        let mut expected = [0u8; 32];
        expected[30..].copy_from_slice(&1234u16.to_be_bytes());
        assert_eq!(row.params[1].1, Value::Word32(expected));
        // The call is attributed to the callee, matching how an event row is attributed to its emitter.
        assert_eq!(row.address, format!("0x{}", hex::encode(addr(1))));
        assert_eq!(row.block_number, 100);
    }

    /// The rule that differs from event decode, and the reason this table exists at all.
    #[test]
    fn an_unknown_selector_on_an_indexed_contract_becomes_a_raw_row() {
        let r = reg(ERC20, &Extract::default());
        let input = hex::decode("deadbeef00").unwrap();
        let row = r.decode_call(addr(1), &input, &ctx()).expect("a raw row");
        assert_eq!(row.table, RAW_CALLS_TABLE);
        assert_eq!(
            row.params[0],
            ("selector".into(), Value::Str("0xdeadbeef".into()))
        );
        assert_eq!(
            row.params[1],
            ("input".into(), Value::Str("0xdeadbeef00".into()))
        );
    }

    /// A selector match is not a decode guarantee. Truncated arguments must not error the block.
    #[test]
    fn a_matching_selector_with_undecodable_args_falls_back_rather_than_failing() {
        let r = reg(ERC20, &Extract::default());
        let mut input = transfer_calldata(addr(9), 1);
        input.truncate(4 + 20); // half an address' worth of argument
        let row = r.decode_call(addr(1), &input, &ctx()).expect("a raw row");
        assert_eq!(
            row.table, RAW_CALLS_TABLE,
            "a selector we know with arguments we cannot decode must still be recorded"
        );
    }

    #[test]
    fn calls_under_four_bytes_are_not_rows() {
        let r = reg(ERC20, &Extract::default());
        assert!(r.decode_call(addr(1), &[], &ctx()).is_none());
        assert!(r.decode_call(addr(1), &[1, 2, 3], &ctx()).is_none());
    }

    #[test]
    fn overloaded_functions_get_distinct_tables() {
        let r = reg(OVERLOADED, &Extract::default());
        let tables: Vec<&str> = r.tables().iter().map(|d| d.table.as_str()).collect();
        assert_eq!(tables.len(), 2);
        assert_ne!(
            tables[0], tables[1],
            "two `swap` overloads sharing one table would put rows of different arity in it"
        );
        assert!(tables.iter().all(|t| t.starts_with("tok__call_swap")));
    }

    /// Call tables must not be able to collide with the event table for a same-named event.
    #[test]
    fn call_tables_are_namespaced_away_from_event_tables() {
        let r = reg(ERC20, &Extract::default());
        assert!(r.tables().iter().all(|d| d.table.starts_with("tok__call_")));
    }

    #[test]
    fn scoping_to_an_unknown_alias_is_refused() {
        let abi: JsonAbi = serde_json::from_str(ERC20).unwrap();
        let extract = Extract {
            traces: true,
            contracts: vec!["typo".into()],
            ..Default::default()
        };
        let err = match CallRegistry::build(vec![("tok".into(), addr(1), abi)], &extract) {
            Err(e) => e,
            Ok(_) => {
                panic!("an alias that matches nothing must not silently widen or narrow the scope")
            }
        };
        assert!(err.to_string().contains("typo"), "{err}");
    }

    #[test]
    fn out_of_scope_addresses_produce_nothing() {
        let abi: JsonAbi = serde_json::from_str(ERC20).unwrap();
        let extract = Extract {
            traces: true,
            contracts: vec!["tok".into()],
            ..Default::default()
        };
        let r = CallRegistry::build(vec![("tok".into(), addr(1), abi)], &extract).unwrap();
        assert!(r
            .decode_call(addr(1), &transfer_calldata(addr(9), 1), &ctx())
            .is_some());
        assert!(
            r.decode_call(addr(2), &transfer_calldata(addr(9), 1), &ctx())
                .is_none(),
            "an address outside the declared scope is not this nest's business"
        );
    }

    #[test]
    fn a_selector_allowlist_drops_everything_else() {
        let extract = Extract {
            traces: true,
            selectors: vec!["0xa9059cbb".into()],
            ..Default::default()
        };
        let r = reg(ERC20, &extract);
        assert_eq!(r.tables().len(), 1, "only the allowed selector registers");
        assert!(r
            .decode_call(addr(1), &transfer_calldata(addr(9), 1), &ctx())
            .is_some());
        // `approve(address,uint256)` = 0x095ea7b3, deliberately not on the list.
        let approve = hex::decode("095ea7b3").unwrap();
        assert!(
            r.decode_call(addr(1), &approve, &ctx()).is_none(),
            "a filtered selector must not reappear as a raw row - that would defeat the filter"
        );
    }

    #[test]
    fn the_hash_changes_with_the_decode_surface_and_not_with_ordering() {
        let abi: JsonAbi = serde_json::from_str(ERC20).unwrap();
        let one = CallRegistry::build(
            vec![
                ("tok".into(), addr(1), abi.clone()),
                ("two".into(), addr(2), abi.clone()),
            ],
            &Extract::default(),
        )
        .unwrap();
        let other = CallRegistry::build(
            vec![
                ("two".into(), addr(2), abi.clone()),
                ("tok".into(), addr(1), abi.clone()),
            ],
            &Extract::default(),
        )
        .unwrap();
        assert_eq!(
            one.hash(),
            other.hash(),
            "declaration order is not identity"
        );

        let narrowed = CallRegistry::build(
            vec![("tok".into(), addr(1), abi)],
            &Extract {
                traces: true,
                selectors: vec!["0xa9059cbb".into()],
                ..Default::default()
            },
        )
        .unwrap();
        assert_ne!(
            one.hash(),
            narrowed.hash(),
            "a different decode surface must be a different decode identity"
        );
    }

    /// A named tuple argument is stored as an object and an unnamed one as an array, so they hash
    /// apart (#1364), while a tuple-free surface keeps its line format. That literal is the v3.6.1
    /// line format, `f|alias|contract|selector|signature|name:type,...`, plus the `implicit|tx_from`
    /// line every call table's sender column added, hashed by hand.
    #[test]
    fn a_named_tuple_argument_moves_the_hash_and_nothing_else_does() {
        assert_eq!(
            hex::encode(reg(ERC20, &Extract::default()).hash()),
            "9a93fa9e219f876f560e24aae134240413cedd1b9aff67229d46cda8b88f39fd"
        );
        let place = |a: &str, b: &str| {
            format!(
                r#"[{{"type":"function","name":"place","inputs":[{{"name":"order","type":"tuple","components":[{{"name":"{a}","type":"uint256"}},{{"name":"{b}","type":"uint256"}}]}}],"outputs":[],"stateMutability":"nonpayable"}}]"#
            )
        };
        assert_ne!(
            reg(&place("y", "z"), &Extract::default()).hash(),
            reg(&place("", ""), &Extract::default()).hash()
        );
    }

    #[test]
    fn schema_declares_the_extraction_tables_only_when_asked() {
        let r = reg(ERC20, &Extract::default());
        let traces_only = r.schema(&Extract {
            traces: true,
            ..Default::default()
        });
        assert!(traces_only.iter().any(|t| t.table == RAW_CALLS_TABLE));
        assert!(!traces_only.iter().any(|t| t.table == STATE_DIFFS_TABLE));

        let both = r.schema(&Extract {
            traces: true,
            state: true,
            ..Default::default()
        });
        assert!(both.iter().any(|t| t.table == STATE_DIFFS_TABLE));

        // Every call table carries the selector, which is what a schema consumer keys on.
        let t = both
            .iter()
            .find(|t| t.table == "tok__call_transfer")
            .expect("the transfer call table");
        assert_eq!(t.selector, "0xa9059cbb");
        assert_eq!(t.kind, TableKind::Call);
        assert!(t.topic0.is_empty(), "a call has no topic0 to report");
    }
}
