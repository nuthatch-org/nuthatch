//! RFC-0023 tier 3 - **pinned-block `eth_call` as a host-side ingestion source.**
//!
//! Tiers 1-2 remove most `eth_call`s by *deriving* state from events (`recipes.rs`) and caching
//! immutable metadata (`metadata.rs`). This module handles what is left: the **irreducible residue** -
//! an oracle read, an ungoverned parameter, a view on a contract whose events we do not fully cover.
//!
//! Three properties make a call result safe to store, and all three come from pinning the block:
//!
//! 1. **Deterministic.** `result = f(code, storage, block, calldata)`. Re-executing the same
//!    declaration at the same block on another machine next year returns the same bytes. `latest`
//!    would break this, which is why [`crate::rpc::RpcClient::eth_call`] is documented as
//!    out-of-band-only and the data path uses `eth_call_at`.
//! 2. **Content-addressable** by `(chain, block, contract, calldata)` - see [`CallKey`]. The address
//!    *is* the identity, so two operators who run the same declaration over the same range produce
//!    byte-identical results and can share segments (tier 4) without trusting each other.
//! 3. **Host-run.** The host issues the call and hands components only *data*, so components stay
//!    zero-capability and pure and may still feed entity derivation.
//!
//! **What this is not:** a way to read `latest`. Nothing here ever touches the chain tip - a nest that
//! wants live state wants a different tool, and asking for it here would trade the determinism
//! non-negotiable for convenience.

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// The identity of one pinned call result: `(chain, block, contract, calldata)`.
///
/// Deliberately *not* including the nest, the declaration's name, or when it ran. Two different nests
/// on two different machines declaring the same read at the same block are asking an identical
/// question of an identical chain state, and must get an identical answer with an identical address -
/// that is what lets tier 4 share segments without a trust relationship. Folding a nest name in would
/// silently make every operator's results incompatible.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CallKey {
    pub chain_id: u64,
    pub block: u64,
    /// Lowercase `0x…` - normalised, because address casing is presentation and must not change the
    /// content address.
    pub contract: String,
    /// Lowercase `0x…` calldata, selector included.
    pub calldata: String,
}

impl CallKey {
    pub fn new(chain_id: u64, block: u64, contract: &str, calldata: &str) -> CallKey {
        CallKey {
            chain_id,
            block,
            contract: contract.to_ascii_lowercase(),
            calldata: calldata.to_ascii_lowercase(),
        }
    }

    /// The content address: SHA-256 over the four fields with an unambiguous separator.
    ///
    /// The separator matters. Concatenating the fields raw would let `(contract "0xab", calldata
    /// "cd")` and `(contract "0xabcd", calldata "")` hash identically - a collision between two
    /// genuinely different reads. A byte that cannot occur in lowercase hex removes the ambiguity.
    pub fn address(&self) -> String {
        let mut h = Sha256::new();
        h.update(self.chain_id.to_be_bytes());
        h.update(b"\x1f");
        h.update(self.block.to_be_bytes());
        h.update(b"\x1f");
        h.update(self.contract.as_bytes());
        h.update(b"\x1f");
        h.update(self.calldata.as_bytes());
        hex::encode(h.finalize())
    }
}

/// One resolved call: its identity, and what the chain answered.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CallResult {
    pub block: u64,
    pub contract: String,
    pub calldata: String,
    /// The raw return data, lowercase `0x…`. `None` means the call **reverted** at that block, which
    /// is a fact about chain state rather than an error - a getter often does not exist before the
    /// contract is initialised, and recording that honestly beats storing an empty string that reads
    /// like a zero.
    pub result: Option<String>,
    /// `CallKey::address()` - carried so a stored row is self-describing and can be re-verified
    /// without recomputing the key from context that may no longer be around.
    pub address: String,
}

/// A nest's declaration of an irreducible read (RFC-0023 §3: "a nest **declares** an irreducible
/// call... the host schedules the batched calls").
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CallDecl {
    /// Table name for the results, e.g. `oracle__latest_answer`.
    pub name: String,
    /// The contract to call, `0x…`. Empty when [`Self::contract_column`] names it instead.
    #[serde(default)]
    pub contract: String,
    /// Hex calldata including the 4-byte selector, for a **sampled** declaration. Fixed arguments
    /// only: with nothing to draw an argument from, a varying one could not be reproduced from the
    /// config alone.
    ///
    /// Empty when the declaration is **row-driven** (`on` + `signature`), where the calldata is built
    /// per source row instead. Exactly one of the two forms is required, checked in [`Self::validate`].
    #[serde(default)]
    pub calldata: String,
    /// RFC-0038 §3: the table whose rows drive this call, e.g. `factory__pool_created`.
    ///
    /// A subgraph mapping reads `c.balanceOf(event.params.to)` - the argument comes from the event.
    /// A declaration naming a source table can say the same thing while staying deterministic: the
    /// source rows are decoded events, the block is the row's own block, and `CallKey` does not care
    /// where the calldata came from. The config declares the *rule*; reproducible data supplies the
    /// arguments.
    ///
    /// Rows are taken from the window being processed, so the call fires as the row is produced -
    /// the same moment a subgraph handler would have made it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on: Option<String>,
    /// Alternative row-driven trigger across several event tables. A fixed getter is still
    /// fetched once per block, even when several of these tables fire in that block.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub on_any: Vec<String>,
    /// Require EIP-1898 hash pinning and canonicality. No block-number fallback.
    #[serde(default, skip_serializing_if = "is_false")]
    pub canonical: bool,
    /// The ABI signature to call, e.g. `balanceOf(address)`. Row-driven declarations only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signature: Option<String>,
    /// Arguments, positionally. `{column}` takes the value from the source row; anything else is a
    /// literal coerced to the signature's parameter type.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub args: Vec<String>,
    /// The contract to call, when it is a `{column}` of the source row rather than a fixed address -
    /// a factory's newly created child, say. Row-driven declarations only.
    ///
    /// `contract` stays the fixed-address form; this is the alternative, and exactly one applies.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub contract_column: Option<String>,
    /// Sample every `every` blocks. State reads are not free and most of them change slowly, so a
    /// declaration says how often it actually needs an answer rather than implying "every block".
    #[serde(default = "default_every")]
    pub every: u64,
    /// First block to sample from. Defaults to the nest's own start.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start: Option<u64>,
}

fn default_every() -> u64 {
    1000
}

fn is_false(value: &bool) -> bool {
    !value
}

impl CallDecl {
    /// True when this declaration is driven by the rows of a table rather than a block schedule.
    pub fn is_row_driven(&self) -> bool {
        self.on.is_some() || !self.on_any.is_empty()
    }

    pub fn matches_source(&self, table: &str) -> bool {
        self.on.as_deref() == Some(table) || self.on_any.iter().any(|name| name == table)
    }

    /// Validate a declaration at load time rather than at the first RPC round trip.
    ///
    /// Every one of these is a config error that would otherwise surface thousands of blocks into a
    /// backfill, as a wall of identical failures.
    pub fn validate(&self) -> Result<()> {
        if self.on.is_some() && !self.on_any.is_empty() {
            bail!("call `{}`: choose `on` or `on_any`, not both", self.name);
        }
        let mut seen = std::collections::HashSet::new();
        if self.on_any.len() > 128
            || self.on_any.iter().any(|s| {
                s.is_empty()
                    || !s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                    || !seen.insert(s)
            })
        {
            bail!(
                "call `{}`: `on_any` requires at most 128 distinct table identifiers",
                self.name
            );
        }
        if self.name.is_empty() {
            bail!("a [[calls]] declaration needs a `name` - it becomes the result table");
        }
        if !self
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        {
            bail!(
                "call `{}`: name must be [A-Za-z0-9_] - it is used as a table identifier",
                self.name
            );
        }

        // The two forms are exclusive. Accepting both would leave it ambiguous whether the call is
        // sampled or row-driven, and a config whose meaning depends on which field the reader
        // noticed first is worse than one that refuses.
        let sampled = !self.calldata.is_empty();
        match (sampled, self.is_row_driven()) {
            (false, false) => bail!(
                "call `{}`: needs either `calldata` (sampled every N blocks) or `on` + `signature` \
                 (one call per row of a table)",
                self.name
            ),
            (true, true) => bail!(
                "call `{}`: has both `calldata` and `on` - a declaration is either sampled or \
                 row-driven, never both. Drop whichever you did not mean.",
                self.name
            ),
            _ => {}
        }

        if self.is_row_driven() {
            self.validate_row_driven()?;
        } else {
            self.validate_sampled()?;
        }

        // `every = 0` would divide by zero when scheduling; it is also meaningless.
        if self.every == 0 {
            bail!("call `{}`: `every` must be at least 1", self.name);
        }
        Ok(())
    }

    fn validate_address(&self, what: &str, addr: &str) -> Result<()> {
        let c = addr.strip_prefix("0x").unwrap_or(addr);
        if c.len() != 40 || !c.chars().all(|ch| ch.is_ascii_hexdigit()) {
            bail!(
                "call `{}`: `{what}` must be a 20-byte 0x address, got {addr:?}",
                self.name
            );
        }
        Ok(())
    }

    fn validate_sampled(&self) -> Result<()> {
        if self.contract_column.is_some() {
            bail!(
                "call `{}`: `contract_column` names a column of a source table, so it needs `on` - a \
                 sampled call has no row to read it from",
                self.name
            );
        }
        if self.signature.is_some() || !self.args.is_empty() {
            bail!(
                "call `{}`: `signature`/`args` build calldata from a source row, so they need `on`. A \
                 sampled call carries its calldata whole.",
                self.name
            );
        }
        self.validate_address("contract", &self.contract)?;
        let d = self.calldata.strip_prefix("0x").unwrap_or(&self.calldata);
        if d.len() < 8 || !d.chars().all(|ch| ch.is_ascii_hexdigit()) {
            bail!(
                "call `{}`: `calldata` must be hex with at least a 4-byte selector, got {:?}",
                self.name,
                self.calldata
            );
        }
        if !d.len().is_multiple_of(2) {
            bail!(
                "call `{}`: `calldata` has an odd number of hex digits ({}) - it is not whole bytes",
                self.name,
                d.len()
            );
        }
        Ok(())
    }

    fn validate_row_driven(&self) -> Result<()> {
        let Some(sig) = self.signature.as_deref() else {
            bail!(
                "call `{}`: `on` names a source table, so `signature` must say what to call, e.g. \
                 `balanceOf(address)`",
                self.name
            );
        };
        // Parsed at load, so a malformed signature is one clear error rather than one per row.
        let f = alloy_json_abi::Function::parse(sig).map_err(|e| {
            anyhow::anyhow!(
                "call `{}`: `signature` {sig:?} is not a valid ABI signature: {e}",
                self.name
            )
        })?;
        if f.inputs.len() != self.args.len() {
            bail!(
                "call `{}`: `{sig}` takes {} argument(s) but `args` has {}",
                self.name,
                f.inputs.len(),
                self.args.len()
            );
        }
        match (self.contract.is_empty(), self.contract_column.is_some()) {
            (true, false) => bail!(
                "call `{}`: needs either `contract` (a fixed address) or `contract_column` (a column \
                 of `{}` holding one)",
                self.name,
                self.on.as_deref().unwrap_or("?")
            ),
            (false, true) => bail!(
                "call `{}`: has both `contract` and `contract_column` - the address comes from one \
                 place or the other",
                self.name
            ),
            (false, false) => self.validate_address("contract", &self.contract)?,
            _ => {}
        }
        // `every` is a block schedule and means nothing for a row-driven call; silently ignoring it
        // would let an operator believe they had throttled something they had not.
        if self.every != default_every() {
            bail!(
                "call `{}`: `every` schedules a sampled call by block and does nothing for a \
                 row-driven one - it fires once per row of `{}`. Remove it.",
                self.name,
                self.on.as_deref().unwrap_or("?")
            );
        }
        Ok(())
    }

    /// The blocks this declaration wants sampled within `[from, to]`.
    ///
    /// Anchored on absolute block numbers (`block % every == 0`), **not** counted from `from`. That is
    /// what makes a resumed or re-ranged backfill sample the *same* blocks as a fresh one - and
    /// therefore produce the same content addresses. Anchoring on `from` would give two operators
    /// different sample sets for the same declaration, which defeats tier 4 sharing entirely.
    pub fn blocks_in(&self, from: u64, to: u64) -> Vec<u64> {
        // A row-driven call's blocks come from its rows. A schedule here only buys headers.
        if self.is_row_driven() {
            return Vec::new();
        }
        let start = self.start.unwrap_or(0).max(from);
        if start > to {
            return Vec::new();
        }
        let first = start.div_ceil(self.every) * self.every;
        (first..=to).step_by(self.every as usize).collect()
    }
}

/// Resolve a batch of declarations at one pinned block.
///
/// Returns one [`CallResult`] per declaration, positionally. Declarations are batched into a single
/// JSON-RPC request because they share a block - the same batched-boundary discipline log extraction
/// uses.
pub async fn resolve_at(
    rpc: &crate::rpc::RpcClient,
    chain_id: u64,
    decls: &[CallDecl],
    block: u64,
) -> Result<Vec<CallResult>> {
    if decls.iter().any(|decl| decl.canonical) {
        bail!("canonical calls require a source block hash; use resolve_pairs_at_hash");
    }
    let pairs: Vec<(String, String)> = decls
        .iter()
        .map(|d| {
            (
                d.contract.to_ascii_lowercase(),
                d.calldata.to_ascii_lowercase(),
            )
        })
        .collect();
    resolve_pairs_at(rpc, chain_id, &pairs, block).await
}

/// Resolve a batch of `(contract, calldata)` pairs at one pinned block.
///
/// The form a **row-driven** declaration needs (RFC-0038 §3): its calldata is built per source row,
/// so there is no `CallDecl` to hand over - only the question. [`resolve_at`] is this with the pairs
/// read off a sampled declaration instead.
pub async fn resolve_pairs_at(
    rpc: &crate::rpc::RpcClient,
    chain_id: u64,
    pairs: &[(String, String)],
    block: u64,
) -> Result<Vec<CallResult>> {
    let raw = rpc
        .eth_call_batch_at(pairs, block)
        .await
        .with_context(|| format!("pinned eth_call batch at block {block}"))?;
    Ok(pairs
        .iter()
        .zip(raw)
        .map(|((contract, calldata), r)| {
            let key = CallKey::new(chain_id, block, contract, calldata);
            CallResult {
                block,
                contract: key.contract.clone(),
                calldata: key.calldata.clone(),
                result: r.map(|s| s.to_ascii_lowercase()),
                address: key.address(),
            }
        })
        .collect())
}

/// Hash-pinned counterpart; the content address includes the fork identity.
pub async fn resolve_pairs_at_hash(
    rpc: &crate::rpc::RpcClient,
    chain_id: u64,
    pairs: &[(String, String)],
    block: u64,
    hash: &str,
) -> Result<Vec<CallResult>> {
    let raw = rpc.eth_call_batch_at_hash(pairs, hash).await?;
    Ok(pairs
        .iter()
        .zip(raw)
        .map(|((contract, calldata), result)| {
            let key = CallKey::new(chain_id, block, contract, calldata);
            let mut digest = Sha256::new();
            digest.update(b"nuthatch-canonical-call-v1\0");
            digest.update(key.address().as_bytes());
            digest.update(hash.to_ascii_lowercase().as_bytes());
            CallResult {
                block,
                contract: key.contract,
                calldata: key.calldata,
                result: result.map(|r| r.to_ascii_lowercase()),
                address: hex::encode(digest.finalize()),
            }
        })
        .collect())
}

/// The table shape a `[[calls]]` declaration produces, in the same form `/tables`, MCP, `llms.txt`
/// and `schema.json` already consume.
///
/// One table per declaration, named by `CallDecl::name`. The implicit `address` column carries the
/// **contract called**, so the content address needs its own name (`content_address`) rather than
/// colliding with it.
pub fn schema(decls: &[CallDecl], timestamps: bool) -> Vec<crate::registry::TableSchema> {
    use crate::registry::{ColumnSchema, TableKind, TableSchema};
    decls
        .iter()
        .map(|d| {
            let mut columns = crate::registry::implicit_columns(timestamps);
            let own = |name: &str, sol: &str, storage: &str| ColumnSchema {
                name: name.to_string(),
                sol_type: sol.to_string(),
                storage: storage.to_string(),
                indexed: false,
                components: Vec::new(),
            };
            columns.push(own("calldata", "bytes", "bytes"));
            columns.push(own("result", "bytes", "bytes"));
            // `Value` has no null, so an empty `result` would be indistinguishable from a call that
            // genuinely returned no bytes. A revert is a fact about chain state - a getter often does
            // not exist before the contract is initialised - and it deserves a column rather than an
            // ambiguity.
            columns.push(own("reverted", "bool", "bool"));
            // `hash32`, not `fixed_bytes`: `Value::Word32` renders a 32-byte value as a *decimal
            // integer*, which turned a content address into a 78-digit number. It is a hash and has
            // to read as one.
            columns.push(own("content_address", "bytes32", "hash32"));
            TableSchema {
                table: d.name.clone(),
                alias: d.name.clone(),
                kind: TableKind::Call,
                event: String::new(),
                topic0: String::new(),
                function: String::new(),
                selector: format!(
                    "0x{}",
                    &d.calldata.trim_start_matches("0x")
                        [..8.min(d.calldata.trim_start_matches("0x").len())]
                ),
                columns,
            }
        })
        .collect()
}

/// Turn a stored [`Value`] into the ABI value a signature's parameter expects.
///
/// The interesting case is [`Value::Hash32`], which is refused. An *indexed* dynamic parameter
/// (`string`, `bytes`, an array) is stored in the log topic as `keccak(value)`, not the value - so
/// the original is not recoverable and cannot be passed on to a call. Encoding the hash instead would
/// produce a well-formed call asking a question nobody meant, and the answer would look like data.
fn dynsol_from_value(
    v: &crate::registry::Value,
    want: &alloy_dyn_abi::DynSolType,
    call: &str,
    col: &str,
) -> Result<alloy_dyn_abi::DynSolValue> {
    use crate::registry::Value as V;
    use alloy_dyn_abi::{DynSolType, DynSolValue};
    use alloy_primitives::{Address, I256, U256};

    let uint = |bytes: &[u8], bits: usize| DynSolValue::Uint(U256::from_be_slice(bytes), bits);
    Ok(match (v, want) {
        (V::Address(a), DynSolType::Address) => DynSolValue::Address(Address::from(*a)),
        (V::Bool(b), DynSolType::Bool) => DynSolValue::Bool(*b),
        (V::U64(n), DynSolType::Uint(bits)) => DynSolValue::Uint(U256::from(*n), *bits),
        (V::Word16(w), DynSolType::Uint(bits)) => uint(w, *bits),
        (V::Word32(w), DynSolType::Uint(bits)) => uint(w, *bits),
        (V::I64(n), DynSolType::Int(bits)) => DynSolValue::Int(I256::try_from(*n)?, *bits),
        (V::IWord16(w), DynSolType::Int(bits)) => {
            DynSolValue::Int(I256::from_be_bytes::<32>(sign_extend(w)), *bits)
        }
        (V::IWord32(w), DynSolType::Int(bits)) => {
            DynSolValue::Int(I256::from_be_bytes::<32>(*w), *bits)
        }
        (V::Bytes(b), DynSolType::FixedBytes(n)) => {
            let mut w = [0u8; 32];
            let take = (*n).min(b.len());
            w[..take].copy_from_slice(&b[..take]);
            DynSolValue::FixedBytes(w.into(), *n)
        }
        (V::Bytes(b), DynSolType::Bytes) => DynSolValue::Bytes(b.clone()),
        (V::Str(t), DynSolType::String) => DynSolValue::String(t.clone()),
        (V::Hash32(_), _) => bail!(
            "call `{call}`: column `{col}` is an *indexed* dynamic parameter, so the log holds \
             `keccak(value)` rather than the value. The original cannot be recovered, so it cannot be \
             passed to `{want:?}`. Use a non-indexed column, or index the contract that emits the \
             value unhashed."
        ),
        (other, _) => bail!(
            "call `{call}`: column `{col}` holds {other:?}, which does not fit the parameter type \
             `{want:?}`"
        ),
    })
}

/// Sign-extend a 16-byte two's-complement integer into 32 bytes.
fn sign_extend(w: &[u8; 16]) -> [u8; 32] {
    let fill = if w[0] & 0x80 != 0 { 0xff } else { 0x00 };
    let mut out = [fill; 32];
    out[16..].copy_from_slice(w);
    out
}

impl CallDecl {
    /// The `(contract, calldata)` this row-driven declaration asks for, given one source row.
    ///
    /// Deterministic by construction: the row is decoded output, the signature and argument
    /// references are config, and nothing here reads a clock or the chain.
    pub fn resolve_for_row(&self, row: &crate::registry::DecodedRow) -> Result<(String, String)> {
        use alloy_dyn_abi::{DynSolType, DynSolValue};

        let col = |name: &str| -> Result<&crate::registry::Value> {
            row.params
                .iter()
                .find(|(k, _)| k == name)
                .map(|(_, v)| v)
                .with_context(|| {
                    format!(
                        "call `{}`: `{}` has no column `{name}` - columns are {:?}",
                        self.name,
                        self.on.as_deref().unwrap_or("?"),
                        row.params
                            .iter()
                            .map(|(k, _)| k.as_str())
                            .collect::<Vec<_>>()
                    )
                })
        };

        let contract = match self.contract_column.as_deref() {
            None => self.contract.to_ascii_lowercase(),
            Some(c) => match col(column_ref(c).unwrap_or(c))? {
                crate::registry::Value::Address(a) => format!("0x{}", hex::encode(a)),
                other => bail!(
                    "call `{}`: `contract_column` points at `{c}`, which holds {other:?} rather than \
                     an address",
                    self.name
                ),
            },
        };

        let sig = self.signature.as_deref().unwrap_or_default();
        let f = alloy_json_abi::Function::parse(sig)
            .map_err(|e| anyhow::anyhow!("call `{}`: bad signature {sig:?}: {e}", self.name))?;
        let mut values = Vec::with_capacity(self.args.len());
        for (arg, input) in self.args.iter().zip(f.inputs.iter()) {
            let ty = DynSolType::parse(&input.ty).map_err(|e| {
                anyhow::anyhow!(
                    "call `{}`: unsupported parameter type {:?}: {e}",
                    self.name,
                    input.ty
                )
            })?;
            values.push(match column_ref(arg) {
                Some(name) => dynsol_from_value(col(name)?, &ty, &self.name, name)?,
                // A literal: `getPool(tokenA, tokenB, 3000)` has a constant fee tier.
                None => ty.coerce_str(arg).map_err(|e| {
                    anyhow::anyhow!(
                        "call `{}`: argument {arg:?} is neither a `{{column}}` reference nor a valid \
                         `{}` literal: {e}",
                        self.name,
                        input.ty
                    )
                })?,
            });
        }
        let mut data = f.selector().to_vec();
        data.extend(DynSolValue::Tuple(values).abi_encode_params());
        Ok((contract, format!("0x{}", hex::encode(data))))
    }
}

/// `"{pool}"` → `Some("pool")`; anything else is a literal.
fn column_ref(s: &str) -> Option<&str> {
    s.strip_prefix('{')?.strip_suffix('}')
}

/// A stable hash over the declared calls, folded into a nest's decode identity.
///
/// Two nests that differ only in what they *read* are different decode versions, exactly as two that
/// differ only in what they extract are (RFC-0014). Without this they would share a decode hash, and
/// segment reuse would happily serve one nest's rows to the other.
pub fn decl_hash(decls: &[CallDecl]) -> [u8; 32] {
    hash_decls(decls, true)
}

/// [`decl_hash`] as identity formula 2 computed it, blind to what a row-driven call reads, so a store
/// recorded under it can be adopted once (#2697).
pub fn decl_hash_formula_2(decls: &[CallDecl]) -> [u8; 32] {
    hash_decls(decls, false)
}

fn hash_decls(decls: &[CallDecl], reads: bool) -> [u8; 32] {
    let mut h = Sha256::new();
    for d in decls {
        // Field-separated for the same reason `CallKey::address` separates: concatenating raw would
        // let two different declarations hash identically.
        h.update(d.name.as_bytes());
        h.update(b"\x1f");
        h.update(d.contract.to_ascii_lowercase().as_bytes());
        h.update(b"\x1f");
        h.update(d.calldata.to_ascii_lowercase().as_bytes());
        h.update(b"\x1f");
        h.update(d.every.to_be_bytes());
        h.update(b"\x1f");
        h.update(d.start.unwrap_or(0).to_be_bytes());
        if reads {
            // Only when set, so a declaration naming none of these keeps its identity. Length
            // prefixed because an argument is arbitrary text and may contain any separator.
            let canonical = d.canonical.then(|| "1".to_string());
            let fields: [(&str, &[String]); 6] = [
                ("on", d.on.as_slice()),
                ("on_any", &d.on_any),
                ("signature", d.signature.as_slice()),
                ("args", &d.args),
                ("contract_column", d.contract_column.as_slice()),
                ("canonical", canonical.as_slice()),
            ];
            for (tag, values) in fields.iter().filter(|(_, v)| !v.is_empty()) {
                h.update(b"\x1d");
                h.update(tag.as_bytes());
                for v in values.iter() {
                    h.update(b"\x1f");
                    h.update((v.len() as u64).to_be_bytes());
                    h.update(v.as_bytes());
                }
            }
        }
        h.update(b"\x1e");
    }
    h.finalize().into()
}

impl CallResult {
    /// Turn a resolved call into a stored row, so it flows through the same store, seal and query
    /// path every other table uses.
    ///
    /// `slot` is this result's position within its block, assigned in a deterministic order
    /// (declarations in config order; within a row-driven declaration, its source rows by
    /// `log_index`). It fixes the row's `log_index` inside the reserved band and therefore its key,
    /// so two operators running the same nest produce the same keys, not merely the same content
    /// addresses.
    ///
    /// There is no transaction behind a pinned read, so `tx_hash` is empty rather than borrowing the
    /// block hash - a block hash sitting in a `tx_hash` column is a lie that reads like data.
    pub fn to_row(
        &self,
        table: &str,
        slot: usize,
        block_hash: &str,
        block_timestamp: u64,
        timestamps: bool,
    ) -> crate::registry::DecodedRow {
        use crate::registry::Value;
        let bytes =
            |h: &str| Value::Bytes(hex::decode(h.trim_start_matches("0x")).unwrap_or_default());
        let mut addr = [0u8; 20];
        if let Ok(b) = hex::decode(self.contract.trim_start_matches("0x")) {
            if b.len() == 20 {
                addr.copy_from_slice(&b);
            }
        }
        let mut content = [0u8; 32];
        if let Ok(b) = hex::decode(&self.address) {
            if b.len() == 32 {
                content.copy_from_slice(&b);
            }
        }
        crate::registry::DecodedRow {
            table: table.to_string(),
            params: vec![
                ("calldata".to_string(), bytes(&self.calldata)),
                (
                    "result".to_string(),
                    bytes(self.result.as_deref().unwrap_or("0x")),
                ),
                ("reverted".to_string(), Value::Bool(self.result.is_none())),
                ("content_address".to_string(), Value::Hash32(content)),
            ],
            block_number: self.block,
            block_hash: block_hash.to_string(),
            block_timestamp,
            timestamps,
            log_index: crate::registry::CALL_ROW_LOG_INDEX_BASE + slot as u64,
            tx_hash: String::new(),
            address: format!("0x{}", hex::encode(addr)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decl(name: &str, every: u64) -> CallDecl {
        CallDecl {
            name: name.into(),
            contract: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".into(),
            contract_column: None,
            calldata: "0x18160ddd".into(),
            every,
            start: None,
            on: None,
            on_any: Vec::new(),
            canonical: false,
            signature: None,
            args: Vec::new(),
        }
    }

    /// **The RFC-0023 acceptance test**: the same declared call at the same block re-executes to a
    /// byte-identical *address*, across runs and machines.
    ///
    /// Address casing is the case worth pinning down, because it is presentation rather than identity:
    /// two operators who wrote the same address differently must still produce the same segment, or
    /// tier 4 sharing silently degrades into everyone re-running everything.
    #[test]
    fn the_content_address_is_stable_across_casing_and_runs() {
        let a = CallKey::new(
            1,
            19_000_000,
            "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
            "0x18160DDD",
        );
        let b = CallKey::new(
            1,
            19_000_000,
            "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
            "0x18160ddd",
        );
        assert_eq!(
            a.address(),
            b.address(),
            "casing is presentation, not identity"
        );
        assert_eq!(a.address(), a.address(), "and it is stable within a run");
        assert_eq!(a.address().len(), 64);
    }

    /// Every field is part of the identity. If any of them could be changed without changing the
    /// address, two different reads would collide into one stored answer.
    #[test]
    fn every_field_changes_the_address() {
        let base = CallKey::new(1, 100, "0xaa", "0xbb");
        let variants = [
            CallKey::new(2, 100, "0xaa", "0xbb"),
            CallKey::new(1, 101, "0xaa", "0xbb"),
            CallKey::new(1, 100, "0xac", "0xbb"),
            CallKey::new(1, 100, "0xaa", "0xbc"),
        ];
        for v in &variants {
            assert_ne!(
                base.address(),
                v.address(),
                "{v:?} must not collide with the base"
            );
        }
    }

    #[tokio::test]
    async fn canonical_calls_pin_the_wire_request_and_content_identity_to_the_hash() {
        use axum::{routing::post, Json, Router};
        use serde_json::{json, Value};
        let app = Router::new().route("/", post(|Json(batch): Json<Value>| async move {
            Json(Value::Array(batch.as_array().unwrap().iter().map(|request| {
                let selector = &request["params"][1];
                assert_eq!(selector["requireCanonical"], true);
                assert!(selector["blockHash"].as_str().unwrap().starts_with("0x"));
                assert!(selector.get("blockNumber").is_none());
                if selector["blockHash"].as_str().unwrap().ends_with('c') {
                    json!({"jsonrpc":"2.0","id":request["id"],"error":{"code":-32000,"message":"block is not canonical"}})
                } else {
                    json!({"jsonrpc":"2.0","id":request["id"],"result":"0x0012"})
                }
            }).collect()))
        }));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        let rpc = crate::rpc::RpcClient::new(vec![format!("http://{address}")]).unwrap();
        let pairs = vec![(
            "0x1111111111111111111111111111111111111111".into(),
            "0x12345678".into(),
        )];
        let a = format!("0x{}", "a".repeat(64));
        let b = format!("0x{}", "b".repeat(64));
        let first = resolve_pairs_at_hash(&rpc, 42161, &pairs, 100, &a)
            .await
            .unwrap();
        let repeated = resolve_pairs_at_hash(
            &rpc,
            42161,
            &pairs,
            100,
            &a.to_uppercase().replacen("0X", "0x", 1),
        )
        .await
        .unwrap();
        let fork = resolve_pairs_at_hash(&rpc, 42161, &pairs, 100, &b)
            .await
            .unwrap();
        assert_eq!(first, repeated);
        assert_eq!(first[0].result, fork[0].result);
        assert_ne!(first[0].address, fork[0].address);
        let error =
            resolve_pairs_at_hash(&rpc, 42161, &pairs, 100, &format!("0x{}", "c".repeat(64)))
                .await
                .unwrap_err();
        assert!(error.to_string().contains("not canonical"));
        assert!(resolve_pairs_at_hash(&rpc, 42161, &pairs, 100, "0xbad")
            .await
            .is_err());
        server.abort();
    }

    /// The separator earns its keep: without it, `("0xab","cd")` and `("0xabcd","")` would hash the
    /// same, silently merging two genuinely different reads.
    #[test]
    fn field_boundaries_cannot_be_smeared() {
        let a = CallKey::new(1, 1, "0xab", "cd");
        let b = CallKey::new(1, 1, "0xabcd", "");
        assert_ne!(a.address(), b.address());
    }

    /// A row-driven call takes its blocks from the rows that fire it. A periodic schedule on top
    /// only makes every window fetch headers for blocks nothing reads.
    #[test]
    fn a_row_driven_call_schedules_no_periodic_blocks() {
        let d = row_driven("clock", "blockNum()", &[]);
        assert!(d.blocks_in(0, 10_000).is_empty());
    }

    #[test]
    fn an_on_any_call_schedules_no_periodic_blocks() {
        let mut d = row_driven("clock", "blockNum()", &[]);
        d.on = None;
        d.on_any = vec!["staking__created".into(), "service__closed".into()];
        assert!(d.blocks_in(0, 10_000).is_empty());
    }

    /// Sampling is anchored on absolute block numbers, so a resumed backfill hits the same blocks as a
    /// fresh one. Anchoring on the range start would give two operators different sample sets for the
    /// same declaration - and therefore different content addresses for the same question.
    #[test]
    fn sampling_is_anchored_on_absolute_blocks_not_on_the_range() {
        let d = decl("x", 1000);
        let fresh = d.blocks_in(0, 5000);
        let resumed = d.blocks_in(2500, 5000);
        assert_eq!(fresh, vec![0, 1000, 2000, 3000, 4000, 5000]);
        assert_eq!(resumed, vec![3000, 4000, 5000]);
        assert!(
            resumed.iter().all(|b| fresh.contains(b)),
            "a resumed run must sample a subset of what a fresh run would - got {resumed:?}"
        );
    }

    #[test]
    fn a_declaration_is_validated_before_it_costs_a_round_trip() {
        assert!(decl("ok", 1000).validate().is_ok());

        let mut d = decl("bad", 1000);
        d.contract = "0x1234".into();
        assert!(d.validate().unwrap_err().to_string().contains("20-byte"));

        let mut d = decl("bad", 1000);
        d.calldata = "0x181".into(); // odd digits, and shorter than a selector
        assert!(d.validate().is_err());

        let mut d = decl("bad", 1000);
        d.calldata = "0x18160ddd0".into(); // odd number of hex digits
        assert!(d
            .validate()
            .unwrap_err()
            .to_string()
            .contains("whole bytes"));

        // `every = 0` would divide by zero in scheduling.
        assert!(decl("bad", 0).validate().is_err());

        let mut d = decl("bad", 1000);
        d.name = "not a table".into();
        assert!(d.validate().is_err());
    }

    /// A revert is recorded as `None`, not as an empty string. An empty string reads like a zero-length
    /// return value; `None` says "this call did not answer at this block", which is what a getter on a
    /// not-yet-initialised contract genuinely does.
    #[test]
    fn a_revert_is_distinguishable_from_an_empty_return() {
        let reverted = CallResult {
            block: 1,
            contract: "0xaa".into(),
            calldata: "0xbb".into(),
            result: None,
            address: "x".into(),
        };
        let empty = CallResult {
            result: Some("0x".into()),
            ..reverted.clone()
        };
        assert_ne!(reverted, empty);
        let j = serde_json::to_string(&reverted).unwrap();
        assert!(
            j.contains("\"result\":null"),
            "a revert must serialise as null: {j}"
        );
    }

    fn result_for(name: &str, block: u64, result: Option<&str>) -> CallResult {
        let d = decl(name, 100);
        let key = CallKey::new(1, block, &d.contract, &d.calldata);
        CallResult {
            block,
            contract: key.contract.clone(),
            calldata: key.calldata.clone(),
            result: result.map(str::to_string),
            address: key.address(),
        }
    }

    fn row_with(params: Vec<(&str, crate::registry::Value)>) -> crate::registry::DecodedRow {
        crate::registry::DecodedRow {
            table: "tok__transfer".into(),
            params: params
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
            block_number: 42,
            block_hash: "0xbh".into(),
            block_timestamp: 7,
            timestamps: true,
            log_index: 3,
            tx_hash: "0xtx".into(),
            address: "0x1111111111111111111111111111111111111111".into(),
        }
    }

    fn row_driven(name: &str, sig: &str, args: &[&str]) -> CallDecl {
        CallDecl {
            name: name.into(),
            contract: "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48".into(),
            contract_column: None,
            calldata: String::new(),
            every: default_every(),
            start: None,
            on: Some("tok__transfer".into()),
            on_any: Vec::new(),
            canonical: false,
            signature: Some(sig.into()),
            args: args.iter().map(|a| a.to_string()).collect(),
        }
    }

    #[test]
    fn multi_source_triggers_are_explicit_and_do_not_change_existing_declaration_identity() {
        let old = row_driven("epoch", "currentEpoch()", &[]);
        let encoded = serde_json::to_value(&old).unwrap();
        assert!(encoded.get("on_any").is_none());
        let mut multi = old.clone();
        multi.on = None;
        multi.on_any = vec!["staking__created".into(), "service__closed".into()];
        multi.validate().unwrap();
        assert!(multi.matches_source("staking__created"));
        assert!(multi.matches_source("service__closed"));
        assert!(!multi.matches_source("other__closed"));
        multi.on = Some("other".into());
        assert!(multi.validate().is_err());
        multi.on = None;
        multi.on_any.push("service__closed".into());
        assert!(multi.validate().is_err());
    }

    /// #2697: every field that changes what a call reads moves the identity. A sampled declaration
    /// keeps the hash it had before, written against that layout so an unconditional append fails.
    #[test]
    fn what_a_row_driven_call_reads_moves_its_identity() {
        let sampled = decl("supply", 100);
        let mut h = Sha256::new();
        h.update(b"supply\x1f0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48\x1f0x18160ddd\x1f");
        h.update(100u64.to_be_bytes());
        h.update(b"\x1f");
        h.update(0u64.to_be_bytes());
        h.update(b"\x1e");
        let before: [u8; 32] = h.finalize().into();
        assert_eq!(decl_hash(std::slice::from_ref(&sampled)), before);
        assert_eq!(decl_hash_formula_2(std::slice::from_ref(&sampled)), before);

        let from = row_driven("bal", "balanceOf(address)", &["{from}"]);
        let edits: [fn(&mut CallDecl); 7] = [
            |d| d.args = vec!["{to}".into()],
            |d| d.signature = Some("balanceOf(address,uint256)".into()),
            |d| d.on = Some("tok__approval".into()),
            |d| {
                d.on = None;
                d.on_any = vec!["tok__transfer".into()];
            },
            |d| d.contract_column = Some("token".into()),
            |d| d.canonical = true,
            |d| d.args = vec!["{fr".into(), "om}".into()],
        ];
        let mut ids = vec![decl_hash(std::slice::from_ref(&from))];
        for edit in edits {
            let mut d = from.clone();
            edit(&mut d);
            assert_eq!(
                decl_hash_formula_2(std::slice::from_ref(&d)),
                decl_hash_formula_2(std::slice::from_ref(&from)),
                "premise: formula 2 could not see {d:?}"
            );
            ids.push(decl_hash(&[d]));
        }
        for i in 0..ids.len() {
            for j in i + 1..ids.len() {
                assert_ne!(ids[i], ids[j], "edits {i} and {j} must be different nests");
            }
        }
    }

    /// The claim RFC-0038 exists for: a declaration can name an event's parameters, which is what a
    /// subgraph mapping does (`c.balanceOf(event.params.to)`).
    ///
    /// The selector is asserted against the **published** `balanceOf(address)` selector rather than
    /// against our own encoder, so this is evidence about ABI encoding and not a tautology.
    #[test]
    fn a_row_driven_call_encodes_the_event_parameter_as_its_argument() {
        let mut to = [0u8; 20];
        to[19] = 0xbe;
        let row = row_with(vec![("to", crate::registry::Value::Address(to))]);
        let (contract, calldata) = row_driven("bal", "balanceOf(address)", &["{to}"])
            .resolve_for_row(&row)
            .unwrap();

        assert_eq!(contract, "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48");
        assert!(
            calldata.starts_with("0x70a08231"),
            "balanceOf(address) is selector 0x70a08231, got {calldata}"
        );
        assert!(
            calldata.ends_with(&format!("{:0>64}", "be")),
            "the address argument must be left-padded to 32 bytes: {calldata}"
        );
        assert_eq!(
            calldata.len(),
            2 + 8 + 64,
            "selector + one word: {calldata}"
        );
    }

    /// An indexed dynamic parameter holds `keccak(value)`, not the value, so it cannot be passed on.
    ///
    /// Encoding the hash would produce a well-formed call asking a question nobody meant, and the
    /// answer would look like data. Refusing is the only honest option.
    #[test]
    fn an_indexed_dynamic_parameter_cannot_be_used_as_a_call_argument() {
        let row = row_with(vec![("name", crate::registry::Value::Hash32([9u8; 32]))]);
        let err = row_driven("x", "resolve(string)", &["{name}"])
            .resolve_for_row(&row)
            .unwrap_err()
            .to_string();
        assert!(err.contains("keccak"), "say why it cannot work: {err}");
        assert!(
            err.contains("non-indexed"),
            "and say what to do instead: {err}"
        );
    }

    /// Mixed arguments: a column reference beside a constant, which is what a pool lookup needs.
    #[test]
    fn a_literal_argument_sits_beside_a_column_reference() {
        let mut a = [0u8; 20];
        a[19] = 1;
        let row = row_with(vec![("token", crate::registry::Value::Address(a))]);
        let (_, calldata) = row_driven(
            "pool",
            "getPool(address,address,uint24)",
            &[
                "{token}",
                "0x0000000000000000000000000000000000000002",
                "3000",
            ],
        )
        .resolve_for_row(&row)
        .unwrap();
        assert_eq!(
            calldata.len(),
            2 + 8 + 64 * 3,
            "three words of arguments: {calldata}"
        );
        assert!(
            calldata.ends_with(&format!("{:0>64x}", 3000u32)),
            "the literal fee tier must encode as the last word: {calldata}"
        );
    }

    /// A factory's child address comes from the row, not the config - RFC-0009's shape, read back.
    #[test]
    fn the_contract_itself_can_come_from_the_row() {
        let mut child = [0u8; 20];
        child[0] = 0xab;
        let row = row_with(vec![("pool", crate::registry::Value::Address(child))]);
        let mut d = row_driven("t0", "token0()", &[]);
        d.contract = String::new();
        d.contract_column = Some("{pool}".into());
        let (contract, calldata) = d.resolve_for_row(&row).unwrap();
        assert_eq!(contract, format!("0x{}", hex::encode(child)));
        assert_eq!(
            calldata.len(),
            2 + 8,
            "token0() takes no arguments: {calldata}"
        );
    }

    /// The two declaration forms are exclusive, and each half-declared shape names what is missing.
    #[test]
    fn a_declaration_is_either_sampled_or_row_driven_never_both() {
        let mut both = row_driven("b", "token0()", &[]);
        both.calldata = "0x18160ddd".into();
        assert!(
            both.validate()
                .unwrap_err()
                .to_string()
                .contains("never both"),
            "both forms must be refused"
        );

        let mut no_sig = row_driven("n", "token0()", &[]);
        no_sig.signature = None;
        assert!(no_sig
            .validate()
            .unwrap_err()
            .to_string()
            .contains("signature"));

        let wrong_arity = row_driven("a", "balanceOf(address)", &[]);
        assert!(
            wrong_arity
                .validate()
                .unwrap_err()
                .to_string()
                .contains("takes 1 argument"),
            "an arity mismatch is a config error, not a per-row surprise"
        );

        // `every` is a block schedule; silently ignoring it would let an operator believe they had
        // throttled something they had not.
        let mut throttled = row_driven("e", "token0()", &[]);
        throttled.every = 500;
        assert!(throttled
            .validate()
            .unwrap_err()
            .to_string()
            .contains("Remove it"));

        // And the valid shape still validates, or every assertion above would pass against a
        // declaration that refuses everything.
        assert!(row_driven("ok", "balanceOf(address)", &["{to}"])
            .validate()
            .is_ok());
    }

    /// A revert must stay distinguishable from a call that genuinely returned no bytes.
    ///
    /// `Value` has no null, so both would serialise as empty `result` bytes. Losing that distinction
    /// would report "this getter does not exist yet" and "this function returns nothing" as the same
    /// fact, which is exactly the silent-wrong-answer shape this project cares most about.
    #[test]
    fn a_revert_is_not_an_empty_return() {
        let reverted = result_for("t", 100, None).to_row("t", 0, "0xbh", 7, true);
        let empty = result_for("t", 100, Some("0x")).to_row("t", 0, "0xbh", 7, true);

        let flag = |r: &crate::registry::DecodedRow| {
            r.params
                .iter()
                .find(|(k, _)| k == "reverted")
                .map(|(_, v)| v.to_json().to_string())
                .unwrap()
        };
        assert_ne!(
            flag(&reverted),
            flag(&empty),
            "a revert and an empty return must not read identically"
        );
        assert_eq!(flag(&reverted), "true");
        assert_eq!(flag(&empty), "false");
    }

    /// Two declarations resolved at one block must not land on the same key.
    ///
    /// This is #642's lesson applied before the fact: rows that descend from no log share a key space
    /// with those that do, and several call results can occur in a single block.
    #[test]
    fn two_declarations_at_one_block_get_distinct_keys() {
        let a = result_for("a", 500, Some("0x01")).to_row("a", 0, "0xbh", 7, true);
        let b = result_for("b", 500, Some("0x02")).to_row("b", 1, "0xbh", 7, true);
        assert_ne!(
            crate::store::Store::entity_key(a.block_number, a.log_index),
            crate::store::Store::entity_key(b.block_number, b.log_index),
            "one declaration must not overwrite another at the same block"
        );
        // And both sit inside the reserved band, clear of any real log.
        for r in [&a, &b] {
            assert!(
                r.log_index >= crate::registry::CALL_ROW_LOG_INDEX_BASE
                    && r.log_index < crate::registry::BLOCK_ROW_LOG_INDEX,
                "call rows belong in the reserved band, below the block row: {}",
                r.log_index
            );
        }
    }

    /// The stored row carries the content address, so a row can be re-verified without recomputing
    /// the key from context that may no longer exist.
    #[test]
    fn the_row_carries_its_content_address_and_the_contract_it_called() {
        let res = result_for("t", 900, Some("0xdeadbeef"));
        let row = res.to_row("t", 0, "0xbh", 7, true);
        let col = |n: &str| {
            row.params
                .iter()
                .find(|(k, _)| k == n)
                .map(|(_, v)| v.to_json().to_string())
                .unwrap()
        };
        assert!(
            col("content_address").contains(&res.address),
            "content_address must be CallKey::address(): {}",
            col("content_address")
        );
        assert_eq!(
            row.address, res.contract,
            "the implicit `address` column is the contract that was called"
        );
        assert!(
            row.tx_hash.is_empty(),
            "a pinned read has no transaction - borrowing the block hash would be a lie"
        );
    }

    /// The declared table is advertised in the same shape everything else consumes.
    #[test]
    fn a_declaration_produces_a_queryable_table_schema() {
        let s = schema(&[decl("oracle__answer", 100)], true);
        assert_eq!(s.len(), 1);
        assert_eq!(s[0].table, "oracle__answer");
        let names: Vec<&str> = s[0].columns.iter().map(|c| c.name.as_str()).collect();
        for want in [
            "block_number",
            "calldata",
            "result",
            "reverted",
            "content_address",
        ] {
            assert!(names.contains(&want), "missing column {want}: {names:?}");
        }
        assert!(
            !names.contains(&"contract"),
            "the contract is the implicit `address` column; a second one would be redundant"
        );
    }

    /// The determinism claim, **against the real chain** rather than a mock.
    ///
    /// A mock proving my own encoder agrees with itself would not be evidence for "re-executes
    /// byte-for-byte across runs and machines" - the claim is about *the chain*, so the test has to
    /// ask the chain. Skipped without `NUTHATCH_ARCHIVE_RPC` so CI stays hermetic, and **loudly**
    /// skipped, because a silent skip is how "verified" quietly becomes untrue.
    #[tokio::test]
    async fn a_pinned_call_re_executes_identically_against_a_real_archive() {
        let Ok(url) = std::env::var("NUTHATCH_ARCHIVE_RPC") else {
            eprintln!(
                "SKIP a_pinned_call_re_executes_identically_against_a_real_archive: \
                 set NUTHATCH_ARCHIVE_RPC to an archive endpoint to run it"
            );
            return;
        };
        let rpc = crate::rpc::RpcClient::new(vec![url]).unwrap();
        // USDC `totalSupply()` at two fixed historical blocks. Both are long final, so the answers are
        // frozen for good - if these ever change, the endpoint is lying about history.
        let usdc = "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48";
        let decls = vec![CallDecl {
            name: "usdc_total_supply".into(),
            contract: usdc.into(),
            contract_column: None,
            calldata: "0x18160ddd".into(),
            every: 1_000_000,
            start: None,
            on: None,
            on_any: Vec::new(),
            canonical: false,
            signature: None,
            args: Vec::new(),
        }];

        let first = resolve_at(&rpc, 1, &decls, 15_000_000).await.unwrap();
        let again = resolve_at(&rpc, 1, &decls, 15_000_000).await.unwrap();
        assert_eq!(
            first, again,
            "the same pinned call must return the same bytes"
        );
        assert!(
            first[0].result.is_some(),
            "USDC totalSupply() must answer at block 15,000,000 - if it reverts, the endpoint is not \
             serving archive state and this test is measuring nothing"
        );

        // A different block must give a different address, and - for a token that was still being
        // minted - a different answer. That is what proves the pin is doing something.
        let earlier = resolve_at(&rpc, 1, &decls, 12_000_000).await.unwrap();
        assert_ne!(
            first[0].address, earlier[0].address,
            "pinning to a different block must change the content address"
        );
        assert_ne!(
            first[0].result, earlier[0].result,
            "USDC supply moved between blocks 12M and 15M; identical answers would mean the block \
             pin is being ignored and every result is really `latest`"
        );
    }
}
