//! RFC-0023 tier 2 - the **immutable-metadata cache.** `decimals()`, `symbol()`, `name()` are
//! write-once ERC-20 constants: they never change, so calling them once and caching forever is safe
//! and deterministic (the value is time-invariant - this is the one place a plain call is fine, and it
//! is *metadata*, never data feeding entity derivation). Tier 1 derives the changing reads; this tier
//! fetches the handful of constants tier 1 can't derive from events, and remembers them.
//!
//! The fetch uses the existing `eth_call` plumbing; the encode (bare 4-byte selectors, no args) and the
//! decode (uint8 / ABI-string returns) are pure and tested here. The cache is a small `metadata.json`
//! in the nest dir, keyed by lowercased address - immutable values, so a present entry is never
//! re-fetched.

use crate::rpc::RpcClient;
use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;

/// Bare 4-byte selectors for the no-arg ERC-20 metadata calls.
pub const DECIMALS_SELECTOR: &str = "0x313ce567";
pub const SYMBOL_SELECTOR: &str = "0x95d89b41";
pub const NAME_SELECTOR: &str = "0x06fdde03";

/// The metadata cache file, in the nest directory.
pub const METADATA_FILE: &str = "metadata.json";

/// One token's immutable metadata. Each field is optional: a contract may not implement it, or return
/// a non-standard shape (e.g. `bytes32` symbol) we don't decode - absent rather than wrong.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct TokenMetadata {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decimals: Option<u8>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub symbol: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// The cache: lowercased address → its metadata.
pub type MetadataCache = BTreeMap<String, TokenMetadata>;

/// Decode a `uint8` return (`decimals()`): the value is the last byte of the single 32-byte word.
pub fn decode_u8(hex: &str) -> Option<u8> {
    let bytes = hex::decode(hex.trim_start_matches("0x")).ok()?;
    (bytes.len() == 32).then(|| bytes[31])
}

/// Decode an ABI-encoded dynamic `string` return (`symbol()`/`name()`). Non-string shapes (e.g. a
/// `bytes32` symbol) decode to `None` rather than a wrong value.
pub fn decode_string(hex: &str) -> Option<String> {
    use alloy_dyn_abi::{DynSolType, DynSolValue};
    let bytes = hex::decode(hex.trim_start_matches("0x")).ok()?;
    match DynSolType::String.abi_decode(&bytes).ok()? {
        DynSolValue::String(s) => Some(s),
        _ => None,
    }
}

/// Fetch a contract's immutable metadata via `eth_call`. A call that reverts is an answer - the contract
/// lacks that function - and leaves its field `None`. Any other error fails the fetch, so a dropped
/// connection is never cached as an answer (#2720).
pub async fn fetch(rpc: &RpcClient, address: &str) -> Result<TokenMetadata> {
    let decimals = answer(rpc.eth_call(address, DECIMALS_SELECTOR).await, "decimals()")?;
    let symbol = answer(rpc.eth_call(address, SYMBOL_SELECTOR).await, "symbol()")?;
    let name = answer(rpc.eth_call(address, NAME_SELECTOR).await, "name()")?;
    Ok(TokenMetadata {
        decimals: decimals.as_deref().and_then(decode_u8),
        symbol: symbol.as_deref().and_then(decode_string),
        name: name.as_deref().and_then(decode_string),
    })
}

/// One `eth_call`'s return: its hex, `None` when the contract reverted, or the error otherwise.
fn answer(reply: Result<String>, what: &str) -> Result<Option<String>> {
    match reply {
        Ok(hex) => Ok(Some(hex)),
        Err(e) if format!("{e:#}").contains("revert") => Ok(None),
        Err(e) => Err(e).with_context(|| what.to_string()),
    }
}

/// Load the nest's metadata cache (empty if absent).
pub fn load(dir: &Path) -> MetadataCache {
    std::fs::read_to_string(dir.join(METADATA_FILE))
        .ok()
        .and_then(|raw| serde_json::from_str(&raw).ok())
        .unwrap_or_default()
}

/// Write the metadata cache to the nest dir (pretty JSON, stable key order).
pub fn save(dir: &Path, cache: &MetadataCache) -> Result<()> {
    let raw = serde_json::to_string_pretty(cache).context("serialize metadata cache")?;
    std::fs::write(dir.join(METADATA_FILE), raw).context("write metadata.json")?;
    Ok(())
}

/// `nuthatch metadata fetch [--dir <d>] [--rpc <url>…]` - fetch + cache the immutable metadata for every
/// contract in the nest. Immutable, so already-cached contracts are skipped (no re-fetch).
pub async fn fetch_cli(dir: &Path, rpc_override: Vec<String>) -> Result<()> {
    let config = crate::config::Config::load(dir)?;
    let rpc_urls = crate::rpc::select_rpcs(&rpc_override, config.nest.rpc_urls.clone());
    if rpc_urls.is_empty() {
        anyhow::bail!("no rpc_urls (set them in nuthatch.toml or pass --rpc)");
    }
    let rpc = RpcClient::new(rpc_urls)?;

    let mut cache = load(dir);
    let mut fetched = 0usize;
    let mut failed = Vec::new();
    for c in &config.contracts {
        let addr = c.address.to_lowercase();
        if cache.contains_key(&addr) {
            continue; // immutable - never re-fetch
        }
        let md = match fetch(&rpc, &c.address).await {
            Ok(md) => md,
            Err(e) => {
                println!("  {} {addr}: not cached: {e:#}", c.alias);
                failed.push(c.alias.clone());
                continue;
            }
        };
        println!(
            "  {} {addr}: decimals={:?} symbol={:?} name={:?}",
            c.alias, md.decimals, md.symbol, md.name
        );
        cache.insert(addr, md);
        fetched += 1;
    }
    save(dir, &cache)?;
    println!(
        "✓ cached token metadata for {fetched} new contract(s) → {}",
        dir.join(METADATA_FILE).display()
    );
    if !failed.is_empty() {
        anyhow::bail!(
            "metadata not cached for {}; rerun to retry",
            failed.join(", ")
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_uint8_decimals() {
        // A single 32-byte word whose value is 6 (USDC decimals).
        let word = format!("0x{}06", "00".repeat(31));
        assert_eq!(decode_u8(&word), Some(6));
        // 18-decimal token.
        let w18 = format!("0x{}12", "00".repeat(31)); // 0x12 = 18
        assert_eq!(decode_u8(&w18), Some(18));
        // Wrong length → None.
        assert_eq!(decode_u8("0x06"), None);
    }

    #[test]
    fn decodes_abi_string_symbol() {
        // ABI encoding of the string "USDC": offset(32), length(4), "USDC" right-padded to 32 bytes.
        let hex = format!(
            "0x{}{}{}",
            "0000000000000000000000000000000000000000000000000000000000000020",
            "0000000000000000000000000000000000000000000000000000000000000004",
            "5553444300000000000000000000000000000000000000000000000000000000",
        );
        assert_eq!(decode_string(&hex).as_deref(), Some("USDC"));
        // A bytes32-style / garbage return doesn't decode to a wrong string.
        assert_eq!(decode_string("0x1234"), None);
    }

    #[test]
    fn a_reverted_call_is_an_answer_and_a_dropped_one_is_not() {
        let reverted = Err(anyhow::anyhow!("eth_call: execution reverted"));
        assert_eq!(answer(reverted, "name()").unwrap(), None);
        let dropped = Err(anyhow::anyhow!("transport error: connection reset"));
        assert!(answer(dropped, "name()").is_err());
    }

    #[test]
    fn cache_round_trips() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load(dir.path()).is_empty());
        let mut cache = MetadataCache::new();
        cache.insert(
            "0xaaaa".into(),
            TokenMetadata {
                decimals: Some(6),
                symbol: Some("USDC".into()),
                name: Some("USD Coin".into()),
            },
        );
        save(dir.path(), &cache).unwrap();
        let back = load(dir.path());
        assert_eq!(back, cache);
        assert_eq!(back["0xaaaa"].decimals, Some(6));
    }
}
