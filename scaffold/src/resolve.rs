//! The choices `init` makes from what it fetched: which proxy slot names an implementation, what
//! Sourcify's answer holds, and what each contract is called. Pure, so a tab makes the same ones.

use std::collections::HashSet;

use anyhow::{anyhow, bail, Result};
use serde_json::Value;

/// Well-known proxy implementation storage slots, tried in order - each holds the implementation
/// address *directly*:
/// - EIP-1967: keccak256("eip1967.proxy.implementation") − 1
/// - EIP-1822 (UUPS "Proxiable"): keccak256("PROXIABLE")
/// - legacy OpenZeppelin/zeppelinos: keccak256("org.zeppelinos.proxy.implementation") (e.g. USDC)
pub const PROXY_IMPL_SLOTS: &[&str] = &[
    "0x360894a13ba1a3210667c828492db98dca3e2076cc3735a920a3ca505d382bbc",
    "0xc5f16f0fcc639fa48a6947836d9850f504798523bf8c9a3a87d5876cf622bcf7",
    "0x7050c9e0f4ca769c69bd3a8ef740bc37934f8e2c036e5a723fd8ee048ed3f8c3",
];

/// EIP-1967 beacon slot: keccak256("eip1967.proxy.beacon") − 1. A *beacon* proxy stores a beacon
/// address here (not the implementation); the implementation comes from calling `implementation()` on
/// that beacon - a common shape for factory-deployed proxies that share one upgradeable logic contract.
pub const PROXY_BEACON_SLOT: &str =
    "0xa3f0ad74e5423aebfd80d3ef4346578335a9a72aeaee59ff6cb3582b35133d50";

/// Selector for `implementation()` - `keccak256("implementation()")[..4]`. Both a beacon and an
/// EIP-897 delegate proxy expose the implementation this way.
pub const IMPLEMENTATION_SELECTOR: &str = "0x5c60da1b";

/// Extract a non-zero implementation address from a 32-byte storage word.
pub fn impl_from_slot(slot: &str) -> Option<String> {
    let h = slot.trim_start_matches("0x");
    if h.len() < 40 {
        return None;
    }
    let addr = &h[h.len() - 40..];
    if addr.chars().all(|c| c == '0') {
        return None;
    }
    Some(format!("0x{addr}"))
}

pub fn parse_sourcify(body: &Value) -> Result<Value> {
    body.get("abi")
        .filter(|a| a.is_array())
        .cloned()
        .ok_or_else(|| anyhow!("Sourcify had no ABI for this contract"))
}

pub fn sourcify_contract_name(body: &Value) -> Option<String> {
    // The first *usable* value, not the first present one: an empty or non-string
    // `compilation.name` must not shadow a legacy top-level name that is fine.
    [body.pointer("/compilation/name"), body.get("name")]
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .find(|s| !s.is_empty())
        .map(str::to_string)
}

/// Minimal sanity check + lowercasing. Full checksum validation is a later concern.
pub fn normalise_address(addr: &str) -> Result<String> {
    let a = addr.trim();
    let hex = a.strip_prefix("0x").unwrap_or(a);
    if hex.len() != 40 || !hex.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("'{addr}' is not a 20-byte hex address");
    }
    Ok(format!("0x{}", hex.to_ascii_lowercase()))
}

pub fn is_valid_alias(a: &str) -> bool {
    let mut chars = a.chars();
    matches!(chars.next(), Some(c) if c.is_ascii_lowercase())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// `DelegationManager` → `delegation_manager`, `Vat` → `vat`, `L2GNS` → `l2gns`.
/// None when the result is empty or would not pass [`is_valid_alias`].
pub fn slugify_alias(name: &str) -> Option<String> {
    let mut out = String::new();
    let mut prev_lower = false;
    for c in name.chars() {
        if c.is_ascii_uppercase() {
            if prev_lower {
                out.push('_');
            }
            out.push(c.to_ascii_lowercase());
            prev_lower = false;
        } else if c.is_ascii_lowercase() || c.is_ascii_digit() {
            out.push(c);
            prev_lower = c.is_ascii_lowercase();
        } else if !out.is_empty() && !out.ends_with('_') {
            out.push('_');
            prev_lower = false;
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    is_valid_alias(&out).then_some(out)
}

/// A name from Sourcify metadata or a wrapped ABI's `contractName`.
pub fn contract_name_from_abi(abi: &serde_json::Value) -> Option<&str> {
    abi.get("contractName").and_then(|v| v.as_str())
}

/// Next unused `cN`, then `cN+1`, skipping anything already taken.
fn next_c_alias(used: &HashSet<&str>, taken: &[String]) -> String {
    let mut k = 0usize;
    loop {
        let cand = format!("c{k}");
        if !used.contains(cand.as_str()) && !taken.contains(&cand) {
            return cand;
        }
        k += 1;
    }
}

/// Prefer an explicit alias, then a slugified contract name, then `cN`.
pub fn pick_alias(
    provided: Option<&str>,
    contract_name: Option<&str>,
    used: &HashSet<&str>,
    taken: &[String],
) -> Result<String> {
    if let Some(a) = provided {
        if !is_valid_alias(a) {
            bail!("alias '{a}' must match [a-z][a-z0-9_]*");
        }
        if used.contains(a) || taken.iter().any(|t| t == a) {
            bail!("alias '{a}' is already used in this nest");
        }
        return Ok(a.to_string());
    }
    if let Some(slug) = contract_name.and_then(slugify_alias) {
        if !used.contains(slug.as_str()) && !taken.contains(&slug) {
            return Ok(slug);
        }
        let mut k = 2usize;
        loop {
            let cand = format!("{slug}_{k}");
            if is_valid_alias(&cand) && !used.contains(cand.as_str()) && !taken.contains(&cand) {
                return Ok(cand);
            }
            k += 1;
        }
    }
    Ok(next_c_alias(used, taken))
}
