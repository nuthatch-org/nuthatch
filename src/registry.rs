//! Re-exports from `nuthatch-decode` plus the config-aware constructor `from_nest`, which lives
//! here because it depends on `Config` and `blob::checked_join` from the main crate.
//!
//! Everything else (the types, `build`, `build_with_templates`, all decode logic) lives in
//! `nuthatch-decode` so fuzz targets can build without pulling in dbsp (nuthatch#581).

pub use nuthatch_decode::registry::*;

use crate::config::Config;
use alloy_json_abi::JsonAbi;
use alloy_primitives::Address;
use anyhow::{anyhow, Context, Result};
use std::path::Path;

fn parse_address_local(s: &str) -> Result<Address> {
    let bytes = hex::decode(s.trim_start_matches("0x")).context("bad address hex")?;
    if bytes.len() != 20 {
        return Err(anyhow!("address is not 20 bytes"));
    }
    Ok(Address::from_slice(&bytes))
}

/// Build from a nest's config: load each contract's vendored ABI and register its events.
///
/// Lives here rather than in `nuthatch-decode` because it depends on [`Config`] and
/// [`crate::blob::checked_join`], both of which are main-crate concerns. The decode logic
/// itself (all pure, no dbsp, no I/O) is in `nuthatch-decode::registry`.
pub fn from_nest(dir: &Path, config: &Config) -> Result<DecodeRegistry> {
    let mut specs = Vec::with_capacity(config.contracts.len());
    for c in &config.contracts {
        let abi_path = crate::blob::checked_join(dir, &c.abi)?;
        let raw = std::fs::read_to_string(&abi_path)
            .with_context(|| format!("reading ABI {}", abi_path.display()))?;
        let abi: JsonAbi = nuthatch_decode::abi::parse_str(&raw)
            .with_context(|| format!("parsing ABI {}", abi_path.display()))?;
        let address = parse_address_local(&c.address)?;
        // A parse that always returns zero used to hang the mutants job: indexer tests wait for a
        // head that never moves (#1281).
        if address.is_zero() {
            return Err(anyhow!(
                "contract `{}` is the zero address, which is not a contract nuthatch will index",
                c.alias
            ));
        }
        specs.push(ContractSpec {
            alias: c.alias.clone(),
            address,
            abi,
            events: c.events.clone(),
        });
    }
    let mut templates = Vec::with_capacity(config.templates.len());
    for t in &config.templates {
        let abi_path = crate::blob::checked_join(dir, &t.abi)?;
        let raw = std::fs::read_to_string(&abi_path)
            .with_context(|| format!("reading template ABI {}", abi_path.display()))?;
        let abi: JsonAbi = nuthatch_decode::abi::parse_str(&raw)
            .with_context(|| format!("parsing template ABI {}", abi_path.display()))?;
        templates.push(TemplateSpec {
            name: t.name.clone(),
            abi,
            events: t.events.clone(),
        });
    }
    Ok(DecodeRegistry::build_with_templates(specs, templates)?
        .with_timestamps(config.nest.block_timestamps)
        .with_blocks(config.extract.blocks)
        .with_l1_blocks(config.extract.l1_blocks))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_address_local_returns_the_decoded_bytes() {
        let a = parse_address_local("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48").unwrap();
        assert_ne!(a, Address::ZERO);
        assert_eq!(
            a.as_slice(),
            &hex::decode("a0b86991c6218b36c1d19d4a2e9eb0ce3606eb48").unwrap()
        );
    }

    #[test]
    fn parse_address_local_rejects_a_short_hex() {
        assert!(parse_address_local("0xabc").is_err());
    }

    #[test]
    fn a_vendored_viem_abi_without_anonymous_still_decodes() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir(dir.path().join("abis")).unwrap();
        std::fs::write(
            dir.path().join("abis/token.json"),
            r#"[{"type":"event","name":"Transfer","inputs":[
                {"name":"from","type":"address","indexed":true},
                {"name":"to","type":"address","indexed":true},
                {"name":"value","type":"uint256","indexed":false}]}]"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("nuthatch.toml"),
            r#"[nest]
name = "t"
chain = "mainnet"
chain_id = 1
rpc_urls = ["http://127.0.0.1:1"]
schema_version = 1

[[contracts]]
alias = "token"
address = "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"
start_block = 1
abi = "abis/token.json"
"#,
        )
        .unwrap();
        let config = Config::load(dir.path()).unwrap();
        let registry = from_nest(dir.path(), &config).unwrap();
        let transfer =
            hex::decode("ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef")
                .unwrap();
        assert!(registry
            .topic0s()
            .iter()
            .any(|t| t.as_slice() == transfer.as_slice()));
    }
}
