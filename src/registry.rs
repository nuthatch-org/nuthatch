//! Re-exports from `nuthatch-decode` plus the config-aware constructor `from_nest`, which lives
//! here because it depends on `Config` and `blob::checked_join` from the main crate.
//!
//! Everything else (the types, `build`, `build_with_templates`, all decode logic) lives in
//! `nuthatch-decode` so fuzz targets can build without pulling in dbsp (nuthatch#581).

pub use nuthatch_decode::registry::*;

use crate::config::Config;
use anyhow::{Context, Result};
use std::path::Path;

/// Build from a nest's config: load each contract's vendored ABI and register its events.
///
/// The construction is `nuthatch_core::nest_config::build_registry`, which an embedding shares; what
/// stays here is reading the ABIs through [`crate::blob::checked_join`].
pub fn from_nest(dir: &Path, config: &Config) -> Result<DecodeRegistry> {
    nuthatch_core::nest_config::build_registry(
        &config.nest,
        &config.contracts,
        &config.templates,
        &config.extract,
        &mut |path, what| {
            let abi_path = crate::blob::checked_join(dir, path)?;
            let raw = std::fs::read_to_string(&abi_path)
                .with_context(|| format!("reading {what} {}", abi_path.display()))?;
            Ok((abi_path.display().to_string(), raw))
        },
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::Address;
    use nuthatch_core::nest_config::parse_address as parse_address_local;

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
