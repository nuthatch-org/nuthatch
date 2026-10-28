//! nuthatch's address history (RFC-0063) as a WebAssembly module, for RFC-0074's S0.
//!
//! The history store, discovery and the `/api` answers are the binary's own source files, compiled
//! here unchanged by path. What they reach in the rest of the binary is replaced by the small modules
//! below. The module opens no file and no socket: every JSON-RPC call goes out through the host's
//! `rpc_batch` import ([`host`]), and the redb store lives in guest memory, exported and imported
//! whole as bytes so the host can keep it where it keeps its own data.

#[path = "../../src/address_history.rs"]
#[allow(dead_code, unused_imports, clippy::all)]
pub mod address_history;

#[path = "../../src/address_discovery.rs"]
#[allow(dead_code, unused_imports, clippy::all)]
pub mod address_discovery;

#[path = "../../src/address_residual.rs"]
#[allow(dead_code, unused_imports, clippy::all)]
pub mod address_residual;

pub mod blocktime;
pub mod driver;
pub mod host;
pub mod memstore;

#[cfg(target_arch = "wasm32")]
mod exports;

/// The binary's `store::Store` reduced to what address history uses of it: the redb handle.
pub mod store {
    use std::sync::Arc;

    #[derive(Clone)]
    pub struct Store {
        db: Arc<redb::Database>,
    }

    impl Store {
        pub fn from_database(db: redb::Database) -> Store {
            Store { db: Arc::new(db) }
        }

        pub fn database(&self) -> &redb::Database {
            &self.db
        }
    }
}

/// `address_discovery` implements its `Rpc` trait for the binary's client; there is none here.
pub mod rpc {
    use anyhow::Result;
    use serde_json::Value;

    pub enum RpcClient {}

    impl RpcClient {
        pub async fn call(&self, _: &str, _: Value) -> Result<Value> {
            match *self {}
        }
        pub async fn call_capped(&self, _: &str, _: Value, _: usize) -> Result<Value> {
            match *self {}
        }
        pub async fn call_each(&self, _: &str, _: Value) -> Result<Vec<Value>> {
            match *self {}
        }
        pub async fn visit_each(
            &self,
            _: &str,
            _: Value,
            _: &mut (dyn FnMut(Result<Value>) + Send),
        ) {
            match *self {}
        }
    }
}

pub mod address_mode {
    pub const DEFAULT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(8);
}

pub mod freshness {
    use std::time::Duration;

    pub fn parse_duration(s: &str) -> Result<Duration, String> {
        let s = s.trim();
        let (digits, unit) = match s.find(|c: char| !c.is_ascii_digit()) {
            Some(i) => s.split_at(i),
            None => (s, "s"),
        };
        let n: u64 = digits
            .parse()
            .map_err(|_| format!("`{s}` is not a duration; try `2s`, `5m` or `1h`"))?;
        let secs = match unit.trim() {
            "s" | "sec" | "secs" => n,
            "m" | "min" | "mins" => n.checked_mul(60).ok_or("that interval does not fit")?,
            "h" | "hr" | "hrs" => n.checked_mul(3600).ok_or("that interval does not fit")?,
            other => return Err(format!("unknown unit `{other}` in `{s}`; use s, m or h")),
        };
        if secs == 0 {
            return Err("the poll interval must be at least one second".into());
        }
        Ok(Duration::from_secs(secs))
    }
}

pub mod analytics {
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub struct QueryBudgetExceeded {
        pub secs: u64,
    }

    impl std::fmt::Display for QueryBudgetExceeded {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "query exceeded the {}s time budget", self.secs)
        }
    }

    impl std::error::Error for QueryBudgetExceeded {}
}

/// Block partitions are not in S0; discovery names their block type in a signature it shares.
pub mod address_partitions {
    use alloy_consensus::Header;
    use alloy_primitives::B256;
    use alloy_rpc_types_eth::Withdrawal;

    pub struct VerifiedBlock {
        pub hash: B256,
        pub header: Header,
        pub withdrawals: Vec<Withdrawal>,
    }
}

pub mod address_proposers {
    use crate::address_partitions::VerifiedBlock;
    use alloy_consensus::Header;
    use alloy_primitives::B256;
    use anyhow::{bail, Context, Result};
    use serde_json::Value;

    pub(crate) fn block_from_rpc_value(v: Value, n: u64) -> Result<VerifiedBlock> {
        if v.is_null() {
            bail!("the RPC has no block {n}");
        }
        let header: Header =
            serde_json::from_value(v.clone()).with_context(|| format!("block {n}'s header"))?;
        let claimed: B256 = serde_json::from_value(v["hash"].clone())
            .with_context(|| format!("block {n}'s hash"))?;
        if header.number != n || header.hash_slow() != claimed {
            bail!("block {n}'s header does not hash to {claimed}");
        }
        Ok(VerifiedBlock {
            hash: claimed,
            header,
            withdrawals: Vec::new(),
        })
    }
}
