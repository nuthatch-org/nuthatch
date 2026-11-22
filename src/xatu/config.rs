//! Turning the operator's flag into a source: `nuthatch dev --history-source xatu [--history-url]`.

use super::{HttpFetch, XatuSource, DEFAULT_BASE_URL};
use crate::cli::{DevArgs, HistorySource};
use crate::config::Config;
use crate::source::Source;
use anyhow::Result;
use std::sync::Arc;

/// Wrap `rpc` in the public-dataset history source when the operator asked for one. Refuses a chain
/// the dataset does not cover, rather than quietly ignoring the flag.
pub fn with_history_source(
    rpc: Arc<dyn Source>,
    config: &Config,
    args: &DevArgs,
) -> Result<Arc<dyn Source>> {
    let Some(HistorySource::Xatu) = args.history_source else {
        return Ok(rpc);
    };
    if config.nest.chain_id != 1 {
        anyhow::bail!(
            "--history-source xatu covers Ethereum mainnet only, and this nest is on chain {} \
             ({}); drop the flag to index it from the RPC",
            config.nest.chain_id,
            config.nest.chain
        );
    }
    let base = args.history_url.as_deref().unwrap_or(DEFAULT_BASE_URL);
    tracing::info!(
        "history source: xatu ({base}); blocks it holds are read from there, the rest from the RPC"
    );
    Ok(Arc::new(XatuSource::new(
        rpc,
        Arc::new(HttpFetch::new()?),
        base,
    )))
}
