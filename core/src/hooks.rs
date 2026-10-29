//! What a node does around a committed window that a tab does not: tell maintained views and
//! offchain-reading entities, queue webhooks for what sealed, and feed the fold writer.

use anyhow::Result;

#[async_trait::async_trait]
pub trait WindowHooks: Send + Sync {
    /// The window through `to` committed, with the chain tip at `tip`. An error fails the window.
    async fn committed(&self, _to: u64, _tip: u64) -> Result<()> {
        Ok(())
    }

    /// Sealing ran for what is final through `finalized_through`, and the store is sealed through
    /// `sealed_through`. Nothing here may fail the window.
    fn sealed(&self, _finalized_through: u64, _sealed_through: u64, _at_tip: bool) {}

    /// After [`WindowHooks::sealed`], with the store as sealing left it. An error fails the window.
    async fn after_seal(&self) -> Result<()> {
        Ok(())
    }

    /// A test seam waited on once the views are fed and again once the commit has landed (#2467).
    async fn seam(&self) {}

    /// A test seam at the start of the part of a window a deadline may abandon. An error fails the
    /// window there.
    async fn started(&self) -> Result<()> {
        Ok(())
    }
}

/// Does nothing around a window.
pub struct NoHooks;

impl WindowHooks for NoHooks {}
