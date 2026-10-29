//! How the window path fails: a fault no retry can clear, and the points where a test makes it fail.

use anyhow::Result;

/// A fault that will re-fail identically on retry (RFC-0026 §3), so the unit it kills is quarantined
/// **until restart** rather than backed off and re-admitted: a reorg below the sealed watermark bails
/// again on the next attempt by construction, and a dead IVM circuit thread cannot be revived
/// in-process. Retrying either is a busy-loop that spams the log and hides the operator's real job.
/// Carried through `anyhow` and recognised by downcast, so the bail sites stay one-liners.
#[derive(Debug)]
pub struct TerminalFault(pub String);

impl std::fmt::Display for TerminalFault {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for TerminalFault {}

/// Whether any link in the error chain is a [`TerminalFault`] - i.e. a retry is pointless. Everything
/// else is assumed retryable: a transient store/RPC/webhook error that a later window may well survive.
pub fn is_terminal(e: &anyhow::Error) -> bool {
    e.chain().any(|c| c.is::<TerminalFault>())
}

/// The points in the window path where the node's tests inject a failure. Every method defaults to
/// doing nothing, which is all a production cursor or a tab ever passes.
pub trait FaultPoints: Send + Sync {
    /// A window is starting. A panic here is a panicking window.
    fn window_started(&self) {}
    /// The views took the window and the commit has not run. An error fails the window there.
    fn folded(&self) -> Result<()> {
        Ok(())
    }
    /// The instant before the window's commit.
    fn committing(&self) {}
    /// A segment is written and its rows are not yet pruned from the hot store.
    fn sealed_before_prune(&self) {}
}

/// Injects nothing.
pub struct NoFaults;

impl FaultPoints for NoFaults {}
