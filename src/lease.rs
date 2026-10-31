//! RFC-0078 S0: a writer's lease held in the mirror, beside the catalogue it guards.
//!
//! The mirror is the one thing a writer and its standbys on other hosts both see, and its
//! conditional put is linearisable per key, so it can arbitrate who writes. Every change is a
//! [`Mirror::put_if`] on the exact bytes last seen: two standbys racing for one lapsed lease put on
//! the same bytes and the store accepts exactly one.
//!
//! Expiry never compares two hosts' clocks. A standby measures, on its own monotonic clock, how
//! long the lease's bytes have gone unchanged ([`Watch`]); a holder measures how long since its last
//! successful renewal ([`Lease::valid`]). Both rely only on clocks running at nearly the same rate.

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

use crate::publish::{CasConflict, Mirror};

pub const LEASE_FILE: &str = "lease.json";

/// The lease's key in a mirror: beside the catalogue, under the dataset's identity.
pub fn lease_key(data_identity: &str) -> String {
    format!("{data_identity}/{LEASE_FILE}")
}

/// What `lease.json` holds.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LeaseDoc {
    /// `None` once released: a standby may take it without waiting out the term.
    pub owner: Option<String>,
    /// Rises by one on every change of owner and never otherwise.
    pub fence: u64,
    pub term_secs: u64,
    /// Bumped on every write, so a renewal always changes the bytes a standby watches.
    pub renewal: u64,
}

/// The lease as last read from the mirror.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Observed {
    Absent,
    Present { bytes: Vec<u8>, doc: LeaseDoc },
}

impl Observed {
    pub fn doc(&self) -> Option<&LeaseDoc> {
        match self {
            Observed::Absent => None,
            Observed::Present { doc, .. } => Some(doc),
        }
    }

    fn bytes(&self) -> Option<&[u8]> {
        match self {
            Observed::Absent => None,
            Observed::Present { bytes, .. } => Some(bytes),
        }
    }
}

/// The lease is someone else's now. Distinct from a failed request: this means stop, not retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LostLease {
    pub fence: u64,
}

impl std::fmt::Display for LostLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "lost the writer lease held at fence {}: another writer has taken it - stop writing, \
             do not retry",
            self.fence
        )
    }
}

impl std::error::Error for LostLease {}

pub async fn observe(mirror: &dyn Mirror, key: &str) -> Result<Observed> {
    let Some(bytes) = mirror.get(key).await? else {
        return Ok(Observed::Absent);
    };
    let doc = serde_json::from_slice(&bytes)
        .with_context(|| format!("{key} in the mirror is not a lease"))?;
    Ok(Observed::Present { bytes, doc })
}

/// A standby's view of how long the lease has gone unchanged.
#[derive(Debug)]
pub struct Watch {
    last: Option<Observed>,
    since: Instant,
}

impl Default for Watch {
    fn default() -> Self {
        Self::new()
    }
}

impl Watch {
    pub fn new() -> Self {
        Self {
            last: None,
            since: Instant::now(),
        }
    }

    /// Record a read; the unchanged-for clock restarts whenever the bytes differ from the last.
    pub fn see(&mut self, observed: &Observed) {
        if self.last.as_ref() != Some(observed) {
            self.last = Some(observed.clone());
            self.since = Instant::now();
        }
    }

    /// Whether a standby may take the lease as last seen: absent, released, or unchanged for longer
    /// than its term plus `margin`.
    pub fn takeable(&self, margin: Duration) -> bool {
        match &self.last {
            None => false,
            Some(Observed::Absent) => true,
            Some(Observed::Present { doc, .. }) if doc.owner.is_none() => true,
            Some(Observed::Present { doc, .. }) => {
                self.since.elapsed() >= Duration::from_secs(doc.term_secs) + margin
            }
        }
    }
}

#[derive(Debug)]
struct Held {
    bytes: Vec<u8>,
    doc: LeaseDoc,
    renewed_at: Instant,
}

/// One process's claim on the lease.
pub struct Lease {
    mirror: Arc<dyn Mirror>,
    key: String,
    owner: String,
    term: Duration,
    held: Option<Held>,
}

impl Lease {
    pub fn new(
        mirror: Arc<dyn Mirror>,
        key: impl Into<String>,
        owner: impl Into<String>,
        term: Duration,
    ) -> Self {
        Self {
            mirror,
            key: key.into(),
            owner: owner.into(),
            term,
            held: None,
        }
    }

    /// Take the lease from exactly what `from` observed, at the next fence. The caller decides it
    /// may ([`Watch::takeable`]); the mirror decides whether it won. A loss is [`LostLease`].
    pub async fn take(&mut self, from: &Observed) -> Result<u64> {
        let fence = from.doc().map_or(0, |d| d.fence) + 1;
        let doc = LeaseDoc {
            owner: Some(self.owner.clone()),
            fence,
            term_secs: self.term.as_secs().max(1),
            renewal: from.doc().map_or(0, |d| d.renewal) + 1,
        };
        self.write(doc, from.bytes(), fence - 1).await?;
        Ok(fence)
    }

    /// Extend the lease this process holds. The fence does not move.
    pub async fn renew(&mut self) -> Result<()> {
        let held = self.held.as_ref().context("renewing a lease never taken")?;
        let doc = LeaseDoc {
            renewal: held.doc.renewal + 1,
            ..held.doc.clone()
        };
        let (expected, fence) = (held.bytes.clone(), held.doc.fence);
        self.write(doc, Some(&expected), fence).await
    }

    /// Give the lease up at its current fence, for a planned handover (RFC-0078 §5.6).
    pub async fn release(&mut self) -> Result<()> {
        let held = self
            .held
            .as_ref()
            .context("releasing a lease never taken")?;
        let doc = LeaseDoc {
            owner: None,
            renewal: held.doc.renewal + 1,
            ..held.doc.clone()
        };
        let (expected, fence) = (held.bytes.clone(), held.doc.fence);
        self.write(doc, Some(&expected), fence).await?;
        self.held = None;
        Ok(())
    }

    /// Whether this process may still commit: it holds the lease and renewed it within the term.
    /// Past that a standby may already have taken it, so the writer fences itself.
    pub fn valid(&self) -> bool {
        self.held
            .as_ref()
            .is_some_and(|h| h.doc.owner.is_some() && h.renewed_at.elapsed() < self.term)
    }

    pub fn fence(&self) -> Option<u64> {
        self.held.as_ref().map(|h| h.doc.fence)
    }

    pub fn term(&self) -> Duration {
        self.term
    }

    pub fn owner(&self) -> &str {
        &self.owner
    }

    async fn write(&mut self, doc: LeaseDoc, expected: Option<&[u8]>, fence: u64) -> Result<()> {
        let bytes = serde_json::to_vec(&doc)?;
        // Taken before the request: the term runs from when the write was sent, not answered, so a
        // slow answer shortens the lease rather than stretching it past a standby's view.
        let sent = Instant::now();
        match self.mirror.put_if(&self.key, &bytes, expected).await {
            Ok(()) => {
                self.held = Some(Held {
                    bytes,
                    doc,
                    renewed_at: sent,
                });
                Ok(())
            }
            Err(e) if e.downcast_ref::<CasConflict>().is_some() => {
                self.held = None;
                Err(LostLease { fence }.into())
            }
            Err(e) => Err(e.context(format!("writing {}", self.key))),
        }
    }
}

/// Why [`hold`] returned. Every way, the store has been fenced off before it returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Stopped {
    /// A renewal found another writer's lease.
    Lost(LostLease),
    /// No renewal succeeded within the term, so a standby may already hold it.
    Lapsed { fence: u64 },
    /// Released on purpose for a standby to take at once (RFC-0078 §5.6).
    HandedOver { fence: u64 },
}

impl std::fmt::Display for Stopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Stopped::Lost(lost) => lost.fmt(f),
            Stopped::Lapsed { fence } => write!(
                f,
                "the writer lease held at fence {fence} lapsed without a renewal, so a standby may \
                 hold it: this writer has stopped committing"
            ),
            Stopped::HandedOver { fence } => write!(
                f,
                "the writer lease held at fence {fence} was handed over: this writer has stopped \
                 committing, and a standby takes it from here"
            ),
        }
    }
}

/// A planned handover asked of a holder, answered with the fence it released once it has.
pub type Handover = tokio::sync::oneshot::Sender<std::result::Result<u64, String>>;

/// How often a holder renews: three tries per term. A holder therefore notices a lapse at most a
/// third of a term late, which is why a standby's margin must exceed this.
pub fn renew_every(term: Duration) -> Duration {
    term / 3
}

/// Renew `lease` until it is lost or lapses, then call `fence_off` so the cursor's next commit is
/// refused. A failed request is retried at the next tick; only the term decides when to give up.
#[cfg(test)]
pub async fn hold(lease: Lease, fence_off: impl Fn() -> Result<()>) -> Stopped {
    let (_keep, handovers) = tokio::sync::mpsc::channel(1);
    hold_with(lease, fence_off, handovers, || async {}, None).await
}

/// [`hold`], also answering planned handovers: fence the store off, wait for `drain` (what was
/// committed reaching the mirror), then release the lease so a standby need not wait out the term.
pub async fn hold_with<D, F>(
    mut lease: Lease,
    fence_off: impl Fn() -> Result<()>,
    mut handovers: tokio::sync::mpsc::Receiver<Handover>,
    drain: D,
    metrics: Option<std::sync::Arc<crate::metrics::NestMetrics>>,
) -> Stopped
where
    D: Fn() -> F,
    F: std::future::Future<Output = ()>,
{
    let every = renew_every(lease.term());
    let fence = lease.fence().unwrap_or(0);
    if let Some(m) = &metrics {
        m.set_lease(fence, true);
    }
    let let_go = |metrics: &Option<std::sync::Arc<crate::metrics::NestMetrics>>| {
        if let Some(m) = metrics {
            m.set_lease(fence, false);
        }
    };
    let mut open = true;
    let stopped = loop {
        tokio::select! {
            _ = tokio::time::sleep(every) => {}
            asked = handovers.recv(), if open => {
                let Some(reply) = asked else {
                    open = false;
                    continue;
                };
                fence_until_done(&fence_off, fence).await;
                drain().await;
                let released = lease.release().await.map(|()| fence).map_err(|e| format!("{e:#}"));
                // Fenced off either way: a release that failed lapses within the term instead.
                let _ = reply.send(released);
                let_go(&metrics);
                tracing::warn!("{}", Stopped::HandedOver { fence });
                return Stopped::HandedOver { fence };
            }
        }
        let renewed = lease.renew().await;
        if let Some(m) = &metrics {
            m.lease_renewed(renewed.is_ok());
        }
        match renewed {
            Ok(()) => continue,
            Err(e) => {
                if let Some(lost) = e.downcast_ref::<LostLease>() {
                    break Stopped::Lost(*lost);
                }
                tracing::warn!("renewing the writer lease at fence {fence}: {e:#}");
            }
        }
        if !lease.valid() {
            break Stopped::Lapsed { fence };
        }
    };
    fence_until_done(&fence_off, fence).await;
    let_go(&metrics);
    tracing::warn!("{stopped}");
    stopped
}

/// Retried until it lands: a writer that cannot fence itself off must not keep committing.
async fn fence_until_done(fence_off: &impl Fn() -> Result<()>, fence: u64) {
    while let Err(e) = fence_off() {
        tracing::error!("fencing off the store at fence {fence}: {e:#}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// Poll the lease until a standby may take it, and return what it last saw, to take from.
pub async fn await_takeable(
    mirror: &dyn Mirror,
    key: &str,
    margin: Duration,
    poll: Duration,
) -> Observed {
    let mut watch = Watch::new();
    loop {
        match observe(mirror, key).await {
            Ok(seen) => {
                watch.see(&seen);
                if watch.takeable(margin) {
                    return seen;
                }
            }
            // Taking needs a write to the same mirror, so there is nothing to do but read again.
            Err(e) => tracing::warn!("reading {key}: {e:#}"),
        }
        tokio::time::sleep(poll).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::publish::open_mirror;

    const TERM: Duration = Duration::from_secs(10);
    const MARGIN: Duration = Duration::from_secs(5);
    const KEY: &str = "ds/lease.json";

    fn fs_mirror() -> (tempfile::TempDir, Arc<dyn Mirror>) {
        let dir = tempfile::tempdir().unwrap();
        let m: Arc<dyn Mirror> = open_mirror(dir.path().to_str().unwrap()).unwrap().into();
        (dir, m)
    }

    fn lost(e: &anyhow::Error) -> Option<LostLease> {
        e.downcast_ref::<LostLease>().copied()
    }

    #[tokio::test]
    async fn the_first_writer_takes_fence_one_and_renews_without_moving_it() {
        let (_d, m) = fs_mirror();
        let mut a = Lease::new(m.clone(), KEY, "a", TERM);
        assert_eq!(a.take(&observe(&*m, KEY).await.unwrap()).await.unwrap(), 1);
        a.renew().await.unwrap();
        a.renew().await.unwrap();
        let doc = observe(&*m, KEY).await.unwrap().doc().cloned().unwrap();
        assert_eq!(
            (doc.owner.as_deref(), doc.fence, doc.renewal),
            (Some("a"), 1, 3)
        );
        assert!(a.valid());
    }

    #[tokio::test]
    async fn two_standbys_racing_for_one_lapsed_lease_produce_one_holder() {
        let (_d, m) = fs_mirror();
        Lease::new(m.clone(), KEY, "dead", TERM)
            .take(&Observed::Absent)
            .await
            .unwrap();
        let seen = observe(&*m, KEY).await.unwrap();
        let mut b = Lease::new(m.clone(), KEY, "b", TERM);
        let mut c = Lease::new(m.clone(), KEY, "c", TERM);
        let (rb, rc) = tokio::join!(b.take(&seen), c.take(&seen));
        assert!(
            rb.is_ok() ^ rc.is_ok(),
            "exactly one take wins: {rb:?} {rc:?}"
        );
        let loser = rb.err().or(rc.err()).unwrap();
        assert_eq!(lost(&loser), Some(LostLease { fence: 1 }));
        let doc = observe(&*m, KEY).await.unwrap().doc().cloned().unwrap();
        assert_eq!(doc.fence, 2);
        assert!(b.valid() ^ c.valid());
    }

    #[tokio::test]
    async fn a_writer_that_lost_its_lease_learns_it_on_the_next_renewal() {
        let (_d, m) = fs_mirror();
        let mut a = Lease::new(m.clone(), KEY, "a", TERM);
        a.take(&Observed::Absent).await.unwrap();
        let mut b = Lease::new(m.clone(), KEY, "b", TERM);
        b.take(&observe(&*m, KEY).await.unwrap()).await.unwrap();
        let e = a.renew().await.unwrap_err();
        assert_eq!(lost(&e), Some(LostLease { fence: 1 }));
        assert!(!a.valid());
        assert_eq!(a.fence(), None);
        b.renew().await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn a_holder_fences_itself_once_a_term_passes_without_a_renewal() {
        let (_d, m) = fs_mirror();
        let mut a = Lease::new(m.clone(), KEY, "a", TERM);
        a.take(&Observed::Absent).await.unwrap();
        tokio::time::advance(TERM - Duration::from_millis(1)).await;
        assert!(a.valid());
        tokio::time::advance(Duration::from_millis(1)).await;
        assert!(!a.valid(), "the term has run out with no renewal");
        a.renew().await.unwrap();
        assert!(a.valid(), "a renewal that wins restores it");
    }

    #[tokio::test(start_paused = true)]
    async fn a_standby_may_take_only_after_the_term_and_margin_without_change() {
        let (_d, m) = fs_mirror();
        let mut a = Lease::new(m.clone(), KEY, "a", TERM);
        a.take(&Observed::Absent).await.unwrap();
        let mut w = Watch::new();
        w.see(&observe(&*m, KEY).await.unwrap());
        tokio::time::advance(TERM).await;
        a.renew().await.unwrap();
        w.see(&observe(&*m, KEY).await.unwrap());
        tokio::time::advance(TERM + MARGIN - Duration::from_millis(1)).await;
        assert!(
            !w.takeable(MARGIN),
            "a renewal restarted the standby's clock"
        );
        tokio::time::advance(Duration::from_millis(1)).await;
        w.see(&observe(&*m, KEY).await.unwrap());
        assert!(w.takeable(MARGIN));
        // The holder's own view ran out a margin earlier, so it stopped committing first.
        assert!(!a.valid());
    }

    #[tokio::test]
    async fn a_released_lease_is_takeable_at_once_and_the_fence_still_rises() {
        let (_d, m) = fs_mirror();
        let mut a = Lease::new(m.clone(), KEY, "a", TERM);
        a.take(&Observed::Absent).await.unwrap();
        a.release().await.unwrap();
        assert!(!a.valid());
        let mut w = Watch::new();
        let seen = observe(&*m, KEY).await.unwrap();
        w.see(&seen);
        assert!(w.takeable(MARGIN));
        let mut b = Lease::new(m.clone(), KEY, "b", TERM);
        assert_eq!(b.take(&seen).await.unwrap(), 2);
    }

    #[tokio::test]
    async fn a_take_from_a_stale_observation_loses() {
        let (_d, m) = fs_mirror();
        let mut a = Lease::new(m.clone(), KEY, "a", TERM);
        a.take(&Observed::Absent).await.unwrap();
        let stale = observe(&*m, KEY).await.unwrap();
        a.renew().await.unwrap();
        let e = Lease::new(m.clone(), KEY, "b", TERM)
            .take(&stale)
            .await
            .unwrap_err();
        assert!(
            lost(&e).is_some(),
            "a renewal since the read must refuse the take: {e}"
        );
        assert!(a.valid());
    }

    #[tokio::test]
    async fn a_failed_request_is_not_a_lost_lease() {
        let m: Arc<dyn Mirror> = Arc::new(Unreachable);
        let e = Lease::new(m, KEY, "a", TERM)
            .take(&Observed::Absent)
            .await
            .unwrap_err();
        assert!(lost(&e).is_none(), "an I/O failure must be retryable: {e}");
    }

    fn counter() -> (Arc<std::sync::atomic::AtomicU32>, impl Fn() -> Result<()>) {
        let n = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let m = n.clone();
        (n, move || {
            m.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        })
    }

    #[tokio::test(start_paused = true)]
    async fn a_holder_whose_lease_is_taken_fences_off_and_says_so() {
        let (_d, m) = fs_mirror();
        let mut a = Lease::new(m.clone(), KEY, "a", TERM);
        a.take(&Observed::Absent).await.unwrap();
        let (fenced, fence_off) = counter();
        let held = tokio::spawn(hold(a, fence_off));
        tokio::time::sleep(TERM).await;
        assert!(!held.is_finished(), "renewals keep it");
        let mut b = Lease::new(m.clone(), KEY, "b", TERM);
        b.take(&observe(&*m, KEY).await.unwrap()).await.unwrap();
        let stopped = held.await.unwrap();
        assert_eq!(stopped, Stopped::Lost(LostLease { fence: 1 }));
        assert_eq!(fenced.load(std::sync::atomic::Ordering::SeqCst), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_holder_that_cannot_reach_the_mirror_fences_off_before_a_standby_may_take() {
        let dir = tempfile::tempdir().unwrap();
        let flaky = Arc::new(Flaky::new(dir.path()));
        let m: Arc<dyn Mirror> = flaky.clone();
        let mut a = Lease::new(m.clone(), KEY, "a", TERM);
        a.take(&Observed::Absent).await.unwrap();
        let mut w = Watch::new();
        w.see(&observe(&*m, KEY).await.unwrap());
        flaky.cut();
        let start = Instant::now();
        let (fenced, fence_off) = counter();
        let stopped = hold(a, fence_off).await;
        assert_eq!(stopped, Stopped::Lapsed { fence: 1 });
        assert_eq!(fenced.load(std::sync::atomic::Ordering::SeqCst), 1);
        let fenced_after = start.elapsed();
        assert!(
            !w.takeable(MARGIN),
            "the holder stopped before a standby could take"
        );
        assert!(fenced_after < TERM + MARGIN, "{fenced_after:?}");
    }

    #[tokio::test(start_paused = true)]
    async fn a_standby_waits_out_a_live_holder_and_takes_once_it_dies() {
        let (_d, m) = fs_mirror();
        let mut a = Lease::new(m.clone(), KEY, "a", TERM);
        a.take(&Observed::Absent).await.unwrap();
        let (_fenced, fence_off) = counter();
        let held = tokio::spawn(hold(a, fence_off));
        let standby = {
            let m = m.clone();
            tokio::spawn(
                async move { await_takeable(&*m, KEY, MARGIN, Duration::from_secs(1)).await },
            )
        };
        tokio::time::sleep(TERM * 3).await;
        assert!(
            !standby.is_finished(),
            "a renewing holder is never takeable"
        );
        held.abort();
        let died = Instant::now();
        let seen = standby.await.unwrap();
        let waited = died.elapsed();
        assert!(waited >= TERM + MARGIN - renew_every(TERM), "{waited:?}");
        assert!(
            waited <= TERM + MARGIN + Duration::from_secs(1),
            "{waited:?}"
        );
        let mut b = Lease::new(m.clone(), KEY, "b", TERM);
        assert_eq!(b.take(&seen).await.unwrap(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn a_handover_fences_drains_and_releases_for_a_standby_to_take_at_once() {
        let (_d, m) = fs_mirror();
        let mut a = Lease::new(m.clone(), KEY, "a", TERM);
        a.take(&Observed::Absent).await.unwrap();
        let (fenced, fence_off) = counter();
        let drained = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (ask, handovers) = tokio::sync::mpsc::channel(1);
        let metrics = Arc::new(crate::metrics::NestMetrics::default());
        let seen_fenced = fenced.clone();
        let d = drained.clone();
        let held = tokio::spawn(hold_with(
            a,
            fence_off,
            handovers,
            move || {
                let (d, seen_fenced) = (d.clone(), seen_fenced.clone());
                async move {
                    assert_eq!(
                        seen_fenced.load(std::sync::atomic::Ordering::SeqCst),
                        1,
                        "fenced first"
                    );
                    d.store(true, std::sync::atomic::Ordering::SeqCst);
                }
            },
            Some(metrics.clone()),
        ));
        let (reply, released) = tokio::sync::oneshot::channel();
        ask.send(reply).await.unwrap();
        assert_eq!(released.await.unwrap(), Ok(1));
        assert_eq!(held.await.unwrap(), Stopped::HandedOver { fence: 1 });
        assert_eq!(
            metrics.lease(),
            (1, false),
            "the gauge says the lease was let go"
        );
        assert!(drained.load(std::sync::atomic::Ordering::SeqCst));
        let seen = observe(&*m, KEY).await.unwrap();
        assert_eq!(seen.doc().unwrap().owner, None);
        let mut w = Watch::new();
        w.see(&seen);
        assert!(w.takeable(MARGIN), "a released lease is takeable at once");
        assert_eq!(
            Lease::new(m.clone(), KEY, "b", TERM)
                .take(&seen)
                .await
                .unwrap(),
            2
        );
    }

    /// A filesystem mirror whose requests can be made to fail.
    struct Flaky {
        inner: Box<dyn Mirror>,
        down: std::sync::atomic::AtomicBool,
    }

    impl Flaky {
        fn new(root: &std::path::Path) -> Self {
            Self {
                inner: open_mirror(root.to_str().unwrap()).unwrap(),
                down: Default::default(),
            }
        }
        fn cut(&self) {
            self.down.store(true, std::sync::atomic::Ordering::SeqCst);
        }
        fn up(&self) -> Result<()> {
            if self.down.load(std::sync::atomic::Ordering::SeqCst) {
                anyhow::bail!("connection refused");
            }
            Ok(())
        }
    }

    #[async_trait::async_trait]
    impl Mirror for Flaky {
        async fn get(&self, k: &str) -> Result<Option<Vec<u8>>> {
            self.up()?;
            self.inner.get(k).await
        }
        async fn put(&self, k: &str, b: &[u8]) -> Result<()> {
            self.up()?;
            self.inner.put(k, b).await
        }
        async fn put_file(&self, k: &str, s: &std::path::Path) -> Result<()> {
            self.up()?;
            self.inner.put_file(k, s).await
        }
        async fn put_if(&self, k: &str, b: &[u8], e: Option<&[u8]>) -> Result<()> {
            self.up()?;
            self.inner.put_if(k, b, e).await
        }
        async fn head(&self, k: &str) -> Result<Option<crate::publish::Head>> {
            self.up()?;
            self.inner.head(k).await
        }
    }

    struct Unreachable;

    #[async_trait::async_trait]
    impl Mirror for Unreachable {
        async fn get(&self, _: &str) -> Result<Option<Vec<u8>>> {
            anyhow::bail!("connection refused")
        }
        async fn put(&self, _: &str, _: &[u8]) -> Result<()> {
            anyhow::bail!("connection refused")
        }
        async fn put_file(&self, _: &str, _: &std::path::Path) -> Result<()> {
            anyhow::bail!("connection refused")
        }
        async fn put_if(&self, _: &str, _: &[u8], _: Option<&[u8]>) -> Result<()> {
            anyhow::bail!("connection refused")
        }
        async fn head(&self, _: &str) -> Result<Option<crate::publish::Head>> {
            anyhow::bail!("connection refused")
        }
        async fn list(&self, _: &str) -> Result<Vec<String>> {
            anyhow::bail!("connection refused")
        }
    }

    #[cfg(feature = "object-store")]
    #[tokio::test]
    async fn the_object_store_mirror_arbitrates_the_race_too() {
        let m: Arc<dyn Mirror> = open_mirror("memory://rfc-0078-s0-race").unwrap().into();
        let seen = observe(&*m, KEY).await.unwrap();
        let mut b = Lease::new(m.clone(), KEY, "b", TERM);
        let mut c = Lease::new(m.clone(), KEY, "c", TERM);
        let (rb, rc) = tokio::join!(b.take(&seen), c.take(&seen));
        assert!(
            rb.is_ok() ^ rc.is_ok(),
            "exactly one take wins: {rb:?} {rc:?}"
        );
        assert!(lost(&rb.err().or(rc.err()).unwrap()).is_some());
    }
}
