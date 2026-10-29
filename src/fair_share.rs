//! Fair share between tenant labels (RFC-0073 FR2): a cursor's SQL permits and the process's response
//! bytes in flight, divided by the opaque tenant string a mount carries.
//!
//! Admitted work is never preempted, so queueing alone cannot bound how long one tenant waits behind
//! another. A gate's permits are divided between its **active** tenants (RFC-0076 §3.1), those with a
//! query queued or running on it, so a tenant with a thousand idle mounts holds nothing. To let a
//! tenant that becomes active find a permit free, the gate keeps a **reserve** of `max(1, permits/8)`
//! that only a tenant holding none may take: one already holding a permit borrows only while the
//! reserve stays free. Weights order the queue when several tenants wait, and set the share a refusal
//! is measured against. Response bytes keep a reservation per mounted tenant: a slice of half the
//! budget.
//!
//! A gate with one tenant, or none, behaves as the plain semaphore it wraps: no reserve is kept
//! for anybody else, and a refusal is `busy` as it always was.
//!
//! The hot-tip rows a query copies (RFC-0073 FR5) are a third budget, one per gate, divided the same
//! way as response bytes. They are a different memory from a response, held while the query runs
//! rather than while its answer is sent, and both can be in flight for one tenant at once.

use std::collections::{BTreeMap, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};

use crate::runtime::TenantShare;

/// How often a waiter re-reads the gate besides being woken: a permit taken straight off the
/// semaphore, as a test does, is released without a wake.
const RECHECK: Duration = Duration::from_millis(20);

/// Why admission said no.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Refusal {
    /// The gate is full and the tenant is under its share.
    Busy,
    /// The tenant is at its own share, or every reservation is taken.
    TenantShare,
}

impl Refusal {
    pub fn metric(self) -> crate::metrics::SqlRejection {
        match self {
            Self::Busy => crate::metrics::SqlRejection::Busy,
            Self::TenantShare => crate::metrics::SqlRejection::TenantShare,
        }
    }
}

/// One cursor's analytical gate, shared by every nest on it.
///
/// Derefs to the semaphore, which is what system work and tests that hold a permit directly use.
#[derive(Clone)]
pub struct SqlGate {
    permits: Arc<Semaphore>,
    shares: Arc<GateShares>,
    hot: Arc<ByteShares>,
}

/// A query's hot copy refused before a row of it was parsed, for the reason given.
#[derive(Debug)]
pub struct HotShareRefused(pub Refusal);

impl std::fmt::Display for HotShareRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "no room for this query's hot-tip copy ({:?})", self.0)
    }
}

impl std::error::Error for HotShareRefused {}

struct GateShares {
    ledger: Mutex<GateLedger>,
    changed: Notify,
}

/// Upper bounds, in seconds, of the admission-wait histogram. The wait is capped at 250 ms, so the
/// last bound is the cap and `+Inf` holds only what overran it.
pub const WAIT_BOUNDS: [f64; 7] = [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25];

/// One tenant's admissions on one gate since the process started.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct WaitStats {
    /// Admissions that found no permit at once and queued.
    pub waits: u64,
    /// Cumulative counts per [`WAIT_BOUNDS`] entry, then `+Inf`, of every admission's wait.
    pub buckets: [u64; WAIT_BOUNDS.len() + 1],
    pub sum_secs: f64,
    pub count: u64,
    pub rejected_busy: u64,
    pub rejected_tenant_share: u64,
}

impl WaitStats {
    fn observe(&mut self, waited: Duration, queued: bool, refused: Option<Refusal>) {
        let secs = waited.as_secs_f64();
        self.waits += u64::from(queued);
        self.count += 1;
        self.sum_secs += secs;
        for (i, bound) in WAIT_BOUNDS.iter().enumerate() {
            if secs <= *bound {
                self.buckets[i] += 1;
            }
        }
        self.buckets[WAIT_BOUNDS.len()] += 1;
        match refused {
            Some(Refusal::Busy) => self.rejected_busy += 1,
            Some(Refusal::TenantShare) => self.rejected_tenant_share += 1,
            None => {}
        }
    }

    /// The smallest bound holding at least `q` of the admissions, `None` past the last bound.
    pub fn quantile(&self, q: f64) -> Option<f64> {
        let want = (self.count as f64 * q).ceil().max(1.0) as u64;
        WAIT_BOUNDS
            .iter()
            .zip(self.buckets)
            .find(|(_, n)| *n >= want)
            .map(|(b, _)| *b)
    }
}

/// What one tenant holds and has waited on one gate, for `GET /_admin/tenants`.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct GateTenantReport {
    pub active: bool,
    pub held: usize,
    pub stats: WaitStats,
}

#[derive(Default)]
struct GateLedger {
    total: usize,
    /// Permits a lowered total still has to take out of circulation as they come back.
    debt: usize,
    tenants: BTreeMap<Arc<str>, GateTenant>,
    shares: BTreeMap<Arc<str>, TenantShare>,
    next_ticket: u64,
    /// Seated tenants holding no permit: those the reserve is kept for.
    idle_seated: usize,
    /// Tenants with a query queued or running here, and their weights summed.
    active: usize,
    active_weight: u64,
    /// Tenants with a waiter, so ordering looks only at them and not at every mount.
    queued: std::collections::BTreeSet<Arc<str>>,
    stats: BTreeMap<Arc<str>, WaitStats>,
}

#[derive(Default)]
struct GateTenant {
    /// One entry per mount seated here, by its dataset directory.
    seats: Vec<PathBuf>,
    held: usize,
    waiting: VecDeque<u64>,
    /// What this tenant adds to the ledger's tallies, so a change can take back exactly that.
    counted_idle: bool,
    counted_weight: Option<u64>,
}

impl GateTenant {
    fn seated(&self) -> bool {
        !self.seats.is_empty()
    }

    fn active(&self) -> bool {
        self.held > 0 || !self.waiting.is_empty()
    }
}

/// Permits only a tenant holding none may take: `max(1, total / 8)`.
pub fn reserve(total: usize) -> usize {
    (total / 8).max(1)
}

impl GateLedger {
    fn share(&self, tenant: &str) -> TenantShare {
        self.shares.get(tenant).copied().unwrap_or_default()
    }

    fn held(&self, tenant: &str) -> usize {
        self.tenants.get(tenant).map_or(0, |t| t.held)
    }

    /// Bring the tallies in line with `tenant` after anything about it changed, then drop it if
    /// nothing is left of it.
    fn refresh(&mut self, tenant: &str) {
        let weight = self.share(tenant).weight as u64;
        let Some((key, t)) = self.tenants.get_key_value(tenant) else {
            return;
        };
        let key = Arc::clone(key);
        let idle = t.seated() && t.held == 0;
        let active = t.active().then_some(weight);
        let (was_idle, was_active) = (t.counted_idle, t.counted_weight);
        let queued = !t.waiting.is_empty();
        let gone = !t.seated() && t.held == 0 && t.waiting.is_empty();
        self.idle_seated = self.idle_seated + usize::from(idle) - usize::from(was_idle);
        if let Some(w) = was_active {
            self.active -= 1;
            self.active_weight -= w;
        }
        if let Some(w) = active {
            self.active += 1;
            self.active_weight += w;
        }
        if queued {
            self.queued.insert(Arc::clone(&key));
        } else {
            self.queued.remove(tenant);
        }
        if gone {
            self.tenants.remove(tenant);
            self.stats.remove(tenant);
        } else if let Some(t) = self.tenants.get_mut(tenant) {
            t.counted_idle = idle;
            t.counted_weight = active;
        }
    }

    fn entry(&mut self, tenant: &str) -> &mut GateTenant {
        if !self.tenants.contains_key(tenant) {
            self.tenants
                .insert(Arc::from(tenant), GateTenant::default());
        }
        self.tenants.get_mut(tenant).expect("inserted above")
    }

    /// Seated tenants other than `tenant` holding no permit.
    fn others_idle(&self, tenant: &str) -> usize {
        let mine = self.tenants.get(tenant).is_some_and(|t| t.counted_idle);
        self.idle_seated - usize::from(mine)
    }

    /// Active tenants other than `tenant`, and their weights.
    fn others_active(&self, tenant: &str) -> (usize, u64) {
        match self.tenants.get(tenant).and_then(|t| t.counted_weight) {
            Some(w) => (self.active - 1, self.active_weight - w),
            None => (self.active, self.active_weight),
        }
    }

    /// The permits `tenant` is entitled to by weight among the active tenants, itself counted as one,
    /// at least one.
    fn fair(&self, tenant: &str) -> usize {
        let mine = self.share(tenant).weight as u64;
        let weights = self.others_active(tenant).1 + mine;
        let fair = ((self.total as u64 * mine) / weights.max(1)) as usize;
        let fair = fair.max(1);
        self.share(tenant)
            .max_permits
            .map_or(fair, |cap| fair.min(cap))
    }

    /// What `tenant` must leave free on the semaphore before it may take one more permit.
    fn kept_from(&self, tenant: &str) -> usize {
        if self.held(tenant) == 0 || self.others_idle(tenant) == 0 {
            0
        } else {
            reserve(self.total)
        }
    }

    /// Whether `tenant` may take one permit with `free` on the semaphore, and why not.
    fn may_take(&self, tenant: &str, free: usize) -> Result<(), Refusal> {
        let held = self.held(tenant);
        if self
            .share(tenant)
            .max_permits
            .is_some_and(|cap| held >= cap)
        {
            return Err(Refusal::TenantShare);
        }
        if free > self.kept_from(tenant) {
            Ok(())
        } else {
            Err(self.reason(tenant, free))
        }
    }

    fn reason(&self, tenant: &str, free: usize) -> Refusal {
        let held = self.held(tenant);
        let capped = self
            .share(tenant)
            .max_permits
            .is_some_and(|cap| held >= cap);
        let others = self.others_active(tenant).0 > 0 || self.others_idle(tenant) > 0;
        if capped || (others && held > 0 && (free > 0 || held >= self.fair(tenant))) {
            Refusal::TenantShare
        } else {
            Refusal::Busy
        }
    }

    /// Whether `a` is served before `b`: fewer permits per unit of weight, then the older ticket.
    fn before(&self, a: (&str, u64), b: (&str, u64)) -> bool {
        let (wa, wb) = (
            self.share(a.0).weight as u128,
            self.share(b.0).weight as u128,
        );
        let (ha, hb) = (self.held(a.0) as u128, self.held(b.0) as u128);
        match (ha * wb).cmp(&(hb * wa)) {
            std::cmp::Ordering::Less => true,
            std::cmp::Ordering::Greater => false,
            std::cmp::Ordering::Equal => a.1 < b.1,
        }
    }

    /// Whether `tenant`, holding `ticket` (`u64::MAX` for a request that has not queued), is next.
    fn next_in_line(&self, tenant: &str, ticket: u64, free: usize) -> bool {
        if let Some(front) = self.tenants.get(tenant).and_then(|t| t.waiting.front()) {
            if *front != ticket {
                return false;
            }
        }
        !self.queued.iter().any(|other| {
            &**other != tenant
                && self
                    .tenants
                    .get(other)
                    .and_then(|t| t.waiting.front())
                    .is_some_and(|&front| {
                        self.may_take(other, free).is_ok()
                            && self.before((other, front), (tenant, ticket))
                    })
        })
    }

    /// Hand back a permit, or take it out of circulation if a lowered total is still owed.
    fn give_back(&mut self, permit: OwnedSemaphorePermit, semaphore: &Semaphore) {
        if self.debt > 0 {
            permit.forget();
            self.debt -= 1;
        } else {
            drop(permit);
        }
        self.settle(semaphore);
    }

    fn settle(&mut self, semaphore: &Semaphore) {
        if self.debt > 0 {
            self.debt -= semaphore.forget_permits(self.debt);
        }
    }
}

/// A permit from a [`SqlGate`], charged to a tenant or to nobody, released on drop.
pub struct GatePermit {
    permit: Option<OwnedSemaphorePermit>,
    semaphore: Arc<Semaphore>,
    shares: Arc<GateShares>,
    tenant: Option<Arc<str>>,
}

impl Drop for GatePermit {
    fn drop(&mut self) {
        let mut l = lock(&self.shares.ledger);
        if let Some(permit) = self.permit.take() {
            l.give_back(permit, &self.semaphore);
        }
        if let Some(tenant) = &self.tenant {
            if let Some(t) = l.tenants.get_mut(tenant) {
                t.held = t.held.saturating_sub(1);
            }
            l.refresh(tenant);
        }
        drop(l);
        self.shares.changed.notify_waiters();
    }
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|p| p.into_inner())
}

impl std::ops::Deref for SqlGate {
    type Target = Arc<Semaphore>;
    fn deref(&self) -> &Arc<Semaphore> {
        &self.permits
    }
}

impl SqlGate {
    pub fn new(permits: usize) -> Self {
        Self::with_hot_bytes(permits, crate::serve::SQL_MAX_HOT_INFLIGHT_BYTES)
    }

    /// A gate whose queries may hold `hot_bytes` of hot-tip copies at once.
    pub fn with_hot_bytes(permits: usize, hot_bytes: u64) -> Self {
        Self {
            permits: Arc::new(Semaphore::new(permits)),
            shares: Arc::new(GateShares {
                ledger: Mutex::new(GateLedger {
                    total: permits,
                    ..GateLedger::default()
                }),
                changed: Notify::new(),
            }),
            hot: Arc::new(ByteShares::uncapped((hot_bytes / 1024).max(1) as usize)),
        }
    }

    pub fn total(&self) -> usize {
        lock(&self.shares.ledger).total
    }

    /// Change how many permits the gate has (RFC-0076 §3.1). A raise is free at once; a cut takes
    /// free permits now and the rest as running queries finish, never preempting one.
    pub fn set_total(&self, permits: usize) {
        let permits = permits.max(1);
        let mut l = lock(&self.shares.ledger);
        let old = l.total;
        l.total = permits;
        if permits > old {
            let owed = l.debt.min(permits - old);
            l.debt -= owed;
            self.permits.add_permits(permits - old - owed);
        } else {
            l.debt += old - permits;
            l.settle(&self.permits);
        }
        drop(l);
        self.shares.changed.notify_waiters();
    }

    /// The hot-tip bytes this gate's queries may hold copied at once.
    pub fn hot_total_bytes(&self) -> u64 {
        self.hot.total as u64 * 1024
    }

    /// Room for a hot copy of `bytes`, charged to `tenant` until the permit drops.
    pub fn charge_hot(&self, bytes: u64, tenant: Option<&str>) -> Result<BytePermit, Refusal> {
        self.hot
            .reserve(usize::try_from(bytes).unwrap_or(usize::MAX), tenant)
    }

    /// KiB of hot copies held per tenant right now.
    pub fn hot_used(&self) -> BTreeMap<String, usize> {
        self.hot.used()
    }

    /// Set what `tenant` is entitled to on this gate.
    pub fn set_share(&self, tenant: &str, share: TenantShare) {
        self.hot.set_share(tenant, share);
        let mut l = lock(&self.shares.ledger);
        if share == TenantShare::default() {
            l.shares.remove(tenant);
        } else {
            l.shares.insert(Arc::from(tenant), share);
        }
        l.refresh(tenant);
        drop(l);
        self.shares.changed.notify_waiters();
    }

    /// Permits held per tenant right now.
    pub fn held(&self) -> BTreeMap<String, usize> {
        lock(&self.shares.ledger)
            .tenants
            .iter()
            .filter(|(_, t)| t.held > 0)
            .map(|(k, t)| (k.to_string(), t.held))
            .collect()
    }

    /// Tenants with a query queued or running on this gate.
    pub fn active(&self) -> usize {
        lock(&self.shares.ledger).active
    }

    /// Every tenant this gate has seen: whether it is active, what it holds and how it has waited.
    pub fn tenant_reports(&self) -> BTreeMap<String, GateTenantReport> {
        let l = lock(&self.shares.ledger);
        let mut out: BTreeMap<String, GateTenantReport> = l
            .stats
            .iter()
            .map(|(k, s)| {
                (
                    k.to_string(),
                    GateTenantReport {
                        stats: s.clone(),
                        ..GateTenantReport::default()
                    },
                )
            })
            .collect();
        for (k, t) in &l.tenants {
            let r = out.entry(k.to_string()).or_default();
            r.active = t.active();
            r.held = t.held;
        }
        out
    }

    /// Record one admission of `tenant`'s: how long it took, whether it queued, and how it ended.
    pub fn observe_admission(
        &self,
        tenant: &str,
        waited: Duration,
        queued: bool,
        refused: Option<Refusal>,
    ) {
        let mut l = lock(&self.shares.ledger);
        // Kept only while the tenant is on this gate, so labels that come and go leave nothing behind.
        if !l.tenants.contains_key(tenant) {
            return;
        }
        match l.stats.get_mut(tenant) {
            Some(s) => s.observe(waited, queued, refused),
            None => {
                let mut s = WaitStats::default();
                s.observe(waited, queued, refused);
                l.stats.insert(Arc::from(tenant), s);
            }
        }
    }

    fn seat(&self, tenant: &Arc<str>, dir: &Path) {
        let mut l = lock(&self.shares.ledger);
        l.entry(tenant).seats.push(dir.to_path_buf());
        l.refresh(tenant);
        drop(l);
        self.hot.seat(tenant);
        self.shares.changed.notify_waiters();
    }

    fn unseat(&self, tenant: &str, dir: &Path) {
        self.hot.unseat(tenant);
        let mut l = lock(&self.shares.ledger);
        if let Some(t) = l.tenants.get_mut(tenant) {
            if let Some(i) = t.seats.iter().position(|d| d == dir) {
                t.seats.remove(i);
            }
        }
        l.refresh(tenant);
        drop(l);
        self.shares.changed.notify_waiters();
    }

    fn permit(&self, permit: OwnedSemaphorePermit, tenant: Option<Arc<str>>) -> GatePermit {
        GatePermit {
            permit: Some(permit),
            semaphore: Arc::clone(&self.permits),
            shares: Arc::clone(&self.shares),
            tenant,
        }
    }

    fn grant(&self, l: &mut GateLedger, tenant: &str, ticket: u64) -> Result<GatePermit, Refusal> {
        let free = self.permits.available_permits();
        l.may_take(tenant, free)?;
        if !l.next_in_line(tenant, ticket, free) {
            return Err(l.reason(tenant, free));
        }
        let permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| l.reason(tenant, 0))?;
        let entry = l.entry(tenant);
        if entry.waiting.front() == Some(&ticket) {
            entry.waiting.pop_front();
        }
        entry.held += 1;
        l.refresh(tenant);
        let key = l
            .tenants
            .get_key_value(tenant)
            .map(|(k, _)| Arc::clone(k))
            .expect("held a permit, so present");
        Ok(self.permit(permit, Some(key)))
    }

    /// A permit now, or why not. `None` is untenanted: the bare semaphore, as before FR2.
    pub fn try_admit(&self, tenant: Option<&str>) -> Result<GatePermit, Refusal> {
        let Some(tenant) = tenant else {
            return Arc::clone(&self.permits)
                .try_acquire_owned()
                .map(|p| self.permit(p, None))
                .map_err(|_| Refusal::Busy);
        };
        let r = self.grant(&mut lock(&self.shares.ledger), tenant, u64::MAX);
        if r.is_ok() {
            self.shares.changed.notify_waiters();
        }
        r
    }

    /// Wait up to `wait` for a permit, in weighted order among the tenants waiting.
    pub async fn wait_admit(
        &self,
        tenant: Option<&str>,
        wait: Duration,
    ) -> Result<GatePermit, Refusal> {
        let Some(tenant) = tenant else {
            return match tokio::time::timeout(wait, Arc::clone(&self.permits).acquire_owned()).await
            {
                Ok(Ok(p)) => Ok(self.permit(p, None)),
                _ => Err(Refusal::Busy),
            };
        };
        let deadline = tokio::time::Instant::now() + wait;
        let ticket = {
            let mut l = lock(&self.shares.ledger);
            let ticket = l.next_ticket;
            l.next_ticket += 1;
            l.entry(tenant).waiting.push_back(ticket);
            l.refresh(tenant);
            ticket
        };
        let _queued = Ticket {
            gate: self,
            tenant,
            ticket,
        };
        loop {
            let changed = self.shares.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            let attempt = self.grant(&mut lock(&self.shares.ledger), tenant, ticket);
            match attempt {
                Ok(p) => {
                    self.shares.changed.notify_waiters();
                    return Ok(p);
                }
                Err(why) if tokio::time::Instant::now() >= deadline => return Err(why),
                Err(_) => {}
            }
            let tick = (tokio::time::Instant::now() + RECHECK).min(deadline);
            tokio::select! {
                _ = changed => {}
                _ = tokio::time::sleep_until(tick) => {}
            }
        }
    }

    /// A permit for background work on `dir`'s behalf, charged to a tenant seated with it, or `None`.
    /// Work for a dataset no tenant has seated borrows, and so leaves the reserve free.
    pub fn try_admit_for(&self, dir: &Path) -> Option<GatePermit> {
        let mut l = lock(&self.shares.ledger);
        let owners: Vec<Arc<str>> = l
            .tenants
            .iter()
            .filter(|(_, t)| t.seats.iter().any(|d| d == dir))
            .map(|(k, _)| Arc::clone(k))
            .collect();
        if owners.is_empty() {
            let kept = if l.idle_seated > 0 {
                reserve(l.total)
            } else {
                0
            };
            if self.permits.available_permits() <= kept {
                return None;
            }
            drop(l);
            return Arc::clone(&self.permits)
                .try_acquire_owned()
                .ok()
                .map(|p| self.permit(p, None));
        }
        let granted = owners
            .iter()
            .find_map(|t| self.grant(&mut l, t, u64::MAX).ok());
        drop(l);
        if granted.is_some() {
            self.shares.changed.notify_waiters();
        }
        granted
    }
}

/// Takes the waiter's ticket out of the queue however its wait ends.
struct Ticket<'a> {
    gate: &'a SqlGate,
    tenant: &'a str,
    ticket: u64,
}

impl Drop for Ticket<'_> {
    fn drop(&mut self) {
        let mut l = lock(&self.gate.shares.ledger);
        if let Some(t) = l.tenants.get_mut(self.tenant) {
            t.waiting.retain(|&k| k != self.ticket);
        }
        l.refresh(self.tenant);
        drop(l);
        self.gate.shares.changed.notify_waiters();
    }
}

/// A budget in KiB divided between tenant labels: the process's response bytes in flight, or a gate's
/// hot-tip copies.
pub struct ByteShares {
    total: usize,
    permits: Arc<Semaphore>,
    ledger: Mutex<ByteLedger>,
    /// Whether a tenant's `max_response_mib` caps it; it names response bytes and nothing else.
    capped: bool,
}

#[derive(Default)]
struct ByteLedger {
    tenants: BTreeMap<Arc<str>, ByteTenant>,
    shares: BTreeMap<Arc<str>, TenantShare>,
}

#[derive(Default)]
struct ByteTenant {
    seats: usize,
    used: usize,
}

impl ByteLedger {
    fn share(&self, tenant: &str) -> TenantShare {
        self.shares.get(tenant).copied().unwrap_or_default()
    }

    /// Each seated tenant's reserved KiB: half the budget, by weight, once there is someone to
    /// reserve it from. The other half is borrowed first come.
    fn reserved(&self, total: usize) -> BTreeMap<&str, usize> {
        let seated: Vec<&str> = self
            .tenants
            .iter()
            .filter(|(_, t)| t.seats > 0)
            .map(|(k, _)| &**k)
            .collect();
        if seated.len() < 2 {
            return BTreeMap::new();
        }
        let weights: u64 = seated.iter().map(|t| self.share(t).weight as u64).sum();
        seated
            .into_iter()
            .map(|t| {
                let w = self.share(t).weight as u64;
                (t, ((total as u64 / 2) * w / weights.max(1)) as usize)
            })
            .collect()
    }

    fn used(&self, tenant: &str) -> usize {
        self.tenants.get(tenant).map_or(0, |t| t.used)
    }

    fn gc(&mut self, tenant: &str) {
        if self
            .tenants
            .get(tenant)
            .is_some_and(|t| t.seats == 0 && t.used == 0)
        {
            self.tenants.remove(tenant);
        }
    }
}

/// Room for one response body or hot copy, released when it is dropped.
pub struct BytePermit {
    permit: Option<OwnedSemaphorePermit>,
    charge: Option<(Arc<ByteShares>, Arc<str>, usize)>,
}

impl Drop for BytePermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        if let Some((shares, tenant, kib)) = self.charge.take() {
            let mut l = lock(&shares.ledger);
            if let Some(t) = l.tenants.get_mut(&tenant) {
                t.used = t.used.saturating_sub(kib);
            }
            l.gc(&tenant);
        }
    }
}

impl ByteShares {
    pub fn new(kib: usize) -> Self {
        Self {
            total: kib,
            permits: Arc::new(Semaphore::new(kib)),
            ledger: Mutex::default(),
            capped: true,
        }
    }

    fn uncapped(kib: usize) -> Self {
        Self {
            capped: false,
            ..Self::new(kib)
        }
    }

    pub fn set_share(&self, tenant: &str, share: TenantShare) {
        let mut l = lock(&self.ledger);
        if share == TenantShare::default() {
            l.shares.remove(tenant);
        } else {
            l.shares.insert(Arc::from(tenant), share);
        }
    }

    /// KiB held per tenant right now.
    pub fn used(&self) -> BTreeMap<String, usize> {
        lock(&self.ledger)
            .tenants
            .iter()
            .filter(|(_, t)| t.used > 0)
            .map(|(k, t)| (k.to_string(), t.used))
            .collect()
    }

    fn seat(&self, tenant: &Arc<str>) {
        lock(&self.ledger)
            .tenants
            .entry(Arc::clone(tenant))
            .or_default()
            .seats += 1;
    }

    fn unseat(&self, tenant: &str) {
        let mut l = lock(&self.ledger);
        if let Some(t) = l.tenants.get_mut(tenant) {
            t.seats = t.seats.saturating_sub(1);
        }
        l.gc(tenant);
    }

    /// Room for `len` bytes now, or why not.
    pub fn reserve(
        self: &Arc<Self>,
        len: usize,
        tenant: Option<&str>,
    ) -> Result<BytePermit, Refusal> {
        let kib = len.div_ceil(1024).clamp(1, self.total);
        let Some(tenant) = tenant else {
            return Arc::clone(&self.permits)
                .try_acquire_many_owned(kib as u32)
                .map(|p| BytePermit {
                    permit: Some(p),
                    charge: None,
                })
                .map_err(|_| Refusal::Busy);
        };
        let mut l = lock(&self.ledger);
        let used = l.used(tenant);
        let cap = l
            .share(tenant)
            .max_response_mib
            .filter(|_| self.capped)
            .map(|mib| (mib as usize).saturating_mul(1024));
        if cap.is_some_and(|cap| used + kib > cap) {
            return Err(Refusal::TenantShare);
        }
        let reserved = l.reserved(self.total);
        let others: usize = reserved
            .iter()
            .filter(|(t, _)| **t != tenant)
            .map(|(t, r)| r.saturating_sub(l.used(t)))
            .sum();
        let mine = reserved.get(tenant).copied();
        let refuse = || match mine {
            Some(mine) if used + kib > mine => Refusal::TenantShare,
            _ => Refusal::Busy,
        };
        if self.permits.available_permits() < kib + others {
            return Err(refuse());
        }
        let permit = Arc::clone(&self.permits)
            .try_acquire_many_owned(kib as u32)
            .map_err(|_| refuse())?;
        let key = Arc::<str>::from(tenant);
        l.tenants.entry(Arc::clone(&key)).or_default().used += kib;
        Ok(BytePermit {
            permit: Some(permit),
            charge: Some((Arc::clone(self), key, kib)),
        })
    }
}

/// A mount's place at its cursor's gate and in the process's response bytes, under its tenant label.
/// Held by the mount's serving state; dropping the last clone gives the reservation back.
pub struct Seat {
    tenant: Arc<str>,
    dir: PathBuf,
    gate: SqlGate,
    bytes: Arc<ByteShares>,
}

impl Seat {
    pub fn new(
        tenant: &str,
        dir: &Path,
        gate: &SqlGate,
        bytes: &Arc<ByteShares>,
        share: TenantShare,
    ) -> Self {
        let tenant: Arc<str> = Arc::from(tenant);
        gate.set_share(&tenant, share);
        bytes.set_share(&tenant, share);
        gate.seat(&tenant, dir);
        bytes.seat(&tenant);
        Self {
            tenant,
            dir: dir.to_path_buf(),
            gate: gate.clone(),
            bytes: Arc::clone(bytes),
        }
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }
}

impl Drop for Seat {
    fn drop(&mut self) {
        self.gate.unseat(&self.tenant, &self.dir);
        self.bytes.unseat(&self.tenant);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn share(weight: u32, max_permits: Option<usize>) -> TenantShare {
        TenantShare {
            weight,
            max_permits,
            max_response_mib: None,
        }
    }

    fn seat(gate: &SqlGate, bytes: &Arc<ByteShares>, tenant: &str) -> Seat {
        Seat::new(
            tenant,
            Path::new(&format!("/data/{tenant}")),
            gate,
            bytes,
            TenantShare::default(),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn one_tenant_uses_the_whole_gate_and_is_told_busy() {
        let (gate, bytes) = (SqlGate::new(2), Arc::new(ByteShares::new(64)));
        let _a = seat(&gate, &bytes, "a");
        let held = [gate.try_admit(Some("a")), gate.try_admit(Some("a"))];
        assert!(held.iter().all(Result::is_ok));
        assert_eq!(gate.try_admit(Some("a")).err(), Some(Refusal::Busy));
        let waited = gate.wait_admit(Some("a"), Duration::from_millis(250)).await;
        assert_eq!(waited.err(), Some(Refusal::Busy));
    }

    #[tokio::test(start_paused = true)]
    async fn a_borrower_leaves_the_reserve_free() {
        let (gate, bytes) = (SqlGate::new(4), Arc::new(ByteShares::new(64)));
        let (_a, _b, _c) = (
            seat(&gate, &bytes, "a"),
            seat(&gate, &bytes, "b"),
            seat(&gate, &bytes, "c"),
        );
        let a: Vec<_> = (0..3).map(|_| gate.try_admit(Some("a")).unwrap()).collect();
        assert_eq!(gate.try_admit(Some("a")).err(), Some(Refusal::TenantShare));
        let b = gate.try_admit(Some("b")).unwrap();
        assert_eq!(gate.available_permits(), 0);
        assert_eq!(gate.try_admit(Some("c")).err(), Some(Refusal::Busy));
        drop((a, b));
        assert_eq!(gate.available_permits(), 4);
        assert!(gate.held().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn a_tenant_under_its_reservation_never_waits_behind_another() {
        let (gate, bytes) = (SqlGate::new(2), Arc::new(ByteShares::new(64)));
        let (_a, _b) = (seat(&gate, &bytes, "a"), seat(&gate, &bytes, "b"));
        let a = gate.try_admit(Some("a")).unwrap();
        let start = tokio::time::Instant::now();
        let refused = gate.wait_admit(Some("a"), Duration::from_millis(250)).await;
        assert_eq!(refused.err(), Some(Refusal::TenantShare));
        assert!(start.elapsed() >= Duration::from_millis(250));
        let start = tokio::time::Instant::now();
        let b = gate.wait_admit(Some("b"), Duration::from_millis(250)).await;
        assert!(b.is_ok());
        assert_eq!(start.elapsed(), Duration::ZERO);
        drop(a);
    }

    #[tokio::test(start_paused = true)]
    async fn more_tenants_than_permits_queue_for_a_reservation() {
        let (gate, bytes) = (SqlGate::new(2), Arc::new(ByteShares::new(64)));
        let (_a, _b, _c) = (
            seat(&gate, &bytes, "a"),
            seat(&gate, &bytes, "b"),
            seat(&gate, &bytes, "c"),
        );
        let a = gate.try_admit(Some("a")).unwrap();
        assert_eq!(gate.try_admit(Some("a")).err(), Some(Refusal::TenantShare));
        let _b = gate.try_admit(Some("b")).unwrap();
        let waiting = {
            let gate = gate.clone();
            tokio::spawn(
                async move { gate.wait_admit(Some("c"), Duration::from_millis(250)).await },
            )
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(a);
        let _c = waiting
            .await
            .unwrap()
            .expect("c takes the permit a gave back");
        // a now holds none and is under its share, so a full gate is `busy` to it.
        let refused = gate.wait_admit(Some("a"), Duration::from_millis(250)).await;
        assert_eq!(refused.err(), Some(Refusal::Busy));
    }

    #[tokio::test(start_paused = true)]
    async fn a_freed_permit_goes_to_the_lighter_tenant_by_weight() {
        let (gate, bytes) = (SqlGate::new(4), Arc::new(ByteShares::new(64)));
        let (_a, _b) = (seat(&gate, &bytes, "a"), seat(&gate, &bytes, "b"));
        gate.set_share("a", share(3, None));
        let mut a: Vec<_> = (0..2).map(|_| gate.try_admit(Some("a")).unwrap()).collect();
        let mut b: Vec<_> = (0..2).map(|_| gate.try_admit(Some("b")).unwrap()).collect();
        // Once b frees one, b at 1/1 is still heavier than a at 2/3, so a's waiter goes first.
        let wb = {
            let gate = gate.clone();
            tokio::spawn(
                async move { gate.wait_admit(Some("b"), Duration::from_millis(250)).await },
            )
        };
        tokio::time::sleep(Duration::from_millis(1)).await;
        let wa = {
            let gate = gate.clone();
            tokio::spawn(
                async move { gate.wait_admit(Some("a"), Duration::from_millis(250)).await },
            )
        };
        tokio::time::sleep(Duration::from_millis(1)).await;
        drop(b.pop());
        let a3 = wa.await.unwrap().expect("a is served first");
        assert_eq!(gate.held().get("a"), Some(&3));
        drop(a.pop());
        let b2 = wb
            .await
            .unwrap()
            .expect("b is served when a's permit frees");
        drop((a, a3, b, b2));
    }

    #[tokio::test(start_paused = true)]
    async fn a_cap_refuses_at_the_cap_even_alone() {
        let (gate, bytes) = (SqlGate::new(4), Arc::new(ByteShares::new(64)));
        let _a = seat(&gate, &bytes, "a");
        gate.set_share("a", share(1, Some(1)));
        let _held = gate.try_admit(Some("a")).unwrap();
        assert_eq!(gate.try_admit(Some("a")).err(), Some(Refusal::TenantShare));
        gate.set_share("a", TenantShare::default());
        assert!(gate.try_admit(Some("a")).is_ok(), "a live change applies");
    }

    #[tokio::test(start_paused = true)]
    async fn an_unseated_tenant_gives_its_reservation_back() {
        let (gate, bytes) = (SqlGate::new(2), Arc::new(ByteShares::new(64)));
        let a = seat(&gate, &bytes, "a");
        let b = seat(&gate, &bytes, "b");
        let _a1 = gate.try_admit(Some("a")).unwrap();
        assert!(gate.try_admit(Some("a")).is_err());
        drop(b);
        assert!(gate.try_admit(Some("a")).is_ok());
        drop(a);
    }

    #[test]
    fn background_work_borrows_only_what_the_reserve_does_not_need() {
        let (gate, bytes) = (SqlGate::new(2), Arc::new(ByteShares::new(64)));
        let (_a, _b) = (seat(&gate, &bytes, "a"), seat(&gate, &bytes, "b"));
        let unowned = gate
            .try_admit_for(Path::new("/elsewhere"))
            .expect("one permit is free past the reserve");
        assert!(gate.try_admit_for(Path::new("/elsewhere")).is_none());
        drop(unowned);
        let build = gate
            .try_admit_for(Path::new("/data/a"))
            .expect("charged to a");
        assert_eq!(gate.held().get("a"), Some(&1));
        assert!(gate.try_admit(Some("b")).is_ok());
        drop(build);
    }

    fn seat_many(gate: &SqlGate, bytes: &Arc<ByteShares>, n: usize) -> Vec<Seat> {
        (0..n)
            .map(|i| seat(gate, bytes, &format!("t{i}")))
            .collect()
    }

    /// Take permits for `tenant` until the gate says no, and say why it did.
    fn take_all(gate: &SqlGate, tenant: &str) -> (Vec<GatePermit>, Refusal) {
        let mut held = Vec::new();
        loop {
            match gate.try_admit(Some(tenant)) {
                Ok(p) => held.push(p),
                Err(why) => return (held, why),
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn idle_tenants_hold_nothing() {
        let (gate, bytes) = (SqlGate::new(16), Arc::new(ByteShares::new(64)));
        let _seats = seat_many(&gate, &bytes, 1000);
        assert_eq!(gate.active(), 0);
        let (held, why) = take_all(&gate, "t0");
        assert_eq!(
            held.len(),
            16 - reserve(16),
            "one active tenant among 1,000 mounted uses all but the reserve"
        );
        assert_eq!(why, Refusal::TenantShare);
        assert_eq!(gate.active(), 1);
        drop(held);
        assert_eq!(gate.active(), 0);
        assert_eq!(gate.available_permits(), 16);
    }

    #[tokio::test(start_paused = true)]
    async fn a_newly_active_tenant_takes_a_reserve_permit_while_borrowers_hold_the_rest() {
        let (gate, bytes) = (SqlGate::new(16), Arc::new(ByteShares::new(64)));
        let _seats = seat_many(&gate, &bytes, 1000);
        // Four tenants become active together, then borrow in turn until the gate stops them.
        let borrowers = ["t1", "t2", "t3", "t4"];
        let mut borrowed: Vec<_> = borrowers
            .iter()
            .map(|b| gate.try_admit(Some(b)).unwrap())
            .collect();
        while let Some(p) = borrowers.iter().find_map(|b| gate.try_admit(Some(b)).ok()) {
            borrowed.push(p);
        }
        assert_eq!(borrowed.len(), 16 - reserve(16));
        assert_eq!(gate.try_admit(Some("t1")).err(), Some(Refusal::TenantShare));
        let start = tokio::time::Instant::now();
        let newcomer = gate
            .wait_admit(Some("t500"), Duration::from_millis(250))
            .await
            .expect("a tenant holding none takes a reserve permit");
        assert_eq!(start.elapsed(), Duration::ZERO, "admitted without waiting");
        drop((newcomer, borrowed));
    }

    #[tokio::test(start_paused = true)]
    async fn a_burst_larger_than_the_reserve_waits() {
        let (gate, bytes) = (SqlGate::new(16), Arc::new(ByteShares::new(64)));
        let _seats = seat_many(&gate, &bytes, 1000);
        let (mut borrowed, _) = take_all(&gate, "t1");
        let newcomers: Vec<_> = (0..reserve(16) + 1)
            .map(|i| {
                let gate = gate.clone();
                tokio::spawn(async move {
                    let start = tokio::time::Instant::now();
                    let p = gate
                        .wait_admit(Some(&format!("t{}", 100 + i)), Duration::from_millis(250))
                        .await;
                    (p, start.elapsed())
                })
            })
            .collect();
        tokio::time::sleep(Duration::from_millis(100)).await;
        drop(borrowed.pop());
        let mut waits = Vec::new();
        let mut admitted = Vec::new();
        for n in newcomers {
            let (p, waited) = n.await.unwrap();
            admitted.push(p.expect("admitted once a borrower's query ends"));
            waits.push(waited);
        }
        waits.sort();
        // The reserve is a bound, not a promise: `reserve` newcomers in one instant are admitted at
        // once, and the one past it waits for a running query to end.
        assert!(
            waits[..reserve(16)].iter().all(|w| w.is_zero()),
            "{waits:?}"
        );
        assert_eq!(waits[reserve(16)], Duration::from_millis(100), "{waits:?}");
        drop((admitted, borrowed));
    }

    #[tokio::test(start_paused = true)]
    async fn a_lowered_total_takes_permits_back_as_queries_end() {
        let gate = SqlGate::new(4);
        let held: Vec<_> = (0..3).map(|_| gate.try_admit(None).unwrap()).collect();
        gate.set_total(2);
        assert_eq!((gate.total(), gate.available_permits()), (2, 0));
        let mut held = held;
        drop(held.pop());
        assert_eq!(gate.available_permits(), 0, "the first returned is owed");
        drop(held.pop());
        assert_eq!(gate.available_permits(), 1);
        gate.set_total(8);
        assert_eq!(gate.available_permits(), 7);
        drop(held);
        assert_eq!(gate.available_permits(), 8);
    }

    #[test]
    fn the_reserve_is_an_eighth_and_at_least_one() {
        let cases = [(1, 1), (2, 1), (8, 1), (15, 1), (16, 2), (64, 8)];
        for (total, want) in cases {
            assert_eq!(reserve(total), want, "permits {total}");
        }
    }

    #[test]
    fn admission_waits_are_kept_per_tenant() {
        let bytes = Arc::new(ByteShares::new(100));
        let gate = SqlGate::new(2);
        let _a = seat(&gate, &bytes, "a");
        gate.observe_admission("a", Duration::ZERO, false, None);
        gate.observe_admission("a", Duration::from_millis(30), true, None);
        gate.observe_admission("a", Duration::from_millis(250), true, Some(Refusal::Busy));
        let reports = gate.tenant_reports();
        let a = &reports["a"].stats;
        assert_eq!((a.count, a.waits, a.rejected_busy), (3, 2, 1));
        assert_eq!(a.quantile(0.5), Some(0.05));
        assert_eq!(a.quantile(0.99), Some(0.25));
        assert!(!reports["a"].active);
    }

    #[test]
    fn a_tenant_that_leaves_takes_its_statistics_with_it() {
        let bytes = Arc::new(ByteShares::new(100));
        let gate = SqlGate::new(2);
        for i in 0..1000 {
            let name = format!("t{i}");
            let seat = seat(&gate, &bytes, &name);
            gate.observe_admission(&name, Duration::from_millis(5), true, None);
            drop(seat);
        }
        gate.observe_admission("never-mounted", Duration::ZERO, false, None);
        assert_eq!(lock(&gate.shares.ledger).stats.len(), 0);
    }

    #[test]
    fn response_bytes_keep_each_tenants_slice() {
        let bytes = Arc::new(ByteShares::new(100));
        let gate = SqlGate::new(2);
        let (_a, _b) = (seat(&gate, &bytes, "a"), seat(&gate, &bytes, "b"));
        // Half of 100 KiB reserved, 25 each; a may borrow up to 75 and no further.
        let a = bytes.reserve(75 * 1024, Some("a")).unwrap();
        assert_eq!(
            bytes.reserve(1024, Some("a")).err(),
            Some(Refusal::TenantShare)
        );
        let b = bytes.reserve(25 * 1024, Some("b")).unwrap();
        drop(a);
        assert_eq!(bytes.used().get("b"), Some(&25));
        drop(b);
        assert!(bytes.used().is_empty());
    }

    #[test]
    fn one_tenants_response_bytes_are_the_whole_budget() {
        let bytes = Arc::new(ByteShares::new(100));
        let gate = SqlGate::new(2);
        let _a = seat(&gate, &bytes, "a");
        let all = bytes.reserve(100 * 1024, Some("a")).unwrap();
        assert_eq!(bytes.reserve(1, Some("a")).err(), Some(Refusal::Busy));
        drop(all);
        bytes.set_share(
            "a",
            TenantShare {
                max_response_mib: Some(0),
                ..TenantShare::default()
            },
        );
        assert_eq!(
            bytes.reserve(1, Some("a")).err(),
            Some(Refusal::TenantShare)
        );
    }
}
