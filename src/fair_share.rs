//! Fair share between tenant labels (RFC-0073 FR2): a cursor's SQL permits and the process's response
//! bytes in flight, divided by the opaque tenant string a mount carries.
//!
//! Admitted work is never preempted, so queueing alone cannot bound how long one tenant waits behind
//! another. Every tenant with a mount on a gate therefore holds a **reservation**: one permit, and a
//! slice of half the response-bytes budget. A tenant may borrow beyond it only while every other
//! seated tenant's reservation stays free, so a tenant under its reservation waits behind its own work
//! and never behind another's. Weights order the queue when several tenants wait, and set the share a
//! refusal is measured against.
//!
//! A gate with one tenant, or none, behaves as the plain semaphore it wraps: no reservation is held
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
    total: usize,
    ledger: Mutex<GateLedger>,
    changed: Notify,
}

#[derive(Default)]
struct GateLedger {
    tenants: BTreeMap<Arc<str>, GateTenant>,
    shares: BTreeMap<Arc<str>, TenantShare>,
    next_ticket: u64,
}

#[derive(Default)]
struct GateTenant {
    /// One entry per mount seated here, by its dataset directory.
    seats: Vec<PathBuf>,
    held: usize,
    waiting: VecDeque<u64>,
}

impl GateTenant {
    fn seated(&self) -> bool {
        !self.seats.is_empty()
    }
}

impl GateLedger {
    fn share(&self, tenant: &str) -> TenantShare {
        self.shares.get(tenant).copied().unwrap_or_default()
    }

    fn seated(&self) -> impl Iterator<Item = (&Arc<str>, &GateTenant)> {
        self.tenants.iter().filter(|(_, t)| t.seated())
    }

    fn held(&self, tenant: &str) -> usize {
        self.tenants.get(tenant).map_or(0, |t| t.held)
    }

    /// The permits `tenant` is entitled to by weight among the seated tenants, at least one.
    fn fair(&self, tenant: &str, total: usize) -> usize {
        let weights: u64 = self
            .seated()
            .map(|(t, _)| self.share(t).weight as u64)
            .sum();
        let mine = self.share(tenant).weight as u64;
        let weights = weights.max(mine);
        let fair = ((total as u64 * mine) / weights.max(1)) as usize;
        let fair = fair.max(1);
        self.share(tenant)
            .max_permits
            .map_or(fair, |cap| fair.min(cap))
    }

    /// Whether `tenant` may take one permit with `free` on the semaphore, and why not.
    fn may_take(&self, tenant: &str, free: usize, total: usize) -> Result<(), Refusal> {
        let held = self.held(tenant);
        if self
            .share(tenant)
            .max_permits
            .is_some_and(|cap| held >= cap)
        {
            return Err(Refusal::TenantShare);
        }
        let seated = self.seated().count();
        let idle_others = self
            .seated()
            .filter(|(t, s)| &***t != tenant && s.held == 0)
            .count();
        let ok = if seated > total {
            free >= 1 && held == 0
        } else {
            free > idle_others
        };
        if ok {
            Ok(())
        } else {
            Err(self.reason(tenant, total))
        }
    }

    fn reason(&self, tenant: &str, total: usize) -> Refusal {
        let seated = self.seated().count();
        let held = self.held(tenant);
        let capped = self
            .share(tenant)
            .max_permits
            .is_some_and(|cap| held >= cap);
        if capped || (seated > 1 && (seated > total || held >= self.fair(tenant, total))) {
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
    fn next_in_line(&self, tenant: &str, ticket: u64, free: usize, total: usize) -> bool {
        if let Some(front) = self.tenants.get(tenant).and_then(|t| t.waiting.front()) {
            if *front != ticket {
                return false;
            }
        }
        !self.tenants.iter().any(|(other, t)| {
            &**other != tenant
                && t.waiting.front().is_some_and(|&front| {
                    self.may_take(other, free, total).is_ok()
                        && self.before((other, front), (tenant, ticket))
                })
        })
    }

    fn gc(&mut self, tenant: &str) {
        if self
            .tenants
            .get(tenant)
            .is_some_and(|t| !t.seated() && t.held == 0 && t.waiting.is_empty())
        {
            self.tenants.remove(tenant);
        }
    }
}

/// A permit from a [`SqlGate`], charged to a tenant or to nobody, released on drop.
pub struct GatePermit {
    permit: Option<OwnedSemaphorePermit>,
    shares: Arc<GateShares>,
    tenant: Option<Arc<str>>,
}

impl Drop for GatePermit {
    fn drop(&mut self) {
        drop(self.permit.take());
        if let Some(tenant) = &self.tenant {
            let mut l = lock(&self.shares.ledger);
            if let Some(t) = l.tenants.get_mut(tenant) {
                t.held = t.held.saturating_sub(1);
            }
            l.gc(tenant);
        }
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
                total: permits,
                ledger: Mutex::default(),
                changed: Notify::new(),
            }),
            hot: Arc::new(ByteShares::uncapped((hot_bytes / 1024).max(1) as usize)),
        }
    }

    pub fn total(&self) -> usize {
        self.shares.total
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

    fn seat(&self, tenant: &Arc<str>, dir: &Path) {
        lock(&self.shares.ledger)
            .tenants
            .entry(Arc::clone(tenant))
            .or_default()
            .seats
            .push(dir.to_path_buf());
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
        l.gc(tenant);
        drop(l);
        self.shares.changed.notify_waiters();
    }

    fn raw(&self, permit: OwnedSemaphorePermit) -> GatePermit {
        GatePermit {
            permit: Some(permit),
            shares: Arc::clone(&self.shares),
            tenant: None,
        }
    }

    fn grant(&self, l: &mut GateLedger, tenant: &str, ticket: u64) -> Result<GatePermit, Refusal> {
        let free = self.permits.available_permits();
        l.may_take(tenant, free, self.shares.total)?;
        if !l.next_in_line(tenant, ticket, free, self.shares.total) {
            return Err(l.reason(tenant, self.shares.total));
        }
        let permit = Arc::clone(&self.permits)
            .try_acquire_owned()
            .map_err(|_| l.reason(tenant, self.shares.total))?;
        let key = l
            .tenants
            .get_key_value(tenant)
            .map_or_else(|| Arc::from(tenant), |(k, _)| Arc::clone(k));
        let entry = l.tenants.entry(Arc::clone(&key)).or_default();
        if entry.waiting.front() == Some(&ticket) {
            entry.waiting.pop_front();
        }
        entry.held += 1;
        Ok(GatePermit {
            permit: Some(permit),
            shares: Arc::clone(&self.shares),
            tenant: Some(key),
        })
    }

    /// A permit now, or why not. `None` is untenanted: the bare semaphore, as before FR2.
    pub fn try_admit(&self, tenant: Option<&str>) -> Result<GatePermit, Refusal> {
        let Some(tenant) = tenant else {
            return Arc::clone(&self.permits)
                .try_acquire_owned()
                .map(|p| self.raw(p))
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
                Ok(Ok(p)) => Ok(self.raw(p)),
                _ => Err(Refusal::Busy),
            };
        };
        let deadline = tokio::time::Instant::now() + wait;
        let ticket = {
            let mut l = lock(&self.shares.ledger);
            let ticket = l.next_ticket;
            l.next_ticket += 1;
            l.tenants
                .entry(Arc::from(tenant))
                .or_default()
                .waiting
                .push_back(ticket);
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
    /// Work for a dataset no tenant has seated takes only a permit no reservation needs.
    pub fn try_admit_for(&self, dir: &Path) -> Option<GatePermit> {
        let mut l = lock(&self.shares.ledger);
        let owners: Vec<Arc<str>> = l
            .tenants
            .iter()
            .filter(|(_, t)| t.seats.iter().any(|d| d == dir))
            .map(|(k, _)| Arc::clone(k))
            .collect();
        if owners.is_empty() {
            let idle = l.seated().filter(|(_, t)| t.held == 0).count();
            if self.permits.available_permits() <= idle {
                return None;
            }
            drop(l);
            return Arc::clone(&self.permits)
                .try_acquire_owned()
                .ok()
                .map(|p| self.raw(p));
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
        l.gc(self.tenant);
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
    async fn a_borrower_leaves_every_idle_reservation_free() {
        let (gate, bytes) = (SqlGate::new(4), Arc::new(ByteShares::new(64)));
        let (_a, _b, _c) = (
            seat(&gate, &bytes, "a"),
            seat(&gate, &bytes, "b"),
            seat(&gate, &bytes, "c"),
        );
        let a1 = gate.try_admit(Some("a")).unwrap();
        let a2 = gate.try_admit(Some("a")).unwrap();
        assert_eq!(gate.try_admit(Some("a")).err(), Some(Refusal::TenantShare));
        let b = gate.try_admit(Some("b")).unwrap();
        let c = gate.try_admit(Some("c")).unwrap();
        assert_eq!(gate.available_permits(), 0);
        drop((a1, a2, b, c));
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
            .expect("c takes the reservation a gave back");
        let refused = gate.wait_admit(Some("a"), Duration::from_millis(250)).await;
        assert_eq!(refused.err(), Some(Refusal::TenantShare));
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
    fn background_work_borrows_only_what_no_reservation_needs() {
        let (gate, bytes) = (SqlGate::new(2), Arc::new(ByteShares::new(64)));
        let (_a, _b) = (seat(&gate, &bytes, "a"), seat(&gate, &bytes, "b"));
        assert!(gate.try_admit_for(Path::new("/elsewhere")).is_none());
        let build = gate
            .try_admit_for(Path::new("/data/a"))
            .expect("charged to a");
        assert_eq!(gate.held().get("a"), Some(&1));
        assert!(gate.try_admit(Some("b")).is_ok());
        drop(build);
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
