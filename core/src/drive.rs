//! How a view's circuit is stepped (RFC-0075 §4): on a thread of its own, as the node always has, or
//! on the caller's thread as each batch is applied, which is the only shape a tab's one worker allows.
//! The choice is a runtime value, so both shapes run the same circuit code.

use anyhow::{Context, Result};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{channel, sync_channel, Sender, SyncSender};
use std::sync::{Arc, Mutex};

/// What a lane does with each message. An error stops the lane for good and marks it unhealthy.
pub type Step<M> = Box<dyn FnMut(M) -> Result<()> + Send>;

/// Starts lanes. `build` makes the circuit and the step that feeds it; where it runs is the driver's.
pub trait ViewDriver {
    fn lane<M, B>(&self, name: &'static str, thread: &str, build: B) -> Result<Lane<M>>
    where
        M: Send + 'static,
        B: FnOnce() -> Result<Step<M>> + Send + 'static;
}

/// One thread per circuit, fed through a channel: the node's shape, which keeps dbsp off the async pool.
#[derive(Clone, Copy, Debug, Default)]
pub struct Threaded;

/// The circuit steps inside `apply`, on the caller's thread, and `flush` has nothing to wait for.
#[derive(Clone, Copy, Debug, Default)]
pub struct Inline;

/// The driver a nest's views run under, chosen when the nest is built.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Driver {
    #[default]
    Threaded,
    Inline,
}

impl ViewDriver for Driver {
    fn lane<M, B>(&self, name: &'static str, thread: &str, build: B) -> Result<Lane<M>>
    where
        M: Send + 'static,
        B: FnOnce() -> Result<Step<M>> + Send + 'static,
    {
        match self {
            Driver::Threaded => Threaded.lane(name, thread, build),
            Driver::Inline => Inline.lane(name, thread, build),
        }
    }
}

enum Envelope<M> {
    Msg(M),
    Flush(SyncSender<()>),
}

enum Kind<M> {
    Off,
    Threaded(Sender<Envelope<M>>),
    Inline(Arc<Mutex<Option<Step<M>>>>),
}

/// A handle on a running circuit. Cheap to clone; every clone feeds the same circuit.
pub struct Lane<M> {
    kind: Kind<M>,
    healthy: Arc<AtomicBool>,
}

impl<M> Clone for Lane<M> {
    fn clone(&self) -> Self {
        let kind = match &self.kind {
            Kind::Off => Kind::Off,
            Kind::Threaded(tx) => Kind::Threaded(tx.clone()),
            Kind::Inline(step) => Kind::Inline(step.clone()),
        };
        Self {
            kind,
            healthy: self.healthy.clone(),
        }
    }
}

impl ViewDriver for Threaded {
    fn lane<M, B>(&self, name: &'static str, thread: &str, build: B) -> Result<Lane<M>>
    where
        M: Send + 'static,
        B: FnOnce() -> Result<Step<M>> + Send + 'static,
    {
        let (tx, rx) = channel::<Envelope<M>>();
        let healthy = Arc::new(AtomicBool::new(true));
        let health = healthy.clone();
        std::thread::Builder::new()
            .name(thread.into())
            .spawn(move || {
                let mut step = match build() {
                    Ok(s) => s,
                    Err(e) => {
                        tracing::error!("{name} circuit failed to start: {e:#}");
                        health.store(false, Ordering::SeqCst);
                        return;
                    }
                };
                while let Ok(env) = rx.recv() {
                    match env {
                        Envelope::Msg(m) => {
                            if let Err(e) = step(m) {
                                tracing::error!("{e:#}");
                                health.store(false, Ordering::SeqCst);
                                break;
                            }
                        }
                        // In order, so every earlier message is folded by the time this is seen.
                        Envelope::Flush(ack) => {
                            let _ = ack.send(());
                        }
                    }
                }
            })
            .with_context(|| format!("failed to spawn {name} thread"))?;
        Ok(Lane {
            kind: Kind::Threaded(tx),
            healthy,
        })
    }
}

impl ViewDriver for Inline {
    fn lane<M, B>(&self, name: &'static str, _thread: &str, build: B) -> Result<Lane<M>>
    where
        M: Send + 'static,
        B: FnOnce() -> Result<Step<M>> + Send + 'static,
    {
        let healthy = Arc::new(AtomicBool::new(true));
        let step = match build() {
            Ok(s) => Some(s),
            Err(e) => {
                tracing::error!("{name} circuit failed to start: {e:#}");
                healthy.store(false, Ordering::SeqCst);
                None
            }
        };
        Ok(Lane {
            kind: Kind::Inline(Arc::new(Mutex::new(step))),
            healthy,
        })
    }
}

impl<M> Lane<M> {
    /// A lane with no circuit: messages are dropped, flushes return at once, and it stays healthy.
    pub fn off() -> Self {
        Self {
            kind: Kind::Off,
            healthy: Arc::new(AtomicBool::new(true)),
        }
    }

    /// `false` once the circuit failed to start or a step failed; it never comes back.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::SeqCst)
    }

    /// Feed one message. Dropped silently once the lane has died, which was logged when it did.
    pub fn send(&self, msg: M) {
        match &self.kind {
            Kind::Off => {}
            Kind::Threaded(tx) => {
                let _ = tx.send(Envelope::Msg(msg));
            }
            Kind::Inline(step) => {
                let mut guard = step.lock().unwrap_or_else(|p| p.into_inner());
                if let Some(s) = guard.as_mut() {
                    if let Err(e) = s(msg) {
                        tracing::error!("{e:#}");
                        self.healthy.store(false, Ordering::SeqCst);
                        *guard = None;
                    }
                }
            }
        }
    }

    /// Return once every message sent so far has been folded.
    pub fn flush(&self) {
        if let Kind::Threaded(tx) = &self.kind {
            let (ack, wait) = sync_channel(0);
            if tx.send(Envelope::Flush(ack)).is_ok() {
                let _ = wait.recv();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicU64;

    fn summing(driver: Driver, total: Arc<AtomicU64>) -> Lane<u64> {
        driver
            .lane("sum", "sum", move || {
                Ok(Box::new(move |n: u64| {
                    anyhow::ensure!(n != 0, "sum step failed: zero");
                    total.fetch_add(n, Ordering::SeqCst);
                    Ok(())
                }) as Step<u64>)
            })
            .unwrap()
    }

    #[test]
    fn both_drivers_fold_the_same_messages_and_die_the_same_way() {
        for driver in [Driver::Threaded, Driver::Inline] {
            let total = Arc::new(AtomicU64::new(0));
            let lane = summing(driver, total.clone());
            lane.send(2);
            lane.clone().send(3);
            lane.flush();
            assert_eq!(total.load(Ordering::SeqCst), 5, "{driver:?}");
            assert!(lane.is_healthy());
            lane.send(0);
            lane.send(7);
            lane.flush();
            assert!(!lane.is_healthy(), "{driver:?}");
            assert_eq!(
                total.load(Ordering::SeqCst),
                5,
                "{driver:?}: a dead lane folds nothing"
            );
        }
    }

    #[test]
    fn inline_has_folded_before_apply_returns() {
        let total = Arc::new(AtomicU64::new(0));
        let lane = summing(Driver::Inline, total.clone());
        lane.send(4);
        assert_eq!(total.load(Ordering::SeqCst), 4);
    }

    #[test]
    fn a_circuit_that_will_not_build_leaves_the_lane_unhealthy() {
        for driver in [Driver::Threaded, Driver::Inline] {
            let lane: Lane<u64> = driver
                .lane("broken", "broken", || anyhow::bail!("no"))
                .unwrap();
            lane.flush();
            assert!(!lane.is_healthy(), "{driver:?}");
        }
    }
}
