//! The crate's only clock. `std::time::Instant::now` panics on wasm32-unknown-unknown, so every read
//! goes through here, and `clippy.toml` refuses the std calls anywhere else.
#![allow(clippy::disallowed_methods)]

pub use std::time::Duration;
pub use web_time::{Instant, SystemTime, UNIX_EPOCH};

pub fn now() -> Instant {
    Instant::now()
}

pub fn system_now() -> SystemTime {
    SystemTime::now()
}

/// The cursor's waits: a refused window's back-off and the loop's pacing. The node sleeps on tokio's
/// timer, a tab on the browser's, so neither runtime is named here.
pub trait Clock: Send + Sync {
    fn sleep(&self, d: Duration)
        -> std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>;
}

/// Gives the executor one turn, as `tokio::task::yield_now` does, without naming a runtime.
pub fn yield_now() -> impl std::future::Future<Output = ()> + Send {
    let mut yielded = false;
    std::future::poll_fn(move |cx| {
        if yielded {
            return std::task::Poll::Ready(());
        }
        yielded = true;
        cx.waker().wake_by_ref();
        std::task::Poll::Pending
    })
}
