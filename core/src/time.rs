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

// The RPC client's own waits (RFC-0075 S3). Native, they are tokio's, so a test's paused clock still
// governs them; in a tab, the host's `setTimeout`.
#[cfg(not(target_arch = "wasm32"))]
pub use tokio::time::{error::Elapsed, sleep, timeout, timeout_at, Instant as Deadline};

#[cfg(not(target_arch = "wasm32"))]
pub fn deadline_now() -> Deadline {
    Deadline::now()
}

#[cfg(target_arch = "wasm32")]
pub use tab::*;

#[cfg(target_arch = "wasm32")]
mod tab {
    use super::Duration;
    use std::future::Future;
    use wasm_bindgen::{JsCast, JsValue};

    pub type Deadline = web_time::Instant;

    #[derive(Debug, PartialEq, Eq)]
    pub struct Elapsed;

    impl std::fmt::Display for Elapsed {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("deadline has elapsed")
        }
    }

    impl std::error::Error for Elapsed {}

    pub fn deadline_now() -> Deadline {
        Deadline::now()
    }

    /// Sound only because a tab's nest runs on one thread (RFC-0075 §4).
    pub fn sleep(d: Duration) -> impl Future<Output = ()> + Send {
        let ms = d.as_millis().min(i32::MAX as u128) as i32;
        let promise = js_sys::Promise::new(&mut |resolve, _| {
            let set_timeout: js_sys::Function =
                js_sys::Reflect::get(&js_sys::global(), &JsValue::from_str("setTimeout"))
                    .expect("a JS host has setTimeout")
                    .unchecked_into();
            let _ = set_timeout.call2(&JsValue::NULL, &resolve, &JsValue::from(ms));
        });
        let wait = send_wrapper::SendWrapper::new(wasm_bindgen_futures::JsFuture::from(promise));
        async move {
            let _ = wait.await;
        }
    }

    pub async fn timeout<F: Future>(d: Duration, f: F) -> Result<F::Output, Elapsed> {
        let f = std::pin::pin!(f);
        let s = std::pin::pin!(sleep(d));
        match futures::future::select(f, s).await {
            futures::future::Either::Left((v, _)) => Ok(v),
            futures::future::Either::Right(_) => Err(Elapsed),
        }
    }

    pub async fn timeout_at<F: Future>(until: Deadline, f: F) -> Result<F::Output, Elapsed> {
        timeout(until.saturating_duration_since(deadline_now()), f).await
    }
}
