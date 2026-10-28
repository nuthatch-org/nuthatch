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
