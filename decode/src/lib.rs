//! ABI-driven, deterministic event decode for nuthatch.
//!
//! Extracted from the main nuthatch crate so fuzz targets can build against this alone,
//! without pulling in dbsp - which causes a rustc ICE under sanitizer instrumentation
//! (nuthatch#581). This crate has no dbsp dependency.

pub mod abi;
pub mod registry;
pub mod rpc;
