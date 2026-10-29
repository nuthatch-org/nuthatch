//! A nest's cursor without the operating system (RFC-0075 §6).
//!
//! The window path and the circuits it feeds, over traits a node and a browser tab both implement.
//! Nothing here opens a socket, spawns a runtime or reads a clock except through [`time`].

pub mod alerts;
pub mod answer;
pub mod backend;
pub mod cursor;
pub mod drive;
pub mod entity_circuit;
pub mod entity_expr;
pub mod entity_plan;
pub mod entity_row;
pub mod entity_view;
pub mod exposure;
pub mod extra;
pub mod factory;
pub mod fault;
pub mod feed;
#[cfg(target_arch = "wasm32")]
pub mod fetch;
pub mod flags;
pub mod hooks;
pub mod labels;
pub mod metrics;
#[cfg(target_arch = "wasm32")]
pub mod opfs;
pub mod redb_store;
pub mod rpc;
pub mod seal;
pub mod source;
pub mod store;
pub mod tap;
pub mod time;
pub mod transport;
pub mod velocity;
pub mod versioned;
pub mod views;
pub mod window;
