//! A nest's cursor without the operating system (RFC-0075 §6).
//!
//! The window path and the circuits it feeds, over traits a node and a browser tab both implement.
//! Nothing here opens a socket, spawns a runtime or reads a clock except through [`time`].

pub mod alerts;
pub mod drive;
pub mod entity_circuit;
pub mod entity_expr;
pub mod entity_plan;
pub mod entity_row;
pub mod entity_view;
pub mod exposure;
pub mod factory;
pub mod flags;
pub mod hooks;
pub mod labels;
pub mod metrics;
pub mod seal;
pub mod source;
pub mod store;
pub mod time;
pub mod velocity;
pub mod views;
