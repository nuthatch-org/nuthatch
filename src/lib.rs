//! nuthatch - be your own indexer.
//!
//! The crate's modules are exposed as a library so a second front-end - such as a colocated reth
//! build (RFC-0003, not built) - could reuse the *same* indexing core (decode → hot store → seal →
//! IVM → serve) rather than fork it. The `nuthatch` binary (`main.rs`) is one such front-end, and
//! drives the pipeline through the `Source` trait.

pub mod abi;
pub mod address_discovery;
pub mod address_history;
pub mod address_mev;
pub mod address_mode;
pub mod address_partitions;
pub mod address_proposers;
pub mod address_proxy;
pub mod alerts;
pub mod allowlist;
pub mod analytics;
pub mod analytics_budget;
#[cfg(feature = "graph")]
mod analytics_scalars;
pub mod audit;
pub mod bench;
pub mod blob;
pub mod calldata;
pub mod calls;
pub mod chains;
pub mod check;
pub mod chunker;
pub mod cid;
pub mod cli;
pub mod config;
/// The control-plane HTTP API (RFC-0022 §3): declare what the fleet should run.
#[cfg(feature = "postgres-store")]
pub mod control_api;
/// The control plane for scaled mode (RFC-0022 §3): desired state and the worker registry.
#[cfg(feature = "postgres-store")]
pub mod controlplane;
pub mod count; // RFC-0054 hook
/// RFC-0046 S2: an optional, local-only x402 counter. The default binary does not compile this
/// module, which is the S0 deletion boundary rather than an assurance about it.
#[cfg(feature = "counter")]
pub mod counter;
pub mod crash;
pub mod distribution;
pub mod doctor;
pub(crate) mod engine;
pub(crate) mod engine_burrmill;
pub mod entities;
pub mod entity_bind;
pub mod entity_circuit;
pub mod entity_expr;
pub mod entity_lower;
pub mod entity_offchain;
pub mod entity_plan;
pub mod entity_row;
pub mod entity_view;
pub mod exposure;
pub mod factory;
pub mod flags;
/// RFC-0059 checkpointed folds, behind the off-by-default `folds` feature.
#[cfg(feature = "folds")]
pub mod folds;
pub mod freshness;
pub mod graft;
#[cfg(feature = "graph")]
pub mod graph_decimal;
#[cfg(feature = "graph")]
pub mod graph_history;
/// RFC-0053 S2: compile an accepted Graph query dialect to SQL over a nest (#1266).
#[cfg(feature = "graph")]
pub mod graph_query;
/// RFC-0053 S1: graph-node's generated schema, from an imported `schema.graphql` (#1265).
#[cfg(feature = "graph")]
pub mod graph_schema;
pub mod graph_validate;
pub mod health;
pub mod help;
pub mod indexer;
pub mod ipfs;
pub mod ipfs_resolve;
pub mod labels;
pub mod lifecycle;
pub mod lists;
pub mod maintained;
pub mod mcp;
pub mod metadata;
pub mod metrics;
pub mod migrate;
pub mod mount_jobs;
pub mod offchain;
pub mod pack;
/// The Postgres hot store (RFC-0022 slice 2). Feature-gated so the default build stays a single
/// binary with no database in its dependency tree.
#[cfg(feature = "postgres-store")]
pub mod pgstore;
pub mod port_emit;
pub mod port_report;
pub mod progress;
pub mod project;
pub mod prune;
pub mod publish;
pub mod recipes;
/// The reconcile loop for scaled mode (RFC-0022 §2/§3): plan in, leases out.
#[cfg(feature = "postgres-store")]
pub mod reconcile;
pub mod registry;
pub mod rpc;
pub mod runtime;
/// Cursor placement for scaled mode (RFC-0022 §2) - pure decision logic, no I/O.
pub mod scheduler;
pub mod seal;
pub mod sealed_audit;
pub mod seed;
pub mod semantic;
pub mod serve;
/// RFC-0046 S3: drain and settle recorded authorisations, outside the nest.
#[cfg(feature = "counter")]
pub mod settle;
pub mod skill;
pub mod source;
pub(crate) mod spill;
pub mod sql_errors;
pub mod sqlmemo;
pub mod store;
pub mod subgraph_import;
pub mod tape;
pub mod velocity;
pub mod views;
pub mod webhooks;
/// The writer-worker role for scaled mode (RFC-0022 §2): the reconcile loop, running.
#[cfg(feature = "postgres-store")]
pub mod worker;
