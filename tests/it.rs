// The ordinary integration suite shares one executable. Process-global measurements and the
// resource-sensitive warm restart stay as explicit Cargo targets in Cargo.toml.

#![allow(clippy::duplicate_mod)] // Existing modules each import their own tests/common helpers.

#[path = "abi_floors_documented.rs"]
mod abi_floors_documented;
#[path = "actions_are_pinned.rs"]
mod actions_are_pinned;
#[path = "authoring_eval_board.rs"]
mod authoring_eval_board;
#[path = "bench_citations.rs"]
mod bench_citations;
#[path = "bench_commits.rs"]
mod bench_commits;
#[path = "bench_helpers_reject_failures.rs"]
mod bench_helpers_reject_failures;
#[path = "bind_drops_slow_headers.rs"]
mod bind_drops_slow_headers;
#[path = "compact_rows_prototype.rs"]
mod compact_rows_prototype;
#[path = "concurrent_sql_does_not_corrupt.rs"]
mod concurrent_sql_does_not_corrupt;
#[path = "control_api.rs"]
mod control_api;
#[path = "control_plane.rs"]
mod control_plane;
#[path = "core_stays_pristine.rs"]
mod core_stays_pristine;
#[path = "cors_reaches_the_bind.rs"]
mod cors_reaches_the_bind;
#[path = "deploy_nest_script.rs"]
mod deploy_nest_script;
#[path = "doc_command_check.rs"]
mod doc_command_check;
#[path = "e2e_bare_help.rs"]
mod e2e_bare_help;
#[path = "e2e_crash_safety.rs"]
mod e2e_crash_safety;
#[path = "e2e_cursor_death_isolation.rs"]
mod e2e_cursor_death_isolation;
#[path = "e2e_early_cutoff.rs"]
mod e2e_early_cutoff;
#[path = "e2e_entity_reorg.rs"]
mod e2e_entity_reorg;
#[path = "e2e_fencing.rs"]
mod e2e_fencing;
#[cfg(feature = "folds")]
#[path = "e2e_fold_writer.rs"]
mod e2e_fold_writer;
#[path = "e2e_l1_blocks.rs"]
mod e2e_l1_blocks;
#[path = "e2e_migrate_parity.rs"]
mod e2e_migrate_parity;
#[path = "e2e_minio_publish.rs"]
mod e2e_minio_publish;
#[path = "e2e_nest_nid.rs"]
mod e2e_nest_nid;
#[path = "e2e_plane_split.rs"]
mod e2e_plane_split;
#[path = "e2e_prune_lifecycle.rs"]
mod e2e_prune_lifecycle;
#[path = "e2e_query_allowlist.rs"]
mod e2e_query_allowlist;
#[path = "e2e_reconcile.rs"]
mod e2e_reconcile;
#[path = "e2e_reorg.rs"]
mod e2e_reorg;
#[path = "e2e_resolution.rs"]
mod e2e_resolution;
#[path = "e2e_runtime_lifecycle.rs"]
mod e2e_runtime_lifecycle;
#[path = "e2e_runtime_parity.rs"]
mod e2e_runtime_parity;
#[path = "e2e_seal_determinism.rs"]
mod e2e_seal_determinism;
#[path = "e2e_seed_from_mirror.rs"]
mod e2e_seed_from_mirror;
#[path = "e2e_serve_local_store.rs"]
mod e2e_serve_local_store;
#[path = "e2e_shared_dataset.rs"]
mod e2e_shared_dataset;
#[path = "e2e_solo.rs"]
mod e2e_solo;
#[path = "e2e_sql_cli_backend.rs"]
mod e2e_sql_cli_backend;
#[path = "e2e_stall_isolation.rs"]
mod e2e_stall_isolation;
#[path = "e2e_stop_releases_store.rs"]
mod e2e_stop_releases_store;
#[path = "e2e_trino_contract.rs"]
mod e2e_trino_contract;
#[path = "engine_batch_boundary.rs"]
mod engine_batch_boundary;
#[path = "entity_provenance_is_atomic.rs"]
mod entity_provenance_is_atomic;
#[path = "eval_harness.rs"]
mod eval_harness;
#[cfg(feature = "folds")]
#[path = "fold_cli_stdout.rs"]
mod fold_cli_stdout;
#[path = "folds_absent.rs"]
mod folds_absent;
#[path = "gate_audit_cases.rs"]
mod gate_audit_cases;
#[path = "gate_refresh_script.rs"]
mod gate_refresh_script;
#[cfg(feature = "graph")]
#[path = "graph_over_indexed_data.rs"]
mod graph_over_indexed_data;
#[cfg(feature = "graph")]
#[path = "graph_schema_golden.rs"]
mod graph_schema_golden;
#[path = "ivm_claims.rs"]
mod ivm_claims;
#[path = "launch_copy.rs"]
mod launch_copy;
#[path = "lodestar_panel.rs"]
mod lodestar_panel;
#[path = "mutants_check.rs"]
mod mutants_check;
#[path = "over_i128_is_reported.rs"]
mod over_i128_is_reported;
#[path = "parity_epoch_shift.rs"]
mod parity_epoch_shift;
#[path = "parity_timer_script.rs"]
mod parity_timer_script;
#[path = "payment_absent.rs"]
mod payment_absent;
#[path = "pg_parity.rs"]
mod pg_parity;
#[path = "port_emit.rs"]
mod port_emit;
#[path = "port_report.rs"]
mod port_report;
#[path = "pr_review_harness.rs"]
mod pr_review_harness;
#[path = "reading_published_nest.rs"]
mod reading_published_nest;
#[path = "release_gate_script.rs"]
mod release_gate_script;
#[path = "release_provenance.rs"]
mod release_provenance;
#[path = "required_checks.rs"]
mod required_checks;
#[path = "required_contexts_script.rs"]
mod required_contexts_script;
#[path = "rfc_index_status.rs"]
mod rfc_index_status;
#[path = "roll_helsinki_script.rs"]
mod roll_helsinki_script;
#[path = "rpc_pool_rate_limit.rs"]
mod rpc_pool_rate_limit;
#[path = "scheduled_workflow_failure_is_reported.rs"]
mod scheduled_workflow_failure_is_reported;
#[path = "sealed_audit.rs"]
mod sealed_audit;
#[path = "secret_isolation.rs"]
mod secret_isolation;
#[path = "semantic_layer.rs"]
mod semantic_layer;
#[path = "tape_clean.rs"]
mod tape_clean;
#[path = "tip_gauges_are_published_per_nest.rs"]
mod tip_gauges_are_published_per_nest;
#[path = "upgrade_golden.rs"]
mod upgrade_golden;
#[path = "verification_non_claims.rs"]
mod verification_non_claims;
#[path = "window_is_a_ceiling.rs"]
mod window_is_a_ceiling;
#[path = "workflow_permission_keys.rs"]
mod workflow_permission_keys;
