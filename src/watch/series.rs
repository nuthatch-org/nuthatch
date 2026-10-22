//! Every `/metrics` series the watcher reads, named once, so a test can hold the list against what
//! `METRICS.render()` emits and a rename breaks the build rather than the screen.

pub const RSS: &str = "nuthatch_rss_bytes";
pub const CPU: &str = "nuthatch_process_cpu_seconds_total";
pub const HTTP_REQUESTS: &str = "nuthatch_http_requests_total";
pub const SQL_QUERIES: &str = "nuthatch_sql_queries_total";

pub const NEST_LAST_BLOCK: &str = "nuthatch_nest_last_block";
pub const NEST_ROWS_SEALED: &str = "nuthatch_nest_rows_sealed_total";
pub const NEST_SEALED_BYTES: &str = "nuthatch_nest_sealed_segments_bytes";
pub const NEST_HOT_BYTES: &str = "nuthatch_nest_hot_store_bytes";
pub const NEST_POOL_RESERVED: &str = "nuthatch_nest_analytics_pool_reserved_bytes";
pub const NEST_POOL_PEAK: &str = "nuthatch_nest_analytics_pool_peak_bytes";
pub const NEST_REORGS: &str = "nuthatch_nest_reorgs_total";

pub const RPC_REQUESTS: &str = "nuthatch_rpc_endpoint_requests_total";
pub const RPC_FAILURES: &str = "nuthatch_rpc_endpoint_failures_total";
pub const RPC_RETRIES: &str = "nuthatch_rpc_endpoint_retries_total";
pub const RPC_DURATION_SUM: &str = "nuthatch_rpc_request_duration_seconds_sum";
pub const RPC_DURATION_COUNT: &str = "nuthatch_rpc_request_duration_seconds_count";

pub const ALL: &[&str] = &[
    RSS,
    CPU,
    HTTP_REQUESTS,
    SQL_QUERIES,
    NEST_LAST_BLOCK,
    NEST_ROWS_SEALED,
    NEST_SEALED_BYTES,
    NEST_HOT_BYTES,
    NEST_POOL_RESERVED,
    NEST_POOL_PEAK,
    NEST_REORGS,
    RPC_REQUESTS,
    RPC_FAILURES,
    RPC_RETRIES,
    RPC_DURATION_SUM,
    RPC_DURATION_COUNT,
];

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    /// Every name the watcher reads is one a fresh registry emits once a nest exists and an RPC call
    /// has been made: the state a running nest is in by the time anyone watches it.
    #[test]
    fn every_series_the_client_reads_is_emitted() {
        let m = crate::metrics::Metrics::fresh_for_test();
        m.nest("watched");
        m.observe_rpc("rpc.example", Duration::from_millis(40), false, false);
        let rendered = crate::watch::prom::Metrics::parse(&m.render());
        let names: Vec<&str> = rendered.names().collect();
        let missing: Vec<&&str> = ALL.iter().filter(|s| !names.contains(s)).collect();
        assert!(missing.is_empty(), "not emitted: {missing:?}");
        assert!(rendered
            .get(NEST_LAST_BLOCK, &[("nest", "watched")])
            .is_some());
        assert!(rendered
            .get(RPC_REQUESTS, &[("endpoint", "rpc.example")])
            .is_some());
    }

    /// No series name appears in the watcher as a bare literal, so the list above is the whole of
    /// what it reads.
    #[test]
    fn the_watcher_names_series_only_here() {
        for (file, text) in super::super::SOURCES {
            if *file == "series.rs" {
                continue;
            }
            let code = text.split("#[cfg(test)]").next().unwrap_or(text);
            assert!(
                !code.contains("\"nuthatch_"),
                "{file} names a series outside series.rs"
            );
        }
    }
}
