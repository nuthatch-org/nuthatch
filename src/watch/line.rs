//! The line mode (RFC-0069 §9): with no terminal to draw on, one logfmt line per nest per interval,
//! every number exactly as the endpoint served it and `NO SIGNAL` where it served nothing.

use super::series;
use super::state::{App, RouteState};

pub const NO_SIGNAL: &str = "\"NO SIGNAL\"";

fn n(v: Option<u64>) -> String {
    v.map_or_else(|| NO_SIGNAL.to_string(), |v| v.to_string())
}

fn quoted(v: Option<String>) -> String {
    match v {
        Some(s) if !s.is_empty() && !s.contains([' ', '"', '=']) => s,
        Some(s) if !s.is_empty() => format!("{s:?}"),
        _ => NO_SIGNAL.to_string(),
    }
}

/// One line per nest: a solo nest's, or each of a runtime's mounts in `/nests` order. Nothing until
/// `/nests` has said which.
pub fn lines(app: &App) -> Vec<String> {
    let app = &app.answered_only();
    let process = process_fields(app);
    if !app.shape_known() {
        return vec![format!(
            "nest={NO_SIGNAL} chain={NO_SIGNAL} tip={NO_SIGNAL} lag={NO_SIGNAL} \
             sealed={NO_SIGNAL} {process}"
        )];
    }
    match app.mounts() {
        None => vec![nest_line(
            app.solo_name(),
            app.root.str("chain"),
            &app.ready,
            &process,
        )],
        Some(mounts) if mounts.is_empty() => vec![format!(
            "nest={NO_SIGNAL} chain={NO_SIGNAL} tip={NO_SIGNAL} lag={NO_SIGNAL} \
             sealed={NO_SIGNAL} {process}"
        )],
        Some(mounts) => {
            let empty = RouteState::default();
            mounts
                .into_iter()
                .map(|m| {
                    let ready = app.nest_ready.get(&m.name).unwrap_or(&empty);
                    nest_line(Some(m.name), m.chain, ready, &process)
                })
                .collect()
        }
    }
}

fn nest_line(
    name: Option<String>,
    chain: Option<String>,
    ready: &RouteState,
    process: &str,
) -> String {
    // `/ready` serves `sealed_through: 0` before anything has sealed, beside a null seal lag.
    let sealed = ready.u64("sealed_through").filter(|s| *s > 0);
    format!(
        "nest={} chain={} tip={} lag={} sealed={} {process}",
        quoted(name),
        quoted(chain),
        n(ready.u64("tip")),
        n(ready.u64("lag_blocks")),
        n(sealed),
    )
}

fn process_fields(app: &App) -> String {
    let rss = app.metric(series::RSS, &[]).map(|v| v as u64);
    let budget = app.budget_bytes().map(|v| v as u64);
    let rpc = app
        .rpc_up()
        .map_or_else(|| NO_SIGNAL.to_string(), |(up, of)| format!("{up}/{of}"));
    let rps = app
        .http_rps()
        .map_or_else(|| NO_SIGNAL.to_string(), |r| format!("{r:.1}"));
    format!(
        "rss={} budget={} rpc_up={rpc} http_rps={rps}",
        n(rss),
        n(budget)
    )
}

#[cfg(test)]
mod tests {
    use super::super::state::Read;
    use super::*;

    const READY: &str = r#"{"version":"5.9.0","ready":true,"tip":23651200,"last_block":23651198,"lag_blocks":2,"sealed_through":23649000,"seal_lag_blocks":2198,"freshness":{"mode":"tip","poll_interval_secs":12}}"#;
    const ROOT: &str = r#"{"name":"nuthatch","chain":"mainnet","nid":"bafyabc"}"#;
    const METRICS_0: &str = "nuthatch_rss_bytes 77905920\nnuthatch_http_requests_total 100\n\
        nuthatch_nest_last_block{nest=\"defi-pulse\"} 23651198\n\
        nuthatch_rpc_endpoint_requests_total{endpoint=\"eth.drpc.org\"} 10\n\
        nuthatch_rpc_endpoint_failures_total{endpoint=\"eth.drpc.org\"} 1\n\
        nuthatch_rpc_endpoint_requests_total{endpoint=\"publicnode.com\"} 9\n\
        nuthatch_rpc_endpoint_failures_total{endpoint=\"publicnode.com\"} 0\n\
        nuthatch_rpc_endpoint_requests_total{endpoint=\"llamarpc.com\"} 30\n\
        nuthatch_rpc_endpoint_failures_total{endpoint=\"llamarpc.com\"} 3\n";
    const METRICS_1: &str = "nuthatch_rss_bytes 78000000\nnuthatch_http_requests_total 105\n\
        nuthatch_nest_last_block{nest=\"defi-pulse\"} 23651198\n\
        nuthatch_rpc_endpoint_requests_total{endpoint=\"eth.drpc.org\"} 12\n\
        nuthatch_rpc_endpoint_failures_total{endpoint=\"eth.drpc.org\"} 2\n\
        nuthatch_rpc_endpoint_requests_total{endpoint=\"publicnode.com\"} 11\n\
        nuthatch_rpc_endpoint_failures_total{endpoint=\"publicnode.com\"} 0\n\
        nuthatch_rpc_endpoint_requests_total{endpoint=\"llamarpc.com\"} 30\n\
        nuthatch_rpc_endpoint_failures_total{endpoint=\"llamarpc.com\"} 3\n";

    fn read(t_ms: u64, route: &str, status: u16, body: &str) -> Read {
        Read {
            t_ms,
            route: route.into(),
            status,
            body: body.into(),
            cycle: None,
        }
    }

    fn solo(ready: &str, root: &str, m0: &str, m1: &str) -> App {
        let mut app = App::default();
        for r in [
            read(0, "/metrics", 200, m0),
            read(2000, "/ready", 200, ready),
            read(2000, "/", 200, root),
            read(2000, "/nests", 404, "not found"),
            read(2000, "/metrics", 200, m1),
        ] {
            app.apply(&r);
        }
        app
    }

    fn field(line: &str, key: &str) -> String {
        let at = line
            .find(&format!(" {key}="))
            .map(|i| i + 1)
            .or_else(|| line.starts_with(&format!("{key}=")).then_some(0))
            .unwrap_or_else(|| panic!("no {key} in {line}"));
        let rest = &line[at + key.len() + 1..];
        if let Some(q) = rest.strip_prefix('"') {
            format!("\"{}\"", &q[..q.find('"').unwrap()])
        } else {
            rest.split(' ').next().unwrap().to_string()
        }
    }

    #[test]
    fn every_number_is_the_endpoints() {
        let lines = lines(&solo(READY, ROOT, METRICS_0, METRICS_1));
        assert_eq!(
            lines,
            vec![
                "nest=defi-pulse chain=mainnet tip=23651200 lag=2 sealed=23649000 \
                 rss=78000000 budget=2147483648 rpc_up=2/3 http_rps=2.5"
            ]
        );
    }

    /// A pass where `/ready` and `/metrics` did not answer prints none of the last pass's figures.
    #[test]
    fn a_read_that_fails_does_not_reprint_the_last_one() {
        let mut app = solo(READY, ROOT, METRICS_0, METRICS_1);
        for r in [
            read(4000, "/ready", 0, ""),
            read(4000, "/", 200, ROOT),
            read(4000, "/nests", 404, "not found"),
            read(4000, "/metrics", 0, ""),
        ] {
            app.apply(&r);
        }
        let line = &lines(&app)[0];
        for key in ["nest", "tip", "lag", "sealed", "rss", "rpc_up", "http_rps"] {
            assert_eq!(field(line, key), NO_SIGNAL, "{key} in {line}");
        }
        assert_eq!(field(line, "chain"), "mainnet");
    }

    /// Each field's source taken away in turn: the field says `NO SIGNAL`, never a zero.
    #[test]
    fn a_missing_source_reads_no_signal() {
        let without = |s: &str, cut: &str| -> String {
            s.lines()
                .filter(|l| !l.starts_with(cut))
                .collect::<Vec<_>>()
                .join("\n")
        };
        let drop_key = |json: &str, key: &str| -> String {
            let mut v: serde_json::Value = serde_json::from_str(json).unwrap();
            v.as_object_mut().unwrap().remove(key);
            v.to_string()
        };
        let cases: Vec<(&str, App)> = vec![
            (
                "nest",
                solo(
                    READY,
                    ROOT,
                    METRICS_0,
                    &without(METRICS_1, "nuthatch_nest_last_block"),
                ),
            ),
            (
                "chain",
                solo(READY, &drop_key(ROOT, "chain"), METRICS_0, METRICS_1),
            ),
            (
                "tip",
                solo(&drop_key(READY, "tip"), ROOT, METRICS_0, METRICS_1),
            ),
            (
                "lag",
                solo(&drop_key(READY, "lag_blocks"), ROOT, METRICS_0, METRICS_1),
            ),
            (
                "sealed",
                solo(
                    &drop_key(READY, "sealed_through"),
                    ROOT,
                    METRICS_0,
                    METRICS_1,
                ),
            ),
            (
                "rss",
                solo(
                    READY,
                    ROOT,
                    METRICS_0,
                    &without(METRICS_1, "nuthatch_rss_bytes"),
                ),
            ),
            (
                "rpc_up",
                solo(
                    READY,
                    ROOT,
                    &without(METRICS_0, "nuthatch_rpc_endpoint_failures_total"),
                    METRICS_1,
                ),
            ),
            (
                "http_rps",
                solo(
                    READY,
                    ROOT,
                    &without(METRICS_0, "nuthatch_http_requests_total"),
                    METRICS_1,
                ),
            ),
        ];
        for (key, app) in cases {
            let line = lines(&app).remove(0);
            assert_eq!(field(&line, key), NO_SIGNAL, "{key} in {line}");
        }

        let mut app = App::default();
        app.apply(&read(0, "/nests", 0, ""));
        assert_eq!(field(&lines(&app)[0], "budget"), NO_SIGNAL);
    }

    #[test]
    fn nothing_sealed_yet_is_no_signal_not_zero() {
        let ready = READY.replace("\"sealed_through\":23649000", "\"sealed_through\":0");
        let line = lines(&solo(&ready, ROOT, METRICS_0, METRICS_1)).remove(0);
        assert_eq!(field(&line, "sealed"), NO_SIGNAL);
    }

    #[test]
    fn a_runtime_writes_one_line_per_mount_from_its_own_ready() {
        let mut app = App::default();
        for r in [
            read(0, "/metrics", 200, METRICS_0),
            read(
                2000,
                "/nests",
                200,
                r#"{"nests":[{"name":"usdc","chain":"mainnet"},{"name":"arb","chain":"arbitrum-one"},{"name":"usdt","chain":"mainnet"}]}"#,
            ),
            read(2000, "/usdc/ready", 200, READY),
            read(
                2000,
                "/arb/ready",
                503,
                r#"{"ready":false,"tip":398000000,"last_block":397999990,"lag_blocks":10,"sealed_through":0}"#,
            ),
            read(2000, "/metrics", 200, METRICS_1),
        ] {
            app.apply(&r);
        }
        let lines = lines(&app);
        assert_eq!(lines.len(), 3);
        assert!(lines[0].starts_with("nest=usdc chain=mainnet tip=23651200 lag=2 sealed=23649000 "));
        assert!(lines[1].starts_with(&format!(
            "nest=arb chain=arbitrum-one tip=398000000 lag=10 sealed={NO_SIGNAL} "
        )));
        assert_eq!(field(&lines[1], "budget"), "4294967296");
        assert!(lines[2].starts_with(&format!("nest=usdt chain=mainnet tip={NO_SIGNAL} ")));
    }
}
