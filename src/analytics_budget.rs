//! Operator-visible resource governance for the embedded analytical engine (RFC-0047 C4, #1225).
//!
//! Runtime knobs, not nest identity: read from the environment, never from `nuthatch.toml`, so two
//! operators with different RAM still share a nest content address. Same shape as
//! `NUTHATCH_SQL_MAX_CONCURRENCY`.

use anyhow::{bail, Result};
use std::path::PathBuf;

/// Per-connection Burrmill ceiling. Operator key `analytics.memory_limit`.
pub const DEFAULT_MEMORY_LIMIT_MB: u64 = 512;
/// Burrmill worker threads. Operator key `analytics.threads`.
pub const DEFAULT_THREADS: i64 = 2;
/// Hard ceiling on `analytics.threads`. Matches [`crate::serve::SQL_MAX_CONCURRENCY_CEILING`].
pub const THREADS_CEILING: i64 = crate::serve::SQL_MAX_CONCURRENCY_CEILING as i64;

pub const ENV_MEMORY_LIMIT: &str = "NUTHATCH_ANALYTICS_MEMORY_LIMIT";
pub const ENV_THREADS: &str = "NUTHATCH_ANALYTICS_THREADS";
pub const ENV_TEMP_DIRECTORY: &str = "NUTHATCH_ANALYTICS_TEMP_DIRECTORY";
pub const ENV_MAX_TEMP_SIZE: &str = "NUTHATCH_ANALYTICS_MAX_TEMP_SIZE";
pub const ENV_INGESTION_RESERVATION: &str = "NUTHATCH_INGESTION_RESERVATION";
/// Burrmill's own bound per session, in a build that carries it. Unset, `analytics.memory_limit`.
pub const ENV_BURRMILL_MEMORY_LIMIT: &str = "NUTHATCH_BURRMILL_MEMORY_LIMIT";
/// The cursor's RSS wall for the split below, where an operator has given the process more than the
/// shipped 2 GiB. Raise-only: the default is what the footprint job measures.
pub const ENV_MAX_RSS: &str = "NUTHATCH_MAX_RSS";

/// `runtime_headroom` when nobody has measured it: zero, since the derived reservation covers it.
/// [`ENV_RUNTIME_HEADROOM`] states a measured one, which is what lets the reservation go lower.
pub const RUNTIME_HEADROOM_MB: u64 = 0;

/// Measured RSS outside the engine pool while it answers: decoded scan batches, result materialisation.
pub const ENV_RUNTIME_HEADROOM: &str = "NUTHATCH_RUNTIME_HEADROOM";
/// The least a measured reservation may be: an idle `serve` of the QoS nest peaked at 144 MiB and one
/// following the chain with no queries at 372 MiB, so a lower figure is a typo, not a measurement.
pub const RESERVATION_MINIMUM_MB: u64 = 256;

/// Live analytics resource settings. Defaults equal the constants the binary already applied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AnalyticsConfig {
    pub memory_limit_mb: u64,
    pub threads: i64,
    /// Parent of per-instance spill directories. `None` → `std::env::temp_dir()`. Instance dirs
    /// are `nuthatch-spill-{pid}-{seq}` under it (#1165).
    pub temp_directory: Option<PathBuf>,
    /// Spill cap per analytics connection. `None` → [`DEFAULT_MAX_TEMP_MB`]; see [`spill_cap_bytes`].
    pub max_temp_size: Option<String>,
    /// Named ingest floor, in MB. `None` → [`derived_ingestion_reservation_mb`].
    pub ingestion_reservation_mb: Option<u64>,
    /// Burrmill's bound per session. `None` → `memory_limit_mb`. Its hash joins and final aggregates
    /// cannot spill, so a nest whose views Burrmill answers in 512 MB may need more here.
    pub burrmill_memory_limit_mb: Option<u64>,
    /// The wall the split is held to. `None` → [`crate::runtime::DEFAULT_MAX_RSS_MB`].
    pub max_rss_mb: Option<u64>,
    /// Measured `runtime_headroom`. `None` → [`RUNTIME_HEADROOM_MB`], which the derived reservation covers.
    pub runtime_headroom_mb: Option<u64>,
}

impl Default for AnalyticsConfig {
    fn default() -> Self {
        Self {
            memory_limit_mb: DEFAULT_MEMORY_LIMIT_MB,
            threads: DEFAULT_THREADS,
            temp_directory: None,
            max_temp_size: None,
            ingestion_reservation_mb: None,
            burrmill_memory_limit_mb: None,
            max_rss_mb: None,
            runtime_headroom_mb: None,
        }
    }
}

impl AnalyticsConfig {
    /// Remainder of the 2 GiB cursor budget after the shipped Burrmill split (2 × 512 MB).
    ///
    /// Not a measured ingest RSS high-water. RFC-0047 §6 leaves that unresolved; this is the floor
    /// today's walls already left for everything that is not Burrmill.
    pub fn reservation_mb(&self) -> u64 {
        self.ingestion_reservation_mb
            .unwrap_or_else(derived_ingestion_reservation_mb)
    }
}

/// Named ingest floor left after the shipped Burrmill split:
/// `2048 - (SQL_MAX_CONCURRENCY × DEFAULT_MEMORY_LIMIT_MB)`. Not an ingest RSS cap.
///
/// **Unset, it is a floor and not a slider.** Nothing caps ingest, DBSP, redb or what a statement
/// holds outside the pool at this figure, so lowering it alone would buy Burrmill room against memory no
/// term counts (#1241). It may go below the floor only beside a measured `runtime_headroom`, the two
/// terms RFC-0047 names in place of the one derived number, and never under
/// [`RESERVATION_MINIMUM_MB`] (#1899). Unless an operator states both, or raises the wall, **no
/// accepted split hands Burrmill more RAM than the shipped default the footprint CI job measures.**
impl AnalyticsConfig {
    pub fn burrmill_limit_mb(&self) -> u64 {
        self.burrmill_memory_limit_mb
            .unwrap_or(self.memory_limit_mb)
    }

    pub fn max_rss(&self) -> u64 {
        self.max_rss_mb
            .unwrap_or(crate::runtime::DEFAULT_MAX_RSS_MB)
    }
}

pub fn derived_ingestion_reservation_mb() -> u64 {
    crate::runtime::DEFAULT_MAX_RSS_MB
        .saturating_sub(crate::serve::SQL_MAX_CONCURRENCY as u64 * DEFAULT_MEMORY_LIMIT_MB)
}

/// Read the live operator settings. Unparseable values fall back to the shipped default and warn.
/// A Burrmill split that dips under the named ingest floor, and a thread count above
/// [`THREADS_CEILING`], are the validator's job - including an explicit zero reservation, which is
/// carried through as `Some(0)` so it is refused rather than read as "unset".
pub fn from_env() -> AnalyticsConfig {
    AnalyticsConfig {
        memory_limit_mb: env_u64_memory(ENV_MEMORY_LIMIT, DEFAULT_MEMORY_LIMIT_MB),
        threads: env_threads(),
        temp_directory: env_path(ENV_TEMP_DIRECTORY),
        max_temp_size: env_size(ENV_MAX_TEMP_SIZE),
        ingestion_reservation_mb: env_optional_memory(ENV_INGESTION_RESERVATION),
        burrmill_memory_limit_mb: env_optional_memory(ENV_BURRMILL_MEMORY_LIMIT),
        max_rss_mb: env_optional_memory(ENV_MAX_RSS),
        runtime_headroom_mb: env_optional_memory(ENV_RUNTIME_HEADROOM),
    }
}

/// Refuses a split that breaches
/// `(pools × analytics.memory_limit) + ingestion_reservation + runtime_headroom ≤ 2 GiB`,
/// an `ingestion_reservation` below [`derived_ingestion_reservation_mb`], and a thread count above
/// [`THREADS_CEILING`]. Does not cap ingest, DBSP, redb, or result materialisation - which is
/// exactly why the reservation may only be raised. 2 GiB is the footprint CI job / process RSS
/// wall; this is the arithmetic that keeps a config from being allowed to breach it quietly.
pub fn validate_cursor_budget(datasets: usize) -> Result<()> {
    validate_on_cursor(&from_env(), crate::serve::sql_max_concurrency(), datasets)
}

/// Under `isolation = "strict"` each tenant of a dataset has a pool of its own (RFC-0073 FR8), so a
/// cursor's datasets no longer bound its pools and every permit is charged one.
pub fn validate_strict_budget() -> Result<()> {
    validate_cursor_budget(usize::MAX)
}

/// A dataset's sessions share one pool (#1792), so no more pools reserve at once than the cursor
/// has datasets, nor more than it has permits.
pub fn validate_on_cursor(cfg: &AnalyticsConfig, permits: usize, datasets: usize) -> Result<()> {
    validate_against(cfg, permits.min(datasets))
}

/// Same check against an explicit config, so a test can drive the inequality without the process
/// environment. `pools` is how many nest pools can reserve at once, clamped to the gate's ceiling;
/// each counts at Burrmill's limit where one is set.
pub fn validate_against(cfg: &AnalyticsConfig, pools: usize) -> Result<()> {
    let pools = pools.clamp(1, crate::serve::SQL_MAX_CONCURRENCY_CEILING) as u64;
    if cfg.memory_limit_mb == 0 {
        bail!(
            "analytics.memory_limit must be greater than zero (set {ENV_MEMORY_LIMIT}, default \
             {DEFAULT_MEMORY_LIMIT_MB}MB)"
        );
    }
    if cfg.threads < 1 {
        bail!(
            "analytics.threads must be at least 1 (set {ENV_THREADS}, default {DEFAULT_THREADS})"
        );
    }
    if cfg.threads > THREADS_CEILING {
        bail!(
            "analytics.threads is {}, above the ceiling of {THREADS_CEILING} (set {ENV_THREADS}, \
             default {DEFAULT_THREADS}). Burrmill worker threads are not an unconstrained config key.",
            cfg.threads
        );
    }
    if cfg.burrmill_memory_limit_mb == Some(0) {
        bail!("{ENV_BURRMILL_MEMORY_LIMIT} must be greater than zero");
    }
    let ceiling = cfg.max_rss();
    if ceiling < crate::runtime::DEFAULT_MAX_RSS_MB {
        bail!(
            "{ENV_MAX_RSS} is {ceiling} MB, below the shipped {} MB. It is the wall the operator \
             has given the process, and it may only be raised: the split is not what bounds ingest.",
            crate::runtime::DEFAULT_MAX_RSS_MB
        );
    }
    let duck = pools.saturating_mul(cfg.burrmill_limit_mb());
    let reservation = cfg.reservation_mb();
    let floor = derived_ingestion_reservation_mb();
    let headroom = cfg.runtime_headroom_mb.unwrap_or(RUNTIME_HEADROOM_MB);
    if reservation < floor && headroom == 0 {
        bail!(
            "ingestion_reservation is {reservation} MB, below the derived floor of {floor} MB (set \
             {ENV_INGESTION_RESERVATION}), with no measured runtime_headroom ({ENV_RUNTIME_HEADROOM}). \
             The derived floor covers ingest, DBSP and redb and also what a statement holds outside \
             the pool, its decoded batches and results. A smaller reservation alone only hands \
             Burrmill room against memory no term counts. Set both from a measurement: the \
             reservation from a cursor following the chain with no queries, the headroom from the \
             release gate's peak RSS less the pool held at that peak. Otherwise raise it, or lower \
             analytics.memory_limit ({ENV_MEMORY_LIMIT}) or NUTHATCH_SQL_MAX_CONCURRENCY."
        );
    }
    if reservation < RESERVATION_MINIMUM_MB {
        bail!(
            "ingestion_reservation is {reservation} MB, below the minimum of \
             {RESERVATION_MINIMUM_MB} MB (set {ENV_INGESTION_RESERVATION}). No measured cursor \
             has run in less, so this is not a measurement."
        );
    }
    let total = duck.saturating_add(reservation).saturating_add(headroom);
    if total > ceiling {
        let limit = cfg.burrmill_limit_mb();
        // Burrmill's limit overrides analytics.memory_limit, so lowering the latter would change nothing.
        let knob = if cfg.burrmill_memory_limit_mb.is_some() {
            ENV_BURRMILL_MEMORY_LIMIT.to_string()
        } else {
            format!("analytics.memory_limit ({ENV_MEMORY_LIMIT})")
        };
        let wall = if cfg.max_rss_mb.is_some() {
            format!("the {ceiling} MB wall {ENV_MAX_RSS} sets")
        } else {
            format!("the {ceiling} MB wall (the per-cursor default; {ENV_MAX_RSS} is unset)")
        };
        let fit = ceiling.saturating_sub(reservation + headroom) / pools;
        let remedy = if fit == 0 {
            format!("No pool fits beside the reservation and the headroom: raise {ENV_MAX_RSS} only if the process has been given more")
        } else {
            format!("Set {knob} at most {fit} MB")
        };
        let permits = if pools > 1 {
            " or lower NUTHATCH_SQL_MAX_CONCURRENCY"
        } else {
            ""
        };
        let raised = if fit > 0 && reservation > floor {
            format!(", or bring ingestion_reservation back towards its {floor} MB floor")
        } else {
            String::new()
        };
        bail!(
            "the analytics split does not fit its wall: (pools × {knob}) + ingestion_reservation + \
             runtime_headroom = ({pools} × {limit} MB) + {reservation} MB + {headroom} MB = {total} \
             MB, above {wall}. pools is the smaller of sql_permits and the cursor's nest datasets, \
             since a dataset's sessions share one pool. {remedy}{permits}{raised}. \
             ingestion_reservation ({ENV_INGESTION_RESERVATION}) goes below its derived floor only beside a \
             measured runtime_headroom ({ENV_RUNTIME_HEADROOM}). \
             {ENV_MAX_RSS} raises the wall only where the process has been given more than the 2 GiB \
             per-cursor budget. This gate refuses that split; it does not cap ingest, DBSP, redb, or \
             result materialisation. analytics.max_temp_size is disk and does not buy RAM. A query \
             that cannot run in its budget fails; it never degrades block processing."
        );
    }
    Ok(())
}

fn env_u64_memory(key: &str, default: u64) -> u64 {
    match std::env::var(key) {
        Err(_) => default,
        Ok(raw) => match parse_memory_mb(&raw) {
            Some(n) if n > 0 => n,
            _ => {
                tracing::warn!(
                    value = %raw,
                    default,
                    "{key} is not a positive size in MB or GB; using the default"
                );
                default
            }
        },
    }
}

/// An explicit `0` is a *value*, and an invalid one - it must reach the validator and be refused
/// there, naming the key. Folding it into `None` made it mean "unset", so the derived default
/// applied and the process started, which is the opposite of what the operator wrote.
fn env_optional_memory(key: &str) -> Option<u64> {
    match std::env::var(key) {
        Err(_) => None,
        Ok(raw) if raw.trim().is_empty() => None,
        Ok(raw) => match parse_memory_mb(&raw) {
            Some(n) => Some(n),
            None => {
                tracing::warn!(
                    value = %raw,
                    "{key} is not a size in MB or GB; leaving it unset"
                );
                None
            }
        },
    }
}

/// `n >= 1` is kept as requested so [`validate_against`] can refuse n above [`THREADS_CEILING`].
fn env_threads() -> i64 {
    match std::env::var(ENV_THREADS) {
        Err(_) => DEFAULT_THREADS,
        Ok(raw) => match raw.trim().parse::<i64>() {
            Ok(n) if n >= 1 => n,
            _ => {
                tracing::warn!(
                    value = %raw,
                    default = DEFAULT_THREADS,
                    "{ENV_THREADS} is not a positive integer; using the default"
                );
                DEFAULT_THREADS
            }
        },
    }
}

fn env_path(key: &str) -> Option<PathBuf> {
    match std::env::var(key) {
        Ok(raw) if !raw.trim().is_empty() => Some(PathBuf::from(raw.trim())),
        _ => None,
    }
}

fn env_size(key: &str) -> Option<String> {
    match std::env::var(key) {
        Err(_) => None,
        Ok(raw) if raw.trim().is_empty() => None,
        Ok(raw) => match parse_size(&raw) {
            Some(s) => Some(s),
            None => {
                tracing::warn!(
                    value = %raw,
                    "{key} is not a size (e.g. 10GB, 512MB); leaving unset"
                );
                None
            }
        },
    }
}

/// `512`, `512MB`, `1G`, `2GiB` → megabytes. No fractional values.
pub fn parse_memory_mb(raw: &str) -> Option<u64> {
    let (n, unit) = split_size(raw)?;
    match unit.as_str() {
        "" | "m" | "mb" | "mib" => Some(n),
        "g" | "gb" | "gib" => n.checked_mul(1024),
        _ => None,
    }
}

/// The spill cap when `max_temp_size` is unset. An engine's own default is far larger (DuckDB's was
/// 90% of the free space where it spills), and that is often a tmpfs `/tmp`, which is RAM.
pub const DEFAULT_MAX_TEMP_MB: u64 = 2048;

/// The bytes one analytics session may spill before its query is stopped. Burrmill is handed this as
/// its spill budget, and the `/sql` watchdog also measures the spill directory against it, which it
/// began doing when DuckDB did not enforce its own cap on every spill (a cross join wrote 1.3 GB past
/// a 128 MB cap).
pub fn spill_cap_bytes(cfg: &AnalyticsConfig) -> u64 {
    let mb = cfg
        .max_temp_size
        .as_deref()
        .and_then(parse_memory_mb)
        .unwrap_or(DEFAULT_MAX_TEMP_MB);
    mb.saturating_mul(1024 * 1024)
}

/// Normalise an operator size: a bare number is megabytes.
pub fn parse_size(raw: &str) -> Option<String> {
    let (n, unit) = split_size(raw)?;
    match unit.as_str() {
        "" | "m" | "mb" | "mib" => Some(format!("{n}MB")),
        "g" | "gb" | "gib" => Some(format!("{n}GB")),
        _ => None,
    }
}

fn split_size(raw: &str) -> Option<(u64, String)> {
    let s = raw.trim();
    let digits = s.find(|c: char| !c.is_ascii_digit()).unwrap_or(s.len());
    if digits == 0 {
        return None;
    }
    let n: u64 = s[..digits].parse().ok()?;
    let unit = s[digits..].trim().to_ascii_lowercase();
    Some((n, unit))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::runtime::DEFAULT_MAX_RSS_MB;
    use crate::serve::{SQL_MAX_CONCURRENCY, SQL_MAX_CONCURRENCY_CEILING};

    /// Held by every test that sets or reads the `NUTHATCH_ANALYTICS_*` environment.
    pub(crate) fn env_lock() -> &'static std::sync::Mutex<()> {
        static L: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        L.get_or_init(|| std::sync::Mutex::new(()))
    }

    /// Sets each variable for the guard's lifetime and puts back what was there on drop, panic
    /// included, so a test holding `env_lock` never hands its values to the next one.
    pub(crate) struct EnvVars(Vec<(&'static str, Option<String>)>);

    impl EnvVars {
        pub(crate) fn set(vars: &[(&'static str, &str)]) -> EnvVars {
            EnvVars(
                vars.iter()
                    .map(|&(k, v)| {
                        let prev = std::env::var(k).ok();
                        std::env::set_var(k, v);
                        (k, prev)
                    })
                    .collect(),
            )
        }
    }

    impl Drop for EnvVars {
        fn drop(&mut self) {
            for (k, prev) in &self.0 {
                match prev {
                    Some(v) => std::env::set_var(k, v),
                    None => std::env::remove_var(k),
                }
            }
        }
    }

    struct EnvRestore {
        prev: Option<String>,
    }

    impl Drop for EnvRestore {
        fn drop(&mut self) {
            match self.prev.as_deref() {
                Some(v) => std::env::set_var(ENV_THREADS, v),
                None => std::env::remove_var(ENV_THREADS),
            }
        }
    }

    #[test]
    fn defaults_equal_todays_constants() {
        let cfg = AnalyticsConfig::default();
        assert_eq!(cfg.memory_limit_mb, 512);
        assert_eq!(cfg.threads, 2);
        assert!(cfg.temp_directory.is_none());
        assert!(cfg.max_temp_size.is_none());
        assert!(cfg.ingestion_reservation_mb.is_none());
        assert_eq!(SQL_MAX_CONCURRENCY, 2);
        assert_eq!(DEFAULT_MEMORY_LIMIT_MB, 512);
        assert_eq!(DEFAULT_THREADS, 2);
        assert_eq!(THREADS_CEILING, SQL_MAX_CONCURRENCY_CEILING as i64);
        assert_eq!(THREADS_CEILING, 16);
        assert_eq!(derived_ingestion_reservation_mb(), 1024);
        assert_eq!(DEFAULT_MAX_RSS_MB, 2048);
        assert_eq!(RUNTIME_HEADROOM_MB, 0);
        assert_eq!(
            SQL_MAX_CONCURRENCY as u64 * DEFAULT_MEMORY_LIMIT_MB
                + derived_ingestion_reservation_mb()
                + RUNTIME_HEADROOM_MB,
            DEFAULT_MAX_RSS_MB,
            "the shipped split must fill the cursor budget exactly, or a default change has moved \
             the wall without a test going red"
        );
    }

    #[test]
    fn unconfigured_config_validates_at_the_shipped_permits() {
        validate_against(&AnalyticsConfig::default(), SQL_MAX_CONCURRENCY)
            .expect("today's walls must still start");
    }

    /// A nest whose views need more than the default: Burrmill's own limit is what a permit
    /// counts at, and the wall moves only when the operator says the process has more.
    #[test]
    fn burrmill_takes_its_own_limit_and_the_wall_only_rises() {
        let cfg = AnalyticsConfig {
            burrmill_memory_limit_mb: Some(2048),
            ..AnalyticsConfig::default()
        };
        assert_eq!(cfg.burrmill_limit_mb(), 2048);
        assert_eq!(AnalyticsConfig::default().burrmill_limit_mb(), 512);
        // One permit: 2,048 + 1,024.
        let err = validate_against(&cfg, 1).unwrap_err().to_string();
        assert!(
            err.contains("3072 MB") && err.contains(ENV_MAX_RSS),
            "{err}"
        );
        let raised = AnalyticsConfig {
            max_rss_mb: Some(4096),
            ..cfg.clone()
        };
        validate_against(&raised, 1).expect("3,072 MB under a 4 GiB wall");
        // Two permits: 2 x 2,048 + 1,024 is over it.
        assert!(validate_against(&raised, 2).is_err());
        let lowered = AnalyticsConfig {
            max_rss_mb: Some(1024),
            ..AnalyticsConfig::default()
        };
        let err = validate_against(&lowered, 1).unwrap_err().to_string();
        assert!(err.contains("may only be raised"), "{err}");
    }

    /// #1899: the QoS nest's settings under the 2 GiB wall. Burrmill's limit is what counted, so the
    /// refusal names it and what would fit, not analytics.memory_limit, which it overrides.
    #[test]
    fn a_refusal_names_the_limit_that_counted_and_what_fits() {
        let qos = AnalyticsConfig {
            burrmill_memory_limit_mb: Some(2048),
            threads: 8,
            ..AnalyticsConfig::default()
        };
        let err = validate_on_cursor(&qos, 2, 1).unwrap_err().to_string();
        assert!(
            err.contains("(1 × 2048 MB) + 1024 MB + 0 MB = 3072 MB")
                && err.contains("above the 2048 MB wall"),
            "{err}"
        );
        assert!(
            err.contains(&format!("{ENV_BURRMILL_MEMORY_LIMIT} at most 1024 MB")),
            "{err}"
        );
        assert!(!err.contains("Lower analytics.memory_limit"), "{err}");
        assert!(
            !err.contains(&format!(
                "or ingestion_reservation ({ENV_INGESTION_RESERVATION})"
            )),
            "lowering the reservation is refused by the floor, so it is not a remedy: {err}"
        );
        let two_pools = validate_on_cursor(&qos, 2, 2).unwrap_err().to_string();
        assert!(
            two_pools.contains(&format!("{ENV_BURRMILL_MEMORY_LIMIT} at most 512 MB")),
            "{two_pools}"
        );
        validate_on_cursor(
            &AnalyticsConfig {
                burrmill_memory_limit_mb: Some(768),
                ..qos
            },
            2,
            1,
        )
        .expect("768 + 1024 is under 2048");
        // RFC-0073 FR8: two tenants of that one dataset are two pools under strict isolation.
        let strict = AnalyticsConfig {
            burrmill_memory_limit_mb: Some(768),
            threads: 8,
            ..AnalyticsConfig::default()
        };
        assert!(validate_on_cursor(&strict, 2, usize::MAX).is_err());
    }

    /// #1899: the derived 1024 MB stands in for two terms RFC-0047 names, ingest and the memory a
    /// statement holds outside the pool. Measured, they may replace it, together.
    fn measured(pool: u64) -> AnalyticsConfig {
        AnalyticsConfig {
            burrmill_memory_limit_mb: Some(pool),
            ingestion_reservation_mb: Some(384),
            runtime_headroom_mb: Some(1088),
            ..AnalyticsConfig::default()
        }
    }

    #[test]
    fn a_measured_reservation_and_headroom_replace_the_derived_floor() {
        validate_on_cursor(&measured(512), 2, 1).expect("512 + 384 + 1088 = 1984");
        let err = validate_on_cursor(&measured(1536), 2, 1)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("(1 × 1536 MB) + 384 MB + 1088 MB = 3008 MB")
                && err.contains(&format!("{ENV_BURRMILL_MEMORY_LIMIT} at most 576 MB")),
            "{err}"
        );
    }

    /// Lowering the reservation alone is #1241's trap: the 1024 also covers what a statement holds
    /// outside the pool, so a smaller one is only honest beside a measured headroom.
    #[test]
    fn a_lowered_reservation_without_a_measured_headroom_is_refused() {
        let cfg = AnalyticsConfig {
            ingestion_reservation_mb: Some(384),
            ..AnalyticsConfig::default()
        };
        let err = validate_on_cursor(&cfg, 2, 1).unwrap_err().to_string();
        assert!(err.contains(ENV_RUNTIME_HEADROOM), "{err}");
        let zero = AnalyticsConfig {
            runtime_headroom_mb: Some(0),
            ..cfg
        };
        let err = validate_on_cursor(&zero, 2, 1).unwrap_err().to_string();
        assert!(err.contains(ENV_RUNTIME_HEADROOM), "{err}");
    }

    #[test]
    fn a_reservation_under_the_minimum_is_refused_even_with_a_headroom() {
        let cfg = AnalyticsConfig {
            ingestion_reservation_mb: Some(128),
            runtime_headroom_mb: Some(512),
            ..AnalyticsConfig::default()
        };
        let err = validate_on_cursor(&cfg, 2, 1).unwrap_err().to_string();
        assert!(
            err.contains(&format!("{RESERVATION_MINIMUM_MB} MB")),
            "{err}"
        );
    }

    /// A headroom beside the default reservation only adds: it is the conservative direction.
    #[test]
    fn a_headroom_beside_the_derived_reservation_counts_on_top() {
        let cfg = AnalyticsConfig {
            runtime_headroom_mb: Some(600),
            ..AnalyticsConfig::default()
        };
        let err = validate_on_cursor(&cfg, 2, 1).unwrap_err().to_string();
        assert!(
            err.contains("512 MB) + 1024 MB + 600 MB = 2136 MB"),
            "{err}"
        );
    }

    #[test]
    fn from_env_reads_the_runtime_headroom() {
        let _g = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let _v = EnvVars::set(&[(ENV_RUNTIME_HEADROOM, "1088MB")]);
        assert_eq!(from_env().runtime_headroom_mb, Some(1088));
    }

    #[test]
    fn a_limit_that_fits_once_starts_at_two_permits_on_one_dataset() {
        let cfg = AnalyticsConfig {
            burrmill_memory_limit_mb: Some(1024),
            ..AnalyticsConfig::default()
        };
        validate_on_cursor(&cfg, 2, 1).expect("one pool: 1024 + 1024 = 2048");
        assert!(
            validate_on_cursor(&cfg, 2, 2).is_err(),
            "two datasets, two pools"
        );
        assert!(
            validate_on_cursor(&cfg, 1, 2).is_ok(),
            "one permit, one pool at a time"
        );
    }

    #[test]
    fn over_budget_config_is_refused_naming_the_keys() {
        let cfg = AnalyticsConfig {
            memory_limit_mb: 2048,
            ..AnalyticsConfig::default()
        };
        let err = validate_against(&cfg, SQL_MAX_CONCURRENCY)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("analytics.memory_limit"),
            "must name analytics.memory_limit: {err}"
        );
        assert!(
            err.contains("ingestion_reservation"),
            "must name ingestion_reservation: {err}"
        );
        assert!(
            err.contains("runtime_headroom"),
            "must name runtime_headroom: {err}"
        );
        assert!(err.contains("sql_permits"), "must name sql_permits: {err}");
        assert!(
            err.contains("max_temp_size") && err.contains("disk"),
            "must say max_temp_size is not RAM: {err}"
        );
        assert!(
            err.contains(ENV_MEMORY_LIMIT) && err.contains(ENV_INGESTION_RESERVATION),
            "must name the env keys an operator can actually set: {err}"
        );
    }

    #[test]
    fn raising_permits_without_lowering_memory_is_refused() {
        let err = validate_against(&AnalyticsConfig::default(), 4)
            .unwrap_err()
            .to_string();
        assert!(err.contains("analytics.memory_limit"), "{err}");
        assert!(err.contains("NUTHATCH_SQL_MAX_CONCURRENCY"), "{err}");
    }

    #[test]
    fn fitting_memory_at_four_permits_is_accepted() {
        let cfg = AnalyticsConfig {
            memory_limit_mb: 256,
            ..AnalyticsConfig::default()
        };
        validate_against(&cfg, 4).expect("4 × 256 + 1024 = 2048");
    }

    #[test]
    fn explicit_reservation_that_overfills_is_refused() {
        let cfg = AnalyticsConfig {
            ingestion_reservation_mb: Some(1536),
            ..AnalyticsConfig::default()
        };
        let err = validate_against(&cfg, SQL_MAX_CONCURRENCY)
            .unwrap_err()
            .to_string();
        assert!(err.contains("ingestion_reservation"), "{err}");
    }

    /// The finding on #1241: `2 × 768 + 512 = 2048` balances, and is still a cursor that can go
    /// over 2 GiB, because the 512 reserves accounting space and caps nothing. A split may not buy
    /// Burrmill room by writing down a smaller number for ingest.
    #[test]
    fn lowering_the_reservation_to_buy_duckdb_memory_is_refused() {
        let cfg = AnalyticsConfig {
            memory_limit_mb: 768,
            ingestion_reservation_mb: Some(512),
            ..AnalyticsConfig::default()
        };
        let err = validate_against(&cfg, SQL_MAX_CONCURRENCY)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("ingestion_reservation") && err.contains("floor"),
            "must say the reservation is a floor: {err}"
        );
        assert!(err.contains(ENV_INGESTION_RESERVATION), "{err}");
    }

    /// Raising it is the conservative direction and stays allowed: Burrmill gets less, not more.
    #[test]
    fn raising_the_reservation_is_allowed_and_shrinks_duckdb() {
        let cfg = AnalyticsConfig {
            memory_limit_mb: 256,
            ingestion_reservation_mb: Some(1536),
            ..AnalyticsConfig::default()
        };
        validate_against(&cfg, SQL_MAX_CONCURRENCY).expect("2 × 256 + 1536 = 2048");
    }

    /// The property the floor buys, stated as a test rather than as a paragraph: whatever an
    /// operator writes, an accepted split never hands Burrmill more than the shipped default that
    /// the footprint CI job measures.
    #[test]
    fn no_accepted_split_gives_duckdb_more_than_the_measured_default() {
        let shipped = SQL_MAX_CONCURRENCY as u64 * DEFAULT_MEMORY_LIMIT_MB;
        let mut accepted = 0;
        for permits in 1..=SQL_MAX_CONCURRENCY_CEILING {
            for memory_limit_mb in [64, 128, 256, 512, 768, 1024, 1536, 2048] {
                for reservation in [0, 256, 512, 1023, 1024, 1536, 2048] {
                    let cfg = AnalyticsConfig {
                        memory_limit_mb,
                        ingestion_reservation_mb: Some(reservation),
                        ..AnalyticsConfig::default()
                    };
                    if validate_against(&cfg, permits).is_err() {
                        continue;
                    }
                    accepted += 1;
                    let duck = permits as u64 * memory_limit_mb;
                    assert!(
                        duck <= shipped,
                        "accepted {permits} × {memory_limit_mb} MB = {duck} MB of Burrmill with a \
                         {reservation} MB reservation, above the measured default of {shipped} MB"
                    );
                }
            }
        }
        assert!(
            accepted > 10,
            "only {accepted} splits were accepted; the gate refuses everything and the assertion \
             above is vacuous"
        );
    }

    #[test]
    fn max_temp_size_does_not_enter_the_ram_equation() {
        let cfg = AnalyticsConfig {
            max_temp_size: Some("100GB".into()),
            ..AnalyticsConfig::default()
        };
        validate_against(&cfg, SQL_MAX_CONCURRENCY).expect("disk cap is not RAM");
    }

    #[test]
    fn permits_above_the_gate_ceiling_are_costed_at_the_ceiling() {
        let cfg = AnalyticsConfig {
            memory_limit_mb: 64,
            ingestion_reservation_mb: Some(1024),
            ..AnalyticsConfig::default()
        };
        validate_against(&cfg, 64).expect("clamped to 16 × 64 + 1024 = 2048");
        let too_big = AnalyticsConfig {
            memory_limit_mb: 128,
            ingestion_reservation_mb: Some(1024),
            ..AnalyticsConfig::default()
        };
        assert!(validate_against(&too_big, 64).is_err());
        assert_eq!(SQL_MAX_CONCURRENCY_CEILING, 16);
    }

    #[test]
    fn zero_memory_limit_is_refused() {
        let cfg = AnalyticsConfig {
            memory_limit_mb: 0,
            ..AnalyticsConfig::default()
        };
        let err = validate_against(&cfg, SQL_MAX_CONCURRENCY)
            .unwrap_err()
            .to_string();
        assert!(err.contains("analytics.memory_limit"), "{err}");
    }

    #[test]
    fn zero_ingest_reservation_is_refused() {
        let cfg = AnalyticsConfig {
            memory_limit_mb: 512,
            ingestion_reservation_mb: Some(0),
            ..AnalyticsConfig::default()
        };
        let err = validate_against(&cfg, 1).unwrap_err().to_string();
        assert!(
            err.contains("ingestion_reservation"),
            "must name ingestion_reservation: {err}"
        );
        assert!(
            err.contains(ENV_INGESTION_RESERVATION),
            "must name the env key an operator can actually set: {err}"
        );
        validate_against(&AnalyticsConfig::default(), SQL_MAX_CONCURRENCY)
            .expect("today's walls must still start");
    }

    #[test]
    fn oversize_thread_count_is_refused() {
        let cfg = AnalyticsConfig {
            threads: i64::MAX,
            ..AnalyticsConfig::default()
        };
        let err = validate_against(&cfg, SQL_MAX_CONCURRENCY)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("analytics.threads"),
            "must name analytics.threads: {err}"
        );
        assert!(
            err.contains(&THREADS_CEILING.to_string()),
            "must name the ceiling: {err}"
        );
        assert!(
            err.contains(ENV_THREADS),
            "must name the env key an operator can actually set: {err}"
        );
        validate_against(
            &AnalyticsConfig {
                threads: THREADS_CEILING,
                ..AnalyticsConfig::default()
            },
            SQL_MAX_CONCURRENCY,
        )
        .expect("the ceiling itself must still start");
    }

    #[test]
    fn from_env_preserves_oversize_threads_for_the_validator() {
        let _g = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var(ENV_THREADS).ok();
        std::env::set_var(ENV_THREADS, "17");
        let _restore = EnvRestore { prev };
        let cfg = from_env();
        assert_eq!(
            cfg.threads, 17,
            "parsing must not clamp 17 to {THREADS_CEILING}"
        );
        let err = validate_against(&cfg, SQL_MAX_CONCURRENCY)
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("analytics.threads"),
            "must name analytics.threads: {err}"
        );
        assert!(
            err.contains(&THREADS_CEILING.to_string()),
            "must name the ceiling: {err}"
        );
        assert!(
            err.contains(ENV_THREADS),
            "must name the env key an operator can actually set: {err}"
        );
    }

    /// The second finding on #1241. `NUTHATCH_INGESTION_RESERVATION=0` used to fall through to
    /// `None`, so the derived 1024 applied and the process started - the documented knob could not
    /// express the one value it is documented to reject.
    #[test]
    fn an_explicit_zero_reservation_reaches_the_validator() {
        let _g = env_lock().lock().unwrap_or_else(|e| e.into_inner());
        let prev = std::env::var(ENV_INGESTION_RESERVATION).ok();
        struct Restore(Option<String>);
        impl Drop for Restore {
            fn drop(&mut self) {
                match self.0.as_deref() {
                    Some(v) => std::env::set_var(ENV_INGESTION_RESERVATION, v),
                    None => std::env::remove_var(ENV_INGESTION_RESERVATION),
                }
            }
        }
        std::env::set_var(ENV_INGESTION_RESERVATION, "0");
        let _restore = Restore(prev);
        let cfg = from_env();
        assert_eq!(
            cfg.ingestion_reservation_mb,
            Some(0),
            "an explicit 0 must not read as unset"
        );
        assert_ne!(
            cfg.reservation_mb(),
            derived_ingestion_reservation_mb(),
            "a written 0 must not silently become the derived default"
        );
        let err = validate_against(&cfg, SQL_MAX_CONCURRENCY)
            .unwrap_err()
            .to_string();
        assert!(err.contains(ENV_INGESTION_RESERVATION), "{err}");
    }

    #[test]
    fn parse_memory_accepts_the_rfc_spellings() {
        assert_eq!(parse_memory_mb("512"), Some(512));
        assert_eq!(parse_memory_mb("512MB"), Some(512));
        assert_eq!(parse_memory_mb(" 512mb "), Some(512));
        assert_eq!(parse_memory_mb("1GB"), Some(1024));
        assert_eq!(parse_memory_mb("2GiB"), Some(2048));
        assert_eq!(parse_memory_mb("1G"), Some(1024));
        assert_eq!(parse_memory_mb(""), None);
        assert_eq!(parse_memory_mb("eight"), None);
        assert_eq!(parse_memory_mb("1.5GB"), None);
        assert_eq!(parse_memory_mb("512TB"), None);
    }

    #[test]
    fn parse_size_normalises() {
        assert_eq!(parse_size("10GB").as_deref(), Some("10GB"));
        assert_eq!(parse_size("512").as_deref(), Some("512MB"));
        assert_eq!(parse_size("512MB").as_deref(), Some("512MB"));
        assert_eq!(parse_size("nope"), None);
    }
}
