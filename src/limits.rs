//! Per-mount query limits (RFC-0073 FR3).
//!
//! The `/sql` guards were compile-time constants. They are now process defaults, each settable by
//! env and never above the constant it replaced, and a mount record may lower any of them for that
//! mount alone. Mount config, like `sql`: outside the NID, so setting one re-indexes nothing.

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The guards one mount's requests run under, already clamped to the process values.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    pub timeout: Duration,
    pub max_rows: usize,
    pub max_result_bytes: usize,
    pub max_hot_rows: usize,
    pub max_hot_bytes: u64,
    pub max_named_scan_bytes: u64,
    /// The analytics pool this mount's statements may hold; `None` is the pool's own size.
    pub analytics_memory_bytes: Option<u64>,
    pub max_queued: usize,
}

/// `[mounts.limits]`: what a mount asks for. Every field optional; absent means the process value.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct MountLimits {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub statement_timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_result_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_hot_rows: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_hot_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_named_scan_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub analytics_memory_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_queued: Option<u64>,
}

pub const ENV_TIMEOUT_MS: &str = "NUTHATCH_SQL_TIMEOUT_MS";
pub const ENV_MAX_ROWS: &str = "NUTHATCH_SQL_MAX_ROWS";
pub const ENV_MAX_RESULT_BYTES: &str = "NUTHATCH_SQL_MAX_RESULT_BYTES";
pub const ENV_MAX_HOT_ROWS: &str = "NUTHATCH_SQL_MAX_HOT_ROWS";
pub const ENV_MAX_HOT_BYTES: &str = "NUTHATCH_SQL_MAX_HOT_BYTES";
pub const ENV_MAX_NAMED_SCAN_BYTES: &str = "NUTHATCH_SQL_MAX_NAMED_SCAN_BYTES";
pub const ENV_MAX_QUEUED: &str = "NUTHATCH_SQL_MAX_QUEUED";

impl Limits {
    /// The constants these guards were before FR3. No env value or mount may exceed one.
    pub fn ceilings() -> Self {
        Self {
            timeout: crate::serve::SQL_TIMEOUT,
            max_rows: crate::serve::SQL_MAX_ROWS,
            max_result_bytes: crate::engine::SQL_MAX_RESULT_BYTES,
            max_hot_rows: crate::serve::SQL_MAX_HOT_ROWS,
            max_hot_bytes: crate::serve::SQL_MAX_HOT_BYTES,
            max_named_scan_bytes: crate::serve::SQL_MAX_NAMED_SCAN_BYTES,
            analytics_memory_bytes: None,
            max_queued: crate::serve::SQL_MAX_QUEUED,
        }
    }

    /// The process values: the ceilings, lowered by env. Read once.
    pub fn process() -> Self {
        static PROCESS: std::sync::OnceLock<Limits> = std::sync::OnceLock::new();
        *PROCESS.get_or_init(|| Self::from_lookup(|k| std::env::var(k).ok()))
    }

    fn from_lookup(get: impl Fn(&str) -> Option<String>) -> Self {
        let c = Self::ceilings();
        let env = |key: &str, ceiling: u64| -> u64 {
            let Some(raw) = get(key) else {
                return ceiling;
            };
            match raw.trim().parse::<u64>() {
                Ok(0) | Err(_) => {
                    tracing::warn!(value = %raw, "{key} is not a positive integer; using {ceiling}");
                    ceiling
                }
                Ok(n) if n > ceiling => {
                    tracing::warn!(
                        requested = n,
                        ceiling,
                        "{key} is above its ceiling; clamping"
                    );
                    ceiling
                }
                Ok(n) => n,
            }
        };
        Self {
            timeout: Duration::from_millis(env(ENV_TIMEOUT_MS, c.timeout.as_millis() as u64)),
            max_rows: env(ENV_MAX_ROWS, c.max_rows as u64) as usize,
            max_result_bytes: env(ENV_MAX_RESULT_BYTES, c.max_result_bytes as u64) as usize,
            max_hot_rows: env(ENV_MAX_HOT_ROWS, c.max_hot_rows as u64) as usize,
            max_hot_bytes: env(ENV_MAX_HOT_BYTES, c.max_hot_bytes),
            max_named_scan_bytes: env(ENV_MAX_NAMED_SCAN_BYTES, c.max_named_scan_bytes),
            analytics_memory_bytes: None,
            max_queued: env(ENV_MAX_QUEUED, c.max_queued as u64) as usize,
        }
    }

    /// The analytics pool a dataset's statements share, which bounds `analytics_memory_bytes`.
    pub fn analytics_pool_bytes() -> u64 {
        crate::analytics_budget::from_env().burrmill_limit_mb() << 20
    }
}

impl MountLimits {
    pub fn is_unset(&self) -> bool {
        *self == Self::default()
    }

    fn fields(&self, process: &Limits, pool: u64) -> [(&'static str, Option<u64>, u64); 8] {
        [
            (
                "statement_timeout_ms",
                self.statement_timeout_ms,
                process.timeout.as_millis() as u64,
            ),
            ("max_rows", self.max_rows, process.max_rows as u64),
            (
                "max_result_bytes",
                self.max_result_bytes,
                process.max_result_bytes as u64,
            ),
            (
                "max_hot_rows",
                self.max_hot_rows,
                process.max_hot_rows as u64,
            ),
            ("max_hot_bytes", self.max_hot_bytes, process.max_hot_bytes),
            (
                "max_named_scan_bytes",
                self.max_named_scan_bytes,
                process.max_named_scan_bytes,
            ),
            ("analytics_memory_bytes", self.analytics_memory_bytes, pool),
            ("max_queued", self.max_queued, process.max_queued as u64),
        ]
    }

    /// Refuses a value above the process's, or zero, naming the field and the ceiling.
    pub fn check(&self) -> Result<()> {
        self.check_against(&Limits::process(), Limits::analytics_pool_bytes())
    }

    fn check_against(&self, process: &Limits, pool: u64) -> Result<()> {
        for (field, asked, ceiling) in self.fields(process, pool) {
            match asked {
                Some(0) => bail!("limits.{field} must be at least 1"),
                Some(n) if n > ceiling => bail!(
                    "limits.{field} = {n} is above this runtime's ceiling of {ceiling}; \
                     a mount may only lower a limit"
                ),
                _ => {}
            }
        }
        Ok(())
    }

    /// The limits a mount's requests run under. A recorded value the process has since lowered
    /// past is held to the process value, and said so, rather than refusing a boot over it.
    pub fn resolve(&self, route: &str) -> Limits {
        self.resolve_against(Limits::process(), Limits::analytics_pool_bytes(), route)
    }

    fn resolve_against(&self, process: Limits, pool: u64, route: &str) -> Limits {
        if let Err(e) = self.check_against(&process, pool) {
            tracing::warn!("mount '{route}': {e:#}; held to the runtime's value");
        }
        let lower = |asked: Option<u64>, ceiling: u64| {
            asked.filter(|n| *n > 0).map_or(ceiling, |n| n.min(ceiling))
        };
        Limits {
            timeout: Duration::from_millis(lower(
                self.statement_timeout_ms,
                process.timeout.as_millis() as u64,
            )),
            max_rows: lower(self.max_rows, process.max_rows as u64) as usize,
            max_result_bytes: lower(self.max_result_bytes, process.max_result_bytes as u64)
                as usize,
            max_hot_rows: lower(self.max_hot_rows, process.max_hot_rows as u64) as usize,
            max_hot_bytes: lower(self.max_hot_bytes, process.max_hot_bytes),
            max_named_scan_bytes: lower(self.max_named_scan_bytes, process.max_named_scan_bytes),
            analytics_memory_bytes: self
                .analytics_memory_bytes
                .filter(|n| *n > 0)
                .map(|n| n.min(pool)),
            max_queued: lower(self.max_queued, process.max_queued as u64) as usize,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn env_lowers_a_limit_and_never_raises_one() {
        let env = |pairs: &'static [(&'static str, &'static str)]| {
            move |k: &str| {
                pairs
                    .iter()
                    .find(|(n, _)| *n == k)
                    .map(|(_, v)| v.to_string())
            }
        };
        let lowered = Limits::from_lookup(env(&[(ENV_MAX_ROWS, "10"), (ENV_TIMEOUT_MS, "1500")]));
        assert_eq!(lowered.max_rows, 10);
        assert_eq!(lowered.timeout, Duration::from_millis(1500));
        let raised =
            Limits::from_lookup(env(&[(ENV_MAX_ROWS, "999999999"), (ENV_MAX_QUEUED, "0")]));
        assert_eq!(raised.max_rows, crate::serve::SQL_MAX_ROWS);
        assert_eq!(raised.max_queued, crate::serve::SQL_MAX_QUEUED);
        assert_eq!(Limits::from_lookup(|_| None), Limits::ceilings());
    }

    #[test]
    fn a_mount_above_the_process_value_is_refused_by_name() {
        let process = Limits::ceilings();
        let over = MountLimits {
            max_rows: Some(process.max_rows as u64 + 1),
            ..Default::default()
        };
        let e = over
            .check_against(&process, 1 << 20)
            .unwrap_err()
            .to_string();
        assert!(
            e.contains("limits.max_rows") && e.contains(&process.max_rows.to_string()),
            "{e}"
        );
        let memory = MountLimits {
            analytics_memory_bytes: Some((1 << 20) + 1),
            ..Default::default()
        };
        assert!(memory.check_against(&process, 1 << 20).is_err());
        let zero = MountLimits {
            max_queued: Some(0),
            ..Default::default()
        };
        assert!(zero.check_against(&process, 1 << 20).is_err());
        let under = MountLimits {
            max_rows: Some(5),
            statement_timeout_ms: Some(100),
            ..Default::default()
        };
        under.check_against(&process, 1 << 20).unwrap();
        let resolved = under.resolve_against(process, 1 << 20, "a");
        assert_eq!(resolved.max_rows, 5);
        assert_eq!(resolved.timeout, Duration::from_millis(100));
        assert_eq!(resolved.max_hot_rows, process.max_hot_rows);
    }

    #[test]
    fn a_recorded_value_the_process_lowered_past_is_held_to_it() {
        let mut process = Limits::ceilings();
        process.max_rows = 10;
        let recorded = MountLimits {
            max_rows: Some(100),
            ..Default::default()
        };
        assert_eq!(recorded.resolve_against(process, 1 << 20, "a").max_rows, 10);
    }
}
