//! The control plane (RFC-0022 §3): **desired state**, and who is available to run it.
//!
//! Two tables and no cleverness. `desired_nest` is what the operator asked for; `worker` is who is
//! currently alive to serve it. The scheduler ([`crate::scheduler::plan`]) reads both and decides
//! placement; nothing here decides anything.
//!
//! ## What this holds, and what it deliberately does not
//!
//! It holds **intent**. It does *not* hold ownership - which worker currently runs which cursor lives
//! in the hot store, next to the fence, for the reasons argued in RFC-0022's lease-placement note: a
//! lease and a fence in two different databases can disagree, and that disagreement is exactly the
//! split brain the fence exists to prevent.
//!
//! The practical consequence is worth stating, because it looks like a gap: **the control plane can
//! be wrong about what is running and nothing breaks.** It is a statement of what *should* be true.
//! Reconciliation reads ownership from the hot stores, which cannot be stale, because a lease is only
//! meaningful in the transaction that took it.
//!
//! ## Secrets
//!
//! MountTable secrets (RFC-0019 §4 credential kind **b**) live here too, keyed by nest. They are stored
//! **outside** the content-addressed bundle on purpose: baking a credential into a bundle would leak
//! it *and* break addressing, because two nests differing only in credentials would hash differently.
//! Rotating a secret is a control-plane write that changes no bundle hash.
//!
//! The interface is **write-only**. Values go in and are handed to the worker that mounts the nest;
//! no method returns one to an operator, and the HTTP surface exposes only key names. A control plane
//! that can read back every credential it holds is a credential dump with extra steps.
//!
//! ## Liveness
//!
//! A worker is alive if it has heartbeated within its TTL, measured on the **database's** clock - the
//! same discipline as the lease, and for the same reason. A worker whose own clock ran fast would
//! otherwise declare itself alive after everyone else had written it off, and the fleet would plan
//! around a machine that is not there.
//!
//! Losing the heartbeat does **not** stop a worker writing: that is the lease's job, and it expires
//! on its own schedule. The two are deliberately independent - a control plane outage must not stop
//! ingestion, only stop *rescheduling*.

use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;

use crate::scheduler::{DesiredNest, Worker};

/// Meta-free, schema-per-fleet. One control plane serves one operator's fleet, so the schema name is
/// fixed rather than parameterised - a second fleet is a second database, not a second namespace in
/// the first.
const SCHEMA: &str = "nuthatch_control";

/// What an endpoint resolves to, fleet-wide (RFC-0022 §4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Resolution {
    /// The endpoint asked for. Under RFC-0020 a breaking update is served at a **new** endpoint
    /// alongside the old, so `usdc` and `usdc-v2` are two rows here, not one row with two versions.
    pub endpoint: String,
    pub chain: String,
    /// `None` when declared but not yet pinned - see [`ControlPlane::resolve`].
    pub version: Option<String>,
    pub bundle_hash: Option<String>,
}

impl Resolution {
    /// Whether an FE may serve this endpoint. An unpinned endpoint is declared-but-not-ready, and
    /// serving it would mean each node choosing a version for itself - the inconsistency pinning
    /// exists to prevent.
    pub fn is_servable(&self) -> bool {
        self.version.is_some() && self.bundle_hash.is_some()
    }
}

/// A worker's heartbeat TTL. Generous on purpose: rescheduling a cursor is expensive (drain, lease
/// handover, re-warm), so a worker that pauses for a few seconds should not lose its work. The lease
/// TTL is the tighter of the two, and it is the one that actually guards correctness.
pub const DEFAULT_WORKER_TTL_SECS: u64 = 30;

pub struct ControlPlane {
    conn: crate::pgstore::Conn,
}

impl ControlPlane {
    /// Connect and ensure the control-plane schema exists.
    pub fn connect(url: &str) -> Result<ControlPlane> {
        let config: postgres::Config = url
            .parse()
            .with_context(|| format!("cannot parse control-plane URL '{}'", redact(url)))?;
        let conn = crate::pgstore::Conn::spawn(config)
            .with_context(|| format!("cannot reach the control plane at '{}'", redact(url)))?;
        let cp = ControlPlane { conn };
        cp.migrate()?;
        Ok(cp)
    }

    /// Create the schema and tables. Idempotent **and concurrency-safe**.
    ///
    /// `CREATE SCHEMA IF NOT EXISTS` and `CREATE TABLE IF NOT EXISTS` are **not atomic** in Postgres:
    /// two processes running one at the same instant both see it missing, both try, and one dies with
    /// `duplicate key value violates unique constraint "pg_namespace_nspname_index"` (or
    /// `pg_type_typname_nsp_index` for a table). Not theoretical - it is what happened the first time
    /// the compose fleet came up, because the control plane and every worker connect and migrate at
    /// startup.
    ///
    /// The advisory lock is taken as **its own statement**, which matters: embedding
    /// `BEGIN; SELECT pg_advisory_xact_lock(…); …; COMMIT;` inside a `batch_execute` does not work,
    /// because the simple query protocol already wraps a multi-statement batch in an implicit
    /// transaction and the explicit BEGIN/COMMIT fight it. The first attempt did exactly that and the
    /// race simply moved from `pg_namespace` to `pg_type`.
    fn migrate(&self) -> Result<()> {
        self.conn.with(move |c| {
            // Arbitrary but fixed: any constant works so long as every migrator uses the same one.
            const LOCK: i64 = 3_141_592_653_589_793;
            c.execute("SELECT pg_advisory_lock($1)", &[&LOCK])?;
            let ddl = format!(
                r#"
                CREATE SCHEMA IF NOT EXISTS "{SCHEMA}";
                CREATE TABLE IF NOT EXISTS "{SCHEMA}".desired_nest (
                    name              TEXT PRIMARY KEY,
                    chain             TEXT        NOT NULL,
                    estimated_rss_mb  BIGINT      NOT NULL,
                    added_at          TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                -- RFC-0022 §4. Added by ALTER rather than baked above so an existing control plane
                -- upgrades in place: `CREATE TABLE IF NOT EXISTS` silently does nothing to a table
                -- that already exists, which would leave an older fleet without these columns and
                -- with no error to explain why.
                ALTER TABLE "{SCHEMA}".desired_nest
                    ADD COLUMN IF NOT EXISTS version     TEXT,
                    ADD COLUMN IF NOT EXISTS bundle_hash TEXT;
                CREATE TABLE IF NOT EXISTS "{SCHEMA}".worker (
                    id            TEXT PRIMARY KEY,
                    budget_mb     BIGINT      NOT NULL,
                    last_seen_at  TIMESTAMPTZ NOT NULL DEFAULT now()
                );
                -- MountTable secrets (RFC-0019 §4 credential kind (b), mechanism per RFC-0022 §5).
                -- Keyed by nest and **never** by bundle: baking a secret into a content-addressed
                -- bundle would both leak it and break addressing, since two nests differing only in
                -- credentials would hash differently. Rotating a secret here changes no bundle hash.
                CREATE TABLE IF NOT EXISTS "{SCHEMA}".nest_secret (
                    nest        TEXT        NOT NULL,
                    key         TEXT        NOT NULL,
                    value       TEXT        NOT NULL,
                    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
                    PRIMARY KEY (nest, key)
                );
                "#
            );
            let result = c.batch_execute(&ddl);
            // Released even if the migration failed, so one bad startup cannot wedge every other
            // process behind a lock nobody is holding for a reason.
            let _ = c.execute("SELECT pg_advisory_unlock($1)", &[&LOCK]);
            result?;
            Ok(())
        })
    }

    // ---- desired state -------------------------------------------------------------------------

    /// Declare a nest should be running. Idempotent - re-declaring updates chain and estimate rather
    /// than failing, so an operator can correct a mistake without a delete-then-add dance that would
    /// briefly drain a healthy cursor.
    pub fn declare_nest(&self, nest: &DesiredNest) -> Result<()> {
        if nest.name.is_empty() || nest.chain.is_empty() {
            return Err(anyhow!("a desired nest needs both a name and a chain"));
        }
        // Every worker joins the name onto its nest root and cache, so it is held to the mount-name rules.
        crate::runtime::check_mount_name(&nest.name, crate::runtime::DEFAULT_TENANT)?;
        let (name, chain, mb) = (
            nest.name.clone(),
            nest.chain.clone(),
            nest.estimated_rss_mb as i64,
        );
        self.conn.with(move |c| {
            c.execute(
                &format!(
                    "INSERT INTO \"{SCHEMA}\".desired_nest (name, chain, estimated_rss_mb) \
                     VALUES ($1, $2, $3) ON CONFLICT (name) DO UPDATE \
                     SET chain = EXCLUDED.chain, estimated_rss_mb = EXCLUDED.estimated_rss_mb"
                ),
                &[&name, &chain, &mb],
            )?;
            Ok(())
        })
    }

    /// Stop wanting a nest. Returns whether it was there - a scheduler logs a removal differently
    /// from a no-op, and an API returns 404 rather than 200 for the second.
    pub fn undeclare_nest(&self, name: &str) -> Result<bool> {
        let name = name.to_string();
        self.conn.with(move |c| {
            // The secrets go with the nest. Without this a deleted nest leaves its provider
            // credentials in the control plane forever: unreachable through any surface that
            // lists nests, so nobody sees them again, and silently inherited by the next nest
            // declared under the same name.
            //
            // Found auditing #426, which reported this one layer up as a test-fixture defect -
            // the fixture cleans up by undeclaring every nest, so a `put_secret` mutated to
            // write nothing still passed against rows a previous run had orphaned. Fixing the
            // fixture would have hidden this.
            c.execute(
                &format!("DELETE FROM \"{SCHEMA}\".nest_secret WHERE nest = $1"),
                &[&name],
            )?;
            let n = c.execute(
                &format!("DELETE FROM \"{SCHEMA}\".desired_nest WHERE name = $1"),
                &[&name],
            )?;
            Ok(n > 0)
        })
    }

    /// Everything the operator currently wants running, name-ordered so the result is stable.
    pub fn desired(&self) -> Result<Vec<DesiredNest>> {
        self.conn.with(move |c| {
            let rows = c.query(
                &format!(
                    "SELECT name, chain, estimated_rss_mb FROM \"{SCHEMA}\".desired_nest \
                     ORDER BY name"
                ),
                &[],
            )?;
            Ok(rows
                .into_iter()
                .map(|r| DesiredNest {
                    name: r.get(0),
                    chain: r.get(1),
                    estimated_rss_mb: r.get::<_, i64>(2).max(0) as u64,
                })
                .collect())
        })
    }

    /// What a worker needs to **fetch** a nest it does not have: see [`crate::worker::NestSource`].
    ///
    /// Separate from [`ControlPlane::desired`] because the scheduler only needs a footprint to place a
    /// cursor, while a worker needs to know *which bundle* to pull. Those columns already existed for
    /// RFC-0022 §4's fleet-wide pinning; nothing was reading them on the worker side, which is why a
    /// worker could be assigned a nest it had no way to locate (issue #250).
    pub fn desired_sources(&self) -> Result<Vec<crate::worker::NestSource>> {
        self.conn.with(move |c| {
            let rows = c.query(
                &format!(
                    "SELECT name, chain, version, bundle_hash FROM \"{SCHEMA}\".desired_nest \
                     ORDER BY name"
                ),
                &[],
            )?;
            Ok(rows
                .into_iter()
                .map(|r| crate::worker::NestSource {
                    name: r.get(0),
                    chain: r.get(1),
                    version: r.get(2),
                    bundle_hash: r.get(3),
                })
                .collect())
        })
    }

    // ---- worker registry -----------------------------------------------------------------------

    /// Register or heartbeat a worker. One call for both: a worker that has been away long enough to
    /// be reaped should be able to rejoin by doing exactly what it does every second anyway, rather
    /// than needing to notice it was reaped and take a different path.
    pub fn heartbeat(&self, id: &str, budget_mb: u64) -> Result<()> {
        if id.is_empty() {
            return Err(anyhow!("a worker needs an id"));
        }
        let (id, budget) = (id.to_string(), budget_mb as i64);
        self.conn.with(move |c| {
            c.execute(
                &format!(
                    "INSERT INTO \"{SCHEMA}\".worker (id, budget_mb, last_seen_at) \
                     VALUES ($1, $2, now()) ON CONFLICT (id) DO UPDATE \
                     SET budget_mb = EXCLUDED.budget_mb, last_seen_at = now()"
                ),
                &[&id, &budget],
            )?;
            Ok(())
        })
    }

    /// Workers seen within `ttl_secs`, by the **database's** clock.
    ///
    /// The comparison is done in SQL rather than by fetching timestamps and comparing here, so a
    /// scheduler with a skewed clock cannot conjure or condemn workers.
    pub fn live_workers(&self, ttl_secs: u64) -> Result<Vec<Worker>> {
        let ttl = ttl_secs as f64;
        self.conn.with(move |c| {
            let rows = c.query(
                &format!(
                    "SELECT id, budget_mb FROM \"{SCHEMA}\".worker \
                     WHERE last_seen_at > now() - make_interval(secs => $1) ORDER BY id"
                ),
                &[&ttl],
            )?;
            Ok(rows
                .into_iter()
                .map(|r| Worker {
                    id: r.get(0),
                    budget_mb: r.get::<_, i64>(1).max(0) as u64,
                })
                .collect())
        })
    }

    // ---- resolution (RFC-0022 §4) --------------------------------------------------------------

    /// Pin the version an endpoint serves, fleet-wide.
    ///
    /// **Why this exists at all**, since RFC-0019 already has a movable `latest` pointer: a movable
    /// pointer read independently by N nodes is not a consistent resolution. FE node A reads
    /// `latest → v2` while node B is still serving v1, and for a window the same endpoint answers
    /// with two different schemas depending on which node a request lands on. That is invisible to
    /// every single-box test and obvious the first time a load balancer is involved.
    ///
    /// So resolution is *pinned here*, and advancing it is a deliberate control-plane write. `latest`
    /// remains the registry's convenience for humans and for `init`; it is not what a fleet serves.
    pub fn pin_version(&self, name: &str, version: &str, bundle_hash: &str) -> Result<bool> {
        if version.is_empty() || bundle_hash.is_empty() {
            return Err(anyhow!("pinning needs both a version and a bundle hash"));
        }
        if !crate::distribution::is_nid_shaped(bundle_hash) {
            return Err(anyhow!(
                "bundle hash '{bundle_hash}' is not 64 hex characters"
            ));
        }
        let (name, version, hash) = (
            name.to_string(),
            version.to_string(),
            bundle_hash.to_string(),
        );
        self.conn.with(move |c| {
            let n = c.execute(
                &format!(
                    "UPDATE \"{SCHEMA}\".desired_nest SET version = $2, bundle_hash = $3 \
                     WHERE name = $1"
                ),
                &[&name, &version, &hash],
            )?;
            Ok(n > 0)
        })
    }

    /// What this endpoint currently serves. `None` if the endpoint is not declared at all;
    /// `Some` with `version: None` if it is declared but unpinned - which an FE must treat as *not
    /// ready to serve* rather than as "serve whatever you have lying about".
    pub fn resolve(&self, name: &str) -> Result<Option<Resolution>> {
        let name = name.to_string();
        self.conn.with(move |c| {
            Ok(c.query_opt(
                &format!(
                    "SELECT name, chain, version, bundle_hash FROM \"{SCHEMA}\".desired_nest \
                     WHERE name = $1"
                ),
                &[&name],
            )?
            .map(|r| Resolution {
                endpoint: r.get(0),
                chain: r.get(1),
                version: r.get(2),
                bundle_hash: r.get(3),
            }))
        })
    }

    // ---- runtime secrets (RFC-0022 §5) ---------------------------------------------------------

    /// Store a secret for a nest - a private RPC URL, an enricher API key.
    ///
    /// **Write-only by design.** There is no method that returns a single secret's value to an
    /// operator, and the HTTP API exposes only key *names*. A control plane that can hand back every
    /// credential it holds is a credential dump with extra steps; the only consumer that needs values
    /// is the worker that is about to mount the nest.
    pub fn set_secret(&self, nest: &str, key: &str, value: &str) -> Result<()> {
        if nest.is_empty() || key.is_empty() {
            return Err(anyhow!("a secret needs both a nest and a key"));
        }
        let (nest, key, value) = (nest.to_string(), key.to_string(), value.to_string());
        self.conn.with(move |c| {
            c.execute(
                &format!(
                    "INSERT INTO \"{SCHEMA}\".nest_secret (nest, key, value) VALUES ($1, $2, $3) \
                     ON CONFLICT (nest, key) DO UPDATE \
                     SET value = EXCLUDED.value, updated_at = now()"
                ),
                &[&nest, &key, &value],
            )?;
            Ok(())
        })
    }

    /// Remove a secret. Reports whether it existed, so rotation scripts can tell a real deletion from
    /// a typo'd key that silently did nothing.
    pub fn delete_secret(&self, nest: &str, key: &str) -> Result<bool> {
        let (nest, key) = (nest.to_string(), key.to_string());
        self.conn.with(move |c| {
            let n = c.execute(
                &format!("DELETE FROM \"{SCHEMA}\".nest_secret WHERE nest = $1 AND key = $2"),
                &[&nest, &key],
            )?;
            Ok(n > 0)
        })
    }

    /// The key *names* held for a nest - never the values. What an operator is allowed to see.
    pub fn secret_keys(&self, nest: &str) -> Result<Vec<String>> {
        let nest = nest.to_string();
        self.conn.with(move |c| {
            Ok(c.query(
                &format!("SELECT key FROM \"{SCHEMA}\".nest_secret WHERE nest = $1 ORDER BY key"),
                &[&nest],
            )?
            .into_iter()
            .map(|r| r.get::<_, String>(0))
            .collect())
        })
    }

    /// Secrets for the nests a worker is **actually assigned**, and no others.
    ///
    /// The scoping is the point, and it is why this takes a list rather than offering a
    /// fetch-everything call: a worker running one nest has no business holding another's
    /// credentials, and the cheapest way to guarantee that is to never send them. `IN`-filtered in
    /// SQL rather than fetched-then-filtered here, so an over-broad query cannot be introduced later
    /// by someone refactoring the filter away.
    pub fn secrets_for(
        &self,
        nests: &[String],
    ) -> Result<HashMap<String, HashMap<String, String>>> {
        if nests.is_empty() {
            return Ok(HashMap::new());
        }
        let nests = nests.to_vec();
        self.conn.with(move |c| {
            let rows = c.query(
                &format!(
                    "SELECT nest, key, value FROM \"{SCHEMA}\".nest_secret \
                     WHERE nest = ANY($1) ORDER BY nest, key"
                ),
                &[&nests],
            )?;
            let mut out: HashMap<String, HashMap<String, String>> = HashMap::new();
            for r in rows {
                out.entry(r.get::<_, String>(0))
                    .or_default()
                    .insert(r.get::<_, String>(1), r.get::<_, String>(2));
            }
            Ok(out)
        })
    }

    /// Forget a worker outright - the graceful-shutdown path, so a worker that is going away on
    /// purpose does not have to be waited out.
    pub fn deregister(&self, id: &str) -> Result<bool> {
        let id = id.to_string();
        self.conn.with(move |c| {
            let n = c.execute(
                &format!("DELETE FROM \"{SCHEMA}\".worker WHERE id = $1"),
                &[&id],
            )?;
            Ok(n > 0)
        })
    }
}

fn redact(url: &str) -> String {
    crate::rpc::redact_url(url)
}
