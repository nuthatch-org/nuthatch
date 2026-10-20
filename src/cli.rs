//! Command-line surface. The whole product is meant to be two commands: `init` and `dev`.

use clap::{Args, Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "nuthatch",
    version,
    about = "Be your own indexer - one binary, one command."
)]
pub struct Cli {
    /// Log output format. `text` (default) is the human-readable line format; `json` emits one
    /// structured object per line - `level`, `target`, `message`/`fields`, `timestamp` - so an
    /// operator can point a log aggregator at stdout instead of scraping text. `global = true` so
    /// it's accepted before or after the subcommand (`nuthatch --log-format json dev`, `nuthatch
    /// dev --log-format json`).
    #[arg(long, global = true, value_enum, default_value_t = LogFormat::Text)]
    pub log_format: LogFormat,

    #[command(subcommand)]
    pub command: Command,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, clap::ValueEnum)]
pub enum LogFormat {
    Text,
    Json,
}

// Declaration order is display order (clap's default `disp_ord` is the order `.subcommand()` is
// called in), and that's what `nuthatch --help` groups by (#674): CORE first (the `init` → `dev` →
// `sql` happy path), then OPERATING, SCALED, COMPLIANCE, ADVANCED - enterprise breadth stays, but
// ranks behind the two-command story instead of interleaved with it by age. `src/help.rs::GROUPS`
// is the heading assignment; `cli::tests::every_visible_subcommand_has_exactly_one_help_group`
// fails the build if a new variant lands here without a matching entry there.
#[derive(Subcommand)]
pub enum Command {
    // --- CORE: the happy path, init -> dev -> sql -> mcp. ---
    /// Scaffold an indexer for a contract: resolve its ABI and write a project here.
    Init(InitArgs),
    /// Add another contract to an existing nest - resolve its ABI and grow the config, no re-init.
    Add(AddArgs),
    /// Run the indexer: poll logs, store entities, and serve the API.
    Dev(DevArgs),
    /// Query a nest's data with SQL - the live tip and sealed history, one surface. Prints a table.
    Sql(SqlArgs),
    /// Serve the Model Context Protocol over stdio (bridges to a running `nuthatch dev`).
    Mcp(McpArgs),

    // --- OPERATING: running and maintaining a nest you already have. ---
    /// Serve a nest without indexing it (RFC-0022 slice 3).
    ///
    /// The query-FE half of scaled mode: it answers entity reads and SQL from state a *writer*
    /// elsewhere is filling, owns no cursor, and never advances one. Read-only and genuinely shared
    /// needs `--hot-store`; without it this still serves the nest's local redb, but exclusively -
    /// see `--hot-store`'s own help.
    Serve(ServeArgs),
    /// Probe an RPC endpoint before trusting a backfill to it: max `eth_getLogs` width, max JSON-RPC
    /// batch size, and archive depth - and print the largest safe `--window`. Each of those limits
    /// otherwise surfaces mid-backfill as a retry loop that looks like slowness.
    Doctor(DoctorArgs),
    /// Run a nest's invariant/parity checks (`checks/*.sql`) against recorded expected results.
    Check(CheckArgs),
    /// Regenerate the derived artifacts (`schema.json`, `llms.txt`, `semantic.toml` footguns) from
    /// `nuthatch.toml` - run after hand-editing the config (e.g. adding a factory `[[template]]`), so
    /// the schema and the derived `*_dec` columns match the tables the config now produces.
    Schema(SchemaArgs),
    /// Benchmark the indexing pipeline (measure first, optimise second - RFC-0004).
    Bench(BenchArgs),
    /// Drain recorded authorisations and settle them (RFC-0046 S3).
    ///
    /// The nest already served. This is the back office: it reads `authorisations.jsonl`, hands each
    /// row to `--exec`, and records settled/failed per payer. Rebuild with `--features counter` or
    /// the command refuses. `--dry-run` lists the queue and writes nothing.
    Settle(SettleArgs),

    // --- SCALED: the docker-compose writer-pool / control-plane topology (RFC-0022). ---
    /// Run a **writer worker** for scaled mode (RFC-0022 §2): reconcile against the control plane,
    /// take cursor leases, index what this worker is assigned.
    ///
    /// This is the scaled-mode counterpart of `dev`. `dev` indexes a nest directory it owns outright;
    /// a worker is told what to run and holds a lease proving it may.
    Worker(WorkerArgs),
    /// Run the control-plane API for scaled mode (RFC-0022 §3): declare what the fleet should run.
    ///
    /// Writes *desired state* only. Workers reconcile against it on their own tick - nothing here
    /// commands a worker or waits for one.
    Control(ControlArgs),

    // --- COMPLIANCE: the labeling/flags/audit substrate (RFC-0008). ---
    /// Manage labeled address sets - the compliance annotation substrate (RFC-0008 C1).
    Labels(LabelsArgs),
    /// Manage sanctions/watch lists as content-addressed snapshots (RFC-0008 C2).
    Lists(ListsArgs),
    /// Build, sign, and verify the signed compliance-pack manifest (RFC-0008 C6).
    Pack(PackArgs),
    /// Audit a nest: `report` summarises its flags, `sealed` re-checks sealed history against a
    /// second endpoint.
    Audit(AuditArgs),

    // --- ADVANCED: authoring extras, packaging, and one-off dataset lifecycle. ---
    /// Derive-first recipes (RFC-0023): add a view that computes a read (e.g. `total_supply`) from
    /// indexed events instead of fetching it with an `eth_call`. No archive node, deterministic, free.
    Recipe(RecipeArgs),
    /// Fetch + cache a token's immutable metadata - `decimals`/`symbol`/`name` (RFC-0023 tier 2). Called
    /// once (they never change) and remembered in `metadata.json`; the constants tier 1 can't derive.
    Metadata(MetadataArgs),
    /// Seal a local file into the explicitly non-chain offchain namespace (RFC-0045 stage 1).
    Offchain(OffchainArgs),
    /// Mirror sealed segments to an object-store prefix (RFC-0052).
    Publish(PublishArgs),
    /// Fill a nest that has not indexed from a published mirror, instead of backfilling over RPC.
    ///
    /// Downloads the mirror's sealed history for this exact nest, checks every file against its
    /// hash, and leaves `dev` to follow the chain from where the mirror ends. The nest directory
    /// must be the one that was published: an edited nest is a different dataset.
    Seed(SeedArgs),
    /// Build verifiable block partitions for address-history nests (RFC-0063 §8).
    ///
    /// Reads every block in a finalized range once, checks each header against its hash and each
    /// withdrawal list against its root, and writes immutable Parquet files and a manifest to a
    /// directory or object store. A nest that opts in through `[address_history.mirror]` downloads
    /// them and verifies them again before using a byte.
    Partitions(PartitionsArgs),
    /// Package a nest as a content-addressed blob - the deploy unit (RFC-0012).
    Nest(NestArgs),
    /// Move a pre-2.0 directory to identity-keyed datasets: `nests/<name>/` becomes `data/<nid>/`,
    /// and `roost.toml` becomes `mounts.toml` (RFC-0032).
    ///
    /// Data is moved, never re-indexed. Idempotent, and safe to run on a directory that is already
    /// partly migrated. Use `--dry-run` first: it prints the entire plan and changes nothing.
    Migrate(MigrateArgs),
    /// Reclaim the disk of datasets nothing mounts any more (RFC-0032 §5).
    ///
    /// Unmounting a nest never deletes its data - re-backfilling is the cost identity-keyed storage
    /// exists to avoid, so unmount/remount is free. This is the explicit way to get the disk back.
    /// It LISTS by default; `--yes` is what deletes.
    Prune(PruneArgs),
    Count(crate::count::CountArgs), // RFC-0054 hook

    /// Regenerate the builder skill's machine-generated references from clap metadata (RFC-0017).
    /// Hidden: a dev/authoring tool, not part of the user-facing two-command story.
    #[command(hide = true)]
    SkillRefs,

    /// Classify a subgraph's schema and mappings into a port report (RFC-0044 S1).
    /// Hidden: authoring tool, not part of the two-command story. Does not scaffold a nest.
    #[command(hide = true)]
    PortReport(PortReportArgs),

    /// Overlay `[[calls]]`, Exact views, checks and a README onto a nest already scaffolded by
    /// `init --from-subgraph` (RFC-0044 S2). Hidden: authoring tool. Does not reimplement the importer.
    #[command(hide = true)]
    PortEmit(PortEmitArgs),

    /// Compare a corpus of GraphQL operations against a reference Graph endpoint and a nest, and
    /// report every field-level divergence (RFC-0053 S0). Hidden: a migration and acceptance
    /// instrument, not part of the two-command story.
    #[command(hide = true)]
    GraphValidate(GraphValidateArgs),

    /// Build checkpoints for a nest's folds over sealed history, or read a fold at a block
    /// (RFC-0059). Hidden, and present only in a build with `--features folds`.
    #[cfg(feature = "folds")]
    #[command(hide = true, subcommand)]
    Fold(FoldCommand),
}

#[derive(Debug, clap::Args)]
pub struct GraphValidateArgs {
    /// A JSON corpus: `[{"name": "...", "query": "..."}]`. Operations are the caller's real
    /// queries; the selection compared is parsed from each one rather than declared separately, so
    /// a corpus cannot under-specify what gets checked.
    #[arg(long)]
    pub corpus: String,
    /// The reference Graph endpoint, taken as correct.
    #[arg(long)]
    pub reference: String,
    /// The nest's GraphQL endpoint, the thing under test.
    #[arg(long)]
    pub nest: String,
}

/// A zero publish interval would have the mirror reconcile in a loop.
fn parse_publish_interval(s: &str) -> Result<std::time::Duration, String> {
    let interval = crate::freshness::parse_span(s)?;
    if interval.is_zero() {
        return Err("the publish interval must be at least one second".into());
    }
    Ok(interval)
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::CommandFactory;

    #[test]
    fn dev_and_serve_refuse_a_zero_publish_interval() {
        for sub in ["dev", "serve"] {
            let err = Cli::command()
                .try_get_matches_from(["nuthatch", sub, "--publish-interval", "0s"])
                .unwrap_err();
            assert_eq!(
                err.kind(),
                clap::error::ErrorKind::ValueValidation,
                "{sub}: {err}"
            );
            assert!(
                err.to_string()
                    .contains("the publish interval must be at least one second"),
                "{sub}: {err}"
            );
        }
    }

    /// #1399: the seal-direct document deadline takes `--poll-interval`'s durations, and `0` for none.
    #[test]
    fn dev_parses_an_ipfs_window_deadline() {
        let deadline = |flag: &[&str]| {
            let argv = ["nuthatch", "dev"].iter().chain(flag);
            match Cli::try_parse_from(argv).unwrap().command {
                Command::Dev(args) => args.ipfs_window_deadline,
                _ => unreachable!(),
            }
        };
        let secs = std::time::Duration::from_secs;
        assert_eq!(deadline(&[]), crate::ipfs_resolve::WINDOW_DEADLINE);
        assert_eq!(deadline(&["--ipfs-window-deadline", "300s"]), secs(300));
        assert_eq!(deadline(&["--ipfs-window-deadline", "5m"]), secs(300));
        assert_eq!(deadline(&["--ipfs-window-deadline", "0"]), secs(0));
        assert!(
            Cli::try_parse_from(["nuthatch", "dev", "--ipfs-window-deadline", "soon"]).is_err()
        );
    }

    /// The grouping in `src/help.rs::GROUPS` must cover exactly the visible subcommands declared
    /// above - no more (a stale name after a rename/removal), no fewer (a new variant nobody sorted
    /// into a heading, which would otherwise fall through render_top_level_help's "unheaded" bucket
    /// silently instead of failing a build). This is the "grouping does not silently drift" gate
    /// #674 asked for.
    #[test]
    fn every_visible_subcommand_has_exactly_one_help_group() {
        let cmd = Cli::command();
        let visible: Vec<&str> = cmd
            .get_subcommands()
            .filter(|s| !s.is_hide_set())
            .map(|s| s.get_name())
            .collect();

        for name in &visible {
            let memberships = crate::help::GROUPS
                .iter()
                .filter(|(_, names)| names.contains(name))
                .count();
            assert_eq!(
                memberships, 1,
                "`{name}` is a visible subcommand but appears in {memberships} help groups (want exactly 1) - update src/help.rs::GROUPS"
            );
        }

        let grouped_count: usize = crate::help::GROUPS
            .iter()
            .map(|(_, names)| names.len())
            .sum();
        assert_eq!(
            grouped_count,
            visible.len(),
            "src/help.rs::GROUPS lists {grouped_count} commands but {} are visible - a group entry \
             names a command that no longer exists",
            visible.len()
        );
    }
}

#[cfg(feature = "folds")]
#[derive(clap::Subcommand)]
pub enum FoldCommand {
    /// Walk sealed history from the latest checkpoint, checkpointing every fold about every
    /// `--window-rows` sealed rows. Refuses while `dev` holds the nest.
    Build {
        #[arg(long, default_value = ".")]
        dir: String,
        #[arg(long, default_value_t = 2_000_000)]
        window_rows: u64,
    },
    /// Print one fold's rows at a block as JSON lines, from the latest checkpoint at or before it.
    Read {
        #[arg(long, default_value = ".")]
        dir: String,
        #[arg(long)]
        fold: String,
        #[arg(long)]
        at: u64,
    },
    /// Measure head evaluation (RFC-0059 S1 gate): one warm process resumes the latest checkpoint
    /// and evaluates every fold at each hot block in turn, discarding each result. Prints JSON.
    Bench {
        #[arg(long, default_value = ".")]
        dir: String,
        /// Evaluations timed after the first, which only warms the connection.
        #[arg(long, default_value_t = 200)]
        iters: usize,
        /// Walk the hot heads through the head snapshots instead (RFC-0059 S3), reporting what the
        /// retained set holds against its declared bound.
        #[arg(long)]
        snapshots: bool,
    },
}

#[derive(Args)]
pub struct PortReportArgs {
    /// Subgraph directory containing `schema.graphql` and the mappings. Nothing is written.
    #[arg(long, default_value = ".")]
    pub dir: String,
}

#[derive(Args)]
pub struct PortEmitArgs {
    /// Subgraph directory containing `schema.graphql` and the mappings.
    #[arg(long, default_value = ".")]
    pub dir: String,
    /// Nest directory already scaffolded by `nuthatch init --from-subgraph`. Overlay is written here.
    #[arg(long)]
    pub out: String,
}

#[derive(Args)]
pub struct OffchainArgs {
    #[command(subcommand)]
    pub what: OffchainWhat,
}

#[derive(Subcommand)]
pub enum OffchainWhat {
    /// Import one CSV, JSON array, or Parquet file as an immutable, content-addressed snapshot.
    Drop(OffchainDropArgs),
    /// Fetch a JSON-array price feed once. Schedule this command on the host; it never runs in the
    /// chain cursor or the query path.
    Pull(OffchainPullArgs),
}

#[derive(Args)]
pub struct OffchainDropArgs {
    /// Source CSV, JSON, or Parquet file. Read once; indexing never reads this path.
    pub file: String,
    /// SQL table name beneath the `offchain__` namespace.
    #[arg(long)]
    pub table: String,
    /// Nest directory containing the offchain namespace.
    #[arg(long, default_value = ".")]
    pub dir: String,
}

#[derive(Args)]
pub struct OffchainPullArgs {
    /// HTTPS URL of the feed. Recorded without its query string, and never part of the nest's
    /// identity.
    pub source: String,
    /// SQL table name beneath the `offchain__` namespace.
    #[arg(long)]
    pub table: String,
    /// The feed's format, declared rather than guessed from the URL.
    #[arg(long, value_enum)]
    pub format: PullFormat,
    /// Nest directory containing the offchain namespace.
    #[arg(long, default_value = ".")]
    pub dir: String,
    /// Refuse a response larger than this many bytes.
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    pub max_bytes: u64,
    /// Give up on one attempt after this many seconds, response body included.
    #[arg(long, default_value_t = 30)]
    pub timeout_secs: u64,
    /// Attempts for a transient failure (connect, timeout, HTTP 429 or 5xx), backing off 1s, 2s,
    /// 4s. Anything else fails at once.
    #[arg(long, default_value_t = 3)]
    pub attempts: u32,
    /// `NAME=ENV_VAR`: send header NAME with ENV_VAR's value. Read at run time, never stored.
    #[arg(long = "header-env", value_name = "NAME=ENV_VAR")]
    pub header_env: Vec<String>,
    /// Report the table stale once its last success is older than this, even with no failure
    /// recorded, so a timer that stopped firing shows. Set it a little above the timer's period.
    #[arg(long)]
    pub stale_after_secs: Option<u64>,
    /// Permit plain http to a loopback address, for a local fixture. Never for a real feed.
    #[arg(long)]
    pub allow_loopback_http: bool,
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum PullFormat {
    /// A JSON array of objects, one row each.
    Json,
}

#[derive(Args)]
pub struct SettleArgs {
    /// Nest directory holding `authorisations.jsonl`.
    #[arg(long, default_value = ".")]
    pub dir: String,
    /// Print the pending queue and write nothing.
    #[arg(long)]
    pub dry_run: bool,
    /// Command that receives one authorisation JSON on stdin. Exit 0 settled, 2 failed, anything
    /// else deferred (row stays pending). Must reconcile retries by network/payer/nonce: a crash
    /// can occur after external settlement but before the local outcome journal is synced.
    #[arg(long)]
    pub exec: Option<String>,
    /// Hand `--exec` up to this many authorisations per invocation, one JSON line each; it prints one
    /// outcome line per row it settled or failed, and an unreported row stays pending.
    #[arg(long, default_value_t = 1, value_parser = clap::value_parser!(u64).range(1..))]
    pub batch: u64,
}

#[derive(Args)]
pub struct SeedArgs {
    /// The mirror `publish sync` wrote: a filesystem path, `s3://bucket/prefix`, or the `https://`
    /// address of a public one.
    #[arg(long)]
    pub from: String,
    /// Nest directory.
    #[arg(long, default_value = ".")]
    pub dir: String,
}

#[derive(Args)]
pub struct PartitionsArgs {
    /// The RPCs to read blocks from, comma-separated and round-robined. Their finalized block bounds
    /// what may be built. Any RPC will do: every consumer verifies what is built against its own.
    #[arg(
        long,
        env = "NUTHATCH_RPC",
        hide_env_values = true,
        value_delimiter = ',',
        required = true
    )]
    pub rpc: Vec<String>,
    /// The chain the RPC must be on, checked before anything is read.
    #[arg(long, default_value_t = 1)]
    pub chain_id: u64,
    /// First block, a multiple of 10,000.
    #[arg(long)]
    pub from: u64,
    /// Last block, one less than a multiple of 10,000.
    #[arg(long)]
    pub to: u64,
    /// A directory, or `s3://bucket/prefix`.
    #[arg(long)]
    pub out: String,
    /// Blocks fetched at once.
    #[arg(long, default_value_t = 8)]
    pub concurrency: usize,
    /// Download every partition the manifest already lists and check its sha256, rebuilding any that
    /// differ. Without it a listed partition is checked by size and the store's ETag alone.
    #[arg(long)]
    pub verify_existing: bool,
    /// Build the proposer catalogue (which validator proposed each block since Dencun) from the
    /// block partitions already in `--out`, instead of the block partitions themselves.
    #[arg(long)]
    pub proposers: bool,
    /// With `--proposers`: a beacon node's API to read headers from. It sees this machine's IP and
    /// the beacon roots asked for.
    #[arg(long, env = "NUTHATCH_BEACON_URL", hide_env_values = true)]
    pub beacon_url: Option<String>,
    /// With `--proposers`: a directory of beacon headers by slot (`slots-<first>.jsonl.gz`), read
    /// before the beacon API. Unverified: every header is checked against the chain like any other.
    #[arg(long)]
    pub beacon_cache: Option<std::path::PathBuf>,
}

#[derive(Args)]
pub struct PublishArgs {
    #[command(subcommand)]
    pub what: PublishWhat,
}

#[derive(Subcommand)]
pub enum PublishWhat {
    /// Reconcile this nest's sealed catalogue onto a prefix (RFC-0052 S1).
    Sync(PublishSyncArgs),
    /// Check every published file's size and ETag against the local segment.
    Verify(PublishVerifyArgs),
    /// Where this nest publishes, how far each side is sealed, and what is still to upload. Writes
    /// nothing.
    Status(PublishStatusArgs),
    /// Mark each table's provisional tail final, so the mirror can carry it (#1851).
    ///
    /// A table's last segment of under 1,000 rows waits to fold into its next seal and is not
    /// published until then. On a chain or contract that has gone quiet that seal never comes. Run
    /// this when you know it will not: it rewrites no file, and later rows start a new segment.
    /// Lists by default; `--yes` acts.
    Finalise(PublishFinaliseArgs),
}

#[derive(Args)]
pub struct PublishStatusArgs {
    /// The prefix `publish sync` writes to.
    #[arg(long)]
    pub target: String,
    /// Nest directory.
    #[arg(long, default_value = ".")]
    pub dir: String,
}

#[derive(Args)]
pub struct PublishSyncArgs {
    /// Filesystem path, `s3://bucket/prefix`, or `memory://…` (tests). Never `nuthatch.toml`.
    #[arg(long)]
    pub target: String,
    /// Nest directory.
    #[arg(long, default_value = ".")]
    pub dir: String,
    /// Print the plan and write nothing.
    #[arg(long)]
    pub dry_run: bool,
}

#[derive(Args)]
pub struct PublishVerifyArgs {
    /// The prefix `publish sync` wrote to.
    #[arg(long)]
    pub target: String,
    /// Nest directory.
    #[arg(long, default_value = ".")]
    pub dir: String,
    /// Re-download and re-hash every object.
    #[arg(long)]
    pub deep: bool,
    /// For stores whose ETag is the object's MD5 (AWS S3 without SSE-KMS or SSE-C, MinIO); use --deep otherwise.
    #[arg(long)]
    pub etag_md5: bool,
}

#[derive(Args)]
pub struct PublishFinaliseArgs {
    /// Nest directory. The nest must be stopped.
    #[arg(long, default_value = ".")]
    pub dir: String,
    /// Mark the listed segments final. Without it, only lists them.
    #[arg(long)]
    pub yes: bool,
}

#[derive(Args)]
pub struct MigrateArgs {
    /// The directory to migrate: a pre-2.0 one with a `roost.toml`, or one already part-migrated.
    #[arg(long, default_value = ".")]
    pub dir: String,
    /// Print the plan - every nest, its identity, and where it would land - without changing
    /// anything.
    #[arg(long)]
    pub dry_run: bool,

    /// Proceed even when a nest's schema would break its consumers. The change is listed either way;
    /// this only decides whether the run stops. Data is safe regardless - this is about queries.
    #[arg(long)]
    pub allow_breaking: bool,
}

#[derive(Args)]
pub struct PruneArgs {
    /// The directory to prune: a 2.0 one with a `mounts.toml`. A pre-2.0 directory is refused with a
    /// pointer to `nuthatch migrate` - nothing is reclaimable until its data is keyed by nid.
    #[arg(long, default_value = ".")]
    pub dir: String,
    /// Actually delete. Without this, prune only reports what it would remove.
    #[arg(long)]
    pub yes: bool,
}

#[derive(Args)]
pub struct NestArgs {
    #[command(subcommand)]
    pub what: NestWhat,
}

#[derive(Args)]
pub struct RecipeArgs {
    #[command(subcommand)]
    pub what: RecipeWhat,
}

#[derive(Args)]
pub struct MetadataArgs {
    #[command(subcommand)]
    pub what: MetadataWhat,
}

#[derive(Subcommand)]
pub enum MetadataWhat {
    /// Fetch + cache immutable metadata for every contract in the nest (skips already-cached ones).
    Fetch(MetadataFetchArgs),
}

#[derive(Args)]
pub struct MetadataFetchArgs {
    /// The nest directory.
    #[arg(long, default_value = ".")]
    pub dir: String,
    /// Use only these `rpc_urls` at runtime (repeatable), without changing the config on disk.
    #[arg(long)]
    pub rpc: Vec<String>,
}

#[derive(Subcommand)]
pub enum RecipeWhat {
    /// List the available derive-first recipes.
    List,
    /// Add a recipe's derived view to a nest's `views/` (never clobbers an existing file).
    Add(RecipeAddArgs),
}

#[derive(Args)]
pub struct RecipeAddArgs {
    /// The recipe name (see `nuthatch recipe list`), e.g. `total_supply`.
    pub name: String,
    /// The contract alias whose table the view derives over. Defaults to the nest's first contract.
    #[arg(long)]
    pub alias: Option<String>,
    /// The nest directory.
    #[arg(long, default_value = ".")]
    pub dir: String,
}

#[derive(Subcommand)]
pub enum NestWhat {
    /// Bundle a nest into one portable, content-addressed `.bundle` file - its authored inputs (config,
    /// ABIs, views, labels, skills) plus a `manifest.json` pinning the expected decode-registry hash.
    /// Share the `.bundle` anywhere (a URL, a file); anyone can `load` it to run your exact nest,
    /// verified by hash. Prints the bundle's content address.
    Bundle(NestBundleArgs),
    /// Load a bundle: verify a `.bundle` (or a URL to one, or an unpacked bundle dir) and install it as
    /// a runnable nest. Checks the manifest format, every file's hash, and that the decode registry
    /// regenerated from the inputs matches the manifest - so a loaded nest decodes exactly as authored.
    /// With `--registry`, the positional is a `name[@version]` reference resolved against that store.
    Load(NestLoadArgs),
    /// Publish a `.bundle` to a registry (RFC-0019) under `name@version`, advancing `latest`. The
    /// registry is a decoupled, optional store: a filesystem path, or S3-compatible object storage
    /// (`s3://bucket/prefix`, configured with the usual `AWS_*` env - needs a build with
    /// on by default). A self-built bundle and `nest load <file|dir>` never need one.
    /// Prints the content address.
    Publish(NestPublishArgs),
    /// Re-key a contract alias: `nuthatch.toml`, the ABI file, and `semantic.toml`.
    ///
    /// Declared, not inferred. `nuthatch schema` will not guess a rename, because it cannot tell
    /// one from a removal, and silent deletion of authored prose is worse than a warning.
    RenameAlias(RenameAliasArgs),
    /// Print the nest's NID: the identity its data is stored under, `data/<nid>/` (RFC-0032 §3).
    ///
    /// Run `nuthatch schema` first. The derived files it writes are part of the identity, and a nest
    /// under `data/<nid>/` never regenerates them.
    Nid(NestNidArgs),
}

#[derive(Args)]
pub struct NestNidArgs {
    /// Nest directory (must contain a nuthatch.toml).
    #[arg(long, default_value = ".")]
    pub dir: String,
}

#[derive(Args)]
pub struct RenameAliasArgs {
    /// Current alias, e.g. `c0`.
    pub old: String,
    /// New alias, e.g. `gns`. Must match `[a-z][a-z0-9_]*`.
    pub new: String,
    /// Nest directory (must contain a nuthatch.toml).
    #[arg(long, default_value = ".")]
    pub dir: String,
}

#[derive(Args)]
pub struct NestBundleArgs {
    /// Nest directory to bundle.
    #[arg(default_value = ".")]
    pub dir: String,

    /// Output path for the `.bundle` (default: `<nest-name>-<hash>.bundle` beside the nest). With
    /// `--as-dir`, an unpacked bundle *directory* is written here instead of a single file.
    #[arg(long)]
    pub out: Option<String>,

    /// Write an unpacked bundle directory instead of a single `.bundle` file (handy for inspecting a
    /// bundle's contents).
    #[arg(long)]
    pub as_dir: bool,

    /// Bundle even though `nuthatch.toml` holds credentials (a keyed RPC URL, a webhook secret).
    /// The bundle carries the file verbatim, so they are published with it.
    #[arg(long)]
    pub allow_secrets: bool,
}

#[derive(Args)]
pub struct NestLoadArgs {
    /// The bundle to load: a `.bundle` file, an `http(s)://` URL to one, or an unpacked bundle
    /// directory - or, with `--registry`, a `name[@version]` reference (no `@version` → `latest`), or a
    /// NID as `nuthatch nest nid` prints it, whose bundle must compute to that NID.
    pub bundle: String,

    /// Target directory to install the nest into (default: the nest's name).
    #[arg(long)]
    pub dir: Option<String>,

    /// Assert the bundle's content-address hash equals this value before installing.
    #[arg(long)]
    pub expect: Option<String>,

    /// Resolve the positional as a `name[@version]` reference against this registry (RFC-0019). A
    /// filesystem path, or `s3://bucket/prefix` (S3/MinIO/R2, via the usual `AWS_*` env). The pulled blob is
    /// hash-verified on install.
    #[arg(long)]
    pub registry: Option<String>,
}

#[derive(Args)]
pub struct NestPublishArgs {
    /// The `.bundle` file to publish (from `nuthatch nest bundle`).
    pub bundle: String,

    /// The registry to publish to (RFC-0019). A filesystem path, or `s3://bucket/prefix` with
    /// configured via the usual `AWS_*` env (S3/MinIO/R2).
    #[arg(long)]
    pub registry: String,

    /// Publish as `name` or `name@version`. Defaults: name = the bundle's nest name; version =
    /// `h<hash12>` (a content-addressed label - semantic versions are RFC-0020's concern).
    #[arg(long = "as")]
    pub as_ref: Option<String>,
}

#[derive(Args)]
pub struct PackArgs {
    #[command(subcommand)]
    pub what: PackWhat,
}

#[derive(Subcommand)]
pub enum PackWhat {
    /// Generate an ed25519 signing keypair into a local JSON file.
    Keygen(PackKeygenArgs),
    /// Assemble `compliance-pack.toml` from the nest's config + artifact hashes (optionally signed).
    Build(PackBuildArgs),
    /// Verify a pack: its signature against the signer's public key you were given, and, when the
    /// nest's config sits beside it, that it still describes that nest. An unsigned pack fails.
    Verify(PackVerifyArgs),
}

#[derive(Args)]
pub struct PackKeygenArgs {
    /// Where to write the keypair JSON. Defaults to ~/.nuthatch/nuthatch-key.json, outside any nest.
    #[arg(long)]
    pub out: Option<String>,
}

#[derive(Args)]
pub struct PackBuildArgs {
    /// Nest directory.
    #[arg(long, default_value = ".")]
    pub dir: String,
    /// Sign the manifest with this keypair file (from `pack keygen`). Omit to write it unsigned.
    #[arg(long)]
    pub key: Option<String>,
}

#[derive(Args)]
pub struct PackVerifyArgs {
    /// Nest directory (must contain a `compliance-pack.toml`).
    #[arg(long, default_value = ".")]
    pub dir: String,
    /// The signer's public key: 64 hex characters, or a file holding them. The key inside the pack
    /// is never trusted; anyone who edits a pack can re-sign it with a key of their own.
    #[arg(long)]
    pub pubkey: String,
}

#[derive(Args)]
pub struct AuditArgs {
    #[command(subcommand)]
    pub what: AuditWhat,
}

#[derive(Subcommand)]
pub enum AuditWhat {
    /// Summarise the threshold flags in a block range (markdown or `--json`).
    Report(AuditReportArgs),
    /// Re-fetch a sample of sealed ranges from a second endpoint and compare them row by row.
    Sealed(AuditSealedArgs),
}

#[derive(Args)]
pub struct AuditSealedArgs {
    /// Nest directory.
    #[arg(long, default_value = ".")]
    pub dir: String,
    /// The endpoint to compare against. One the nest indexes from is refused.
    #[arg(long, value_name = "URL")]
    pub rpc: String,
    /// Ranges to sample.
    #[arg(long, default_value_t = 4)]
    pub samples: u64,
    /// The same seed over the same segments picks the same ranges. Defaults to the current unix
    /// time, which is printed.
    #[arg(long)]
    pub seed: Option<u64>,
    /// Blocks in each sampled range.
    #[arg(long, default_value_t = crate::sealed_audit::DEFAULT_SPAN, value_parser = clap::value_parser!(u64).range(1..))]
    pub span: u64,
    /// Audit exactly `--from..=--to` instead of sampling, to re-check a reported range.
    #[arg(long, requires = "to")]
    pub from: Option<u64>,
    #[arg(long, requires = "from")]
    pub to: Option<u64>,
}

#[derive(Args)]
pub struct AuditReportArgs {
    /// Nest directory.
    #[arg(long, default_value = ".")]
    pub dir: String,
    /// First block of the range (inclusive).
    #[arg(long)]
    pub from: u64,
    /// Last block of the range (inclusive).
    #[arg(long)]
    pub to: u64,
    /// Emit JSON instead of markdown.
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct ListsArgs {
    #[command(subcommand)]
    pub what: ListsWhat,
}

#[derive(Subcommand)]
pub enum ListsWhat {
    /// Fetch a sanctions/watch list (host-side, out-of-band) into a content-addressed snapshot.
    Fetch(ListsFetchArgs),
    /// List the fetched list snapshots and how many addresses each carries.
    List(ListsListArgs),
}

#[derive(Args)]
pub struct ListsFetchArgs {
    /// List name. Known: `ofac-sdn`, `eu-consolidated` (have default URLs). Any other name needs
    /// `--url` or `--file`.
    pub list: String,

    /// Read the list from a local file instead of downloading (any text: XML/CSV/JSON/plain - the
    /// fetcher extracts every `0x…40hex` address).
    #[arg(long)]
    pub file: Option<String>,

    /// Override the download URL for this list.
    #[arg(long)]
    pub url: Option<String>,

    /// Nest directory to fetch into (writes `lists/<hash>.json`).
    #[arg(long, default_value = ".")]
    pub dir: String,
}

#[derive(Args)]
pub struct ListsListArgs {
    /// Nest directory to read list snapshots from.
    #[arg(long, default_value = ".")]
    pub dir: String,
}

#[derive(Args)]
pub struct LabelsArgs {
    #[command(subcommand)]
    pub what: LabelsWhat,
}

#[derive(Subcommand)]
pub enum LabelsWhat {
    /// Import a labeled address set (CSV `address,label` or JSON) as a content-addressed snapshot.
    Import(LabelsImportArgs),
    /// List the imported label snapshots and how many addresses each carries.
    List(LabelsListArgs),
}

#[derive(Args)]
pub struct LabelsImportArgs {
    /// Path to the label file: CSV (`address,label` per line, optional header) or JSON (an array of
    /// `{address,label}` objects, or an `{address: label}` map).
    pub file: String,

    /// Nest directory to import into (writes `labels/<hash>.json`).
    #[arg(long, default_value = ".")]
    pub dir: String,
}

#[derive(Args)]
pub struct LabelsListArgs {
    /// Nest directory to read labels from.
    #[arg(long, default_value = ".")]
    pub dir: String,
}

#[derive(Args)]
pub struct BenchArgs {
    #[command(subcommand)]
    pub what: BenchWhat,
}

#[derive(Subcommand)]
pub enum BenchWhat {
    /// Measure backfill throughput (events/sec, wall-clock, peak RSS) over a pinned block range.
    Backfill(BackfillBenchArgs),
    /// Measure the read path: entity point-read latency (p50/p99) and the `/sql` hot∪cold scan cost
    /// (query latency + peak RSS). The regression guard for the perf refactors - run offline against
    /// an already-indexed nest.
    Query(QueryBenchArgs),
}

#[derive(Args)]
pub struct QueryBenchArgs {
    /// Nest directory (must contain an indexed `nuthatch.redb`). Stop `dev` first - the bench opens
    /// the store directly.
    #[arg(long, default_value = ".")]
    pub dir: String,

    /// The `/sql` query to time (over hot∪cold). Defaults to `SELECT count(*)` on the largest hot
    /// table - the full-tip-materialising scan whose cost is the #1 RAM risk on deep-finality L2s.
    #[arg(long)]
    pub sql: Option<String>,

    /// Entity point-reads to time (keys in chain order from the oldest - a prefix, so set this at or
    /// above the hot row count to cover the whole store).
    #[arg(long, default_value_t = 1000)]
    pub reads: usize,

    /// `/sql` query repetitions to time (the report is the p50/p99 across them).
    #[arg(long, default_value_t = 20)]
    pub iters: usize,

    /// Write the bench-report JSON here. Prints to stdout regardless.
    #[arg(long)]
    pub out: Option<String>,

    /// A label for the report (e.g. "R1: horizon tip 12k rows").
    #[arg(long)]
    pub label: Option<String>,

    /// Fail if the entity point-read p50 exceeds this many microseconds. Omitted, the bench only
    /// reports; passed, it is a gate (CI uses it - CLAUDE.md: benchmark regressions fail the build).
    #[arg(long)]
    pub max_point_read_p50_us: Option<f64>,

    /// Fail if the entity point-read p99 exceeds this many microseconds.
    #[arg(long)]
    pub max_point_read_p99_us: Option<f64>,

    /// Fail unless at least this many point-reads were actually timed. **Pass this whenever you pass
    /// a ceiling.** An empty nest samples no keys and reports p50 = 0µs, which is under every ceiling
    /// anyone would set - so without a floor the gate passes most confidently when it has measured
    /// nothing at all.
    #[arg(long)]
    pub min_reads: Option<usize>,
}

#[derive(Args)]
pub struct BackfillBenchArgs {
    /// Nest directory (must contain a `nuthatch.toml`).
    #[arg(long, default_value = ".")]
    pub dir: String,

    /// First block of the pinned range (inclusive).
    #[arg(long)]
    pub from: u64,

    /// Last block of the pinned range (inclusive).
    #[arg(long)]
    pub to: u64,

    /// How many runs to take (the report is the median). Public RPC is noisy - 3 is sensible.
    #[arg(long, default_value_t = 3)]
    pub runs: usize,

    /// Override the nest's `rpc_urls` (e.g. point at your own archive node for a T2 run).
    #[arg(long)]
    pub rpc: Option<String>,

    /// Archive endpoint(s) for resolving declared `[[calls]]` (RFC-0023 tier 3), same flag and
    /// reasoning as `nuthatch dev --state-rpc`. Without it, a nest declaring `[[calls]]` is refused
    /// rather than silently benched with zero of them resolved (#725).
    #[arg(long = "state-rpc", value_name = "URL")]
    pub state_rpc: Vec<String>,

    /// Write the bench-report JSON here (e.g. `docs/bench/w1.json`). Prints to stdout regardless.
    #[arg(long)]
    pub out: Option<String>,

    /// A label for the workload in the report (e.g. "W1: USDC 100k dense").
    #[arg(long)]
    pub label: Option<String>,

    /// Measure the seal-direct path (decode → Parquet, bypassing the hot store) instead of the
    /// default decode → redb hot-store path. Use to compare the two backfill storage paths.
    #[arg(long)]
    pub seal_direct: bool,

    /// Adapt the `eth_getLogs` window during the run (RFC-0004 §2) instead of holding it fixed at
    /// the chain default - independent of `--seal-direct`.
    ///
    /// Before this flag the window strategy was welded to the storage path
    /// (`window_adaptive: args.seal_direct`), so every "vs hot store" ratio this project has
    /// published compared two variables at once with no way to isolate either. Pass this with or
    /// without `--seal-direct` to reach all four combinations: hot-fixed (the default), hot-adaptive,
    /// seal-fixed, seal-adaptive. Has no effect on the pipelined (`--concurrency > 1`) or factory
    /// paths, which always adapt, the same as `nuthatch dev` (#744).
    #[arg(long)]
    pub window_adaptive: bool,

    /// Concurrent window fetches (seal-direct only). >1 overlaps RPC round-trip latency; results are
    /// still consumed in block order so segments are identical. Try 8-16 against your own node.
    #[arg(long, default_value_t = 1)]
    pub concurrency: usize,

    /// Keep the run's data at this path instead of the cache dir that is discarded.
    ///
    /// The bench normally measures throughput and throws the rows away, which is right for a timing
    /// number and wrong for a benchmark whose criterion is a **row count** - OBIB case 3 asks for
    /// 100,001 block records, and a harness that deletes them can never demonstrate one. Query the
    /// result with `nuthatch sql --dir <that path>`.
    ///
    /// Every run clears this directory first, so the data left behind is the **last** run's. Refuses
    /// a path holding a `nuthatch.toml`, since clearing a nest is never what was meant.
    #[arg(long)]
    pub keep: Option<String>,

    /// Record every source call this run makes into a tape at `<path>`, alongside the live run
    /// (RFC-0039).
    ///
    /// A benchmark against a live endpoint measures the endpoint's mood: #722 found a **3.8x spread
    /// inside a single arm** in one session. Record once from a real provider, then `--replay` that
    /// tape and the run becomes a function of the code alone. Written as a directory -
    /// `manifest.json` plus a `entries.jsonl` sorted by key - so it is diffable in review and
    /// content-addressable.
    #[arg(long, conflicts_with = "replay")]
    pub record: Option<String>,

    /// Replay a tape recorded by `--record` instead of touching the network (RFC-0039).
    ///
    /// Mutually exclusive with `--record`, `--rpc` and `--state-rpc`: a replay source holds no RPC
    /// client, so there is nothing to point at a live endpoint even by mistake. A call the tape does
    /// not contain is a loud, named failure rather than a synthesised empty result - a rig that
    /// quietly invents data is worse than no rig.
    #[arg(long, conflicts_with_all = ["record", "rpc", "state_rpc"])]
    pub replay: Option<String>,

    /// Publish each run's sealed segments to this target while it indexes (RFC-0052 S2), so the
    /// report measures ingestion with a mirror uploading beside it. Each run gets its own prefix.
    #[arg(long, value_name = "URL", conflicts_with = "keep")]
    pub publish_target: Option<String>,
}

#[derive(Args)]
pub struct CheckArgs {
    /// Optional check-name filter (substring). Omit to run every `checks/*.sql`. E.g. `parity`.
    pub name: Option<String>,

    /// Nest directory (must contain a `checks/` folder).
    #[arg(long, default_value = ".")]
    pub dir: String,

    /// Record current query results as the expected fixtures (`checks/expected/*.json`) instead of
    /// comparing - the authoring mode, run once against known-good sealed data.
    #[arg(long)]
    pub update: bool,

    /// RFC-0059 S2: recompute every fold's latest checkpoint from the one before it and compare, in
    /// place of the parity checks. Reads only, so it runs beside a `dev` holding the nest.
    #[cfg(feature = "folds")]
    #[arg(long, hide = true)]
    pub folds: bool,

    /// With --folds, walk every checkpoint from genesis instead of the last window only.
    #[cfg(feature = "folds")]
    #[arg(long, requires = "folds", hide = true)]
    pub from_genesis: bool,
}

#[derive(Args)]
pub struct SchemaArgs {
    /// Nest directory (must contain a `nuthatch.toml`).
    #[arg(long, default_value = ".")]
    pub dir: String,
}

#[derive(Args)]
pub struct McpArgs {
    /// Base URL of the running `nuthatch dev` HTTP API to bridge to.
    #[arg(long, default_value = "http://127.0.0.1:8288")]
    pub url: String,

    /// Print a copy-paste MCP client config (Claude Code `.mcp.json` + the `claude mcp add` one-liner)
    /// and exit, instead of running the stdio server. This is the "wire it up in one step" helper.
    #[arg(long)]
    pub print_config: bool,
}

#[derive(Args)]
pub struct SqlArgs {
    /// The SQL query (SELECT/WITH). Tables are `{alias}__{event}`, e.g. `usdc__transfer`. Omit to open
    /// an interactive REPL (`.tables`, `.schema <t>`, history; `.exit` to quit).
    pub query: Option<String>,

    /// Nest directory. Queried directly when it holds a store no `nuthatch dev` has open; never
    /// created by the query, so a directory without one falls back to --url.
    #[arg(long, default_value = ".")]
    pub dir: String,

    /// The running instance's API, used when --dir holds no store or `nuthatch dev` has it open.
    #[arg(long, default_value = "http://127.0.0.1:8288")]
    pub url: String,

    /// Emit newline-delimited JSON instead of a table (for piping to jq etc.).
    #[arg(long)]
    pub json: bool,
}

#[derive(Args)]
pub struct InitArgs {
    /// Blockscout instance to resolve ABIs from, for a chain with no keyless root we have verified.
    ///
    /// The site origin, e.g. `https://testnet.arcscan.app` - the v2 API path is appended. Tried
    /// after Sourcify and before the built-in list, and when it answers definitively (including
    /// "not verified") that answer stands rather than falling through to a demand for an
    /// `ETHERSCAN_API_KEY` on a chain Etherscan does not index.
    ///
    /// Used for this invocation only and never written to the nest: an ABI source is an access
    /// path, and must not enter the content address.
    #[arg(long, value_name = "URL")]
    pub explorer: Option<String>,

    /// One or more contract addresses to index, e.g. 0xA0b8…eB48 (USDC). Omit when using `--from`.
    #[arg(num_args = 0..)]
    pub addresses: Vec<String>,

    /// Initialise from a published nest instead of addresses: a git URL or a local directory. The
    /// nest is self-contained (ABIs vendored), so nothing is resolved - just cloned/copied + validated.
    #[arg(long, conflicts_with = "addresses")]
    pub from: Option<String>,

    /// Scaffold from a Graph Protocol subgraph manifest: an IPFS CID (`QmVPhL…`, `ipfs://QmVPhL…`)
    /// or a URL to a `subgraph.yaml`. Maps `dataSources` → `[[contracts]]` and `templates` →
    /// `[[templates]]`, vendors every ABI from its pinned CID, and carries `startBlock` across.
    ///
    /// The ABIs a manifest pins are strictly better than Sourcify for proxy-heavy codebases:
    /// Sourcify returns the proxy ABI, which declares none of the events the contract emits.
    ///
    /// Bytes fetched by CID are checked against that CID, the file first and then block by block;
    /// bytes that contradict it are refused, and bytes no gateway can prove are accepted with an
    /// UNVERIFIED warning. A manifest given by URL names a location, not a content, and is trusted.
    ///
    /// Which template a factory creates lives in the mapping WASM, not the manifest, so factory
    /// rules are inferred only where a parameter unambiguously names a template. Everything else is
    /// reported by name with candidates for you to resolve - never guessed.
    #[arg(long, conflicts_with_all = ["addresses", "from", "alias", "abi"], value_name = "CID_OR_URL")]
    pub from_subgraph: Option<String>,

    /// IPFS gateway(s) to try, in order, when resolving `--from-subgraph` (repeatable). Each is a
    /// URL prefix that a CID is appended to. Defaults to The Graph's gateway, then ipfs.io.
    #[arg(long, value_name = "URL_PREFIX")]
    pub ipfs: Vec<String>,

    /// Optional aliases, one per address in order (comma-separated). When omitted, the alias is
    /// the contract name from the ABI (`DelegationManager` → `delegation_manager`), falling back
    /// to c0, c1, … only when no usable name is present.
    #[arg(long, value_delimiter = ',')]
    pub alias: Vec<String>,

    /// Use these local ABI file(s) instead of resolving from Sourcify/Etherscan, one per address in
    /// order (comma-separated; use an empty entry to resolve that address normally). The escape
    /// hatch for a proxy whose implementation ABI the public resolvers don't return - point this at
    /// the implementation's ABI and the nest decodes the events the proxy actually emits.
    #[arg(long, value_delimiter = ',')]
    pub abi: Vec<String>,

    /// Chain to index, e.g. mainnet, arbitrum-one, base. Omit it and nuthatch probes each known
    /// chain for the contract's bytecode and picks the one it lives on - you rarely need to say.
    #[arg(long)]
    pub chain: Option<String>,

    /// Use only these RPC URL(s) (repeatable). They are written to the nest's `rpc_urls` and used
    /// for ABI/deploy-block resolution during init. Point at your own node to avoid public-RPC limits.
    #[arg(long)]
    pub rpc: Vec<String>,

    /// Directory to scaffold into (defaults to the current directory; for `--from`, defaults to the
    /// nest's own name).
    #[arg(long, default_value = ".")]
    pub dir: String,

    /// Deployment start block per address, in the same order as the addresses (comma-separated).
    /// Skips the RPC probe for that address. An empty entry still probes.
    ///
    /// Use this when the probe 429s or the endpoint prunes state: without it, a failed probe
    /// silently backfills a recent window instead of the contract's history.
    #[arg(long, value_delimiter = ',', value_name = "BLOCK")]
    pub start_block: Vec<String>,

    /// Don't index block timestamps: drop the implicit `block_timestamp` column from every table.
    ///
    /// Fetching timestamps is roughly 85% of backfill wall clock (RFC-0029 §4) because they arrive
    /// per-block over a separate round trip that `eth_getLogs` doesn't carry - so a nest that never
    /// needs them backfills dramatically faster.
    ///
    /// **Declare it here or not at all.** This is not a flag you can flip later: it removes a column
    /// from every table (breaking, for anyone querying it) and changes the bytes of every sealed
    /// segment, so a nest that switches cannot reuse its own segments and must re-index from scratch.
    /// A nest that has already indexed refuses to start if this disagrees with what it was built
    /// with; changing your mind means a new nest served alongside the old one (RFC-0020 slice 3).
    ///
    /// Say no to this if you are unsure - `block_timestamp` is the column most views end up wanting,
    /// and time-series questions ("per day", "per hour") cannot be asked without it.
    #[arg(long)]
    pub no_timestamps: bool,
}

#[derive(Args)]
pub struct AddArgs {
    /// Blockscout instance to resolve ABIs from, for a chain with no keyless root we have verified.
    ///
    /// The site origin, e.g. `https://testnet.arcscan.app` - the v2 API path is appended. Tried
    /// after Sourcify and before the built-in list, and when it answers definitively (including
    /// "not verified") that answer stands rather than falling through to a demand for an
    /// `ETHERSCAN_API_KEY` on a chain Etherscan does not index.
    ///
    /// Used for this invocation only and never written to the nest: an ABI source is an access
    /// path, and must not enter the content address.
    #[arg(long, value_name = "URL")]
    pub explorer: Option<String>,

    /// One or more contract addresses to add to the nest, e.g. 0xC02a…6Cc2 (WETH).
    #[arg(num_args = 1..)]
    pub addresses: Vec<String>,

    /// Optional aliases, one per address in order (comma-separated). When omitted, the alias is
    /// the contract name from the ABI (`DelegationManager` → `delegation_manager`), falling back
    /// to the next free c<N> slot only when no usable name is present.
    #[arg(long, value_delimiter = ',')]
    pub alias: Vec<String>,

    /// Use these local ABI file(s) instead of resolving from Sourcify/Etherscan, one per address in
    /// order (comma-separated; an empty entry resolves that address normally). Same proxy escape
    /// hatch as `init --abi`.
    #[arg(long, value_delimiter = ',')]
    pub abi: Vec<String>,

    /// Deployment start block per address, in the same order as the addresses (comma-separated).
    /// Same shape as `init --start-block`.
    #[arg(long, value_delimiter = ',', value_name = "BLOCK")]
    pub start_block: Vec<String>,

    /// The nest directory to grow (must contain a nuthatch.toml). Defaults to the current directory.
    #[arg(long, default_value = ".")]
    pub dir: String,

    /// Use only these RPC URL(s) for ABI/deploy-block resolution (repeatable). Point at your own
    /// node to avoid public-RPC limits.
    #[arg(long)]
    pub rpc: Vec<String>,
}

#[derive(Args)]
pub struct WorkerArgs {
    /// The control-plane Postgres URL - desired state and the worker registry.
    #[arg(long)]
    pub control_db: String,

    /// The hot-store Postgres URL - where indexed state lives. Usually the same server as the control
    /// plane, but a distinct database: one holds *what should run*, the other holds *indexed data*.
    #[arg(long)]
    pub hot_store: String,

    /// Chains this worker can host. The scheduler decides which it *should* run and the lease decides
    /// which it *does*; this is only what the machine is capable of.
    #[arg(long, value_delimiter = ',', required = true)]
    pub chains: Vec<String>,

    /// This worker's identity in the fleet. Defaults to the hostname, which is unique per container
    /// under compose. **Two workers sharing an id would look like one to the registry** and could each
    /// hold what it believed was its own lease, so this is derived rather than left to chance.
    #[arg(long)]
    pub id: Option<String>,

    /// RAM ceiling for everything this worker runs, in MB. The scheduler will not commit cursors past
    /// it - Σ assigned cursors ≤ this.
    #[arg(long, default_value_t = 2048)]
    pub budget_mb: u64,

    /// Where to find the nests this worker is asked to index. `<root>/<name>` if that exists, else
    /// `<root>` itself when it is a single nest - which is what the compose topology mounts.
    ///
    /// What is here wins over the registry: a nest an operator placed on the machine is a deliberate
    /// act, and this mount is read-only in the writer-node topology. Anything not here is pulled from
    /// `--registry` into `--nest-cache`.
    #[arg(long, default_value = "/nest")]
    pub nest_root: String,

    /// Registry to pull declared nests from when they are not under `--nest-root` (RFC-0019).
    ///
    /// A locator the bundle store understands - a local directory, or `s3://…` for a shared registry.
    /// When the control plane has pinned a `bundle_hash`, the fetch goes **by that hash** and the
    /// registry's mutable index is never consulted: re-tagging a version cannot change what a worker
    /// runs. Unpinned, a worker is exactly as trustworthy as its registry - so pin.
    ///
    /// Without this, a worker can only run nests an operator has already placed on the machine.
    #[arg(long)]
    pub registry: Option<String>,

    /// Where pulled nests are cached. Written to; keep it off the read-only `--nest-root` mount.
    ///
    /// Laid out `<cache>/<name>/<bundle-hash>`, so a re-pinned fleet resolves to a different
    /// directory and re-pulls, while an unchanged pin costs no download. Safe to delete: everything
    /// in it is re-fetchable, and a worker rebuilds what it needs on the next tick.
    #[arg(long, default_value = "/var/lib/nuthatch/nests")]
    pub nest_cache: String,

    /// Skip fetching runtime secrets for assigned nests (RFC-0022 §5).
    #[arg(long)]
    pub no_secrets: bool,
}

#[derive(Args)]
pub struct ControlArgs {
    /// Address to bind the control-plane API to. Off-localhost requires NUTHATCH_CONTROL_TOKEN and
    /// refuses to start without it.
    #[arg(long, default_value = "127.0.0.1:8290")]
    pub listen: String,

    /// The control-plane Postgres URL, e.g. `postgres://user:pass@host/db`. This is desired state,
    /// **not** a nest's hot store - keep them separate so a fleet-wide outage and a single cursor's
    /// outage are different events.
    #[arg(long)]
    pub db: String,
}

#[derive(Args)]
pub struct ServeArgs {
    /// Project directory (must contain a nuthatch.toml).
    #[arg(long, default_value = ".")]
    pub dir: String,

    /// Address to bind the HTTP API to.
    #[arg(long, default_value = "127.0.0.1:8288")]
    pub listen: String,

    /// Postgres hot store to serve from, e.g. `postgres://user:pass@host/db`. Requires a build with
    /// `--features postgres-store`. Omit to serve the nest's local redb instead - but that store
    /// must already exist (`serve` never creates or writes to it), and redb's exclusive flock means
    /// exactly one process may hold it: local redb does not read-scale a box or share with `dev`,
    /// only `--hot-store` does.
    #[arg(long)]
    pub hot_store: Option<String>,

    /// Allow browser front ends on these origins to call this nest directly (repeatable, or `*`).
    ///
    /// Off by default: without it a nest sends no `Access-Control-Allow-Origin`, and a page on
    /// another origin cannot call it at all - the standing advice was a reverse proxy, which is a
    /// wall for anyone with a weekend rather than a deployment. Sets allow-origin, allow-methods
    /// `GET,POST,OPTIONS` and allow-headers, and answers preflight. `POST` is there for the GraphQL
    /// routes, which take their query in a body; nothing on the router accepts a write.
    ///
    /// Values are exact origins - `scheme://host[:port]` and nothing more, such as
    /// `https://app.example.com` or `http://localhost:3000` - or a single `*` for any. A path,
    /// query, fragment or trailing slash is refused at startup, because an `Origin` header never
    /// carries one and the value would silently match nothing.
    ///
    /// Not a `nuthatch.toml` field: who may call a nest is an access-path decision and must not
    /// enter its content address.
    #[arg(long = "cors", value_name = "ORIGIN")]
    pub cors: Vec<String>,

    /// Serve the admin UI. Off by default and deliberately *not* symmetrical with `dev`: an FE node
    /// owns no cursor, so the lifecycle routes it would expose have nothing to act on.
    #[arg(long)]
    pub admin: bool,

    /// Mirror sealed segments to this prefix as they seal (RFC-0052), exactly as on `dev`.
    #[arg(long, value_name = "TARGET")]
    pub publish_target: Option<String>,

    /// How often the mirror reconciles when no seal has woken it.
    #[arg(long, default_value = "60s", value_name = "DURATION", value_parser = parse_publish_interval)]
    pub publish_interval: std::time::Duration,

    /// Objects the mirror uploads at once.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u64).range(1..=16))]
    pub publish_parallelism: u64,
}

#[derive(Args)]
pub struct DevArgs {
    /// The directory to run. A `nuthatch.toml` runs that one nest; a `mounts.toml` runs every nest
    /// it mounts, one isolated cursor per chain (RFC-0032). One command either way.
    #[arg(long, default_value = ".")]
    pub dir: String,

    /// Exit on the first fault instead of quarantining it (RFC-0026 §6). Only meaningful with more
    /// than one nest: by default a failed nest or cursor is quarantined and its healthy siblings keep
    /// indexing and serving.
    #[arg(long)]
    pub fail_fast: bool,

    /// Address to bind the HTTP API to.
    #[arg(long, default_value = "127.0.0.1:8288")]
    pub listen: String,

    /// Allow browser front ends on these origins to call this nest directly (repeatable, or `*`).
    ///
    /// Off by default: without it a nest sends no `Access-Control-Allow-Origin`, and a page on
    /// another origin cannot call it at all - the standing advice was a reverse proxy, which is a
    /// wall for anyone with a weekend rather than a deployment. Sets allow-origin, allow-methods
    /// `GET,POST,OPTIONS` and allow-headers, and answers preflight. `POST` is there for the GraphQL
    /// routes, which take their query in a body; nothing on the router accepts a write.
    ///
    /// Values are exact origins - `scheme://host[:port]` and nothing more, such as
    /// `https://app.example.com` or `http://localhost:3000` - or a single `*` for any. A path,
    /// query, fragment or trailing slash is refused at startup, because an `Origin` header never
    /// carries one and the value would silently match nothing.
    ///
    /// Not a `nuthatch.toml` field: who may call a nest is an access-path decision and must not
    /// enter its content address.
    #[arg(long = "cors", value_name = "ORIGIN")]
    pub cors: Vec<String>,

    /// IPFS gateway(s) or a local node for resolving declared `[[ipfs]]` documents (repeatable).
    ///
    /// Empty falls back to public gateways. Point it at your own node if you want no third party in
    /// the path - non-negotiable 3 requires that to be possible for every optional integration.
    /// Not a `nuthatch.toml` field: a gateway is an access path, and two operators resolving the same
    /// CID through different gateways must get the same bytes, so it must not enter the nest's
    /// content address.
    #[arg(long = "ipfs", value_name = "URL")]
    pub ipfs: Vec<String>,

    /// Archive endpoint(s) for resolving declared `[[calls]]` - RFC-0023 tier 3 (repeatable).
    ///
    /// A pinned `eth_call` needs historical state, which the ingestion endpoints usually cannot
    /// serve. Deliberately a flag rather than a `nuthatch.toml` field: an archive endpoint almost
    /// always carries an API key, and the config is pinned into the nest's content address.
    #[arg(long = "state-rpc", value_name = "URL")]
    pub state_rpc: Vec<String>,

    /// Use only these `rpc_urls` at runtime without editing the config (repeatable). Point at your
    /// own node.
    #[arg(long, env = "NUTHATCH_RPC", hide_env_values = true)]
    pub rpc: Vec<String>,

    /// Endpoint(s) asked only while every other endpoint is failing (repeatable): a paid key kept
    /// behind free public endpoints, so it bills only for the calls they could not answer. A flag
    /// rather than config, because a keyed URL does not belong in the nest.
    #[arg(long = "rpc-fallback", value_name = "URL")]
    pub rpc_fallback: Vec<String>,

    /// Endpoint(s) answering `trace_filter` for an address-history nest (RFC-0063, repeatable).
    /// Defaults to the nest's RPC; give an index-backed trace API here when that one re-executes
    /// blocks to answer.
    #[arg(
        long = "trace-rpc",
        value_name = "URL",
        env = "NUTHATCH_TRACE_RPC",
        hide_env_values = true
    )]
    pub trace_rpc: Vec<String>,

    /// Index only this many blocks back from the tip (recent-history mode). Explicitly overrides a
    /// nest's vendored `start_block`s. Omit to backfill from deployment when the nest declares start
    /// blocks, else from a default recent window.
    #[arg(long)]
    pub backfill: Option<u64>,

    /// Backfill finalized history straight to Parquet (skip the hot store) before tip-following.
    /// Prerequisite for `--concurrency` pipelining, which is where the measured speedup is; the
    /// storage path alone is not "faster" - see `docs/benchmarks.md`. The near-tip window still uses
    /// the hot path; the IVM view is rebuilt from the sealed segments.
    #[arg(long)]
    pub seal_direct: bool,

    /// Concurrent window fetches during the seal-direct history backfill (overlaps RPC latency).
    /// Try 8-16 against your own node; keep low on rate-limited public RPC.
    #[arg(long, default_value_t = 1)]
    pub concurrency: usize,

    /// How long a `--seal-direct` backfill waits for one `[[ipfs]]` document before giving up on it so
    /// its window can seal. `300s`, `5m`, or bare seconds; `0` waits until its attempts run out. A
    /// document given up on is absent from the sealed segment and is not fetched again (#1410), so an
    /// operator with one gateway who would rather wait can raise this or turn it off.
    #[arg(long, default_value = "300s", value_name = "DURATION", value_parser = crate::freshness::parse_span)]
    pub ipfs_window_deadline: std::time::Duration,

    /// The widest `eth_getLogs` block range to ask for, and where the adaptive window starts. Without
    /// it the window starts at the chain default and grows over empty ranges, to 100000 blocks for a
    /// nest that reads only logs. With
    /// it no request spans more than this many blocks; the window still shrinks below it when a
    /// provider refuses a range or a range is dense. Set it to your provider's `getLogs` block-range
    /// limit, or high (e.g. 50000) to cross a sparse contract's history in few requests.
    #[arg(long)]
    pub window: Option<u64>,

    /// How long a caught-up cursor waits before asking for the tip again (RFC-0040 §3 knob 1).
    /// `2s`, `5m`, `1h`, or bare seconds. The default is 5 minutes: new rows can take that long to appear,
    /// so pass a shorter interval for a nest that must follow the tip closely. Every poll is
    /// paid for - a tip call, a reorg check, a checkpoint and a `finalized` probe per window,
    /// whether or not a block carried an event. A nest whose readers refresh on a cron of minutes
    /// can wait minutes here and index the same rows for roughly a hundredth of the requests.
    /// `/ready` reports the interval and scales its stall threshold to it.
    #[arg(long, value_name = "DURATION", value_parser = crate::freshness::parse_duration)]
    pub poll_interval: Option<std::time::Duration>,

    /// Index only up to the chain's finality boundary, never the unfinalised tip (RFC-0040 §3 knob 2).
    /// Nothing indexed can be reorged, so the reorg check is not paid and the hot store holds only
    /// rows waiting to seal. `/ready` reports the mode, and `lag_blocks` is then the deliberate
    /// distance to the tip, not a fault.
    #[arg(long)]
    pub finality_only: bool,

    /// Mirror sealed segments to this prefix as they seal (RFC-0052): a directory, or
    /// `s3://bucket/prefix` with the usual `AWS_*` env. Absent, nothing is published and no client is
    /// built. A flag rather than a `nuthatch.toml` field, because a target is not the nest's identity.
    #[arg(long, value_name = "TARGET")]
    pub publish_target: Option<String>,

    /// How often the mirror reconciles when no seal has woken it.
    #[arg(long, default_value = "60s", value_name = "DURATION", value_parser = parse_publish_interval)]
    pub publish_interval: std::time::Duration,

    /// Objects the mirror uploads at once.
    #[arg(long, default_value_t = 2, value_parser = clap::value_parser!(u64).range(1..=16))]
    pub publish_parallelism: u64,

    /// Audit sealed history against this endpoint in the background (#1786): re-fetch sampled
    /// sealed ranges, compare them row by row, and report a difference on `/metrics` and in the log.
    /// Off when absent. One the nest indexes from is refused. A flag, since a keyed URL does not
    /// belong in the nest.
    #[arg(long = "audit-rpc", value_name = "URL")]
    pub audit_rpc: Option<String>,

    /// Ranges the background audit samples a day, spread evenly.
    #[arg(long, default_value_t = crate::sealed_audit::DEFAULT_PER_DAY, value_parser = clap::value_parser!(u64).range(1..=86_400))]
    pub audit_per_day: u64,

    /// Blocks in each range the background audit samples.
    #[arg(long, default_value_t = crate::sealed_audit::DEFAULT_SPAN, value_parser = clap::value_parser!(u64).range(1..))]
    pub audit_span: u64,

    /// Seed for the background audit's samples. Defaults to the start time; it is logged with every
    /// sample, as is the range, which `nuthatch audit sealed --from --to` re-checks.
    #[arg(long)]
    pub audit_seed: Option<u64>,

    /// Disable the built-in admin UI (`/_admin/`) entirely - no routes, for hosted deployments that
    /// front their own dashboard (RFC-0010 Part A). Off-localhost the UI requires `NUTHATCH_ADMIN_TOKEN`
    /// to be set AND each request to present it as `?token=…` (or it self-disables with a log line).
    #[arg(long)]
    pub no_admin: bool,

    /// A runtime (`--dir` holding a `mounts.toml`): the registry a live mount fetches a NID from when
    /// this runtime does not hold it (RFC-0019, #1543). A filesystem path, or `s3://bucket/prefix`.
    #[arg(long)]
    pub registry: Option<String>,
}

#[derive(Args)]
pub struct DoctorArgs {
    /// Endpoint(s) to probe. Repeatable. Omit to probe whatever the nest in `--dir` is configured to
    /// use - which is where "my backfill is slow" usually starts.
    #[arg(long)]
    pub rpc: Vec<String>,

    /// Nest directory to read `rpc_urls` from when no `--rpc` is given.
    #[arg(long, default_value = ".")]
    pub dir: String,

    /// Probe `eth_getLogs` width filtered to this address, rather than unfiltered. Closer to what a
    /// real nest asks for, and some endpoints cap an unfiltered query harder than a filtered one.
    #[arg(long)]
    pub address: Option<String>,

    /// Print one JSON object per endpoint on stdout (no prose). The live-endpoints gate keys on
    /// these fields, not on the wording of the human report (#716).
    #[arg(long)]
    pub json: bool,

    /// Check the segment catalogue in `--dir`: every entry's file exists and hashes. Does not
    /// quarantine (that is startup). Exit 1 if anything disagrees. With `--json` and no `--rpc`,
    /// stdout is the catalogue check only, so the live-endpoints gate is unchanged.
    #[arg(long)]
    pub catalogue: bool,

    /// Check the mirror of the nest in `--dir` at this target, as `publish verify` does without
    /// `--deep`. Exit 1 if an object is missing, differs, or cannot be content-checked.
    #[arg(long, value_name = "TARGET", conflicts_with = "json")]
    pub publish: Option<String>,

    /// For stores whose ETag is the object's MD5 (AWS S3 without SSE-KMS or SSE-C, MinIO); use `publish verify --deep` otherwise.
    #[arg(long, requires = "publish")]
    pub publish_etag_md5: bool,
}
