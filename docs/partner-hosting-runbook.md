# Partner hosting runbook: one shared runtime, many organisations' nests

For an operator who hosts other organisations' nests in one runtime (RFC-0073, RFC-0076). It is an
ordering of procedures that already exist, with where each one is tested. It adds no behaviour:
the reference for every call is [admin-api.md](admin-api.md) and for every flag
[operators.md](operators.md). Nuthatch does no billing, no accounts and no identity. A tenant is an
opaque label, and sign-in, keys, plans and invoices are the operator's gateway's job.

**Not claimed.** `[runtime] isolation = "strict"` is not claimed as a guarantee until the
independent review described in [security/shared-runtime.md](security/shared-runtime.md) has run
(RFC-0073 FR8). This runbook does not review it and does not say `strict` is safe for mutually
hostile tenants. Until the review, host only organisations you would be willing to run on `shared`.

## How each step is covered

Every step names what exercises it. **Test** means a Rust test in this tree, run by `yatr run --agent
ci` unless marked *CI only*, which needs a versitygw S3 server and skips locally without
`NUTHATCH_MINIO_ENDPOINT`. **Manual** means no test runs that exact command, and says what to look
for. The `curl` spellings in this page are Manual throughout: the tests call the same endpoints
in-process, not through a listener with a token. `tests/partner_runbook_refs.rs` fails if a test
named below is renamed or removed.

## 0. The runtime

```toml
# runtime/mounts.toml
[runtime]
name = "partner-1"
isolation = "strict"     # or "shared"; see the note above
default_tenant = "internal"

[runtime.publish]        # the S3 mirror that section 6 restores from
target = "s3://nests/mirror"
interval = "60s"
followers = true
```

```sh
export NUTHATCH_ADMIN_TOKEN=…          # never on the command line, never in the repo
export AWS_ACCESS_KEY_ID=… AWS_SECRET_ACCESS_KEY=… AWS_REGION=…   # registry and mirror credentials
nuthatch dev --dir runtime --listen 127.0.0.1:8288 --registry s3://nests/registry
```

A runtime may start with no chain and nothing mounted; a chain is declared live, and the first mount
onto it starts its cursor.

| Command | Covered by |
|---|---|
| `nuthatch dev --dir runtime` over a bare `[runtime]` | Test: `tests/e2e_runtime_chains.rs::a_bare_runtime_starts_with_no_chain_and_takes_one_live` |
| `POST /_admin/chains` | Test: `tests/e2e_runtime_chains.rs::a_chain_declared_live_takes_a_mount_without_a_restart` |
| `PUT /_admin/chains/<chain>/rpc` | Test: `tests/e2e_runtime_chains.rs::a_running_cursors_pool_is_replaced_live` |
| The admin token on every route | Test: `tests/e2e_runtime_lifecycle.rs::the_lifecycle_routes_demand_the_admin_token_before_they_act` |
| `--registry s3://…` with `AWS_*` credentials | Test, *CI only*: `tests/e2e_s3_registry.rs::e2e_s3_registry_mounts_by_nid_and_codes_each_failure` |

## 1. Provision a tenant label

There is nothing to create. A tenant is the first part of a mount's name (`acme/usdc` is tenant
`acme`) and exists while it has a mount. Give it a share before it mounts anything if it should not
get the default:

```sh
curl -X PUT -H "Authorization: Bearer $NUTHATCH_ADMIN_TOKEN" -H 'Content-Type: application/json' \
  -d '{"weight": 2, "max_permits": 1, "max_response_mib": 32}' \
  http://127.0.0.1:8288/_admin/tenants/acme
curl -H "Authorization: Bearer $NUTHATCH_ADMIN_TOKEN" http://127.0.0.1:8288/_admin/tenants
```

The share is written to `mounts.toml` first, applied to running mounts with no restart, and read again
at the next boot. `{}` restores the default. A tenant past its share is answered `429` with
`"reason": "tenant_share"`; a full gate is `503` `busy`.

| Command | Covered by |
|---|---|
| `PUT /_admin/tenants/<tenant>`, `GET /_admin/tenants`, share survives a restart | Test: `src/runtime.rs::a_tenant_share_applies_live_and_survives_a_restart` |
| `tenant_share` told apart from `busy` | Test: `src/serve.rs::tenant_share_and_busy_are_told_apart` |
| A tenant under its reservation never waits behind another's work | Test: `src/fair_share.rs::a_tenant_under_its_reservation_never_waits_behind_another` |
| Mount names (`tenant/alias`) and what is refused | Test: `tests/e2e_runtime_lifecycle.rs::names_boot_would_refuse_are_refused_at_the_api` |

## 2. Mount a nest from a bundle

The customer builds a bundle, or the operator does. Inspect it first; nothing is written.

```sh
nuthatch nest nid --dir ./usdc                       # the NID
nuthatch nest bundle ./usdc                          # writes the .bundle
nuthatch nest inspect usdc.bundle --json             # egress, extraction cost, entities
```

Over the admin API, with the bundle as the body (`Content-Type: application/octet-stream`):

```sh
curl -X POST -H "Authorization: Bearer $T" -H 'Content-Type: application/octet-stream' \
  --data-binary @usdc.bundle 'http://127.0.0.1:8288/_admin/bundles?dry_run=true'         # check and price
curl -X POST -H "Authorization: Bearer $T" -H 'Content-Type: application/octet-stream' \
  --data-binary @usdc.bundle 'http://127.0.0.1:8288/_admin/bundles?as=usdc@1'            # publish to --registry
curl -X POST -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  -d '{"name": "acme/usdc", "nid": "<nid>"}' 'http://127.0.0.1:8288/_admin/nests?dry_run=true'
curl -X POST -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  -d '{"name": "acme/usdc", "nid": "<nid>"}' http://127.0.0.1:8288/_admin/nests
curl -H "Authorization: Bearer $T" http://127.0.0.1:8288/_admin/mounts/acme/usdc        # poll the job
```

A mount answers `202` and a job that ends `live` or `failed` with a `code` (`not_found`,
`access_denied`, `verification_failed`, `other`). `live` means indexing and serving, not caught up:
read `/acme/usdc/ready` for `lag_blocks`. A refusal (`409` for a taken name or an undeclared chain,
`507` over the cursor's memory ceiling) leaves nothing on disk. Two tenants mounting one NID share one
dataset and index it once.

| Command | Covered by |
|---|---|
| `nest nid`, `nest inspect --json` | Test: `tests/e2e_nest_nid.rs`, `tests/e2e_nest_inspect.rs` (whole files) |
| `POST /_admin/bundles?dry_run=true`, `?as=`, prune | Test, *CI only*: `tests/e2e_s3_registry.rs::e2e_s3_registry_publishes_and_prunes_through_the_admin_api` |
| `POST /_admin/nests?dry_run=true` agrees with a real mount | Test: `tests/e2e_runtime_lifecycle.rs::a_dry_run_and_a_real_mount_agree_on_every_refusal` |
| A mount by NID from the registry, job polled to `live` | Test: `tests/e2e_runtime_lifecycle.rs::a_mount_fetches_a_nid_the_runtime_does_not_hold` |
| The job answers `202` at once and reads until live | Test: `tests/e2e_runtime_lifecycle.rs::a_mount_is_accepted_at_once_and_read_until_it_is_live` |
| A failed job carries its reason | Test: `tests/e2e_runtime_lifecycle.rs::a_failed_mount_is_reported_with_its_reason` |
| Each `code`, against a private S3 bucket | Test, *CI only*: `tests/e2e_s3_registry.rs::e2e_s3_registry_mounts_by_nid_and_codes_each_failure` |
| Two tenants, one NID, one dataset | Test: `tests/e2e_shared_dataset.rs::two_tenants_mounting_one_nest_share_it` |
| Unmount, reclaim disk | Test: `tests/e2e_runtime_lifecycle.rs::reclaim_over_the_admin_api_frees_a_dataset_only_once_nothing_mounts_it` |
| Suspend, resume | Test: `tests/e2e_runtime_lifecycle.rs::suspend_and_resume_over_the_admin_api` |
| Move a name to a new NID without a gap | Test: `tests/e2e_runtime_lifecycle.rs::a_name_moves_to_another_nid_without_a_gap` |
| `strict`: a live mount queries in its tenant's partition | Test: `tests/e2e_runtime_lifecycle.rs::a_strict_runtimes_live_mount_queries_in_its_tenants_partition` |

## 3. Limits

Per mount, lowered only and never raised past the process value; a bad value is `400`, never silently
clamped:

```sh
curl -X PATCH -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  -d '{"limits": {"statement_timeout_ms": 5000, "max_rows": 1000, "analytics_memory_bytes": 134217728}}' \
  http://127.0.0.1:8288/_admin/mounts/acme/usdc
```

Per runtime, live, written to `mounts.toml` first:

```sh
curl -X PUT -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  -d '{"max_rss_mb": 4096, "sql_permits": 6}' http://127.0.0.1:8288/_admin/runtime
```

The 2 GB per cursor figure is the budget nuthatch's own design must fit at the default ceiling; an
operator may raise it by choice and `GET /_admin/runtime` says so. Under `strict` every SQL permit is
an engine pool, so the permit count that fits is smaller (`sql_permits_ceiling` in the same answer).

| Command | Covered by |
|---|---|
| `PATCH /_admin/mounts/<name>` binds one mount and not its co-tenant | Test: `tests/e2e_runtime_lifecycle.rs::per_mount_limits_bound_one_mount_and_not_its_co_tenant` |
| Limits survive a restart | Test: `tests/e2e_runtime_lifecycle.rs::a_restart_resumes_an_interrupted_mount_with_its_sql_and_limits` |
| `PUT /_admin/runtime` | Manual: `GET /_admin/runtime` before and after; the figures it returns are the check |

## 4. Rolling upgrade

Followers first, one at a time, the writer last. While the writer is down, followers answer from sealed
data and their last hot state at the `as_of` they last saw, and report `"writer": "absent"`.

```sh
NUTHATCH_ADMIN_TOKEN=… nuthatch serve --runtime runtime --follow http://127.0.0.1:8288 --listen 127.0.0.1:8289
scripts/rolling-upgrade.sh <previous-binary> <current-binary>
```

The procedure, with what to wait for between steps, is operators.md
[Upgrading a runtime without an outage](operators.md#upgrading-a-runtime-without-an-outage).

| Command | Covered by |
|---|---|
| A follower answers as the writer does, through reorg, seal and a writer restart | Test: `tests/e2e_follower.rs::a_follower_answers_as_the_writer_does_through_reorg_seal_and_restart` |
| A follower under `strict` isolates as the writer does | Test: `tests/e2e_follower.rs::a_strict_writers_followers_isolate_as_it_does` |
| `scripts/rolling-upgrade.sh`: replace the followers, then the writer, a probe every 200 ms, none unanswered | CI only: the `rolling-upgrade` job, the release gate's step. Not run in this slice |
| Restart to ready, by mount count | Measured: [bench/rfc-0073-fr7-restart-to-ready.md](bench/rfc-0073-fr7-restart-to-ready.md), `tests/bench_restart_mounts.rs`, `#[ignore]`d |

The measured restart is 9 ms for 1 mount and 992 ms for 100, on five-block nests, on one laptop. It
says nothing about real nests with long histories. Before promising anyone a window, take the figure
on your own runtime: stop the writer, start it, and time `GET /ready` to `200` (Manual).

## 5. What an operator can see per tenant

| Question | Where | Covered by |
|---|---|---|
| Who holds how many permits, who waited, who was refused | `GET /_admin/tenants` | Test: `src/runtime.rs::a_tenant_share_applies_live_and_survives_a_restart` |
| How long each tenant waited for a permit | `nuthatch_sql_admission_wait_seconds{tenant}` | Test: `src/fair_share.rs::admission_waits_are_kept_per_tenant` |
| Requests, SQL queries, rejections by reason and SQL duration, per mount | `nuthatch_nest_http_requests_total{nest}`, `nuthatch_nest_sql_queries_total{nest}`, `nuthatch_nest_sql_rejections_total{nest,reason}`, `nuthatch_nest_sql_duration_seconds{nest}` | Test: `tests/nest_serving_metrics.rs::each_mount_counts_its_own_requests` |
| Disk, last block, tip lag and health, per mount | `nuthatch_nest_hot_store_bytes`, `nuthatch_nest_sealed_segments_bytes`, `nuthatch_nest_last_block`, `nuthatch_nest_tip_lag_blocks`, `nuthatch_nest_health` | Test: `tests/e2e_runtime_lifecycle.rs::a_runtime_serves_metrics_at_its_root` |
| Undecoded logs per mount, contract and topic | `nuthatch_nest_logs_undecoded_total{nest,contract,topic0}`, the `undecoded` block of `/<name>/ready` | Test: `src/indexer.rs::an_undecoded_log_is_counted_once_per_contract_and_topic0` |
| Faults and quarantine | `GET /nests` (`health`, `quarantine`) | Test: `tests/e2e_runtime_chains.rs::an_unloadable_dataset_boots_quarantined_beside_a_serving_co_tenant` |
| What one request cost | the `Nuthatch-Cost` response header, on a request that sends `Nuthatch-Cost: 1` | Test: `src/serve.rs::the_same_statement_over_the_same_segments_reports_the_same_sealed_bytes` |

The runtime's root `/metrics` is the operator's and shows every mount. Under `strict`, a mount's own
`/metrics` keeps only that mount's series, labelled by its route
(`src/serve.rs::a_strict_mounts_metrics_carry_only_its_own_series`,
`src/serve.rs::a_strict_mounts_metrics_use_its_route_as_the_only_nest_label`). Scope `/metrics` to the
operator's network at the gateway, as operators.md says.

### Metering is the partner's, from these inputs

Nuthatch reports; it does not price. A partner that bills its own customers reads the per-mount series
above and the `Nuthatch-Cost` header, has its gateway add `Nuthatch-Cost: 1` to the requests it wants
costed, and decides what a `sealed_bytes` or an `engine_us` is worth. The header's fields, and which
are stable between runs (`sealed_bytes` is; `sealed_bytes_read` and the CPU figures are not), are in
operators.md [The cost header](operators.md#the-cost-header). A remembered answer from the analytical
memo reports `0` for the scan fields, which is correct, since it ran nothing. Mapping a tenant label
to a customer, a key to a tenant and a count to an invoice is the gateway's. Nothing here adds
authentication, accounts or billing to nuthatch.

## 6. Restore from the S3 mirror

A lost runtime is rebuilt without re-indexing over RPC, provided `[runtime.publish]` was on. On the
new box, with the same chains and `[runtime.publish]` in `mounts.toml`, start `nuthatch dev` as in
section 0 and mount each NID again with `"seed": true`:

```sh
curl -X POST -H "Authorization: Bearer $T" -H 'Content-Type: application/json' \
  -d '{"name": "acme/usdc", "nid": "<nid>", "seed": true}' http://127.0.0.1:8288/_admin/nests
curl -H "Authorization: Bearer $T" http://127.0.0.1:8288/_admin/mounts/acme/usdc   # phase: seeding, seed_progress
```

Every file is checked against its content address. A refused or interrupted seed ends the job
`failed` and keeps what it verified; a second `POST` of the same body resumes it, and so does a
restart. The mount then follows the chain from the block after the mirror ends. The hashes show a file
is the one the mirror's catalogue names, not that the catalogue is true to the chain, so seed only from
a mirror you wrote.

| Command | Covered by |
|---|---|
| `"seed": true` from a filesystem mirror, then serve and follow the chain | Test: `tests/e2e_seed_mount.rs::a_mount_seeds_from_its_mirror_then_serves_and_follows_the_chain` |
| A corrupt segment fails the seed and the retry keeps what was fetched | Test: `tests/e2e_seed_mount.rs::a_corrupt_segment_fails_the_seed_and_the_retry_keeps_what_it_fetched` |
| A seed with no mirror fails; a seed cannot wait | Test: `tests/e2e_seed_mount.rs::a_seed_with_no_mirror_fails_and_a_seed_cannot_wait` |
| The same against S3, each failure `code` | Test, *CI only*: `tests/e2e_s3_seed_mount.rs::e2e_s3_seed_mount_seeds_from_a_mirror_and_codes_each_failure` |
| A standby that takes a dead writer's lease seeds from the mirror | Test: `tests/e2e_seed_mount.rs::a_standby_that_takes_a_dead_writers_lease_seeds_from_the_mirror` |

The mirror holds data and the registry holds each nest's definition, so a restore needs both buckets.

## 7. Still open

- **FR8, the independent review of `isolation = "strict"`.** Commissioned by the partner, not run, not
  claimed. Our side is the threat model, fuzz targets and test harness, and taking findings first.
- **FR7, restart time on real nests.** Only small nests are measured (section 4).
- **FR1, readers on another host.** Built (RFC-0076 FR17) and tested in-process
  (`tests/e2e_remote_follower.rs`, with an S3 variant). Not run on two real hosts against a real
  mirror; this slice did not run it.
- **A staging run.** Nothing in this tree runs the `curl` commands above through a real listener with
  a real token. Make a first pass against a staging runtime part of onboarding.
