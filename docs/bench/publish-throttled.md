# Publishing to a throttled store does not slow ingestion (RFC-0052 S2)

Measured 2026-09-13 on an Apple M5 Pro (18 cores, 48 GB), commit `396fa4df` plus the bench wiring,
with `scripts/publish-throughput-gate.sh`. RFC-0052 §7 fails S2 if RFC-0004's backfill harness
measures ingestion throughput outside noise while publishing to a MinIO throttled to 1 MB/s. It does
not.

These figures were taken against MinIO. Open-source MinIO has since been withdrawn upstream (#1492),
and the script now runs versitygw; a rerun measures against that.

## The scenario

`nuthatch bench backfill --seal-direct --concurrency 4` over blocks 1 to 20,000 of the chain
`fixture-rpc footprint` (tools/fixture-rpc) serves: one contract, four `Transfer` logs a block, 80,000 rows,
sealed on the production row cut (`SEAL_DIRECT_BATCH`, 20,000) into four segments during the run.
No network and no third party, so every run indexes exactly the same logs.

The mirror arm adds `--publish-target s3://mirror/gate`. MinIO (`RELEASE.2025-10-15T17-29-55Z`,
built with `CGO_ENABLED=0`) sits behind Toxiproxy with a 1024 KB/s bandwidth toxic in both directions.
Each run publishes under its own prefix, so no run finds another's objects already uploaded.

The script refuses to report if the throttle does not bite (a 3 MB PUT through the proxy took 3 s
here) or if any publishing run uploaded nothing.

## Result

Four batches of 15 runs, alternated as no mirror, mirror, mirror, no mirror.

| batch | median ev/s | peak RSS (median) |
| --- | ---: | ---: |
| no mirror (a) | 120,727 | 228 MB |
| MinIO at 1 MB/s (a) | 119,039 | 224 MB |
| MinIO at 1 MB/s (b) | 118,165 | 218 MB |
| no mirror (b) | 119,038 | 213 MB |

Pooled over 30 runs per arm: **119,046 ev/s without the mirror, 118,502 ev/s with it, 0.46% apart.**
The two no-mirror batches differ from each other by 1.4%, so the harness drifts more between its own
identical batches than publishing moves it, and both figures sit well inside the 5% floor
`noise-floor.md` sets.

Every publishing run had uploaded between 363,106 and 541,059 bytes (median 541,059) when its
ingestion finished in about 0.67 s. The mirror was therefore mid-upload behind the throttle for the
whole run, which is the condition the criterion asks about, rather than finished before ingestion
noticed it.

Peak RSS is not higher with the mirror. Uploads stream in 8 MiB parts, one in flight per object
(`publish::PART_BUFFER`), so `publish_headroom` is `parallelism × 8 MiB`, 16 MiB at the defaults.

## Lag on a live nest

The first S2 criterion fails if publish lag exceeds one seal plus one interval. `nuthatch dev` over the
same chain with `--backfill 20000 --publish-target <dir> --publish-interval 60s`, with `/metrics`
sampled every second for 94 s: `nuthatch_publish_sealed_through` equalled `nuthatch_sealed_through`
at every sample, including across the seal that moved it from 10,800 to 19,800, and
`nuthatch_publish_lag_blocks` never read above zero. The seal wake-up, not the 60 s interval, is
what published each segment. That mirror was a local directory, not the throttled store.

## What this does not cover

A CPU-starved box: eighteen cores leave the publisher's thread uncontended. A large segment: these are
small, and the streaming bound is pinned by
`publish::tests::an_object_store_upload_never_holds_more_than_one_part` rather than by this run. The
tip path, where seals are rare and the question is lag rather than throughput. Reports:
`publish-gate-{baseline,publish}-{a,b}.json`.
