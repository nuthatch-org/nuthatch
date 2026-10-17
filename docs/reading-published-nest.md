# Reading a published nest

A nest that runs `nuthatch publish sync`, or `dev`/`serve` with `--publish-target`, mirrors its sealed
history to an object-store prefix or a directory (RFC-0052). This page is the difference between
reading that mirror and reading a nest's own directory. Everything it does not mention is exactly as
[Reading Nuthatch segments without Nuthatch](reading-segments.md) says: ordering, `union_by_name`,
256-bit values as decimal text, and `c_dec`/`c_overflow` not being in the file.

`tests/reading_published_nest.rs` publishes a nest and checks this page against what landed. If the
two disagree, the build fails.

## Layout

```
<target>/
  <dataset>/                  64 lowercase hex characters: the nest's data identity
    publish.json              the provenance envelope, below
    manifest.json             the nest's segments/manifest.json less its provisional entries
    manifest.json.lock        the publisher's lock on a directory target; empty, not for readers
    schema.json               present when the nest has one
    <table>/
      <hash>.parquet          one object per catalogued, non-provisional segment
    _seed/                    for `nuthatch seed` only; rewritten in place, not for readers
```

`<dataset>` is the data identity, not the NID, so a cosmetic edit to a nest does not fork its mirror.
`publish.json` carries the NID and bundle hash of the nest that last published.

## Resolving a file

Resolution rule 0, before the two rules in `reading-segments.md`: on a published prefix, a catalogue
entry for table `t` with hash `h` is the object `<dataset>/<t>/<h>.parquet`. The entry's `file` field
is not used. Provisional segments are never published and the mirrored catalogue leaves their entries
out, so every entry it names is an object under the prefix. When the nest has no provisional entries,
`manifest.json` is byte-identical to its local catalogue.

A provisional segment is a table's last few hundred rows, waiting to fold into its next segment. It
becomes final at 1,000 rows, or once its oldest row is three hours of chain time older than the
newest row of a seal, whichever comes first, so on a nest that is still sealing no table's tail is
withheld for much more than three hours. On a nest that has stopped sealing, or whose rows carry no
`block_timestamp`, a tail can wait indefinitely, and the mirror stops short of those rows until the
operator runs `nuthatch publish finalise` (see [operators](operators.md)). `sealed_through` does not tell you this: it is the
highest block a published segment reaches, and a quiet table's withheld tail can sit below it.

## Globbing is allowed here, and only here

Locally, never glob `segments/`: it holds provisional segments and files from other nests.
A published `<dataset>/<table>/` holds only catalogued, non-provisional segments of that one table,
plus at most the batch a publisher is uploading now. Those segments are sealed past finality and never
change, so a glob that sees one before the catalogue does sees fresher data, not wrong data. So
`s3://bucket/prefix/<dataset>/usdc__transfer/*.parquet` is a correct table location for Trino, Snowflake,
BigQuery, Databricks or DuckDB.

The mirror is append-only: an object is never removed. When a table's columns differ between segments,
set the engine's by-name column mapping. For Trino's Hive connector that is
`hive.parquet.use-column-names=true`; without it, a table whose columns drifted across seals reads
silently wrong.

## `publish.json`

<!-- publish.json fields: kept in step with the envelope by tests/reading_published_nest.rs -->
| field | meaning |
| --- | --- |
| `layout_version` | the version of this prefix layout; `1` |
| `nuthatch_version` | the binary that published |
| `chain_id` | the chain the dataset indexes |
| `data_identity` | the `<dataset>` directory name |
| `nid` | the nest's content address when it last published |
| `bundle_hash` | the nest bundle's hash when it last published |
| `sealed_through` | the highest block any published segment reaches; a table's provisional tail may sit below it, unpublished |
| `published_at` | RFC 3339 UTC time of the publish; the only non-deterministic field |
| `catalogue_sha256` | SHA-256 of the `manifest.json` this envelope describes |
| `schema_sha256` | SHA-256 of `schema.json`; absent when the nest has none |
| `tables` | the catalogue's table names |
<!-- /publish.json fields -->

**Check `catalogue_sha256` before trusting `sealed_through`.** A publisher writes `manifest.json` first
and `publish.json` second, so for a moment the two can describe different publishes. Read both, and if
`sha256(manifest.json)` is not `catalogue_sha256`, read both again. A reader that skips this can take a
freshness figure for a catalogue it did not read.

## A resolver

For a dataset at `$DATASET`, either a directory or an `s3://` URL. It needs bash, `jq`, and
`sha256sum` (coreutils) or `shasum`; the AWS CLI only for an `s3://` dataset.

```sh
sha256() {
  if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi | cut -d' ' -f1
}

fetch() {
  case "$DATASET" in
    s3://*) aws s3 cp "$DATASET/$1" - ;;
    *)      cat "$DATASET/$1" ;;
  esac
}

tmp="$(mktemp -d)"
matched=""
for _ in 1 2 3; do
  fetch manifest.json > "$tmp/manifest.json"
  fetch publish.json > "$tmp/publish.json"
  if [ "$(sha256 < "$tmp/manifest.json")" = \
       "$(jq -r .catalogue_sha256 "$tmp/publish.json")" ]; then
    matched=1
    break
  fi
done
[ -n "$matched" ] || { echo "publish.json never matched manifest.json" >&2; exit 1; }

jq -r --arg t "$TABLE" --arg d "$DATASET" \
  '.tables[$t] | sort_by(.from_block, .to_block, .hash)[] | "\($d)/\($t)/\(.hash).parquet"' \
  "$tmp/manifest.json"
```

It prints one file per segment in block order, ready for `read_parquet([...], union_by_name=true)` or
any engine that takes a list. The hash is taken over the fetched file, not a shell variable, because
command substitution drops trailing newlines and the check would never match.

## What this page does not cover

How a warehouse registers the prefix. RFC-0052 §5 has the recipes, and a recipe is listed as supported
only once it has been run against a real mirror.
