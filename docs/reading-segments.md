# Reading Nuthatch segments without Nuthatch

The sealed directory is the escape hatch: plain Parquet, a JSON catalogue, no nuthatch process
required. This page is the contract for that, as the binary writes it today. A type change in
`src/seal.rs::rows_to_batch` that this page does not match is a broken contract, not a docs lag.

It is not a second catalogue format, and it is not a promise about files nuthatch has not yet
sealed. The hot tip lives in `nuthatch.redb` and is not Parquet; an external engine pointed at
this directory sees history through the last sealed block, not the unfinalised window.

RFC-0047 C1. FLBA32 (`FIXED_LEN_BYTE_ARRAY(32)`) is not what is written; that would be a format
version ([#1222](https://github.com/nuthatch-org/nuthatch/issues/1222)), not a write-down of
current behaviour.

## Directory layout

A solo nest (`nuthatch dev` in the nest directory) looks like this:

```
<dataset>/
  nuthatch.toml
  nuthatch.redb          # hot store; not this page
  segments/
    manifest.json        # the catalogue
    {table}-{hash}.parquet
```

The catalogue lives **inside** `segments/`, at `segments/manifest.json`. It is not a sibling of
that directory. nuthatch installs it by writing `segments/manifest.json.tmp`, fsyncing the bytes,
and renaming over the target, so a reader or a crash sees either the previous catalogue or the new
one, never a torn file (`src/seal.rs::save_manifest`).

When the dataset is a mount, it sits at `<runtime>/data/<nid>/`. The catalogue still lives on the
dataset, at `data/<nid>/segments/manifest.json`. The Parquet bytes live in a **shared store** at
the runtime root, keyed only by content address:

```
<runtime>/
  data/<nid>/
    segments/manifest.json
  segments/
    {hash}.parquet
```

The catalogue's `file` field is always `{table}-{hash}.parquet`, even when the bytes are in the
shared store as `{hash}.parquet`. Resolve a segment the way `src/seal.rs::segment_path` does:

1. If the dataset path is `<runtime>/data/<nid>` **and** `<runtime>/segments/{hash}.parquet`
   exists, that is the file.
2. Otherwise `<dataset>/segments/{file}`.

A dataset migrated before the shared store still reads from (2). A missing shared copy is not
corruption; it is a layout that has not been relocated yet.

**Do not glob.** A file not named by the catalogue is not a sealed segment. Globbing
`segments/*.parquet` misses the shared store, picks up strays, and skips the resolution rule
above. The catalogue is the source of truth; the files are what it points at.

A segment's `hash` is the lowercase hex sha256 of the **Parquet file bytes**. Those bytes include
the `created_by` string arrow-rs stamps, so the same rows sealed by two nuthatch versions built
on different arrow-rs releases may hash differently (F-D3). Same binary, identical bytes,
identical hash. Do not use a hash as a cross-version equality proof; compare decoded rows for
that. See [operators.md](operators.md#data-lifecycle).

Startup moves a per-dataset file that is missing, unreadable, or hash-mismatched into a sibling
`quarantine/` directory. It never moves a shared-store file (other mounts may still name it).
`quarantine/` is outside `segments/` and is not in the catalogue.

## Catalogue schema

`segments/manifest.json` is one JSON object, pretty-printed:

```json
{
  "manifest_version": 1,
  "tables": {
    "usdc__transfer": [
      {
        "hash": "ab…",
        "from_block": 1,
        "to_block": 100,
        "rows": 20000,
        "file": "usdc__transfer-ab….parquet",
        "writer_profile": "zstd-bloom-v1",
        "input_hash": "ab…"
      }
    ]
  }
}
```

`manifest_version` sits on the object, not the segment. A catalogue written before it existed has
no such key and **is version 0**; treat an absent key as 0 rather than as unknown.

Per segment, today:

| field | required | meaning |
| --- | --- | --- |
| `hash` | yes | sha256 of the file bytes, lowercase hex |
| `from_block` | yes | first block in the sealed range, inclusive |
| `to_block` | yes | last block in the sealed range, inclusive |
| `rows` | yes | row count in the file |
| `file` | yes | `{table}-{hash}.parquet` |
| `registry_snapshot` | no | factory-discovered child-set hash at seal time; absent on a static nest and on pre-RFC-0009 manifests |
| `provisional` | no | `true` when this table had fewer than 1,000 rows, under 16 MiB of row JSON, and under three hours of chain time between its oldest row and the cut's newest, and the next seal will fold it; omitted when `false`, which is every segment sealed before this existed |
| `writer_profile` | no | the writer settings the file was produced with. Absent means `"snappy"`, **not** unknown: every segment sealed before the field existed was that profile |
| `input_hash` | no | sha256 of the Parquet this seal was given, before a fold rewrote the file (#1631); equal to `hash` when nothing was folded. Absent on segments sealed before 4.1.1 |

Two named writer profiles exist:

| profile | compression | blooms | dictionary | sort metadata |
| --- | --- | --- | --- | --- |
| `snappy` | SNAPPY | none | crate default (on) | none |
| `zstd-bloom-v1` | ZSTD level 3 | address, topic and hash columns (`address`, `from`, `to`, `owner`, `spender`, `topic0`, `*_hash`, `*_address`) | off on near-unique 32-byte hashes | rows sorted by `(block_number, log_index)` |

A reader that only decodes Parquet needs neither: the footer describes the file, and both profiles
are ordinary Parquet. The name is there so a reader can tell the two apart **without** opening the
footer, and so a change of bytes is a change of name rather than a silent rewrite. Nuthatch writes
`zstd-bloom-v1` for new seals and never rewrites an existing segment, so one catalogue routinely
holds both.

There is no `sort_order`, no per-column `logical_type`, and no segment-level stats in the
catalogue. Those are [#1223](https://github.com/nuthatch-org/nuthatch/issues/1223),
not this page. Unknown fields should be ignored (the `registry_snapshot` / `provisional`
defaults already work that way).

A catalogue entry whose file is missing is skipped at query time; nuthatch reduces that table
rather than failing the whole nest. An external reader should do the same, or refuse the
table, but not invent rows.

## Ordering

The catalogue **appends**. Listing order is seal order, which is not necessarily block order
(a later seal of an earlier range, or a re-seal, can sit after a newer one).

The order a reader who wants block order must impose, and the one `read_table_rows` uses:

```
(from_block, to_block, hash)
```

Content address breaks ties inside a range, so a re-seal cannot reorder rows. Within one
`zstd-bloom-v1` file, rows are sorted by `(block_number, log_index)`, ties broken by the row's
canonical JSON (`src/seal.rs::sort_rows_for_seal`); a `snappy` file holds its rows in ingest order
for that seal.

nuthatch's views union the files in **catalogue order** and do not sort. `ORDER BY
block_number, log_index` if you need a sequence; do not assume the scan comes back in chain
order.

Column order inside a file is the sorted set of JSON keys present on the rows of that seal
(`BTreeSet`). A column no row in the file carries is absent, not null-filled, which is why
`read_parquet(..., union_by_name=true)` (DuckDB) or an equivalent by-name union is the right
scan when a table's schema has drifted across seals.

## 256-bit values, and the rest of the physical schema

`rows_to_batch` writes four columns as `UInt64` and **everything else as `Utf8`**:

| column | Parquet / Arrow type |
| --- | --- |
| `block_number` | `UInt64` (0 if the key is missing) |
| `log_index` | `UInt64` |
| `_seq` | `UInt64` |
| `block_timestamp` | `UInt64` |
| every other key | `Utf8`, nullable (missing or JSON null → null) |

That is the whole type system on disk. A Solidity `uint24` named `fee`, a `bool`, an `address`,
a `bytes32`, and a `uint256` named `value` are all `Utf8`. The four names above are `UInt64`
because they are those names, not because of their Solidity type.

A `uint256` / `int256` (and any other integer wider than 64 bits) is stored as its **canonical
decimal text**: `U256::to_string` / `I256::to_string`, unpadded, no hex, no leading zeros except
for zero itself (`"0"`). Signed values carry a leading minus (`"-100000"`). Addresses and hashes
are `0x` plus lowercase hex. Bools that arrived as JSON booleans stringify to `"true"` /
`"false"`.

That decimal text is lossless. It is also **not bytewise-sortable in numeric order**: `"9"` >
`"10"` as UTF-8. Parquet min/max on a `value` column are lexicographic. An external reader who
wants numeric order casts (checked; see below) or sorts after parsing.

**No silent narrowing.** The exact text is the stored form. Every conversion out of it is either
checked or not offered. nuthatch does not write a `DECIMAL`, a `DOUBLE`, or a 32-byte binary
alongside it.

The gate is `sealed_parquet_writes_uint64_counters_and_utf8_uint256_without_dec_companions` in
`src/seal.rs`. Changing those physical types without changing this page is a red test.

## `c_dec` and `c_overflow` are not in the file

nuthatch's SQL surface derives two view columns for each big-integer column `c` (schema storage
`word16` or `word32`) at view definition, by the engine:

```sql
TRY_CAST("c" AS DECIMAL(38,0)) AS "c_dec",
("c" IS NOT NULL AND TRY_CAST("c" AS DECIMAL(38,0)) IS NULL) AS "c_overflow"
```

`c_dec` is the value as `DECIMAL(38,0)` when it fits, else NULL. `c_overflow` is true when the
exact text is present and that cast is NULL. On nuthatch's own `/sql`, `MIN` or `MAX` of `c_dec`
is refused when a value under it did not fit, rather than answered from the ones that did;
`SUM` and `AVG` of `c_dec` are the values that fit (#1664), exactly as an external reader that
re-derives the column gets them, so check `c_overflow` before trusting either. `SUM(c)` is the
footgun (it concatenates text, or fails, depending on the engine). Values with
more than 38 digits (a Uniswap `sqrtPriceX96` can) stay exact in `c` and flag on `c_overflow`.

These columns are **never written to Parquet**. An external reader of the raw files does not
get them. Re-derive with the same `TRY_CAST`, or stay on the text. An engine without a checked
cast should not offer a narrowed type; inventing `CAST(c AS DOUBLE)` as the default is silent
narrowing and is not a compatible reading of this format.

Which Utf8 columns are big integers is not in the Parquet schema. nuthatch knows from
`schema.json` (and the live decode registry). An external reader uses that file, or treats
every Utf8 column as text. A published mirror carries `schema.json` with its hash.

The conversion, per engine:

| engine | conversion | exact over |
| --- | --- | --- |
| DuckDB | `TRY_CAST(c AS DECIMAL(38,0))`, NULL past 38 digits | values up to 38 digits |
| DuneSQL | `cast(c as uint256)`, or `int256` for a signed column | every 256-bit value |

Another engine is listed here once its recipe has been run, not before (RFC-0052 S5).

Overflow is concentrated, not general. Measured on the Arbitrum Network corpus for RFC-0061
(2026-09-21):

- **Approvals:** 46.9% of `graph_token__approval.value` has more than 38 digits, most of it the
  unlimited-allowance sentinel `2^256 − 1`.
- **Transfers:** none of the 6.59M `graph_token__transfer.value` rows does.

Amounts fit `DECIMAL(38,0)`; `c_overflow` mostly marks the sentinel.

The physical type stays text. RFC-0061 records why: `DECIMAL(76,0)` cannot hold every 256-bit
value, and DuckDB, the engine at the time, read it as `DOUBLE`. `FIXED_LEN_BYTE_ARRAY(32)` reads
as an opaque blob.

The query-side gate is `bigint_columns_get_decimal_and_overflow_views` in `src/analytics.rs`.

## Querying with another engine

List the catalogue, resolve each file, optionally sort, then hand that list to the engine.
Do not glob. `union_by_name` (or the engine's equivalent) is required once a table has more
than one segment whose columns differ.

DuckDB, against a solo nest, after resolving paths into `files`:

```sql
SELECT *
FROM read_parquet(['/abs/path/a.parquet', '/abs/path/b.parquet'], union_by_name=true);
```

The same files, with the checked decimal companion a nuthatch view would have offered:

```sql
SELECT
  *,
  TRY_CAST("value" AS DECIMAL(38,0)) AS "value_dec",
  ("value" IS NOT NULL AND TRY_CAST("value" AS DECIMAL(38,0)) IS NULL) AS "value_overflow"
FROM read_parquet(['/abs/path/a.parquet'], union_by_name=true);
```

DataFusion and other Parquet readers take the same path list. Their cast names differ; the
storage contract does not. If the engine cannot express a checked 38-digit decimal, leave the
column as text.

A small resolver matching `segment_path` and the block-order sort, for a dataset directory
`dataset`:

```python
import json
from pathlib import Path

def segment_files(dataset: Path, table: str) -> list[Path]:
    manifest = json.loads((dataset / "segments" / "manifest.json").read_text())
    segs = sorted(
        manifest["tables"][table],
        key=lambda s: (s["from_block"], s["to_block"], s["hash"]),
    )
    parent = dataset.parent
    shared = parent.parent / "segments" if parent.name == "data" else None
    out = []
    for s in segs:
        if shared is not None:
            p = shared / f"{s['hash']}.parquet"
            if p.exists():
                out.append(p)
                continue
        out.append(dataset / "segments" / s["file"])
    return [p for p in out if p.exists()]
```

That is the migration path. Reading `src/seal.rs` to reconstruct it is not.

## Writer footer, for orientation

`write_parquet` writes the `zstd-bloom-v1` profile above and nothing more. A footer read of a
4.10.1 segment (2026-10-05): ZSTD on every column, Bloom filters on the columns the profile names,
column statistics on every column, one row group per file, `parquet-rs` 58.3.0. A 2026-08-29 footer
read of production `snappy` segments is in [segment-layout.md](bench/segment-layout.md). Those
measurements are not a second schema. Changing them is a writer-config decision and is not implied
by this page.

## What this page does not cover

- The hot store, reorgs, or anything unsealed.
- Authored SQL views and incremental entities; those are query-layer, not segment layout.
- The rest of the versioned catalogue (`sort_order`, `logical_type`, stats): #1223. `manifest_version`
  and `writer_profile` shipped ahead of it and are documented above.
- A physical-type change for 256-bit values: #1222.
- A mirror in object storage or another directory: [Reading a published nest](reading-published-nest.md).
