# Compaction

`pg-cdc compact` merges delta files into a new base snapshot. This reduces file count, speeds up consumer refresh, and manages tombstones.

## What Happens

```
pg-cdc compact
  For each active table:
    1. Read current base Parquet into memory (keyed by PK)
    2. Read all delta files newer than base_epoch
    3. Apply changes ordered by (__epoch, __seq):
       - I (insert): add row
       - U (update): replace row by PK
       - D (delete): mark row with __deleted_at tombstone
    4. Purge tombstones older than 30 days
    5. Write a new base (one per partition for partitioned tables)
    6. Update manifest (bump base_epoch, row_count, partition list) BEFORE deleting
    7. Delete superseded base generations and applied delta files
```

A table with no new deltas and a single base generation on disk is skipped entirely ("up to date"): no files are written and the manifest is untouched.

## Usage

```bash
pg-cdc compact --config pg-cdc.yml
```

## When to Compact

- **After accumulating many deltas** — hundreds of small files slow down consumer refresh
- **On a schedule** — hourly or daily via cron
- **Before a consumer's first refresh** — fewer files to download

## Business-Date Partitioning and Moves

For a table partitioned by a business date (see [Streaming → Partitioning](04-streaming.md#business-date-partitioning)), compaction indexes **the whole table by primary key across all partitions**, applies the deltas in `(__epoch, __seq)` order, and re-buckets the surviving rows by their final date. Each non-empty partition gets a fresh base:

```
public.orders/order_date=2026-01-01/base/base_000007.parquet   # rebuilt
public.orders/order_date=2026-01-02/base/base_000007.parquet   # moved row lands here
```

This is what guarantees **no active copy in the old partition** after a move: if an `UPDATE` changed a row's date from `2026-01-01` to `2026-01-02`, only `2026-01-02`'s new base contains the row. A partition that loses its last row gets no new base, and its previous base file is deleted as stale.

Two further rules keep the layout honest:

- **Self-healing generations.** Any base file whose generation (`base_<N>`) differs from the new `base_epoch` is deleted, so the leftovers of an interrupted compaction are cleaned up on the next run.
- **Crash-safe ordering.** New bases are written first, then the manifest, then the old bases and deltas are deleted. A crash before the manifest write leaves the previous generation authoritative (its files are still present).

A business-date value that is `NULL` or unparseable keeps living in the default partition `__HIVE_DEFAULT_PARTITION__`; a row whose date is cleared by an `UPDATE` moves there on the next compaction.

## Tombstones

When a row is deleted, compaction doesn't remove it immediately. Instead, it marks it with `__deleted_at`:

```
Row: {id: 50, name: "...", __deleted_at: "2026-04-12T14:00:00Z"}
```

This ensures consumers who haven't refreshed recently can still propagate the delete. Tombstones are purged after 30 days (configurable).

If a consumer is offline longer than the tombstone retention period, their next `pg-warehouse refresh` detects the gap (local epoch < base_epoch) and does a full re-pull from the latest base.

## Governance During Compaction

Compaction only processes tables in `manifest.ActiveTables()`. Excluded tables are untouched. Their manifest entries (status=excluded, tags, reason) are preserved.

## Output

```
pg-cdc compact
Compacting 3 tables
  public.orders: base_000015 (10042 rows, 18 applied, 0 purged, 2 partitions, -3 bases, -15 deltas)
  public.customers: up to date (base=0)
  public.products: base_000003 (100 rows, 5 applied, 1 purged, 1 partitions, -1 bases, -3 deltas)
Compaction complete
```

The partition count is the number of landing directories after the rewrite; `-N bases` / `-N deltas` are the stale generations and applied deltas that were removed. When every table is up to date the run prints `Nothing to compact`.
