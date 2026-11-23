# Streaming

`pg-cdc start` connects to the replication slot and streams WAL changes into delta Parquet files.

## What Happens

```
pg-cdc start
  1. Read manifest.json for init LSN and active table list
  2. Connect to replication slot at init LSN
  3. Decode pgoutput v2 WAL messages (INSERT/UPDATE/DELETE)
  4. Buffer events in memory
  5. Flush to delta Parquet on interval or row count
  6. Update manifest.json (bump latest_delta_epoch)
  7. Send standby status to PostgreSQL (prevent WAL accumulation)
```

## Usage

```bash
pg-cdc start --config pg-cdc.yml
```

Runs as a long-lived daemon. Stop with SIGINT (Ctrl+C) or SIGTERM.

## Delta Parquet Format

Each delta file contains CDC events with three metadata columns:

| Column | Type | Values |
|--------|------|--------|
| `__op` | UTF8 | `I` (insert), `U` (update), `D` (delete) |
| `__epoch` | INT64 | Epoch number |
| `__seq` | INT64 | Event order within the flush (0-based) |
| *(table columns)* | typed | PostgreSQL values (NULL for D ops except PK) |

`__seq` gives compaction a deterministic order for several changes to the same primary key inside one epoch, which is what lets an `UPDATE` move a record between business-date partitions (see below).

## Epochs

An epoch is an atomic batch of changes. Each flush creates one epoch file:

```
public.orders/deltas/epoch=000001.parquet   ← first batch
public.orders/deltas/epoch=000002.parquet   ← second batch
```

Epochs are monotonically increasing. pg-warehouse uses epoch numbers as watermarks to track what it has already pulled.

## Flush Behavior

```yaml
flush:
  interval_sec: 10    # flush every 10 seconds
  max_rows: 1000      # or every 1000 rows
```

Whichever threshold is hit first triggers a flush. On graceful shutdown, remaining buffered events are flushed.

## Business-Date Partitioning

By default a table ingests into a single `<table>/deltas/` directory. Opt in to Hive-style partitioning by naming a business-date column under `partitions` (see [Configuration](../1-getting-started/02-configuration.md#partitions)):

```yaml
partitions:
  date_column: order_date
```

Then each flush writes one delta per date bucket, at the same epoch number:

```
public.orders/order_date=2026-01-01/base/base_000000.parquet
public.orders/order_date=2026-01-02/base/base_000000.parquet
public.orders/order_date=__HIVE_DEFAULT_PARTITION__/base/base_000000.parquet
public.orders/order_date=2026-01-02/deltas/epoch=000001.parquet
```

- The date column is **omitted from the Parquet files** and reconstructed from the path when read with `hive_partitioning=true` (standard Hive layout; also required so Glue/Athena can declare it as a partition key).
- Rows with an empty or `NULL` business date land in the registered default partition `__HIVE_DEFAULT_PARTITION__`, which readers map back to `NULL`.
- An `UPDATE` that changes the business date writes its delta into the **new** date's directory. The old partition keeps its previous data until the next compaction; see [Compaction](05-compaction.md#business-date-partitioning-and-moves) for how the moved row leaves the old partition.
- The manifest records the actual landing locations for each table (`partition.column` plus one entry per `<col>=<value>` directory with its row count), so downstream consumers and the Glue catalog know which partitions exist.

Consumers prune to a date range by filtering the reconstructed column, e.g. `WHERE order_date IN (DATE '2026-01-01', DATE '2026-01-02')`.

## Governance During Streaming

The WAL stream is scoped by the PostgreSQL publication (created during init). Only tables included at init time will appear in the stream. As a safety net, the streaming loop also checks each event's table against `manifest.ActiveTables()` and skips unknown tables.

## Monitoring

While streaming, check health with:

```bash
pg-cdc status --config pg-cdc.yml
```
