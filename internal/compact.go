package internal

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/burnside-project/burnside-go/epoch"
	"github.com/burnside-project/burnside-go/manifest"
	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/layout"
	"github.com/burnside-project/pg-cdc-core/internal/partition"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
	"github.com/burnside-project/pg-cdc-core/internal/writer"

	goparquet "github.com/parquet-go/parquet-go"
)

// Compact merges delta Parquet files into a new base snapshot for each table.
// Applies INSERT/UPDATE/DELETE operations, maintains tombstones with TTL, and
// — for business-date partitioned tables — buckets the surviving rows into
// per-date bases so a row moved by an UPDATE leaves no live copy in its old
// partition.
func Compact(ctx context.Context, cfg config.Config, sink ports.Sink) error {
	m, err := sink.ReadManifest(ctx)
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}

	tables := m.ActiveTables()
	names := sortedTableNames(tables)
	fmt.Printf("Compacting %d tables\n", len(names))

	var compacted int
	for _, tableName := range names {
		schema, ok := m.Schemas[tableName]
		if !ok {
			fmt.Printf("  %s: skip (no schema)\n", tableName)
			continue
		}
		did, err := compactTable(ctx, sink, m, cfg, tableName, tables[tableName], schema)
		if err != nil {
			return fmt.Errorf("compact %s: %w", tableName, err)
		}
		if did {
			compacted++
		}
	}

	if compacted == 0 {
		fmt.Println("Nothing to compact")
		return nil
	}

	// Write updated manifest
	m.Compaction.LastCompactedAt = time.Now().UTC()
	m.UpdatedAt = time.Now().UTC()
	if err := sink.WriteManifest(ctx, m); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	// Re-sync Glue partition metadata best-effort; `catalog register` is the
	// repair path if this fails.
	if cfg.Catalog.Type == "glue" {
		if err := registerCatalog(ctx, cfg, m); err != nil {
			fmt.Printf("  warning: catalog re-sync after compaction: %v\n", err)
		}
	}

	fmt.Println("Compaction complete")
	return nil
}

func sortedTableNames(tables map[string]manifest.Table) []string {
	names := make([]string, 0, len(tables))
	for n := range tables {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

type deltaRow struct {
	epoch int64
	seq   int64
	op    string
	row   writer.Row
}

// compactTable rewrites one table's base to a single generation. It reports
// whether it changed anything: a table whose live base already reflects every
// delta is left completely untouched.
func compactTable(ctx context.Context, sink ports.Sink, m *manifest.Manifest, cfg config.Config, tableName string, table manifest.Table, schema manifest.Schema) (bool, error) {
	pkCols := table.PrimaryKey
	if len(pkCols) == 0 {
		return false, fmt.Errorf("no primary key for %s", tableName)
	}

	// Resolve the partition column. A configured column wins; otherwise keep
	// whatever the manifest already records (so a table stays partitioned even
	// if the config later drops it). The column must exist in the schema.
	spec := partitionSpecFor(cfg, tableName, schema)
	if !spec.Enabled() {
		spec = partition.ParseState(table.Partition).Spec()
	}

	// Step 1: list everything under the table root (both layouts).
	files, err := sink.ListFiles(ctx, table.Path+"/")
	if err != nil {
		return false, fmt.Errorf("list table files: %w", err)
	}
	bases, deltas := layout.Classify(table.Path, files)
	liveBaseFiles, baseGen := layout.SelectGeneration(bases, table.BaseEpoch)
	pendingFiles := pendingEpochFiles(deltas, table.BaseEpoch)

	// Nothing to fold in and the base generation is already the only one on
	// disk => a true no-op. Only dir membership is compared, not row counts:
	// streaming grows the partition list optimistically, and compaction is the
	// authority that reconciles it.
	if len(deltas) == 0 && generationsCurrent(bases, baseGen) && partitionDirsCurrent(table.Path, bases, baseGen, spec, existingPartitionDirs(m, tableName)) {
		fmt.Printf("  %s: up to date (base=%d)\n", tableName, table.BaseEpoch)
		return false, nil
	}

	// Step 2: build the global PK index from the live base. The partition
	// column is not stored in partitioned base files, so it is restored from
	// the directory before the row can be bucketed again.
	indexed := make(map[string]writer.Row)
	tombstones := make(map[string]string)
	for _, f := range liveBaseFiles {
		rows, err := readParquetRows(ctx, sink, f)
		if err != nil {
			return false, fmt.Errorf("read base %s: %w", f, err)
		}
		dir := layout.PartitionDirOf(table.Path, f)
		for _, row := range rows {
			restorePartitionValue(spec, dir, row)
			key := pkKey(row, pkCols)
			indexed[key] = row
			if v := row[btypes.ColDeletedAt]; v != nil {
				tombstones[key] = fmt.Sprint(v)
			}
		}
	}

	// Step 3: read and apply deltas newer than the base, ordered by
	// (epoch, seq) so repeated changes to one PK are applied deterministically.
	pending, err := readDeltasSince(ctx, sink, table.Path, spec, pendingFiles)
	if err != nil {
		return false, err
	}
	sort.SliceStable(pending, func(i, j int) bool {
		if pending[i].epoch != pending[j].epoch {
			return pending[i].epoch < pending[j].epoch
		}
		return pending[i].seq < pending[j].seq
	})

	for _, d := range pending {
		key := pkKey(d.row, pkCols)
		switch d.op {
		case btypes.OpInsert, btypes.OpUpdate:
			imported := copyRow(d.row, btypes.ColOp, btypes.ColEpoch, writer.ColSeq, btypes.ColDeletedAt)
			indexed[key] = imported
			delete(tombstones, key)
		case btypes.OpDelete:
			if existing, ok := indexed[key]; ok {
				// The tombstone keeps the row (and therefore its business
				// date) so downstream consumers can propagate the delete.
				if tombstones[key] == "" {
					tombstones[key] = time.Now().UTC().Format(time.RFC3339)
				}
				indexed[key] = existing
			}
		}
	}

	// Step 4: purge expired tombstones (default 30 days).
	const tombstoneRetention = 30 * 24 * time.Hour
	now := time.Now().UTC()
	var purged int
	for key, ts := range tombstones {
		if t, err := time.Parse(time.RFC3339, ts); err == nil && now.Sub(t) > tombstoneRetention {
			delete(indexed, key)
			delete(tombstones, key)
			purged++
		}
	}

	// Step 5: materialise live rows sorted by PK. Partitioned tables drop the
	// partition column from the file — it is encoded in the directory.
	newBaseEpoch := newestEpoch(table.LatestDeltaEpoch, baseGen)
	for _, d := range pending {
		if d.epoch > newBaseEpoch {
			newBaseEpoch = d.epoch
		}
	}
	// The new generation must be strictly newer than the live one. If it were
	// equal — e.g. a legacy→partitioned migration with no new deltas — the new
	// base files would share a generation with the still-present old base, and
	// a crash before the manifest write would leave both readable, so a reader
	// selecting that generation would double-count the rows.
	if newBaseEpoch <= baseGen {
		newBaseEpoch = baseGen + 1
	}
	liveRows := make([]writer.Row, 0, len(indexed))
	for key, row := range indexed {
		out := copyRow(row)
		if ts, ok := tombstones[key]; ok {
			out[btypes.ColDeletedAt] = ts
		}
		liveRows = append(liveRows, out)
	}
	sort.SliceStable(liveRows, func(i, j int) bool {
		return pkKey(liveRows[i], pkCols) < pkKey(liveRows[j], pkCols)
	})

	// Step 6: write one base per non-empty partition (or a single base for a
	// legacy unpartitioned table).
	groups := layout.GroupByPartition(spec, liveRows)
	observed := make(map[string]int64, len(groups))
	omit := omitSet(spec)
	writeOpts := writer.BaseOptions{Omit: omit, Tombstones: len(tombstones) > 0}
	for _, dir := range layout.SortedDirs(groups) {
		groupRows := groups[dir]
		remotePath := layout.BasePath(table.Path, dir, newBaseEpoch)
		tmpPath := filepath.Join("/tmp", "pgcdc-compact", remotePath)
		if err := writer.WriteBaseParquetOpts(tmpPath, groupRows, schema.Columns, writeOpts); err != nil {
			return false, fmt.Errorf("write new base for %s [%s]: %w", tableName, dir, err)
		}
		if err := uploadFile(ctx, sink, remotePath, tmpPath); err != nil {
			return false, fmt.Errorf("upload base for %s [%s]: %w", tableName, dir, err)
		}
		observed[dir] = int64(len(groupRows))
	}

	// Step 7: update the manifest before deleting anything, so a crash leaves
	// the previous generation authoritative.
	t := m.Tables[tableName]
	t.BaseEpoch = newBaseEpoch
	if t.LatestDeltaEpoch < newBaseEpoch {
		t.LatestDeltaEpoch = newBaseEpoch
	}
	t.RowCount = int64(len(liveRows))
	m.Tables[tableName] = t
	if spec.Enabled() {
		mutatePartitionState(m, tableName, spec.Column, observed)
	} else if t.Partition != "" {
		cleared := m.Tables[tableName]
		cleared.Partition = ""
		m.Tables[tableName] = cleared
	}
	if err := sink.WriteManifest(ctx, m); err != nil {
		return false, fmt.Errorf("write manifest: %w", err)
	}

	// Step 8: delete superseded base generations, bases left in partitions that
	// no longer hold a live row, and every delta folded into the new base.
	var basesDeleted, deltasDeleted int
	for _, f := range bases {
		n, ok := layout.ParseBaseEpoch(f)
		if !ok || n != newBaseEpoch {
			if err := sink.DeleteFile(ctx, f); err == nil {
				basesDeleted++
			}
			continue
		}
		if spec.Enabled() {
			if _, ok := observed[layout.PartitionDirOf(table.Path, f)]; !ok {
				if err := sink.DeleteFile(ctx, f); err == nil {
					basesDeleted++
				}
			}
		}
	}
	for _, d := range deltas {
		if n, ok := epochOfOK(d); !ok || n > newBaseEpoch {
			continue
		}
		if err := sink.DeleteFile(ctx, d); err == nil {
			deltasDeleted++
		}
	}

	fmt.Printf("  %s: base_%06d (%d rows, %d applied, %d purged, %d partitions, -%d bases, -%d deltas)\n",
		tableName, newBaseEpoch, len(liveRows), len(pending), purged, len(observed), basesDeleted, deltasDeleted)
	return true, nil
}

// generationsCurrent reports whether every base file on disk belongs to the
// selected generation.
func generationsCurrent(bases []string, baseGen int64) bool {
	for _, f := range bases {
		if n, ok := layout.ParseBaseEpoch(f); !ok || n != baseGen {
			return false
		}
	}
	return true
}

// partitionDirsCurrent reports whether the set of directories that hold a base
// of the live generation exactly matches the set the manifest records. A
// mismatch in either direction forces a rewrite: an unrecorded directory means
// a date landed without a manifest entry, and a recorded directory with no base
// means a stale entry (e.g. from an interrupted compaction) must be dropped.
func partitionDirsCurrent(tablePath string, bases []string, baseGen int64, spec partition.Spec, recorded map[string]int64) bool {
	if !spec.Enabled() {
		return true
	}
	onDisk := make(map[string]bool)
	for _, f := range bases {
		if n, ok := layout.ParseBaseEpoch(f); !ok || n != baseGen {
			continue
		}
		onDisk[layout.PartitionDirOf(tablePath, f)] = true
	}
	if len(onDisk) != len(recorded) {
		return false
	}
	for dir := range onDisk {
		if _, ok := recorded[dir]; !ok {
			return false
		}
	}
	return true
}

func newestEpoch(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// restorePartitionValue fills in a partitioned table's business-date column for
// a row read from dir, where the column is not stored in the file. A legacy
// (unpartitioned) file keeps its own column and is left alone.
func restorePartitionValue(spec partition.Spec, dir string, row writer.Row) {
	if !spec.Enabled() || dir == "" {
		return
	}
	if v, ok := row[spec.Column]; ok && v != nil {
		return
	}
	value := partitionValueOf(spec.Column, dir)
	if value == partition.NullValue {
		row[spec.Column] = nil
		return
	}
	row[spec.Column] = value
}

func readDeltasSince(ctx context.Context, sink ports.Sink, tablePath string, spec partition.Spec, files []string) ([]deltaRow, error) {
	var out []deltaRow
	for _, f := range files {
		rows, err := readParquetRows(ctx, sink, f)
		if err != nil {
			return nil, fmt.Errorf("read delta %s: %w", f, err)
		}
		dir := layout.PartitionDirOf(tablePath, f)
		for i, row := range rows {
			restorePartitionValue(spec, dir, row)
			op, _ := row[btypes.ColOp].(string)
			seq, ok := vInt64(row[writer.ColSeq])
			if !ok {
				seq = int64(i)
			}
			out = append(out, deltaRow{epoch: epochOf(f), seq: seq, op: op, row: row})
		}
	}
	return out, nil
}

// pendingEpochFiles returns delta files whose epoch is greater than baseEpoch,
// sorted ascending. Objects that do not parse as epoch files are ignored.
func pendingEpochFiles(deltas []string, baseEpoch int64) []string {
	var out []string
	for _, d := range deltas {
		if n, ok := epochOfOK(d); ok && n > baseEpoch {
			out = append(out, d)
		}
	}
	sort.Strings(out)
	return out
}

// epochOf returns the epoch encoded in a delta filename, or 0 if it cannot be
// parsed.
func epochOf(path string) int64 {
	n, _ := epochOfOK(path)
	return n
}

func epochOfOK(path string) (int64, bool) {
	n, err := epoch.ParseFromFilename(filepath.Base(path))
	if err != nil {
		return 0, false
	}
	return n, true
}

func readParquetRows(ctx context.Context, sink ports.Sink, path string) ([]writer.Row, error) {
	rc, err := sink.ReadFile(ctx, path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rc.Close() }()

	// Download to temp for parquet-go (needs seekable file).
	tmpFile, err := os.CreateTemp("", "pgcdc-read-*.parquet")
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	if _, err := tmpFile.ReadFrom(rc); err != nil {
		return nil, err
	}
	if err := tmpFile.Close(); err != nil {
		return nil, err
	}

	f, err := os.Open(tmpFile.Name())
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	pf, err := goparquet.OpenFile(f, stat.Size())
	if err != nil {
		return nil, fmt.Errorf("open parquet %s: %w", path, err)
	}

	schema := pf.Schema()
	fields := schema.Fields()
	colNames := make([]string, len(fields))
	for i, field := range fields {
		colNames[i] = field.Name()
	}

	var rows []writer.Row
	for _, rg := range pf.RowGroups() {
		rgRows := rg.NumRows()
		pqRows := make([]goparquet.Row, rgRows)
		for i := range pqRows {
			pqRows[i] = make(goparquet.Row, len(fields))
		}

		rgReader := rg.Rows()
		n, _ := rgReader.ReadRows(pqRows)
		_ = rgReader.Close()

		for i := 0; i < n; i++ {
			r := make(writer.Row, len(colNames))
			for j, colName := range colNames {
				if j < len(pqRows[i]) {
					r[colName] = parquetValueToGo(pqRows[i][j])
				}
			}
			rows = append(rows, r)
		}
	}

	return rows, nil
}

func parquetValueToGo(v goparquet.Value) any {
	if v.IsNull() {
		return nil
	}
	switch v.Kind() {
	case goparquet.Boolean:
		return v.Boolean()
	case goparquet.Int32:
		return int64(v.Int32())
	case goparquet.Int64:
		return v.Int64()
	case goparquet.Float:
		return float64(v.Float())
	case goparquet.Double:
		return v.Double()
	case goparquet.ByteArray:
		return string(v.ByteArray())
	default:
		return string(v.ByteArray())
	}
}

func pkKey(row writer.Row, pks []string) string {
	key := ""
	for _, pk := range pks {
		key += fmt.Sprintf("%v|", row[pk])
	}
	return key
}

func vInt64(v any) (int64, bool) {
	switch val := v.(type) {
	case int64:
		return val, true
	case int32:
		return int64(val), true
	case int:
		return int64(val), true
	case float64:
		return int64(val), true
	default:
		return 0, false
	}
}
