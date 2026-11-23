package internal

import (
	"context"
	"strings"
	"testing"
	"time"

	btypes "github.com/burnside-project/burnside-go/types"

	"github.com/burnside-project/pg-cdc-core/internal/adapters/sink/filesystem"
	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/layout"
	"github.com/burnside-project/pg-cdc-core/internal/partition"
	"github.com/burnside-project/pg-cdc-core/internal/ports"
)

// fakeSource serves a fixed single-table snapshot and lets a test inject the
// streamed events it wants the flush to see.
type fakeSource struct {
	table  string
	cols   []ports.ColumnInfo
	rows   []map[string]any
	events []ports.CDCEvent
}

func (f *fakeSource) ListTables(context.Context, string) ([]string, error) {
	return []string{f.table}, nil
}

func (f *fakeSource) GetTableSchema(context.Context, string, string) ([]ports.ColumnInfo, error) {
	return f.cols, nil
}

func (f *fakeSource) GetPrimaryKeys(context.Context, string, string) ([]string, error) {
	return []string{"id"}, nil
}

func (f *fakeSource) Version(context.Context) (string, error) { return "PostgreSQL 16.2", nil }

func (f *fakeSource) CreateReplicationSlot(context.Context, string) (ports.Snapshot, error) {
	return ports.Snapshot{LSN: "0/1", SnapshotName: "snap"}, nil
}

func (f *fakeSource) ReleaseSnapshot(context.Context) {}

func (f *fakeSource) CreatePublication(context.Context, string, []string) error { return nil }

func (f *fakeSource) CopyTable(context.Context, string, string, string) ([]map[string]any, []ports.ColumnInfo, error) {
	return f.rows, f.cols, nil
}

func (f *fakeSource) Stream(_ context.Context, _, _, _ string, events chan<- ports.CDCEvent) error {
	for _, e := range f.events {
		events <- e
	}
	close(events)
	return nil
}

func (f *fakeSource) BuildRoleProfiles(context.Context, []string) (map[string]ports.RoleProfile, error) {
	return nil, nil
}

func (f *fakeSource) DropReplicationSlot(context.Context, string) error  { return nil }
func (f *fakeSource) DropPublication(context.Context, string) error      { return nil }
func (f *fakeSource) QueryRow(context.Context, string, ...any) ports.Row { return nil }
func (f *fakeSource) Close()                                             {}

func ordersSource() *fakeSource {
	return &fakeSource{
		table: "orders",
		cols: []ports.ColumnInfo{
			{Name: "id", Type: "bigint", Nullable: false, Position: 1},
			{Name: "order_date", Type: "date", Nullable: true, Position: 2},
			{Name: "amount", Type: "numeric", Nullable: true, Position: 3},
		},
		rows: []map[string]any{
			{"id": int64(1), "order_date": time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), "amount": 100.0},
			{"id": int64(2), "order_date": time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC), "amount": 200.0},
			{"id": int64(3), "order_date": nil, "amount": 300.0},
		},
	}
}

func partitionConfig(dir string) config.Config {
	return config.Config{
		Source:  config.SourceConfig{Postgres: config.PostgresConfig{Schemas: []string{"public"}}},
		Storage: config.StorageConfig{Type: "filesystem", Path: dir},
		Partitions: config.PartitionsConfig{
			DateColumn: "order_date",
		},
	}
}

func listTable(t *testing.T, s ports.Sink, table string) []string {
	t.Helper()
	files, err := s.ListFiles(context.Background(), table+"/")
	if err != nil {
		t.Fatalf("list files: %v", err)
	}
	return files
}

func hasFile(files []string, want string) bool {
	for _, f := range files {
		if f == want {
			return true
		}
	}
	return false
}

func countUnder(files []string, prefix string) int {
	n := 0
	for _, f := range files {
		if strings.HasPrefix(f, prefix) {
			n++
		}
	}
	return n
}

func TestPartitioningEndToEnd(t *testing.T) {
	dir := t.TempDir()
	cfg := partitionConfig(dir)
	src := ordersSource()
	sink, err := filesystem.New(dir)
	if err != nil {
		t.Fatalf("filesystem sink: %v", err)
	}
	ctx := context.Background()

	// --- init: rows land in three partitions, incl. the default one.
	if err := Init(ctx, cfg, src, sink); err != nil {
		t.Fatalf("init: %v", err)
	}
	files := listTable(t, sink, "public.orders")
	for _, want := range []string{
		"public.orders/order_date=2026-01-01/base/base_000000.parquet",
		"public.orders/order_date=2026-01-02/base/base_000000.parquet",
		"public.orders/order_date=" + partition.NullValue + "/base/base_000000.parquet",
	} {
		if !hasFile(files, want) {
			t.Fatalf("init missing %s\nfiles: %v", want, files)
		}
	}

	m, err := sink.ReadManifest(ctx)
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	state := partition.ParseState(m.Tables["public.orders"].Partition)
	if state.Column != "order_date" {
		t.Fatalf("manifest partition column = %q", state.Column)
	}
	rowsByValue := map[string]int64{}
	for _, e := range state.Partitions {
		rowsByValue[e.Value] = e.Rows
		if !strings.HasSuffix(e.Path, "order_date="+e.Value) {
			t.Fatalf("entry path %q does not point at %q", e.Path, e.Value)
		}
	}
	if rowsByValue["2026-01-01"] != 1 || rowsByValue["2026-01-02"] != 1 || rowsByValue[partition.NullValue] != 1 {
		t.Fatalf("init partition row counts = %v", rowsByValue)
	}

	// --- stream: UPDATE moves PK 1 from 2026-01-01 to 2026-01-02.
	src.events = []ports.CDCEvent{{
		Table: "public.orders",
		Op:    btypes.OpUpdate,
		Row:   map[string]any{"id": int64(1), "order_date": "2026-01-02", "amount": 111.0},
	}}
	buf := &eventBuffer{cfg: cfg, source: src, sink: sink, manifest: m, tables: m.ActiveTables()}
	for _, e := range src.events {
		buf.add(e)
	}
	if err := buf.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}

	files = listTable(t, sink, "public.orders")
	if !hasFile(files, "public.orders/order_date=2026-01-02/deltas/epoch=000001.parquet") {
		t.Fatalf("delta not written to the new date partition\nfiles: %v", files)
	}
	if countUnder(files, "public.orders/order_date=2026-01-01/deltas/") != 0 {
		t.Fatalf("delta should not land in the old partition\nfiles: %v", files)
	}

	// The manifest now reflects the newly-seen landing location too.
	m, _ = sink.ReadManifest(ctx)
	state = partition.ParseState(m.Tables["public.orders"].Partition)
	seen := map[string]bool{}
	for _, e := range state.Partitions {
		seen[e.Value] = true
	}
	if !seen["2026-01-02"] || !seen["2026-01-01"] {
		t.Fatalf("stream did not record partition dirs: %+v", state.Partitions)
	}

	// --- compact: the row now lives only in 2026-01-02; the old partition has
	// no live copy and its base file is gone.
	if err := Compact(ctx, cfg, sink); err != nil {
		t.Fatalf("compact: %v", err)
	}
	files = listTable(t, sink, "public.orders")
	if countUnder(files, "public.orders/order_date=2026-01-01/") != 0 {
		t.Fatalf("old partition still has files after compaction\nfiles: %v", files)
	}
	if !hasFile(files, "public.orders/order_date=2026-01-02/base/base_000001.parquet") {
		t.Fatalf("new base missing in 2026-01-02\nfiles: %v", files)
	}
	if countUnder(files, "public.orders/order_date=2026-01-02/deltas/") != 0 {
		t.Fatalf("applied deltas should be deleted\nfiles: %v", files)
	}

	m, _ = sink.ReadManifest(ctx)
	tbl := m.Tables["public.orders"]
	if tbl.BaseEpoch != 1 || tbl.LatestDeltaEpoch != 1 {
		t.Fatalf("epochs after compact: base=%d latest=%d", tbl.BaseEpoch, tbl.LatestDeltaEpoch)
	}
	state = partition.ParseState(tbl.Partition)
	rowsByValue = map[string]int64{}
	for _, e := range state.Partitions {
		rowsByValue[e.Value] = e.Rows
	}
	if _, ok := rowsByValue["2026-01-01"]; ok {
		t.Fatalf("emptied partition still in manifest: %v", rowsByValue)
	}
	if rowsByValue["2026-01-02"] != 2 || rowsByValue[partition.NullValue] != 1 {
		t.Fatalf("post-compact row counts = %v", rowsByValue)
	}

	// The moved row is actually present in the 2026-01-02 base, with the new
	// amount, and the date column is reconstructed from the path.
	basePath := "public.orders/order_date=2026-01-02/base/base_000001.parquet"
	rows, err := readParquetRows(ctx, sink, basePath)
	if err != nil {
		t.Fatalf("read base: %v", err)
	}
	found := false
	for _, r := range rows {
		if vInt64Eq(r["id"], 1) {
			found = true
			if _, ok := r["order_date"]; ok {
				t.Fatalf("partition column should be omitted from the file: %v", r)
			}
			if r["amount"] != 111.0 {
				t.Fatalf("moved row kept stale amount: %v", r["amount"])
			}
		}
	}
	if !found {
		t.Fatalf("moved row not in new partition base: %v", rows)
	}

	// --- second compaction with nothing new is a genuine no-op.
	before := listTable(t, sink, "public.orders")
	beforeManifest, _ := sink.ReadManifest(ctx)
	if err := Compact(ctx, cfg, sink); err != nil {
		t.Fatalf("second compact: %v", err)
	}
	after := listTable(t, sink, "public.orders")
	afterManifest, _ := sink.ReadManifest(ctx)
	if len(before) != len(after) {
		t.Fatalf("no-op compaction changed files: before=%v after=%v", before, after)
	}
	if !beforeManifest.Compaction.LastCompactedAt.Equal(afterManifest.Compaction.LastCompactedAt) {
		t.Fatal("no-op compaction rewrote the manifest")
	}
}

func TestCompactionPersistsTombstones(t *testing.T) {
	dir := t.TempDir()
	cfg := partitionConfig(dir)
	src := ordersSource()
	sink, err := filesystem.New(dir)
	if err != nil {
		t.Fatalf("filesystem sink: %v", err)
	}
	ctx := context.Background()
	if err := Init(ctx, cfg, src, sink); err != nil {
		t.Fatalf("init: %v", err)
	}
	m, _ := sink.ReadManifest(ctx)

	src.events = []ports.CDCEvent{{
		Table: "public.orders",
		Op:    btypes.OpDelete,
		Row:   map[string]any{"id": int64(2)},
	}}
	buf := &eventBuffer{cfg: cfg, source: src, sink: sink, manifest: m, tables: m.ActiveTables()}
	for _, e := range src.events {
		buf.add(e)
	}
	if err := buf.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := Compact(ctx, cfg, sink); err != nil {
		t.Fatalf("compact: %v", err)
	}

	rows, err := readParquetRows(ctx, sink, "public.orders/order_date=2026-01-02/base/base_000001.parquet")
	if err != nil {
		t.Fatalf("read base: %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("expected one row (tombstone) in 2026-01-02, got %d", len(rows))
	}
	row := rows[0]
	if vInt64Eq(row["id"], 2) == false {
		t.Fatalf("unexpected survivor: %v", row)
	}
	ts, ok := row[btypes.ColDeletedAt].(string)
	if !ok || ts == "" {
		t.Fatalf("tombstone timestamp not persisted: %v", row)
	}
	if _, err := time.Parse(time.RFC3339, ts); err != nil {
		t.Fatalf("tombstone timestamp malformed: %q", ts)
	}
}

func TestLegacyTableStaysUnpartitioned(t *testing.T) {
	dir := t.TempDir()
	cfg := partitionConfig(dir)
	cfg.Partitions = config.PartitionsConfig{} // no date column configured
	src := ordersSource()
	sink, err := filesystem.New(dir)
	if err != nil {
		t.Fatalf("filesystem sink: %v", err)
	}
	ctx := context.Background()
	if err := Init(ctx, cfg, src, sink); err != nil {
		t.Fatalf("init: %v", err)
	}
	files := listTable(t, sink, "public.orders")
	basePath := layout.BasePath("public.orders", "", 0)
	if !hasFile(files, basePath) {
		t.Fatalf("legacy base missing, files: %v", files)
	}
	for _, f := range files {
		if strings.Contains(f, "order_date=") {
			t.Fatalf("unexpected partition directory in legacy layout: %s", f)
		}
	}
	m, _ := sink.ReadManifest(ctx)
	if m.Tables["public.orders"].Partition != "" {
		t.Fatalf("legacy table should have no partition state: %q", m.Tables["public.orders"].Partition)
	}

	// Streaming an update keeps the legacy single-deltas dir.
	src.events = []ports.CDCEvent{{
		Table: "public.orders", Op: btypes.OpUpdate,
		Row: map[string]any{"id": int64(1), "order_date": "2026-01-03", "amount": 5.0},
	}}
	buf := &eventBuffer{cfg: cfg, source: src, sink: sink, manifest: m, tables: m.ActiveTables()}
	for _, e := range src.events {
		buf.add(e)
	}
	if err := buf.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := Compact(ctx, cfg, sink); err != nil {
		t.Fatalf("compact: %v", err)
	}
	files = listTable(t, sink, "public.orders")
	if !hasFile(files, layout.BasePath("public.orders", "", 1)) {
		t.Fatalf("legacy compacted base missing: %v", files)
	}
	if countUnder(files, "public.orders/base/") != 1 {
		t.Fatalf("legacy layout should keep exactly one base: %v", files)
	}
}

// vInt64Eq compares a decoded parquet value with an int64.
func vInt64Eq(v any, want int64) bool {
	n, ok := vInt64(v)
	return ok && n == want
}

// TestLegacyToPartitionedMigration covers turning partitioning on for a table
// that was snapshotted unpartitioned: the date column is read from the legacy
// base file, the rows are re-bucketed, and the legacy base is garbage-collected.
func TestLegacyToPartitionedMigration(t *testing.T) {
	dir := t.TempDir()
	legacyCfg := partitionConfig(dir)
	legacyCfg.Partitions = config.PartitionsConfig{}
	sink, err := filesystem.New(dir)
	if err != nil {
		t.Fatalf("filesystem sink: %v", err)
	}
	ctx := context.Background()

	src := ordersSource()
	if err := Init(ctx, legacyCfg, src, sink); err != nil {
		t.Fatalf("init: %v", err)
	}
	if !hasFile(listTable(t, sink, "public.orders"), "public.orders/base/base_000000.parquet") {
		t.Fatal("expected a legacy base after init")
	}

	// Operator turns partitioning on afterwards; compaction migrates the layout.
	cfg := partitionConfig(dir)
	if err := Compact(ctx, cfg, sink); err != nil {
		t.Fatalf("compact: %v", err)
	}

	files := listTable(t, sink, "public.orders")
	if hasFile(files, "public.orders/base/base_000000.parquet") {
		t.Fatalf("legacy base should have been collected: %v", files)
	}
	// Generation bumps past the legacy base so a crash mid-migration can never
	// leave two readable copies of the same generation.
	for _, want := range []string{
		"public.orders/order_date=2026-01-01/base/base_000001.parquet",
		"public.orders/order_date=2026-01-02/base/base_000001.parquet",
		"public.orders/order_date=" + partition.NullValue + "/base/base_000001.parquet",
	} {
		if !hasFile(files, want) {
			t.Fatalf("migrated layout missing %s\nfiles: %v", want, files)
		}
	}

	m, _ := sink.ReadManifest(ctx)
	state := partition.ParseState(m.Tables["public.orders"].Partition)
	if state.Column != "order_date" || len(state.Partitions) != 3 {
		t.Fatalf("manifest not migrated: %+v", state)
	}

	rows, err := readParquetRows(ctx, sink, "public.orders/order_date=2026-01-02/base/base_000001.parquet")
	if err != nil {
		t.Fatalf("read migrated base: %v", err)
	}
	if len(rows) != 1 || !vInt64Eq(rows[0]["id"], 2) {
		t.Fatalf("unexpected rows in 2026-01-02: %v", rows)
	}
	if _, ok := rows[0]["order_date"]; ok {
		t.Fatalf("partition column should be omitted after migration: %v", rows[0])
	}
}

// TestMigrationThenStreamIsApplied guards the epoch bookkeeping across a
// legacy→partitioned migration: the migrated base advances the generation, and
// the next streamed delta must still be strictly newer than the base so
// compaction applies it rather than treating it as already folded in.
func TestMigrationThenStreamIsApplied(t *testing.T) {
	dir := t.TempDir()
	legacyCfg := partitionConfig(dir)
	legacyCfg.Partitions = config.PartitionsConfig{}
	sink, err := filesystem.New(dir)
	if err != nil {
		t.Fatalf("filesystem sink: %v", err)
	}
	ctx := context.Background()
	src := ordersSource()
	if err := Init(ctx, legacyCfg, src, sink); err != nil {
		t.Fatalf("init: %v", err)
	}

	cfg := partitionConfig(dir)
	if err := Compact(ctx, cfg, sink); err != nil {
		t.Fatalf("migration compact: %v", err)
	}
	m, _ := sink.ReadManifest(ctx)
	// LatestDeltaEpoch must not fall behind the new base, or the next flush
	// would reuse an epoch that compaction considers already applied.
	if m.Tables["public.orders"].LatestDeltaEpoch < m.Tables["public.orders"].BaseEpoch {
		t.Fatalf("epoch invariant broken: base=%d latest=%d",
			m.Tables["public.orders"].BaseEpoch, m.Tables["public.orders"].LatestDeltaEpoch)
	}

	src.events = []ports.CDCEvent{{
		Table: "public.orders", Op: btypes.OpUpdate,
		Row: map[string]any{"id": int64(3), "order_date": "2026-01-05", "amount": 3.0},
	}}
	buf := &eventBuffer{cfg: cfg, source: src, sink: sink, manifest: m, tables: m.ActiveTables()}
	for _, e := range src.events {
		buf.add(e)
	}
	if err := buf.flush(ctx); err != nil {
		t.Fatalf("flush: %v", err)
	}
	if err := Compact(ctx, cfg, sink); err != nil {
		t.Fatalf("second compact: %v", err)
	}

	rows, err := readParquetRows(ctx, sink, "public.orders/order_date=2026-01-05/base/base_000002.parquet")
	if err != nil {
		t.Fatalf("read new partition base: %v", err)
	}
	found := false
	for _, r := range rows {
		if vInt64Eq(r["id"], 3) && r["amount"] == 3.0 {
			found = true
		}
	}
	if !found {
		t.Fatalf("streamed update after migration was not applied: %v", rows)
	}
}
