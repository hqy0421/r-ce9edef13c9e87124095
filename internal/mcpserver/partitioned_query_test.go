package mcpserver

import (
	"context"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/layout"
	"github.com/burnside-project/pg-cdc-core/internal/partition"
	"github.com/burnside-project/pg-cdc-core/internal/writer"
)

// TestQueryPartitionedEndToEnd writes a real Hive-partitioned table (partition
// column omitted from the files) and checks that the MCP query tool
// reconstructs the date column and can target two date partitions.
func TestQueryPartitionedEndToEnd(t *testing.T) {
	if _, err := exec.LookPath("duckdb"); err != nil {
		t.Skip("duckdb CLI not on PATH — skipping partitioned end-to-end query test")
	}
	dir := t.TempDir()
	ctx := context.Background()

	cols := []manifest.Column{
		{Name: "id", PgType: "bigint", ParquetType: "INT64"},
		{Name: "order_date", PgType: "date", ParquetType: "DATE", Nullable: true},
		{Name: "amount", PgType: "numeric", ParquetType: "DOUBLE", Nullable: true},
	}
	schema := manifest.Schema{Version: 1, Columns: cols}
	spec := partition.Spec{Column: "order_date"}

	rows := []writer.Row{
		{"id": int64(1), "order_date": "2026-01-01", "amount": 10.0},
		{"id": int64(2), "order_date": "2026-01-01", "amount": 20.0},
		{"id": int64(3), "order_date": "2026-01-02", "amount": 30.0},
		{"id": int64(4), "order_date": nil, "amount": 40.0},
	}
	groups := layout.GroupByPartition(spec, rows)
	for _, pdir := range layout.SortedDirs(groups) {
		path := filepath.Join(dir, filepath.FromSlash(layout.BasePath("public.orders", pdir, 0)))
		if err := writer.WriteBaseParquetOpts(path, groups[pdir], cols,
			writer.BaseOptions{Omit: map[string]bool{"order_date": true}}); err != nil {
			t.Fatalf("write base: %v", err)
		}
	}

	m := &manifest.Manifest{
		Version: 1,
		Tables: map[string]manifest.Table{"public.orders": {
			Status:     "active",
			PrimaryKey: []string{"id"},
			Path:       "public.orders",
			RowCount:   int64(len(rows)),
			Partition: `{"column":"order_date","partitions":[
				{"value":"2026-01-01","path":"public.orders/order_date=2026-01-01","rows":2},
				{"value":"2026-01-02","path":"public.orders/order_date=2026-01-02","rows":1},
				{"value":"__HIVE_DEFAULT_PARTITION__","path":"public.orders/order_date=__HIVE_DEFAULT_PARTITION__","rows":1}]}`,
		}},
		Schemas: map[string]manifest.Schema{"public.orders": schema},
	}
	s := &server{
		cfg:  config.Config{Storage: config.StorageConfig{Type: "filesystem", Path: dir}},
		sink: &fakeSink{listRoot: dir, m: m},
	}

	// All rows: the date column comes back, NULL stays NULL.
	res := s.query(ctx, []byte(`{"sql":"SELECT id, order_date FROM public.orders ORDER BY id"}`))
	text := textOf(t, res)
	for _, want := range []string{`"id": 1`, `"order_date": "2026-01-01"`, `"id": 4`, `"order_date": null`} {
		if !strings.Contains(text, want) {
			t.Fatalf("expected %q in result:\n%s", want, text)
		}
	}

	// Two-date slice: only the two selected partitions contribute.
	res = s.query(ctx, []byte(`{"sql":"SELECT id FROM public.orders WHERE order_date IN (DATE '2026-01-01', DATE '2026-01-02') ORDER BY id"}`))
	text = textOf(t, res)
	if !strings.Contains(text, "3 row(s)") {
		t.Fatalf("expected exactly 3 rows for the two-date slice:\n%s", text)
	}
}

func textOf(t *testing.T, res map[string]any) string {
	t.Helper()
	content, ok := res["content"].([]map[string]any)
	if !ok || len(content) == 0 {
		t.Fatalf("bad content shape: %#v", res)
	}
	if err, _ := res["isError"].(bool); err {
		t.Fatalf("tool returned an error: %v", content[0]["text"])
	}
	return content[0]["text"].(string)
}

func TestQueryMixedLayoutDuringMigration(t *testing.T) {
	if _, err := exec.LookPath("duckdb"); err != nil {
		t.Skip("no duckdb")
	}
	dir := t.TempDir()
	cols := []manifest.Column{
		{Name: "id", PgType: "bigint", ParquetType: "INT64"},
		{Name: "order_date", PgType: "date", ParquetType: "DATE", Nullable: true},
	}
	// Legacy base holds the date as a file column.
	legacy := filepath.Join(dir, "public.orders/base/base_000000.parquet")
	if err := writer.WriteBaseParquet(legacy, []writer.Row{
		{"id": int64(1), "order_date": "2026-01-01"},
		{"id": int64(2), "order_date": nil},
	}, cols); err != nil {
		t.Fatal(err)
	}
	// Streaming after enabling partitions writes a partitioned delta.
	delta := filepath.Join(dir, "public.orders/order_date=2026-01-03/deltas/epoch=000001.parquet")
	if err := writer.WriteDeltaParquetOpts(delta, []writer.Row{
		{"__op": "I", "id": int64(3)},
	}, cols, 1, writer.DeltaOptions{Omit: map[string]bool{"order_date": true}}); err != nil {
		t.Fatal(err)
	}

	m := &manifest.Manifest{
		Version: 1,
		Tables: map[string]manifest.Table{"public.orders": {
			Status: "active", Path: "public.orders", BaseEpoch: 0, LatestDeltaEpoch: 1,
		}},
		Schemas: map[string]manifest.Schema{"public.orders": {Version: 1, Columns: cols}},
	}
	s := &server{cfg: config.Config{Storage: config.StorageConfig{Type: "filesystem", Path: dir}},
		sink: &fakeSink{listRoot: dir, m: m}}

	res := s.query(context.Background(), []byte(`{"sql":"SELECT id, order_date FROM public.orders ORDER BY id"}`))
	text := textOf(t, res)
	for _, want := range []string{`"id": 1`, `"order_date": "2026-01-01"`, `"id": 2`, `"order_date": null`, `"id": 3`, `"order_date": "2026-01-03"`} {
		if !strings.Contains(text, want) {
			t.Fatalf("missing %q in %s", want, text)
		}
	}

	res = s.recentChanges(context.Background(), []byte(`{"name":"public.orders"}`))
	text = textOf(t, res)
	if !strings.Contains(text, `"order_date": "2026-01-03"`) {
		t.Fatalf("delta date not reconstructed: %s", text)
	}
}
