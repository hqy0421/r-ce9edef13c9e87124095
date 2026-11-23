package mcpserver

import (
	"context"
	"strings"
	"testing"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/config"
)

// listingSink is a fakeSink that serves a fixed file listing.
type listingSink struct {
	fakeSink
	files map[string][]string
}

func (l *listingSink) ListFiles(_ context.Context, prefix string) ([]string, error) {
	return l.files[strings.TrimSuffix(prefix, "/")], nil
}

func manifestWithTables(tables map[string]manifest.Table, schemas map[string]manifest.Schema) *manifest.Manifest {
	return &manifest.Manifest{Version: 1, Tables: tables, Schemas: schemas}
}

func ddlServer(files map[string][]string, m *manifest.Manifest) *server {
	return &server{
		cfg:  config.Config{Storage: config.StorageConfig{Type: "filesystem", Path: "/data/out"}},
		sink: &listingSink{fakeSink: fakeSink{m: m}, files: files},
	}
}

func TestBuildViewDDL_Partitioned(t *testing.T) {
	tbl := manifest.Table{
		Status:    "active",
		Path:      "public.orders",
		BaseEpoch: 3,
		Partition: `{"column":"order_date","partitions":[{"value":"2026-01-01","path":"p","rows":1},{"value":"2026-01-02","path":"p","rows":1}]}`,
	}
	m := manifestWithTables(
		map[string]manifest.Table{"public.orders": tbl},
		map[string]manifest.Schema{"public.orders": {Version: 1, Columns: []manifest.Column{
			{Name: "id", ParquetType: "INT64"},
			{Name: "order_date", ParquetType: "DATE"},
		}}},
	)
	files := []string{
		"public.orders/order_date=2026-01-01/base/base_000003.parquet",
		"public.orders/order_date=2026-01-02/base/base_000003.parquet",
		"public.orders/order_date=2026-01-03/base/base_000002.parquet", // superseded generation
		"public.orders/order_date=2026-01-02/deltas/epoch=000004.parquet",
		"public.orders/order_date=2026-01-02/deltas/epoch=000002.parquet", // already applied
	}
	s := ddlServer(map[string][]string{"public.orders": files}, m)

	ddl, err := s.buildViewDDL(context.Background(), m)
	if err != nil {
		t.Fatalf("buildViewDDL: %v", err)
	}
	if !strings.Contains(ddl, "hive_partitioning=true") {
		t.Fatalf("partitioned view must enable hive partitioning:\n%s", ddl)
	}
	// Superseded base and already-applied delta must not be read.
	if strings.Contains(ddl, "base_000002") {
		t.Fatalf("superseded base leaked into the view:\n%s", ddl)
	}
	if strings.Contains(ddl, "epoch=000002") {
		t.Fatalf("applied delta leaked into the view:\n%s", ddl)
	}
	for _, want := range []string{"base_000003.parquet", "epoch=000004.parquet"} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("live file %s missing from view:\n%s", want, ddl)
		}
	}
	if !strings.Contains(ddl, `/data/out/public.orders/order_date=2026-01-02/base/base_000003.parquet`) {
		t.Fatalf("view paths must be storage-rooted:\n%s", ddl)
	}
}

func TestBuildViewDDL_MixedLayoutUnions(t *testing.T) {
	tbl := manifest.Table{Status: "active", Path: "public.orders", BaseEpoch: 1}
	m := manifestWithTables(
		map[string]manifest.Table{"public.orders": tbl},
		map[string]manifest.Schema{"public.orders": {Version: 1, Columns: []manifest.Column{{Name: "id", ParquetType: "INT64"}}}},
	)
	files := []string{
		"public.orders/base/base_000001.parquet",
		"public.orders/order_date=2026-01-01/base/base_000001.parquet",
	}
	s := ddlServer(map[string][]string{"public.orders": files}, m)

	ddl, err := s.buildViewDDL(context.Background(), m)
	if err != nil {
		t.Fatalf("buildViewDDL: %v", err)
	}
	if !strings.Contains(ddl, "UNION ALL BY NAME") {
		t.Fatalf("mixed legacy/partitioned layout must union the two reads:\n%s", ddl)
	}
	if strings.Count(ddl, "hive_partitioning=true") != 1 {
		t.Fatalf("only the partitioned half should enable hive partitioning:\n%s", ddl)
	}
}

func TestBuildViewDDL_NoFilesYieldsTypedEmptyView(t *testing.T) {
	tbl := manifest.Table{Status: "active", Path: "public.orders", Partition: `{"column":"order_date","partitions":[]}`}
	m := manifestWithTables(
		map[string]manifest.Table{"public.orders": tbl},
		map[string]manifest.Schema{"public.orders": {Version: 1, Columns: []manifest.Column{
			{Name: "id", ParquetType: "INT64"},
			{Name: "order_date", ParquetType: "DATE"},
		}}},
	)
	s := ddlServer(map[string][]string{}, m)
	ddl, err := s.buildViewDDL(context.Background(), m)
	if err != nil {
		t.Fatalf("buildViewDDL: %v", err)
	}
	if !strings.Contains(ddl, "WHERE false") {
		t.Fatalf("empty table should still resolve to an empty view:\n%s", ddl)
	}
	if !strings.Contains(ddl, "CAST(NULL AS DATE)") {
		t.Fatalf("partition column should be typed DATE in the empty view:\n%s", ddl)
	}
}

func TestDeltaRelation(t *testing.T) {
	s := ddlServer(nil, manifestWithTables(nil, nil))

	legacy, err := s.deltaRelation("public.orders", []string{
		"public.orders/deltas/epoch=000001.parquet",
		"public.orders/deltas/epoch=000002.parquet",
	})
	if err != nil {
		t.Fatalf("deltaRelation: %v", err)
	}
	if strings.Contains(legacy, "hive_partitioning") {
		t.Fatalf("legacy deltas must not enable hive partitioning:\n%s", legacy)
	}
	if !strings.Contains(legacy, "epoch=000001.parquet") || !strings.Contains(legacy, "epoch=000002.parquet") {
		t.Fatalf("delta files missing:\n%s", legacy)
	}
	if !strings.Contains(legacy, "/data/out/public.orders/deltas/") {
		t.Fatalf("delta paths must be storage-rooted:\n%s", legacy)
	}

	partitioned, err := s.deltaRelation("public.orders", []string{
		"public.orders/order_date=2026-01-01/deltas/epoch=000003.parquet",
	})
	if err != nil {
		t.Fatalf("deltaRelation: %v", err)
	}
	if !strings.Contains(partitioned, "hive_partitioning=true") {
		t.Fatalf("partitioned deltas must enable hive partitioning:\n%s", partitioned)
	}

	mixed, err := s.deltaRelation("public.orders", []string{
		"public.orders/deltas/epoch=000001.parquet",
		"public.orders/order_date=2026-01-01/deltas/epoch=000003.parquet",
	})
	if err != nil {
		t.Fatalf("deltaRelation: %v", err)
	}
	if !strings.Contains(mixed, "UNION ALL BY NAME") {
		t.Fatalf("mixed delta layouts must union:\n%s", mixed)
	}
}
