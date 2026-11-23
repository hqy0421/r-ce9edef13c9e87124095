package layout

import (
	"reflect"
	"testing"

	"github.com/burnside-project/pg-cdc-core/internal/partition"
)

func TestBaseAndDeltaPath(t *testing.T) {
	if got := BasePath("public.orders", "order_date=2026-01-01", 7); got != "public.orders/order_date=2026-01-01/base/base_000007.parquet" {
		t.Fatalf("BasePath partitioned = %q", got)
	}
	if got := BasePath("public.orders", "", 0); got != "public.orders/base/base_000000.parquet" {
		t.Fatalf("BasePath legacy = %q", got)
	}
	if got := DeltaPath("public.orders", "order_date=2026-01-01", "epoch=000003.parquet"); got != "public.orders/order_date=2026-01-01/deltas/epoch=000003.parquet" {
		t.Fatalf("DeltaPath = %q", got)
	}
}

func TestGroupByPartition(t *testing.T) {
	rows := []Row{
		{"id": 1, "d": "2026-01-01"},
		{"id": 2, "d": "2026-01-02"},
		{"id": 3, "d": nil},
		{"id": 4, "d": "2026-01-01"},
	}
	spec := partition.Spec{Column: "d"}
	groups := GroupByPartition(spec, rows)
	if len(groups["d=2026-01-01"]) != 2 {
		t.Fatalf("date1 group: %d", len(groups["d=2026-01-01"]))
	}
	if len(groups["d="+partition.NullValue]) != 1 {
		t.Fatalf("default group: %d", len(groups["d="+partition.NullValue]))
	}

	// Disabled spec: everything under "".
	flat := GroupByPartition(partition.Spec{}, rows)
	if len(flat) != 1 || len(flat[""]) != 4 {
		t.Fatalf("unpartitioned grouping = %+v", flat)
	}

	// A disabled spec over zero rows still yields one bucket, so an empty
	// legacy table writes a single (empty) base instead of none.
	empty := GroupByPartition(partition.Spec{}, nil)
	if len(empty) != 1 {
		t.Fatalf("empty unpartitioned grouping = %+v", empty)
	}
	if _, ok := empty[""]; !ok {
		t.Fatalf("empty unpartitioned grouping missing the legacy bucket: %+v", empty)
	}

	// A partitioned spec over zero rows yields no buckets (no phantom partition).
	if got := GroupByPartition(spec, nil); len(got) != 0 {
		t.Fatalf("empty partitioned grouping = %+v", got)
	}

	got := SortedDirs(groups)
	want := []string{"d=2026-01-01", "d=2026-01-02", "d=" + partition.NullValue}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("SortedDirs = %v, want %v", got, want)
	}
}

func TestPartitionDirOf(t *testing.T) {
	cases := []struct{ file, want string }{
		{"public.orders/order_date=2026-01-01/base/base_000000.parquet", "order_date=2026-01-01"},
		{"public.orders/order_date=2026-01-01/deltas/epoch=000001.parquet", "order_date=2026-01-01"},
		{"public.orders/base/base_000000.parquet", ""},
		{"public.orders/deltas/epoch=000001.parquet", ""},
	}
	for _, c := range cases {
		if got := PartitionDirOf("public.orders", c.file); got != c.want {
			t.Fatalf("PartitionDirOf(%q) = %q, want %q", c.file, got, c.want)
		}
	}
}

func TestClassify(t *testing.T) {
	files := []string{
		"public.orders/base/base_000000.parquet",
		"public.orders/deltas/epoch=000001.parquet",
		"public.orders/order_date=2026-01-01/base/base_000002.parquet",
		"public.orders/order_date=2026-01-01/deltas/epoch=000003.parquet",
		"public.orders/README.md",
		"public.customers/base/base_000000.parquet",
	}
	bases, deltas := Classify("public.orders", files)
	wantBases := []string{
		"public.orders/base/base_000000.parquet",
		"public.orders/order_date=2026-01-01/base/base_000002.parquet",
	}
	wantDeltas := []string{
		"public.orders/deltas/epoch=000001.parquet",
		"public.orders/order_date=2026-01-01/deltas/epoch=000003.parquet",
	}
	if !reflect.DeepEqual(bases, wantBases) {
		t.Fatalf("bases = %v", bases)
	}
	if !reflect.DeepEqual(deltas, wantDeltas) {
		t.Fatalf("deltas = %v", deltas)
	}
}

func TestParseBaseEpoch(t *testing.T) {
	if n, ok := ParseBaseEpoch("public.orders/base/base_000012.parquet"); !ok || n != 12 {
		t.Fatalf("got (%d,%v)", n, ok)
	}
	if _, ok := ParseBaseEpoch("public.orders/base/notbase.parquet"); ok {
		t.Fatal("non-base name should fail")
	}
	if _, ok := ParseBaseEpoch("public.orders/base/base_abc.parquet"); ok {
		t.Fatal("non-numeric generation should fail")
	}
}

func TestSelectGeneration(t *testing.T) {
	bases := []string{
		"t/order_date=2026-01-01/base/base_000002.parquet",
		"t/order_date=2026-01-02/base/base_000002.parquet",
		"t/order_date=2026-01-01/base/base_000005.parquet",
	}
	files, gen := SelectGeneration(bases, 5)
	if gen != 5 || len(files) != 1 {
		t.Fatalf("want gen 5 with 1 file, got gen %d files %v", gen, files)
	}

	// Requested generation absent -> newest present wins.
	files, gen = SelectGeneration(bases, 99)
	if gen != 5 || len(files) != 1 {
		t.Fatalf("fallback: gen=%d files=%v", gen, files)
	}

	// No bases at all.
	if files, gen = SelectGeneration(nil, 3); files != nil || gen != 3 {
		t.Fatalf("empty: files=%v gen=%d", files, gen)
	}

	// Several files of the same chosen generation (multiple partitions).
	multi := []string{
		"t/d=2026-01-01/base/base_000004.parquet",
		"t/d=2026-01-02/base/base_000004.parquet",
	}
	files, gen = SelectGeneration(multi, 4)
	if gen != 4 || len(files) != 2 {
		t.Fatalf("multi: files=%v gen=%d", files, gen)
	}
}

func TestLiveFiles(t *testing.T) {
	files := []string{
		"t/base/base_000000.parquet",                       // superseded
		"t/base/base_000004.parquet",                       // live base
		"t/deltas/epoch=000004.parquet",                    // applied (== base epoch)
		"t/deltas/epoch=000005.parquet",                    // live delta
		"t/deltas/epoch=000006.parquet",                    // live delta
		"t/order_date=2026-01-01/base/base_000004.parquet", // live base
	}
	bases, deltas := LiveFiles("t", files, 4)
	wantBases := []string{
		"t/base/base_000004.parquet",
		"t/order_date=2026-01-01/base/base_000004.parquet",
	}
	wantDeltas := []string{
		"t/deltas/epoch=000005.parquet",
		"t/deltas/epoch=000006.parquet",
	}
	if !reflect.DeepEqual(bases, wantBases) {
		t.Fatalf("bases = %v", bases)
	}
	if !reflect.DeepEqual(deltas, wantDeltas) {
		t.Fatalf("deltas = %v", deltas)
	}
}
