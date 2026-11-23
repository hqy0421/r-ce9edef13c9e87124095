// Package partition implements Hive-style business-date partitioning for the
// CDC output layout.
//
// A partitioned table keeps its base and delta files inside directories named
// "<column>=<value>", e.g.:
//
//	public.orders/order_date=2026-01-01/base/base_000000.parquet
//	public.orders/order_date=2026-01-01/deltas/epoch=000001.parquet
//
// Rows whose business date is NULL (or cannot be interpreted as a date) land in
// the Hive default partition, "<column>=__HIVE_DEFAULT_PARTITION__". DuckDB,
// Spark, Athena and Hive all map that sentinel back to NULL when the directory
// is read as a Hive partition, so the layout stays queryable without any
// special-casing on the consumer side.
package partition

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// NullValue is the Hive/Spark sentinel used for rows with no business date.
// Readers translate this directory value back to SQL NULL.
const NullValue = "__HIVE_DEFAULT_PARTITION__"

// Spec describes the partitioning of a single table. An empty Column means the
// table is not partitioned (legacy single-delta-directory layout).
type Spec struct {
	Column string
}

// Enabled reports whether the table is partitioned by a business-date column.
func (s Spec) Enabled() bool { return s.Column != "" }

// ValueDir returns the partition directory for the given row value, e.g.
// "order_date=2026-01-01". It returns "" when the spec is disabled.
func (s Spec) ValueDir(v any) string {
	if !s.Enabled() {
		return ""
	}
	return Dir(s.Column, Value(v))
}

// Dir renders a "<column>=<value>" directory name.
func Dir(column, value string) string {
	return column + "=" + value
}

// Value normalises a business-date value to a "YYYY-MM-DD" string. Values that
// are NULL, blank, or not parseable as a date map to NullValue so that they
// land in the registered default partition rather than breaking the stream.
func Value(v any) string {
	date, ok := Normalize(v)
	if !ok {
		return NullValue
	}
	return date
}

// Normalize converts a source value into a "YYYY-MM-DD" business date.
//
// The decoder emits every column as a Go string (postgres text format), while
// the initial snapshot path yields time.Time for date/timestamp columns. Both
// shapes are accepted. Timestamps are truncated to their calendar date in the
// value's own location — a business date is not a UTC conversion.
func Normalize(v any) (string, bool) {
	switch val := v.(type) {
	case nil:
		return "", false
	case time.Time:
		if val.IsZero() {
			return "", false
		}
		return val.Format("2006-01-02"), true
	case *time.Time:
		if val == nil || val.IsZero() {
			return "", false
		}
		return val.Format("2006-01-02"), true
	case []byte:
		return Normalize(string(val))
	case string:
		return normalizeString(val)
	default:
		return normalizeString(fmt.Sprintf("%v", val))
	}
}

func normalizeString(raw string) (string, bool) {
	s := strings.TrimSpace(raw)
	if s == "" || s == NullValue {
		return "", false
	}
	// Plain date is by far the common case.
	if len(s) >= len("2006-01-02") {
		if t, err := time.Parse("2006-01-02", s[:len("2006-01-02")]); err == nil {
			return t.Format("2006-01-02"), true
		}
	}
	for _, layout := range dateLayouts {
		if t, err := time.Parse(layout, s); err == nil {
			return t.Format("2006-01-02"), true
		}
	}
	return "", false
}

// dateLayouts covers the timestamp shapes Postgres emits in text format
// (with and without fractional seconds, with and without a zone offset) plus
// RFC3339 variants produced by non-Postgres sources and tests.
var dateLayouts = []string{
	time.RFC3339Nano,
	time.RFC3339,
	"2006-01-02 15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05Z07:00",
	"2006-01-02 15:04:05.999999999-07",
	"2006-01-02 15:04:05-07",
	"2006-01-02 15:04:05.999999999",
	"2006-01-02 15:04:05",
	"2006-01-02T15:04:05.999999999",
	"2006-01-02T15:04:05",
	"2006-01-02 15:04",
	"2006-01-02T15:04",
}

// Entry is one landed partition as recorded in the manifest: the partition
// value ("2026-01-01" or NullValue), the storage-relative directory, and the
// number of live rows it holds.
type Entry struct {
	Value string `json:"value"`
	Path  string `json:"path"`
	Rows  int64  `json:"rows"`
}

// State is the value stored in manifest.Table.Partition for a partitioned
// table. It is empty for unpartitioned tables.
type State struct {
	Column     string  `json:"column"`
	Partitions []Entry `json:"partitions"`
}

// Spec returns the partition spec described by the state.
func (s State) Spec() Spec { return Spec{Column: s.Column} }

// Marshal renders the state as compact JSON for manifest.Table.Partition.
func (s State) Marshal() string {
	if s.Column == "" {
		return ""
	}
	b, err := json.Marshal(s)
	if err != nil {
		return ""
	}
	return string(b)
}

// ParseState reads manifest.Table.Partition. It tolerates the empty string
// (unpartitioned) and a bare column name (forward compatibility with producers
// that only recorded the column).
func ParseState(raw string) State {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return State{}
	}
	if !strings.HasPrefix(raw, "{") {
		return State{Column: raw}
	}
	var s State
	if err := json.Unmarshal([]byte(raw), &s); err != nil {
		return State{}
	}
	return s
}

// SortEntries orders entries by value with the default partition last, so the
// manifest is stable across runs.
func SortEntries(entries []Entry) {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].Value == NullValue {
			return false
		}
		if entries[j].Value == NullValue {
			return true
		}
		return entries[i].Value < entries[j].Value
	})
}
