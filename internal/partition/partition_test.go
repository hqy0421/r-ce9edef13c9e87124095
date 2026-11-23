package partition

import (
	"testing"
	"time"
)

func TestNormalize(t *testing.T) {
	tokyo := time.FixedZone("JST", 9*3600)
	cases := []struct {
		name string
		in   any
		want string
		ok   bool
	}{
		{"nil", nil, "", false},
		{"empty string", "", "", false},
		{"blank string", "   ", "", false},
		{"sentinel", NullValue, "", false},
		{"plain date", "2026-01-02", "2026-01-02", true},
		{"plain date padded", " 2026-01-02 ", "2026-01-02", true},
		{"postgres date text", "2026-01-02 00:00:00", "2026-01-02", true},
		{"timestamp", "2026-01-02 15:04:05", "2026-01-02", true},
		{"timestamp fraction", "2026-01-02 15:04:05.123456", "2026-01-02", true},
		{"timestamp zone", "2026-01-02 15:04:05+00", "2026-01-02", true},
		{"rfc3339", "2026-01-02T23:59:59Z", "2026-01-02", true},
		{"time only", "15:04:05", "", false},
		{"garbage", "not-a-date", "", false},
		{"bytes", []byte("2026-03-04"), "2026-03-04", true},
		{
			"time.Time keeps own calendar date",
			time.Date(2026, 1, 2, 23, 30, 0, 0, tokyo),
			"2026-01-02", true,
		},
		{"zero time", time.Time{}, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, ok := Normalize(c.in)
			if ok != c.ok || got != c.want {
				t.Fatalf("Normalize(%#v) = (%q,%v), want (%q,%v)", c.in, got, ok, c.want, c.ok)
			}
		})
	}
}

func TestNormalizePointer(t *testing.T) {
	d := time.Date(2026, 5, 6, 0, 0, 0, 0, time.UTC)
	got, ok := Normalize(&d)
	if !ok || got != "2026-05-06" {
		t.Fatalf("pointer time: got (%q,%v)", got, ok)
	}
	var nilPtr *time.Time
	if _, ok := Normalize(nilPtr); ok {
		t.Fatal("nil pointer should not normalize")
	}
}

func TestValueFallsBackToSentinel(t *testing.T) {
	if got := Value(nil); got != NullValue {
		t.Fatalf("Value(nil) = %q, want sentinel", got)
	}
	if got := Value("nope"); got != NullValue {
		t.Fatalf("Value(garbage) = %q, want sentinel", got)
	}
	if got := Value("2026-07-08"); got != "2026-07-08" {
		t.Fatalf("Value(date) = %q", got)
	}
}

func TestSpecValueDir(t *testing.T) {
	if got := (Spec{}).ValueDir("2026-01-01"); got != "" {
		t.Fatalf("disabled spec should yield empty dir, got %q", got)
	}
	spec := Spec{Column: "order_date"}
	if got := spec.ValueDir("2026-01-01"); got != "order_date=2026-01-01" {
		t.Fatalf("ValueDir = %q", got)
	}
	if got := spec.ValueDir(nil); got != "order_date="+NullValue {
		t.Fatalf("ValueDir(nil) = %q", got)
	}
	if !spec.Enabled() || (Spec{}).Enabled() {
		t.Fatal("Enabled mismatch")
	}
}

func TestStateRoundTrip(t *testing.T) {
	st := State{Column: "order_date", Partitions: []Entry{
		{Value: NullValue, Path: "p/order_date=__HIVE_DEFAULT_PARTITION__", Rows: 1},
		{Value: "2026-01-02", Path: "p/order_date=2026-01-02", Rows: 5},
	}}
	raw := st.Marshal()
	if raw == "" {
		t.Fatal("Marshal returned empty for a non-empty state")
	}
	got := ParseState(raw)
	if got.Column != "order_date" || len(got.Partitions) != 2 {
		t.Fatalf("round trip lost data: %+v", got)
	}
	if got.Spec().Column != "order_date" {
		t.Fatal("Spec lost column")
	}
}

func TestParseStateTolerant(t *testing.T) {
	if s := ParseState(""); s.Column != "" {
		t.Fatalf("empty: %+v", s)
	}
	if s := ParseState("event_date"); s.Column != "event_date" {
		t.Fatalf("bare column: %+v", s)
	}
	if s := ParseState("{not json"); s.Column != "" {
		t.Fatalf("bad json should degrade to empty, got %+v", s)
	}
}

func TestMarshalEmpty(t *testing.T) {
	if raw := (State{}).Marshal(); raw != "" {
		t.Fatalf("empty state should marshal to empty string, got %q", raw)
	}
}

func TestSortEntries(t *testing.T) {
	entries := []Entry{
		{Value: NullValue},
		{Value: "2026-01-02"},
		{Value: "2025-12-31"},
	}
	SortEntries(entries)
	want := []string{"2025-12-31", "2026-01-02", NullValue}
	for i, w := range want {
		if entries[i].Value != w {
			t.Fatalf("entry[%d] = %q, want %q", i, entries[i].Value, w)
		}
	}
}
