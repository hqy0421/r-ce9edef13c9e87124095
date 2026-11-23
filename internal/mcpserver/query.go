package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/burnside-project/pg-cdc-core/internal/layout"
)

// queryTimeout is the per-statement budget for DuckDB invocations. Generous
// for a single-user laptop tool; tighten if abuse becomes a concern.
const queryTimeout = 15 * time.Second

func (s *server) query(ctx context.Context, args json.RawMessage) map[string]any {
	var a struct {
		SQL   string `json:"sql"`
		Limit int    `json:"limit"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return errorContent(fmt.Errorf("invalid arguments: %w", err))
	}
	if a.Limit <= 0 || a.Limit > 1000 {
		a.Limit = 100
	}
	if err := validateSelect(a.SQL); err != nil {
		return errorContent(err)
	}
	if err := s.duckdbReady(); err != nil {
		return errorContent(err)
	}

	m, err := s.sink.ReadManifest(ctx)
	if err != nil {
		return errorContent(fmt.Errorf("read manifest: %w (run `pg-cdc init` first?)", err))
	}

	setup, err := s.buildViewDDL(ctx, m)
	if err != nil {
		return errorContent(err)
	}
	stmt := setup + strings.TrimRight(strings.TrimSpace(a.SQL), ";")

	rows, raw, err := runDuckDB(ctx, stmt)
	if err != nil {
		return errorContent(err)
	}
	return formatResult(rows, raw, a.Limit)
}

func (s *server) recentChanges(ctx context.Context, args json.RawMessage) map[string]any {
	var a struct {
		Name  string `json:"name"`
		Limit int    `json:"limit"`
	}
	if err := json.Unmarshal(args, &a); err != nil {
		return errorContent(fmt.Errorf("invalid arguments: %w", err))
	}
	if a.Name == "" {
		return errorContent(fmt.Errorf("name is required"))
	}
	if a.Limit <= 0 || a.Limit > 200 {
		a.Limit = 20
	}
	if err := s.duckdbReady(); err != nil {
		return errorContent(err)
	}

	m, err := s.sink.ReadManifest(ctx)
	if err != nil {
		return errorContent(fmt.Errorf("read manifest: %w", err))
	}
	t, ok := m.Tables[a.Name]
	if !ok {
		return errorContent(fmt.Errorf("table not found in manifest: %s", a.Name))
	}

	// Read the deltas that are still on disk (compaction deletes the ones it
	// has folded into a base). Classify handles both the flat <table>/deltas/
	// and the per-date <table>/<col>=<value>/deltas/ layouts.
	files, err := s.sink.ListFiles(ctx, t.Path+"/")
	if err != nil {
		return errorContent(fmt.Errorf("list files: %w", err))
	}
	_, deltas := layout.Classify(t.Path, files)
	if len(deltas) == 0 {
		return textContent(fmt.Sprintf("no streaming changes recorded yet for %s (deltas dir empty)", a.Name))
	}
	relation, err := s.deltaRelation(t.Path, deltas)
	if err != nil {
		return errorContent(err)
	}
	rows, raw, err := runRecentQuery(ctx, relation, a.Limit)
	if err != nil {
		return errorContent(err)
	}
	return formatResult(rows, raw, a.Limit)
}

// runRecentQuery selects from a delta relation newest-first. __seq exists only
// on deltas written after this column was introduced, so the query falls back
// to ordering by __epoch alone when it is absent.
func runRecentQuery(ctx context.Context, relation string, limit int) ([]map[string]any, string, error) {
	withSeq := fmt.Sprintf(
		"SELECT * FROM %s ORDER BY __epoch DESC NULLS LAST, __seq DESC NULLS LAST LIMIT %d",
		relation, limit)
	rows, raw, err := runDuckDB(ctx, withSeq)
	if err == nil {
		return rows, raw, nil
	}
	if !strings.Contains(err.Error(), "__seq") {
		return nil, "", err
	}
	byEpoch := fmt.Sprintf(
		"SELECT * FROM %s ORDER BY __epoch DESC NULLS LAST LIMIT %d",
		relation, limit)
	return runDuckDB(ctx, byEpoch)
}

// deltaRelation renders a relation over a set of delta files, splitting legacy
// and partitioned layouts because DuckDB refuses to mix them in one read.
func (s *server) deltaRelation(tablePath string, deltas []string) (string, error) {
	var legacy, partitioned []string
	for _, f := range deltas {
		if layout.PartitionDirOf(tablePath, f) == "" {
			legacy = append(legacy, f)
		} else {
			partitioned = append(partitioned, f)
		}
	}
	switch {
	case len(partitioned) == 0:
		return parquetRelation(s.cfg.Storage.Path, legacy, false), nil
	case len(legacy) == 0:
		return parquetRelation(s.cfg.Storage.Path, partitioned, true), nil
	default:
		return unionRelation(
			parquetRelation(s.cfg.Storage.Path, legacy, false),
			parquetRelation(s.cfg.Storage.Path, partitioned, true)), nil
	}
}

// duckdbReady gates the query tools on (a) filesystem sink and (b) duckdb on PATH.
// Both are open-core scaffold limitations; the commercial edition handles S3/GCS
// and may embed DuckDB.
func (s *server) duckdbReady() error {
	if s.cfg.Storage.StorageType() != "filesystem" {
		return fmt.Errorf("query tools currently support only filesystem storage in open core (configured: %s)", s.cfg.Storage.StorageType())
	}
	if _, err := exec.LookPath("duckdb"); err != nil {
		return errors.New("duckdb CLI not found on PATH — install with `brew install duckdb` (macOS) or see duckdb.org/docs/installation/ (other platforms) to enable the query tools")
	}
	return nil
}

// validateSelect rejects anything that isn't a read-only SELECT or WITH … SELECT.
// This is belt-and-suspenders — DuckDB is also reading Parquet files via globs,
// so there's no live database to mutate, but we still keep the surface tight.
func validateSelect(sql string) error {
	s := strings.TrimSpace(strings.ToLower(sql))
	if s == "" {
		return fmt.Errorf("sql is required")
	}
	s = strings.TrimRight(s, ";")
	if !strings.HasPrefix(s, "select") && !strings.HasPrefix(s, "with") {
		return fmt.Errorf("only SELECT (or WITH … SELECT) statements are allowed")
	}
	for _, kw := range []string{"insert ", "update ", "delete ", "drop ", "alter ", "create ", "attach ", "copy ", "pragma ", "load "} {
		if strings.Contains(s, kw) {
			return fmt.Errorf("query may not contain: %s", strings.TrimSpace(kw))
		}
	}
	return nil
}

func splitQualified(name string) (schema, table string) {
	parts := strings.SplitN(name, ".", 2)
	if len(parts) == 2 {
		return parts[0], parts[1]
	}
	return "main", parts[0]
}

func quoteIdent(s string) string {
	return `"` + strings.ReplaceAll(s, `"`, `""`) + `"`
}

func quoteSQL(s string) string {
	return "'" + strings.ReplaceAll(s, "'", "''") + "'"
}

// runDuckDB executes the given SQL via the DuckDB CLI in JSON mode. Returns
// the parsed row array on success, or the raw stdout if it isn't a JSON array
// (e.g. for statements that return no rowset).
func runDuckDB(ctx context.Context, sql string) (rows []map[string]any, raw string, err error) {
	qctx, cancel := context.WithTimeout(ctx, queryTimeout)
	defer cancel()

	// No -readonly: it requires an explicit DB file, and we use in-memory.
	// Read-only semantics are enforced by validateSelect upstream.
	cmd := exec.CommandContext(qctx, "duckdb", "-json", "-c", sql)
	stdout, runErr := cmd.Output()
	if runErr != nil {
		var ee *exec.ExitError
		if errors.As(runErr, &ee) {
			return nil, "", fmt.Errorf("duckdb: %s", strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, "", fmt.Errorf("run duckdb: %w", runErr)
	}
	out := strings.TrimSpace(string(stdout))
	if out == "" {
		return nil, "", nil
	}
	if err := json.Unmarshal([]byte(out), &rows); err != nil {
		// Not a JSON array — return as text (rare with -json, but tolerate).
		return nil, out, nil
	}
	return rows, "", nil
}

func formatResult(rows []map[string]any, raw string, limit int) map[string]any {
	if raw != "" {
		return textContent(raw)
	}
	if len(rows) > limit {
		rows = rows[:limit]
	}
	if len(rows) == 0 {
		return textContent("0 rows")
	}
	body, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		return errorContent(fmt.Errorf("marshal result: %w", err))
	}
	return textContent(fmt.Sprintf("%d row(s):\n\n%s", len(rows), body))
}
