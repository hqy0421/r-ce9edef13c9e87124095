package internal

import (
	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/config"
	"github.com/burnside-project/pg-cdc-core/internal/layout"
	"github.com/burnside-project/pg-cdc-core/internal/partition"
)

// specForColumn returns a partition spec for column if the schema actually
// carries it. A typo, an empty name, or a CDC metadata column therefore
// degrades to the legacy unpartitioned layout instead of writing rows under a
// bogus partition.
func specForColumn(col string, schema manifest.Schema) partition.Spec {
	if col == "" {
		return partition.Spec{}
	}
	switch col {
	case "__op", "__epoch", "__deleted_at", "__seq":
		return partition.Spec{}
	}
	for _, c := range schema.Columns {
		if c.Name == col {
			return partition.Spec{Column: col}
		}
	}
	return partition.Spec{}
}

// partitionSpecFor resolves the business-date partition column for a table from
// configuration and the table's schema.
func partitionSpecFor(cfg config.Config, tableName string, schema manifest.Schema) partition.Spec {
	return specForColumn(cfg.Partitions.ColumnFor(tableName), schema)
}

// mutatePartitionState updates the recorded partition list for a table.
//
// observed maps a partition directory ("col=value") to the number of live rows
// it now holds. The entry list is rebuilt from observed, so partitions that no
// longer carry rows drop out; existing entries for directories that still have
// rows keep their Path. The column comes from column when non-empty, else from
// whatever was already recorded.
func mutatePartitionState(m *manifest.Manifest, tableName, column string, observed map[string]int64) {
	t := m.Tables[tableName]
	recordPartitionsInto(&t, column, observed)
	m.Tables[tableName] = t
}

// recordPartitions sets a table's Partition state from observed row counts.
func recordPartitions(tables map[string]manifest.Table, tableName, column string, observed map[string]int64) {
	t := tables[tableName]
	recordPartitionsInto(&t, column, observed)
	tables[tableName] = t
}

func recordPartitionsInto(t *manifest.Table, column string, observed map[string]int64) {
	prev := partition.ParseState(t.Partition)
	if column == "" {
		column = prev.Column
	}
	if column == "" {
		return
	}

	knownPath := make(map[string]string, len(prev.Partitions))
	for _, e := range prev.Partitions {
		knownPath[e.Value] = e.Path
	}

	entries := make([]partition.Entry, 0, len(observed))
	for dir, rows := range observed {
		value := partitionValueOf(column, dir)
		p := knownPath[value]
		if p == "" {
			p = t.Path + "/" + dir
		}
		entries = append(entries, partition.Entry{Value: value, Path: p, Rows: rows})
	}
	partition.SortEntries(entries)
	t.Partition = partition.State{Column: column, Partitions: entries}.Marshal()
}

// omitSet returns the set of columns to leave out of Parquet files for a
// partitioned table: the partition column is encoded in the directory instead.
func omitSet(spec partition.Spec) map[string]bool {
	if !spec.Enabled() {
		return nil
	}
	return map[string]bool{spec.Column: true}
}

// partitionValueOf extracts the "value" half of a "col=value" directory.
func partitionValueOf(column, dir string) string {
	prefix := column + "="
	if len(dir) > len(prefix) && dir[:len(prefix)] == prefix {
		return dir[len(prefix):]
	}
	return dir
}

// existingPartitionDirs returns the partition directories currently recorded
// for a table in the manifest, mapped to their live row counts.
func existingPartitionDirs(m *manifest.Manifest, tableName string) map[string]int64 {
	observed := make(map[string]int64)
	state := partition.ParseState(m.Tables[tableName].Partition)
	for _, e := range state.Partitions {
		observed[layout.DirOf(state.Column, e.Value)] = e.Rows
	}
	return observed
}

// copyRow returns a shallow copy of row without the listed keys.
func copyRow(row map[string]any, drop ...string) map[string]any {
	out := make(map[string]any, len(row))
	for k, v := range row {
		out[k] = v
	}
	for _, k := range drop {
		delete(out, k)
	}
	return out
}
