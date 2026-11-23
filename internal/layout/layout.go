// Package layout centralises the on-disk shape of the CDC output so that the
// streaming writer, compaction and the query layer agree on where files live.
//
// Two shapes are supported for every table:
//
//	legacy (unpartitioned)    <table>/base/base_NNNNNN.parquet
//	                          <table>/deltas/epoch=NNNNNN.parquet
//
//	partitioned               <table>/<col>=<YYYY-MM-DD>/base/base_NNNNNN.parquet
//	                          <table>/<col>=<YYYY-MM-DD>/deltas/epoch=NNNNNN.parquet
//
// with NULL business dates landing under <col>=__HIVE_DEFAULT_PARTITION__.
package layout

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/burnside-project/burnside-go/epoch"

	"github.com/burnside-project/pg-cdc-core/internal/partition"
)

// Row is the generic row shape shared with the parquet writer (writer.Row).
type Row = map[string]any

// BasePath returns the storage-relative path of a base file. dir is the
// partition directory ("order_date=2026-01-01"), or "" for an unpartitioned
// table.
func BasePath(tablePath, dir string, epochNum int64) string {
	return filepath.ToSlash(filepath.Join(tablePath, dir, "base", BaseFilename(epochNum)))
}

// DeltaPath returns the storage-relative path of a delta file.
func DeltaPath(tablePath, dir, filename string) string {
	return filepath.ToSlash(filepath.Join(tablePath, dir, "deltas", filename))
}

// BaseFilename renders the standard base filename for a generation.
func BaseFilename(epochNum int64) string {
	return fmt.Sprintf("base_%06d.parquet", epochNum)
}

// DirOf renders the partition directory ("col=value") for a value.
func DirOf(column, value string) string {
	return column + "=" + value
}

// GroupByPartition buckets rows by their partition directory. For an
// unpartitioned spec every row lands in the "" bucket — including when rows is
// empty, so a snapshot of an empty legacy table still writes one (empty) base
// file and downstream readers see a stable layout.
func GroupByPartition(spec partition.Spec, rows []Row) map[string][]Row {
	groups := make(map[string][]Row, 1)
	for _, row := range rows {
		dir := spec.ValueDir(row[spec.Column])
		groups[dir] = append(groups[dir], row)
	}
	if !spec.Enabled() && len(groups) == 0 {
		groups[""] = nil
	}
	return groups
}

// SortedDirs returns the bucket keys in lexical order (deterministic output).
func SortedDirs(groups map[string][]Row) []string {
	dirs := make([]string, 0, len(groups))
	for dir := range groups {
		dirs = append(dirs, dir)
	}
	sort.Strings(dirs)
	return dirs
}

// PartitionDirOf returns the partition directory ("col=value") that a file
// lives in, relative to the table root. It returns "" for files in the legacy
// layout (<table>/base/…, <table>/deltas/…).
func PartitionDirOf(tablePath, filePath string) string {
	prefix := filepath.ToSlash(tablePath) + "/"
	f := filepath.ToSlash(filePath)
	if !strings.HasPrefix(f, prefix) {
		return ""
	}
	segments := strings.Split(strings.TrimPrefix(f, prefix), "/")
	// Partitioned paths have at least "<dir>/<kind>/<file>"; the first segment
	// is a "key=value" directory.
	if len(segments) >= 3 && strings.Contains(segments[0], "=") {
		return segments[0]
	}
	return ""
}

// Classify splits a recursive listing under a table's root into base and delta
// files, for both layouts. Files are returned sorted.
func Classify(tablePath string, files []string) (bases, deltas []string) {
	prefix := filepath.ToSlash(tablePath) + "/"
	for _, f := range files {
		f = filepath.ToSlash(f)
		if !strings.HasPrefix(f, prefix) || !strings.HasSuffix(f, ".parquet") {
			continue
		}
		segments := strings.Split(strings.TrimPrefix(f, prefix), "/")
		if len(segments) < 2 {
			continue
		}
		switch segments[len(segments)-2] {
		case "base":
			bases = append(bases, f)
		case "deltas":
			deltas = append(deltas, f)
		}
	}
	sort.Strings(bases)
	sort.Strings(deltas)
	return bases, deltas
}

// ParseBaseEpoch extracts the generation number from a base filename.
func ParseBaseEpoch(path string) (int64, bool) {
	name := filepath.Base(filepath.ToSlash(path))
	if !strings.HasPrefix(name, "base_") || !strings.HasSuffix(name, ".parquet") {
		return 0, false
	}
	num := strings.TrimSuffix(strings.TrimPrefix(name, "base_"), ".parquet")
	n, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}

// SelectGeneration picks the base files that make up the live base. Files of
// generation wantEpoch win across every partition; if that generation is absent
// (a legacy deployment, or an interrupted compaction) the newest generation
// present is used instead. The returned epoch is the generation actually
// selected.
func SelectGeneration(bases []string, wantEpoch int64) (files []string, epochNum int64) {
	byEpoch := make(map[int64][]string)
	for _, f := range bases {
		n, ok := ParseBaseEpoch(f)
		if !ok {
			continue
		}
		byEpoch[n] = append(byEpoch[n], f)
	}
	if len(byEpoch) == 0 {
		return nil, wantEpoch
	}
	chosen := wantEpoch
	if _, ok := byEpoch[chosen]; !ok {
		chosen = 0
		first := true
		for n := range byEpoch {
			if first || n > chosen {
				chosen = n
				first = false
			}
		}
	}
	files = byEpoch[chosen]
	sort.Strings(files)
	return files, chosen
}

// LiveFiles returns the files that make up a table's current state: base files
// of generation baseEpoch, plus every delta file with an epoch greater than
// baseEpoch. Superseded base generations and already-applied deltas are
// excluded so a reader never double-counts.
func LiveFiles(tablePath string, files []string, baseEpoch int64) (baseFiles, deltaFiles []string) {
	bases, deltas := Classify(tablePath, files)
	baseFiles, _ = SelectGeneration(bases, baseEpoch)
	for _, d := range deltas {
		n, err := epoch.ParseFromFilename(d)
		if err != nil || n <= baseEpoch {
			continue
		}
		deltaFiles = append(deltaFiles, d)
	}
	sort.Strings(deltaFiles)
	return baseFiles, deltaFiles
}
