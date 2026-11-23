package glue

import (
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/partition"
)

// tableInput converts a manifest table + schema into a Glue TableInput.
//
// The manifest key is "schema.table" (e.g., "public.users"). Glue tables
// have only (database, name), so we flatten to "schema_table" to avoid
// collisions across Postgres schemas published into the same Glue database.
//
// A business-date partitioned table declares that column as a PartitionKey.
// The column is omitted from the Parquet files and reconstructed from the
// "<col>=<value>" directory, so it must not appear again in the data-column
// list — Glue rejects a table whose partition key duplicates a column.
func tableInput(qualified string, mt manifest.Table, ms manifest.Schema, location string) (*gluetypes.TableInput, error) {
	pgSchema, pgTable, ok := strings.Cut(qualified, ".")
	if !ok {
		return nil, fmt.Errorf("glue: manifest key %q is not schema.table", qualified)
	}

	state := partition.ParseState(mt.Partition)
	partCol := state.Column
	partType := "date"

	cols := make([]gluetypes.Column, 0, len(ms.Columns))
	for _, c := range ms.Columns {
		if c.Name == partCol {
			partType = hiveType(c.ParquetType, c.Precision, c.Scale)
			continue
		}
		cols = append(cols, gluetypes.Column{
			Name: aws.String(c.Name),
			Type: aws.String(hiveType(c.ParquetType, c.Precision, c.Scale)),
		})
	}

	in := &gluetypes.TableInput{
		Name:      aws.String(glueTableName(pgSchema, pgTable)),
		TableType: aws.String("EXTERNAL_TABLE"),
		Parameters: map[string]string{
			"classification":        "parquet",
			"pg_cdc.pg_schema":      pgSchema,
			"pg_cdc.pg_table":       pgTable,
			"pg_cdc.schema_version": fmt.Sprintf("%d", ms.Version),
		},
		StorageDescriptor: &gluetypes.StorageDescriptor{
			Columns:      cols,
			Location:     aws.String(location),
			InputFormat:  aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"),
			OutputFormat: aws.String("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"),
			SerdeInfo: &gluetypes.SerDeInfo{
				SerializationLibrary: aws.String("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
				Parameters: map[string]string{
					"serialization.format": "1",
				},
			},
		},
	}
	if partCol != "" {
		in.PartitionKeys = []gluetypes.Column{{
			Name: aws.String(partCol),
			Type: aws.String(partType),
		}}
		in.Parameters["pg_cdc.partition_column"] = partCol
	}
	return in, nil
}

// partitionInputs builds Glue partition registrations for every landed
// partition recorded in the manifest, rooted at tableLocation. Each partition
// points at its own directory so Athena/Hive/Spark prune correctly; rows with a
// NULL business date are registered under __HIVE_DEFAULT_PARTITION__.
func partitionInputs(tableLocation string, sd *gluetypes.StorageDescriptor, state partition.State) []gluetypes.PartitionInput {
	if state.Column == "" || sd == nil {
		return nil
	}
	root := strings.TrimSuffix(tableLocation, "/") + "/"
	out := make([]gluetypes.PartitionInput, 0, len(state.Partitions))
	for _, e := range state.Partitions {
		psd := *sd
		if sd.SerdeInfo != nil {
			serde := *sd.SerdeInfo
			psd.SerdeInfo = &serde
		}
		psd.Location = aws.String(root + partition.Dir(state.Column, e.Value) + "/")
		out = append(out, gluetypes.PartitionInput{
			Values:            []string{e.Value},
			StorageDescriptor: &psd,
		})
	}
	return out
}

func glueTableName(pgSchema, pgTable string) string {
	if pgSchema == "" || pgSchema == "public" {
		return pgTable
	}
	return pgSchema + "_" + pgTable
}

// hiveType converts a Parquet logical type (as produced by burnside-go's
// types.ParquetType) into the Hive type string Glue stores in column
// definitions.
func hiveType(parquetType string, precision, scale int) string {
	switch parquetType {
	case "INT32":
		return "int"
	case "INT64":
		return "bigint"
	case "BOOLEAN":
		return "boolean"
	case "FLOAT":
		return "float"
	case "DOUBLE":
		return "double"
	case "DECIMAL":
		if precision <= 0 {
			precision = 38
		}
		if scale < 0 {
			scale = 0
		}
		return fmt.Sprintf("decimal(%d,%d)", precision, scale)
	case "UTF8", "JSON", "UUID":
		return "string"
	case "TIMESTAMP_MICROS":
		return "timestamp"
	case "DATE":
		return "date"
	case "TIME_MICROS":
		// Hive has no native TIME; surface as string to preserve precision.
		return "string"
	case "BYTE_ARRAY":
		return "binary"
	default:
		return "string"
	}
}
