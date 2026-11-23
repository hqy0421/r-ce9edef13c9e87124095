package glue

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"

	"github.com/burnside-project/burnside-go/manifest"

	"github.com/burnside-project/pg-cdc-core/internal/partition"
)

// tableAPI is the subset of the Glue client used during registration.
// Narrowing it makes register testable without standing up a real client.
type tableAPI interface {
	CreateTable(ctx context.Context, in *awsglue.CreateTableInput, opts ...func(*awsglue.Options)) (*awsglue.CreateTableOutput, error)
	UpdateTable(ctx context.Context, in *awsglue.UpdateTableInput, opts ...func(*awsglue.Options)) (*awsglue.UpdateTableOutput, error)
	BatchCreatePartition(ctx context.Context, in *awsglue.BatchCreatePartitionInput, opts ...func(*awsglue.Options)) (*awsglue.BatchCreatePartitionOutput, error)
}

// Register catalogues every active table in the manifest. CreateTable is
// attempted first; on AlreadyExistsException the table is updated instead.
// The operation is idempotent: running it twice with the same manifest is
// indistinguishable from running it once.
//
// locationFor maps a manifest key ("schema.table") to a fully-qualified
// storage URI (e.g., "s3://bucket/prefix/public.users.db/"). The caller
// owns this mapping because pg-cdc supports filesystem / GCS / S3 sinks.
func (a *Adapter) Register(ctx context.Context, m *manifest.Manifest, locationFor func(string) string) error {
	return register(ctx, a.client, a.database, m, locationFor)
}

func register(ctx context.Context, api tableAPI, database string, m *manifest.Manifest, locationFor func(string) string) error {
	if m == nil {
		return fmt.Errorf("glue: manifest is nil")
	}
	if locationFor == nil {
		return fmt.Errorf("glue: locationFor is nil")
	}

	names := make([]string, 0, len(m.Tables))
	for qualified := range m.Tables {
		names = append(names, qualified)
	}
	sort.Strings(names)

	for _, qualified := range names {
		mt := m.Tables[qualified]
		if mt.Status != "active" {
			continue
		}
		ms, ok := m.Schemas[qualified]
		if !ok {
			return fmt.Errorf("glue: no schema entry for %q", qualified)
		}

		location := locationFor(qualified)
		input, err := tableInput(qualified, mt, ms, location)
		if err != nil {
			return err
		}

		if err := upsert(ctx, api, database, input); err != nil {
			return fmt.Errorf("glue: register %s: %w", qualified, err)
		}

		state := partition.ParseState(mt.Partition)
		if err := registerPartitions(ctx, api, database, *input.Name, location, input.StorageDescriptor, state); err != nil {
			return fmt.Errorf("glue: register partitions for %s: %w", qualified, err)
		}
	}
	return nil
}

// registerPartitions declares each landed partition on the table. It is
// idempotent: Glue reports AlreadyExists per partition, which is ignored, so a
// re-run after a new date appears only creates the new ones.
func registerPartitions(ctx context.Context, api tableAPI, database, tableName, location string, sd *gluetypes.StorageDescriptor, state partition.State) error {
	inputs := partitionInputs(location, sd, state)
	if len(inputs) == 0 {
		return nil
	}
	out, err := api.BatchCreatePartition(ctx, &awsglue.BatchCreatePartitionInput{
		DatabaseName:       aws.String(database),
		TableName:          aws.String(tableName),
		PartitionInputList: inputs,
	})
	if err != nil {
		var alreadyExists *gluetypes.AlreadyExistsException
		if errors.As(err, &alreadyExists) || strings.Contains(err.Error(), "AlreadyExistsException") {
			return nil
		}
		return err
	}
	for _, pe := range out.Errors {
		if pe.ErrorDetail == nil {
			continue
		}
		if code := aws.ToString(pe.ErrorDetail.ErrorCode); strings.Contains(code, "AlreadyExists") {
			continue
		}
		return fmt.Errorf("partition %v: %s: %s", pe.PartitionValues, aws.ToString(pe.ErrorDetail.ErrorCode), aws.ToString(pe.ErrorDetail.ErrorMessage))
	}
	return nil
}

func upsert(ctx context.Context, api tableAPI, database string, input *gluetypes.TableInput) error {
	_, err := api.CreateTable(ctx, &awsglue.CreateTableInput{
		DatabaseName: aws.String(database),
		TableInput:   input,
	})
	if err == nil {
		return nil
	}

	var alreadyExists *gluetypes.AlreadyExistsException
	if errors.As(err, &alreadyExists) || strings.Contains(err.Error(), "AlreadyExistsException") {
		_, uerr := api.UpdateTable(ctx, &awsglue.UpdateTableInput{
			DatabaseName: aws.String(database),
			TableInput:   input,
		})
		return uerr
	}
	return err
}
