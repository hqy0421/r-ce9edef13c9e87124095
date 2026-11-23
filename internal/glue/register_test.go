package glue

import (
	"context"
	"errors"
	"testing"

	awsglue "github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/burnside-project/burnside-go/manifest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeGlue struct {
	created    []string
	updated    []string
	partitions map[string][]string // table name -> partition values
	batchErr   error
	createErr  error
}

func (f *fakeGlue) CreateTable(_ context.Context, in *awsglue.CreateTableInput, _ ...func(*awsglue.Options)) (*awsglue.CreateTableOutput, error) {
	if f.createErr != nil {
		return nil, f.createErr
	}
	f.created = append(f.created, *in.TableInput.Name)
	return &awsglue.CreateTableOutput{}, nil
}

func (f *fakeGlue) UpdateTable(_ context.Context, in *awsglue.UpdateTableInput, _ ...func(*awsglue.Options)) (*awsglue.UpdateTableOutput, error) {
	f.updated = append(f.updated, *in.TableInput.Name)
	return &awsglue.UpdateTableOutput{}, nil
}

func (f *fakeGlue) BatchCreatePartition(_ context.Context, in *awsglue.BatchCreatePartitionInput, _ ...func(*awsglue.Options)) (*awsglue.BatchCreatePartitionOutput, error) {
	if f.batchErr != nil {
		return nil, f.batchErr
	}
	if f.partitions == nil {
		f.partitions = make(map[string][]string)
	}
	name := *in.TableName
	for _, p := range in.PartitionInputList {
		f.partitions[name] = append(f.partitions[name], p.Values...)
	}
	return &awsglue.BatchCreatePartitionOutput{}, nil
}

func sampleManifest() *manifest.Manifest {
	return &manifest.Manifest{
		Tables: map[string]manifest.Table{
			"public.users":    {Status: "active", Path: "public.users.db/"},
			"public.sessions": {Status: "excluded"},
		},
		Schemas: map[string]manifest.Schema{
			"public.users": {
				Version: 1,
				Columns: []manifest.Column{
					{Name: "id", ParquetType: "INT64"},
				},
			},
		},
	}
}

func TestRegister_CreateNew(t *testing.T) {
	api := &fakeGlue{}
	loc := func(q string) string { return "s3://bucket/" + q + "/" }

	err := register(context.Background(), api, "db", sampleManifest(), loc)
	require.NoError(t, err)

	assert.Equal(t, []string{"users"}, api.created, "only active tables registered")
	assert.Empty(t, api.updated)
}

func TestRegister_FallsBackToUpdate(t *testing.T) {
	api := &fakeGlue{createErr: &gluetypes.AlreadyExistsException{}}
	loc := func(q string) string { return "s3://bucket/" + q + "/" }

	err := register(context.Background(), api, "db", sampleManifest(), loc)
	require.NoError(t, err)

	assert.Empty(t, api.created)
	assert.Equal(t, []string{"users"}, api.updated, "AlreadyExists triggers UpdateTable")
}

func TestRegister_PropagatesUnexpectedError(t *testing.T) {
	api := &fakeGlue{createErr: errors.New("AccessDeniedException: no perms")}
	loc := func(q string) string { return "s3://bucket/" + q + "/" }

	err := register(context.Background(), api, "db", sampleManifest(), loc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "AccessDenied")
}

func TestRegister_NilGuards(t *testing.T) {
	api := &fakeGlue{}
	require.Error(t, register(context.Background(), api, "db", nil, func(string) string { return "" }))
	require.Error(t, register(context.Background(), api, "db", sampleManifest(), nil))
}

func TestRegister_MissingSchema(t *testing.T) {
	api := &fakeGlue{}
	m := &manifest.Manifest{
		Tables:  map[string]manifest.Table{"public.users": {Status: "active"}},
		Schemas: map[string]manifest.Schema{},
	}
	err := register(context.Background(), api, "db", m, func(string) string { return "s3://x/" })
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no schema entry")
}

func TestRegister_PartitionedTable(t *testing.T) {
	api := &fakeGlue{}
	m := &manifest.Manifest{
		Tables: map[string]manifest.Table{
			"public.orders": {
				Status:    "active",
				Path:      "public.orders",
				Partition: `{"column":"order_date","partitions":[{"value":"2026-01-01","path":"public.orders/order_date=2026-01-01","rows":4},{"value":"__HIVE_DEFAULT_PARTITION__","path":"public.orders/order_date=__HIVE_DEFAULT_PARTITION__","rows":1}]}`,
			},
		},
		Schemas: map[string]manifest.Schema{
			"public.orders": {
				Version: 1,
				Columns: []manifest.Column{
					{Name: "id", ParquetType: "INT64"},
					{Name: "order_date", ParquetType: "DATE"},
				},
			},
		},
	}
	loc := func(q string) string { return "s3://bucket/prefix/" + q + "/" }

	require.NoError(t, register(context.Background(), api, "db", m, loc))
	assert.Equal(t, []string{"orders"}, api.created)
	assert.Equal(t, []string{"2026-01-01", "__HIVE_DEFAULT_PARTITION__"}, api.partitions["orders"])
}

func TestRegister_PartitionErrorsTolerated(t *testing.T) {
	api := &fakeGlue{batchErr: &gluetypes.AlreadyExistsException{}}
	m := &manifest.Manifest{
		Tables: map[string]manifest.Table{
			"public.orders": {
				Status:    "active",
				Path:      "public.orders",
				Partition: `{"column":"order_date","partitions":[{"value":"2026-01-01","path":"p","rows":1}]}`,
			},
		},
		Schemas: map[string]manifest.Schema{
			"public.orders": {Version: 1, Columns: []manifest.Column{{Name: "id", ParquetType: "INT64"}, {Name: "order_date", ParquetType: "DATE"}}},
		},
	}
	err := register(context.Background(), api, "db", m, func(string) string { return "s3://x/" })
	require.NoError(t, err, "AlreadyExists on partitions is idempotent success")
}
