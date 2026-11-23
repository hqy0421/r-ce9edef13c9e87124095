// Package config loads pg-cdc.yml configuration.
package config

// Config is the top-level pg-cdc configuration.
type Config struct {
	Source      SourceConfig      `yaml:"source"`
	Storage     StorageConfig     `yaml:"storage"`
	Replication ReplicationConfig `yaml:"replication"`
	Flush       FlushConfig       `yaml:"flush"`
	Tables      TablesConfig      `yaml:"tables"`
	Profiles    ProfilesConfig    `yaml:"profiles"`
	State       StateConfig       `yaml:"state"`
	Catalog     CatalogConfig     `yaml:"catalog"`
	Partitions  PartitionsConfig  `yaml:"partitions"`
}

// SourceConfig holds the database source settings.
type SourceConfig struct {
	Postgres PostgresConfig `yaml:"postgres"`
}

// PostgresConfig holds the Postgres URL and schema list.
// Authentication is handled via the connection URL:
//   - Password: postgresql://user:pass@host/db
//   - mTLS: postgresql://user@host/db?sslmode=verify-full&sslcert=client.crt&sslkey=client.key&sslrootcert=ca.crt
//   - AWS IAM: postgresql://user:${RDS_TOKEN}@host/db?sslmode=verify-full&sslrootcert=rds-ca.pem
//   - GCP IAM: postgresql://sa@project.iam@localhost/db?sslmode=disable (via Cloud SQL Auth Proxy)
type PostgresConfig struct {
	URL            string   `yaml:"url"`
	Schemas        []string `yaml:"schemas"`
	MaxConns       int      `yaml:"max_conns"`
	ConnectTimeout string   `yaml:"connect_timeout"`
}

// StorageConfig holds output storage settings.
// Type selects the sink adapter: "filesystem" (default), "gcs", or "s3".
type StorageConfig struct {
	Type            string `yaml:"type"`              // "filesystem" (default), "gcs", "s3"
	Path            string `yaml:"path"`              // filesystem: output directory
	Bucket          string `yaml:"bucket"`            // gcs/s3: bucket name
	Prefix          string `yaml:"prefix"`            // gcs/s3: object key prefix
	Region          string `yaml:"region"`            // s3: AWS region
	Endpoint        string `yaml:"endpoint"`          // s3: custom endpoint (MinIO, R2)
	CredentialsFile string `yaml:"credentials_file"`  // gcs: service account JSON path
	PartitionByTag  bool   `yaml:"partition_by_tag"`  // group tables by tag in storage
}

// ReplicationConfig holds replication slot/publication settings.
type ReplicationConfig struct {
	Publication string `yaml:"publication"`
	Slot        string `yaml:"slot"`
}

// FlushConfig holds micro-batch flush settings.
type FlushConfig struct {
	IntervalSec int `yaml:"interval_sec"`
	MaxRows     int `yaml:"max_rows"`
}

// TablesConfig holds table filtering and tagging settings.
type TablesConfig struct {
	Exclude []string            `yaml:"exclude"`
	Tags    map[string][]string `yaml:"tags"`   // tag name -> list of table patterns (globs)
	Policy  map[string]string   `yaml:"policy"` // tag name -> "include" or "exclude"
}

// ProfilesConfig holds ACL-driven profile settings.
type ProfilesConfig struct {
	Source       string            `yaml:"source"`        // "pg_acl" to derive from Postgres
	Roles        []string          `yaml:"roles"`         // which Postgres roles to publish
	Aliases      map[string]string `yaml:"aliases"`       // role -> friendly name
	SyncInterval string            `yaml:"sync_interval"` // how often to re-read ACLs
}

// StateConfig holds state store settings.
type StateConfig struct {
	Type string `yaml:"type"` // "sqlite" (default)
	Path string `yaml:"path"` // SQLite file path (default: .pgcdc/state.db)
}

// CatalogConfig holds optional metadata-catalog settings. When Type is
// "glue", pg-cdc registers active tables in AWS Glue after init writes the
// manifest. Leave Type empty to skip catalog registration.
type CatalogConfig struct {
	Type     string `yaml:"type"`     // "" (disabled) or "glue"
	Database string `yaml:"database"` // glue: database name
	Region   string `yaml:"region"`   // glue: AWS region (defaults to Storage.Region)
	Endpoint string `yaml:"endpoint"` // glue: custom endpoint (LocalStack / testing)
}

// PartitionsConfig selects a Hive business-date partition column per table.
// When DateColumn is empty and no per-table override applies, tables keep the
// legacy unpartitioned layout.
type PartitionsConfig struct {
	DateColumn string            `yaml:"date_column"` // default business-date column
	Tables     map[string]string `yaml:"tables"`      // per-table override; "" disables
}

// ColumnFor returns the configured business-date column for a table. A
// per-table entry wins (an explicit "" disables partitioning for that table);
// otherwise the default DateColumn applies.
func (p PartitionsConfig) ColumnFor(table string) string {
	if p.Tables != nil {
		if col, ok := p.Tables[table]; ok {
			return col
		}
	}
	return p.DateColumn
}

// StorageType returns the resolved storage type, defaulting to "filesystem".
func (c *StorageConfig) StorageType() string {
	if c.Type == "" {
		return "filesystem"
	}
	return c.Type
}

// StatePath returns the resolved state path, defaulting to ".pgcdc/state.db".
func (c *StateConfig) StatePath() string {
	if c.Path == "" {
		return ".pgcdc/state.db"
	}
	return c.Path
}
