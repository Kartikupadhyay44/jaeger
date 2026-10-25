// Copyright (c) 2025 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package clickhouse

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"text/template"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"go.opentelemetry.io/collector/featuregate"

	"github.com/jaegertracing/jaeger/internal/storage/v1"
	"github.com/jaegertracing/jaeger/internal/storage/v1/api/metricstore"
	"github.com/jaegertracing/jaeger/internal/storage/v1/api/metricstore/metricstoremetrics"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/depstore"
	"github.com/jaegertracing/jaeger/internal/storage/v2/api/tracestore"
	chdepstore "github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/depstore"
	chmetricstore "github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/metricstore"
	"github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/sql"
	chtracestore "github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/tracestore"
	"github.com/jaegertracing/jaeger/internal/telemetry"
)

// ClickHouse is always available as a storage backend and is selected through
// configuration; no feature gate is required to enable it. The gate IDs below
// remain registered as Stable only so that existing --feature-gates values keep
// working (as no-ops) until they are removed in v2.23.0.
var (
	_ = featuregate.GlobalRegistry().MustRegister(
		"jaeger.clickhouse",
		featuregate.StageStable,
		featuregate.WithRegisterFromVersion("v2.21.0"),
		featuregate.WithRegisterToVersion("v2.23.0"),
		featuregate.WithRegisterDescription("No-op: ClickHouse is always available as a storage backend and is selected through configuration. Retained for backward compatibility and removed in v2.23.0."),
		featuregate.WithRegisterReferenceURL("https://github.com/jaegertracing/jaeger/issues/9016"),
	)
	_ = featuregate.GlobalRegistry().MustRegister(
		"storage.clickhouse",
		featuregate.StageStable,
		featuregate.WithRegisterFromVersion("v2.18.0"),
		featuregate.WithRegisterToVersion("v2.23.0"),
		featuregate.WithRegisterDescription("No-op: deprecated alias for jaeger.clickhouse, retained for backward compatibility and removed in v2.23.0."),
		featuregate.WithRegisterReferenceURL("https://github.com/jaegertracing/jaeger/issues/9016"),
	)
)

var (
	_ io.Closer                  = (*Factory)(nil)
	_ depstore.Factory           = (*Factory)(nil)
	_ tracestore.Factory         = (*Factory)(nil)
	_ storage.MetricStoreFactory = (*Factory)(nil)
	_ storage.Purger             = (*Factory)(nil)
)

// schemaTemplateParams is what the CREATE TABLE and materialized view templates under sql/ are rendered with.
type schemaTemplateParams struct {
	// TableName is the name of the table being created (e.g. spans or spans_local).
	TableName string
	// LocalTableName is the name of the local storage table wrapped by a Distributed table.
	LocalTableName string
	// SourceTable is the source table for a materialized view (e.g. spans or spans_local).
	SourceTable string
	// TargetTable is the target table for a materialized view (e.g. services or services_local).
	TargetTable string
	// Cluster is the ClickHouse cluster name for Distributed tables.
	Cluster string
	// Database is the ClickHouse database name for Distributed tables.
	Database string
	// ShardingKey is the sharding expression for Distributed tables (e.g. cityHash64(trace_id)).
	ShardingKey string
	// TTLSeconds is the retention of spans and trace_id_timestamps; zero renders no TTL clause.
	TTLSeconds int64
	// MergeTree is the ENGINE clause body of the tables that hold raw rows.
	MergeTree string
	// AggregatingMergeTree is the ENGINE clause body of the derived tables, whose rows
	// collapse on their sort key.
	AggregatingMergeTree string
}

type schemaStatement struct {
	name  string
	query string
}

type schemaBuilder struct {
	statements []schemaStatement
}

func resolveCluster(cfg Configuration) string {
	if cfg.TableEngine.Replicated.HasValue() {
		if c := cfg.TableEngine.Replicated.Get().Cluster; c != "" {
			return c
		}
	}
	if cfg.Database != "" {
		return cfg.Database
	}
	return defaultDatabase
}

func detectDatabaseCluster(ctx context.Context, conn driver.Conn, cfg Configuration) (string, error) {
	var configuredCluster string
	if cfg.TableEngine.Replicated.HasValue() {
		configuredCluster = cfg.TableEngine.Replicated.Get().Cluster
	}

	dbName := cfg.Database
	if dbName == "" {
		dbName = defaultDatabase
	}

	var dbEngine string
	row := conn.QueryRow(ctx, "SELECT engine FROM system.databases WHERE name = ?", dbName)
	if err := row.Scan(&dbEngine); err == nil {
		switch dbEngine {
		case "Shared":
			if configuredCluster != "" {
				return "", errors.New("cluster set on a Shared database is a configuration error")
			}
			return "default", nil
		case "Replicated":
			if configuredCluster != "" {
				return "", errors.New("cluster set on a Replicated database is a configuration error")
			}
			return dbName, nil
		default:
			if configuredCluster != "" {
				return configuredCluster, nil
			}
			return dbName, nil
		}
	}

	if configuredCluster != "" {
		return configuredCluster, nil
	}
	return dbName, nil
}

func newSchemaBuilder(cfg Configuration) (*schemaBuilder, error) {
	return newSchemaBuilderWithCluster(cfg, resolveCluster(cfg))
}

func newSchemaBuilderWithCluster(cfg Configuration, cluster string) (*schemaBuilder, error) {
	mergeTree, err := cfg.TableEngine.engineClause(mergeTreeFamily)
	if err != nil {
		return nil, err
	}
	aggregatingMergeTree, err := cfg.TableEngine.engineClause(aggregatingMergeTreeFamily)
	if err != nil {
		return nil, err
	}

	dbName := cfg.Database
	if dbName == "" {
		dbName = defaultDatabase
	}

	builder := &schemaBuilder{}

	if !cfg.Distributed {
		params := schemaTemplateParams{
			TableName:            "",
			SourceTable:          "spans",
			TTLSeconds:           int64(cfg.TTL / time.Second),
			MergeTree:            mergeTree,
			AggregatingMergeTree: aggregatingMergeTree,
		}
		schema := []struct {
			name        string
			query       string
			targetTable string
		}{
			{"spans table", sql.CreateSpansTable, ""},
			{"services table", sql.CreateServicesTable, ""},
			{"services materialized view", sql.CreateServicesMaterializedView, "services"},
			{"operations table", sql.CreateOperationsTable, ""},
			{"operations materialized view", sql.CreateOperationsMaterializedView, "operations"},
			{"trace id timestamps table", sql.CreateTraceIDTimestampsTable, ""},
			{"trace id timestamps materialized view", sql.CreateTraceIDTimestampsMaterializedView, "trace_id_timestamps"},
			{"attribute metadata table", sql.CreateAttributeMetadataTable, ""},
			{"attribute metadata materialized view", sql.CreateAttributeMetadataMaterializedView, "attribute_metadata"},
			{"event attribute metadata materialized view", sql.CreateEventAttributeMetadataMaterializedView, "attribute_metadata"},
			{"link attribute metadata materialized view", sql.CreateLinkAttributeMetadataMaterializedView, "attribute_metadata"},
			{"dependencies table", sql.CreateDependenciesTable, ""},
		}
		for _, s := range schema {
			p := params
			p.TargetTable = s.targetTable
			query, err := loadTemplate(s.name, s.query, p)
			if err != nil {
				return nil, err
			}
			builder.statements = append(builder.statements, schemaStatement{name: s.name, query: query})
		}
		return builder, nil
	}

	// Distributed schema layout:
	// 1. Six local storage tables (_local suffix)
	// 2. Six Distributed wrapper tables (public table names)
	// 3. Six materialized views attached to local tables
	localTables := []struct {
		name      string
		tableName string
		query     string
	}{
		{"spans local table", "spans_local", sql.CreateSpansTable},
		{"services local table", "services_local", sql.CreateServicesTable},
		{"operations local table", "operations_local", sql.CreateOperationsTable},
		{"trace id timestamps local table", "trace_id_timestamps_local", sql.CreateTraceIDTimestampsTable},
		{"attribute metadata local table", "attribute_metadata_local", sql.CreateAttributeMetadataTable},
		{"dependencies local table", "dependencies_local", sql.CreateDependenciesTable},
	}
	for _, lt := range localTables {
		p := schemaTemplateParams{
			TableName:            lt.tableName,
			TTLSeconds:           int64(cfg.TTL / time.Second),
			MergeTree:            mergeTree,
			AggregatingMergeTree: aggregatingMergeTree,
		}
		query, err := loadTemplate(lt.name, lt.query, p)
		if err != nil {
			return nil, err
		}
		builder.statements = append(builder.statements, schemaStatement{name: lt.name, query: query})
	}

	distributedWrappers := []struct {
		name           string
		tableName      string
		localTableName string
		shardingKey    string
	}{
		{"spans distributed table", "spans", "spans_local", "cityHash64(trace_id)"},
		{"services distributed table", "services", "services_local", ""},
		{"operations distributed table", "operations", "operations_local", ""},
		{"trace id timestamps distributed table", "trace_id_timestamps", "trace_id_timestamps_local", "cityHash64(trace_id)"},
		{"attribute metadata distributed table", "attribute_metadata", "attribute_metadata_local", ""},
		{"dependencies distributed table", "dependencies", "dependencies_local", "cityHash64(timestamp)"},
	}
	for _, dw := range distributedWrappers {
		p := schemaTemplateParams{
			TableName:      dw.tableName,
			LocalTableName: dw.localTableName,
			Cluster:        cluster,
			Database:       dbName,
			ShardingKey:    dw.shardingKey,
		}
		query, err := loadTemplate(dw.name, sql.CreateDistributedTable, p)
		if err != nil {
			return nil, err
		}
		builder.statements = append(builder.statements, schemaStatement{name: dw.name, query: query})
	}

	materializedViews := []struct {
		name        string
		targetTable string
		query       string
	}{
		{"services materialized view", "services_local", sql.CreateServicesMaterializedView},
		{"operations materialized view", "operations_local", sql.CreateOperationsMaterializedView},
		{"trace id timestamps materialized view", "trace_id_timestamps_local", sql.CreateTraceIDTimestampsMaterializedView},
		{"attribute metadata materialized view", "attribute_metadata_local", sql.CreateAttributeMetadataMaterializedView},
		{"event attribute metadata materialized view", "attribute_metadata_local", sql.CreateEventAttributeMetadataMaterializedView},
		{"link attribute metadata materialized view", "attribute_metadata_local", sql.CreateLinkAttributeMetadataMaterializedView},
	}
	for _, mv := range materializedViews {
		p := schemaTemplateParams{
			SourceTable: "spans_local",
			TargetTable: mv.targetTable,
		}
		query, err := loadTemplate(mv.name, mv.query, p)
		if err != nil {
			return nil, err
		}
		builder.statements = append(builder.statements, schemaStatement{name: mv.name, query: query})
	}

	return builder, nil
}

func (b *schemaBuilder) build(ctx context.Context, conn driver.Conn) error {
	for _, statement := range b.statements {
		if err := conn.Exec(ctx, statement.query); err != nil {
			return fmt.Errorf("failed to create %s: %w", statement.name, err)
		}
	}
	return nil
}

type Factory struct {
	config Configuration
	telset telemetry.Settings
	conn   driver.Conn
}

func NewFactory(ctx context.Context, cfg Configuration, telset telemetry.Settings) (*Factory, error) {
	var builder *schemaBuilder
	if cfg.CreateSchema {
		var err error
		builder, err = newSchemaBuilder(cfg)
		if err != nil {
			return nil, err
		}
	}

	f := &Factory{
		config: cfg,
		telset: telset,
	}
	opts := &clickhouse.Options{
		Protocol: getProtocol(f.config.Protocol),
		Addr:     f.config.Addresses,
		Auth: clickhouse.Auth{
			Database: f.config.Database,
		},
		DialTimeout: f.config.DialTimeout,
	}
	basicAuth := f.config.Auth.Basic.Get()
	if basicAuth != nil {
		opts.Auth.Username = basicAuth.Username
		opts.Auth.Password = string(basicAuth.Password)
	}
	if tlsCfg := f.config.TLS.Get(); tlsCfg != nil {
		loaded, tlsErr := tlsCfg.LoadTLSConfig(ctx)
		if tlsErr != nil {
			return nil, fmt.Errorf("failed to load TLS configuration: %w", tlsErr)
		}
		opts.TLS = loaded
	}
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to create ClickHouse connection: %w", err)
	}

	success := false
	defer func() {
		if !success {
			_ = conn.Close()
		}
	}()

	if err = conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	if builder != nil {
		if cfg.Distributed {
			detectedCluster, detectErr := detectDatabaseCluster(ctx, conn, cfg)
			if detectErr != nil {
				return nil, detectErr
			}
			if detectedCluster != resolveCluster(cfg) {
				var err error
				builder, err = newSchemaBuilderWithCluster(cfg, detectedCluster)
				if err != nil {
					return nil, err
				}
			}
		}
		if err := builder.build(ctx, conn); err != nil {
			return nil, err
		}
	}

	success = true
	f.conn = conn
	return f, nil
}

func (f *Factory) CreateTraceReader() (tracestore.Reader, error) {
	return chtracestore.NewReader(f.conn, chtracestore.ReaderConfig{
		DefaultSearchDepth:            f.config.DefaultSearchDepth,
		MaxSearchDepth:                f.config.MaxSearchDepth,
		AttributeMetadataCacheTTL:     f.config.AttributeMetadataCacheTTL,
		AttributeMetadataCacheMaxSize: f.config.AttributeMetadataCacheMaxSize,
	}), nil
}

func (f *Factory) CreateTraceWriter() (tracestore.Writer, error) {
	return chtracestore.NewWriter(f.conn), nil
}

func (f *Factory) CreateDependencyReader() (depstore.Reader, error) {
	return chdepstore.NewDependencyReader(f.conn), nil
}

func (f *Factory) CreateDependencyWriter() (depstore.Writer, error) {
	return chdepstore.NewDependencyWriter(f.conn), nil
}

func (f *Factory) CreateMetricsReader() (metricstore.Reader, error) {
	return metricstoremetrics.NewReaderDecorator(
		chmetricstore.NewReader(f.conn),
		f.telset.Metrics,
	), nil
}

func (f *Factory) Close() error {
	return f.conn.Close()
}

func (f *Factory) Purge(ctx context.Context) error {
	tables := []struct {
		name  string
		query string
	}{
		{"spans", sql.TruncateSpans},
		{"services", sql.TruncateServices},
		{"operations", sql.TruncateOperations},
		{"trace_id_timestamps", sql.TruncateTraceIDTimestamps},
		{"attribute_metadata", sql.TruncateAttributeMetadata},
		{"dependencies", sql.TruncateDependencies},
	}

	for _, table := range tables {
		if err := f.conn.Exec(ctx, table.query); err != nil {
			return fmt.Errorf("failed to purge %s: %w", table.name, err)
		}
	}
	return nil
}

func getProtocol(protocol string) clickhouse.Protocol {
	if protocol == "http" {
		return clickhouse.HTTP
	}
	return clickhouse.Native
}

// loadTemplate is defined as a variable to allow overriding it in tests.
var loadTemplate func(name, tmplBody string, data any) (string, error) = loadTemplateImpl

func loadTemplateImpl(name, tmplBody string, data any) (string, error) {
	tmpl, err := template.New(name).Parse(tmplBody)
	if err != nil {
		return "", fmt.Errorf("failed to parse %s template: %w", name, err)
	}
	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return "", fmt.Errorf("failed to execute %s template: %w", name, err)
	}
	return buf.String(), nil
}
