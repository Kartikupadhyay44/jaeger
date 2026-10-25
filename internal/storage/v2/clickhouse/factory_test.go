// Copyright (c) 2025 The Jaeger Authors.
// SPDX-License-Identifier: Apache-2.0

package clickhouse

import (
	"context"
	"errors"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/basicauthextension"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configtls"

	"github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/clickhousetest"
	"github.com/jaegertracing/jaeger/internal/storage/v2/clickhouse/sql"
	"github.com/jaegertracing/jaeger/internal/telemetry"
)

func TestFactory(t *testing.T) {
	tests := []struct {
		name         string
		createSchema bool
	}{
		{
			name:         "without schema creation",
			createSchema: false,
		},
		{
			name:         "with schema creation",
			createSchema: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := clickhousetest.NewServer(clickhousetest.FailureConfig{})
			defer srv.Close()

			cfg := Configuration{
				Protocol: "http",
				Addresses: []string{
					srv.Listener.Addr().String(),
				},
				Database: "default",
				Auth: Authentication{
					Basic: configoptional.Some(basicauthextension.ClientAuthSettings{
						Username: "user",
						Password: "password",
					}),
				},
				CreateSchema: tt.createSchema,
				TableEngine:  localTableEngine,
			}

			f, err := NewFactory(context.Background(), cfg, telemetry.NoopSettings())
			require.NoError(t, err)
			require.NotNil(t, f)

			tr, err := f.CreateTraceReader()
			require.NoError(t, err)
			require.NotNil(t, tr)

			tw, err := f.CreateTraceWriter()
			require.NoError(t, err)
			require.NotNil(t, tw)

			dr, err := f.CreateDependencyReader()
			require.NoError(t, err)
			require.NotNil(t, dr)

			mr, err := f.CreateMetricsReader()
			require.NoError(t, err)
			require.NotNil(t, mr)

			err = f.Purge(context.Background())
			require.NoError(t, err)

			require.NoError(t, f.Close())
		})
	}
}

// localTableEngine is the engine the unit tests create the schema with.
var localTableEngine = TableEngine{
	MergeTree: configoptional.Some(MergeTreeEngine{}),
}

// schemaQuery returns the statement newSchemaBuilder renders under the given name for
// localTableEngine and no TTL, so the tests expect exactly what the factory executes.
func schemaQuery(t *testing.T, name string) string {
	builder, err := newSchemaBuilder(Configuration{CreateSchema: true, TableEngine: localTableEngine})
	require.NoError(t, err)
	for _, stmt := range builder.statements {
		if stmt.name == name {
			return stmt.query
		}
	}
	require.Failf(t, "unknown schema statement", "%s", name)
	return ""
}

func TestNewFactory_Errors(t *testing.T) {
	tests := []struct {
		name          string
		failureConfig clickhousetest.FailureConfig
		expectedError string
	}{
		{
			name: "ping error",
			failureConfig: clickhousetest.FailureConfig{
				clickhousetest.PingQuery: assert.AnError,
			},
			expectedError: "failed to ping ClickHouse",
		},
		{
			name: "spans table creation error",
			failureConfig: clickhousetest.FailureConfig{
				schemaQuery(t, "spans table"): assert.AnError,
			},
			expectedError: "failed to create spans table",
		},
		{
			name: "services table creation error",
			failureConfig: clickhousetest.FailureConfig{
				schemaQuery(t, "services table"): assert.AnError,
			},
			expectedError: "failed to create services table",
		},
		{
			name: "services materialized view creation error",
			failureConfig: clickhousetest.FailureConfig{
				schemaQuery(t, "services materialized view"): assert.AnError,
			},
			expectedError: "failed to create services materialized view",
		},
		{
			name: "operations table creation error",
			failureConfig: clickhousetest.FailureConfig{
				schemaQuery(t, "operations table"): assert.AnError,
			},
			expectedError: "failed to create operations table",
		},
		{
			name: "operations materialized view creation error",
			failureConfig: clickhousetest.FailureConfig{
				schemaQuery(t, "operations materialized view"): assert.AnError,
			},
			expectedError: "failed to create operations materialized view",
		},
		{
			name: "trace id timestamps table creation error",
			failureConfig: clickhousetest.FailureConfig{
				schemaQuery(t, "trace id timestamps table"): assert.AnError,
			},
			expectedError: "failed to create trace id timestamps table",
		},
		{
			name: "trace id timestamps materialized view creation error",
			failureConfig: clickhousetest.FailureConfig{
				schemaQuery(t, "trace id timestamps materialized view"): assert.AnError,
			},
			expectedError: "failed to create trace id timestamps materialized view",
		},
		{
			name: "attribute metadata table creation error",
			failureConfig: clickhousetest.FailureConfig{
				schemaQuery(t, "attribute metadata table"): assert.AnError,
			},
			expectedError: "failed to create attribute metadata table",
		},
		{
			name: "attribute metadata materialized view creation error",
			failureConfig: clickhousetest.FailureConfig{
				schemaQuery(t, "attribute metadata materialized view"): assert.AnError,
			},
			expectedError: "failed to create attribute metadata materialized view",
		},
		{
			name: "event attribute metadata materialized view creation error",
			failureConfig: clickhousetest.FailureConfig{
				schemaQuery(t, "event attribute metadata materialized view"): assert.AnError,
			},
			expectedError: "failed to create event attribute metadata materialized view",
		},
		{
			name: "link attribute metadata materialized view creation error",
			failureConfig: clickhousetest.FailureConfig{
				schemaQuery(t, "link attribute metadata materialized view"): assert.AnError,
			},
			expectedError: "failed to create link attribute metadata materialized view",
		},
		{
			name: "dependencies table creation error",
			failureConfig: clickhousetest.FailureConfig{
				schemaQuery(t, "dependencies table"): assert.AnError,
			},
			expectedError: "failed to create dependencies table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := clickhousetest.NewServer(tt.failureConfig)
			defer srv.Close()

			cfg := Configuration{
				Protocol: "http",
				Addresses: []string{
					srv.Listener.Addr().String(),
				},
				DialTimeout:  1 * time.Second,
				CreateSchema: true,
				TableEngine:  localTableEngine,
			}

			f, err := NewFactory(context.Background(), cfg, telemetry.NoopSettings())
			require.ErrorContains(t, err, tt.expectedError)
			require.Nil(t, f)
		})
	}
}

func TestPurge(t *testing.T) {
	tests := []struct {
		name          string
		failureConfig clickhousetest.FailureConfig
		expectedError string
	}{
		{
			name: "truncate spans table error",
			failureConfig: clickhousetest.FailureConfig{
				sql.TruncateSpans: assert.AnError,
			},
			expectedError: "failed to purge spans",
		},
		{
			name: "truncate services table error",
			failureConfig: clickhousetest.FailureConfig{
				sql.TruncateServices: assert.AnError,
			},
			expectedError: "failed to purge services",
		},
		{
			name: "truncate operations table error",
			failureConfig: clickhousetest.FailureConfig{
				sql.TruncateOperations: assert.AnError,
			},
			expectedError: "failed to purge operations",
		},
		{
			name: "truncate trace_id_timestamps table error",
			failureConfig: clickhousetest.FailureConfig{
				sql.TruncateTraceIDTimestamps: assert.AnError,
			},
			expectedError: "failed to purge trace_id_timestamps",
		},
		{
			name: "truncate attribute_metadata table error",
			failureConfig: clickhousetest.FailureConfig{
				sql.TruncateAttributeMetadata: assert.AnError,
			},
			expectedError: "failed to purge attribute_metadata",
		},
		{
			name: "truncate dependencies table error",
			failureConfig: clickhousetest.FailureConfig{
				sql.TruncateDependencies: assert.AnError,
			},
			expectedError: "failed to purge dependencies",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := clickhousetest.NewServer(tt.failureConfig)
			defer srv.Close()

			cfg := Configuration{
				Protocol: "http",
				Addresses: []string{
					srv.Listener.Addr().String(),
				},
				DialTimeout:  1 * time.Second,
				CreateSchema: true,
				TableEngine:  localTableEngine,
			}

			f, err := NewFactory(context.Background(), cfg, telemetry.NoopSettings())
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, f.Close())
			})

			err = f.Purge(context.Background())
			require.ErrorContains(t, err, tt.expectedError)
		})
	}
}

func TestGetProtocol(t *testing.T) {
	tests := []struct {
		protocol string
		expected clickhouse.Protocol
	}{
		{
			protocol: "http",
			expected: clickhouse.HTTP,
		},
		{
			protocol: "native",
			expected: clickhouse.Native,
		},
		{
			protocol: "",
			expected: clickhouse.Native,
		},
		{
			protocol: "unknown",
			expected: clickhouse.Native,
		},
	}

	for _, tt := range tests {
		t.Run(tt.protocol, func(t *testing.T) {
			result := getProtocol(tt.protocol)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestNewFactory_TLSLoadError(t *testing.T) {
	cfg := Configuration{
		Protocol:  "native",
		Addresses: []string{"localhost:9440"},
		TLS: configoptional.Some(configtls.ClientConfig{
			Config: configtls.Config{
				CAFile: "/nonexistent/ca.pem",
			},
		}),
	}
	f, err := NewFactory(context.Background(), cfg, telemetry.NoopSettings())
	require.ErrorContains(t, err, "failed to load TLS configuration")
	require.Nil(t, f)
}

func TestNewFactory_TLSLoadSuccess(t *testing.T) {
	srv := clickhousetest.NewServer(clickhousetest.FailureConfig{})
	defer srv.Close()
	cfg := Configuration{
		Protocol:  "native",
		Addresses: []string{srv.Listener.Addr().String()},
		TLS: configoptional.Some(configtls.ClientConfig{
			InsecureSkipVerify: true,
		}),
	}
	// TLS config loads successfully; connection fails because the test server is plain (no TLS).
	_, err := NewFactory(context.Background(), cfg, telemetry.NoopSettings())
	require.Error(t, err)
	require.NotContains(t, err.Error(), "failed to load TLS configuration")
}

func TestNewSchemaBuilder_Errors(t *testing.T) {
	originalLoadTemplate := loadTemplate
	t.Cleanup(func() { loadTemplate = originalLoadTemplate })

	tests := []struct {
		name          string
		mockFn        func(name, tmplBody string, data any) (string, error)
		distributed   bool
		expectedError string
	}{
		{
			name: "first loadTemplate call fails",
			mockFn: func(_, _ string, _ any) (string, error) {
				return "", errors.New("mock template error")
			},
			expectedError: "mock template error",
		},
		{
			name: "second loadTemplate call fails",
			mockFn: func() func(name, tmplBody string, data any) (string, error) {
				calls := 0
				return func(name, tmplBody string, data any) (string, error) {
					calls++
					if calls >= 2 {
						return "", errors.New("mock template error")
					}
					return loadTemplateImpl(name, tmplBody, data)
				}
			}(),
			expectedError: "mock template error",
		},
		{
			name: "distributed first loadTemplate call fails",
			mockFn: func(_, _ string, _ any) (string, error) {
				return "", errors.New("mock template error")
			},
			distributed:   true,
			expectedError: "mock template error",
		},
		{
			name: "distributed wrapper loadTemplate call fails",
			mockFn: func() func(name, tmplBody string, data any) (string, error) {
				calls := 0
				return func(name, tmplBody string, data any) (string, error) {
					calls++
					if calls >= 7 { // 6 local tables succeed, 7th (distributed wrapper) fails
						return "", errors.New("mock wrapper template error")
					}
					return loadTemplateImpl(name, tmplBody, data)
				}
			}(),
			distributed:   true,
			expectedError: "mock wrapper template error",
		},
		{
			name: "distributed mv loadTemplate call fails",
			mockFn: func() func(name, tmplBody string, data any) (string, error) {
				calls := 0
				return func(name, tmplBody string, data any) (string, error) {
					calls++
					if calls >= 13 { // 6 local tables + 6 distributed wrappers succeed, 13th (MV) fails
						return "", errors.New("mock mv template error")
					}
					return loadTemplateImpl(name, tmplBody, data)
				}
			}(),
			distributed:   true,
			expectedError: "mock mv template error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			loadTemplate = tt.mockFn
			_, err := NewFactory(
				context.Background(),
				Configuration{CreateSchema: true, Distributed: tt.distributed, TableEngine: localTableEngine},
				telemetry.NoopSettings(),
			)
			require.ErrorContains(t, err, tt.expectedError)
		})
	}
}

func TestLoadTemplate(t *testing.T) {
	tests := []struct {
		name     string
		tmplBody string
		data     any
		expected string
		errorMsg string
	}{
		{
			name:     "valid template",
			tmplBody: "Hello {{ .Name }}",
			data:     struct{ Name string }{Name: "Jaeger"},
			expected: "Hello Jaeger",
		},
		{
			name:     "parse error",
			tmplBody: "{{ bad syntax",
			data:     nil,
			errorMsg: "failed to parse",
		},
		{
			name:     "execution error",
			tmplBody: "Hello {{ .Name.Invalid }}",
			data:     struct{ Name string }{Name: "Jaeger"},
			errorMsg: "failed to execute",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			res, err := loadTemplate("test_tmpl", tt.tmplBody, tt.data)
			if tt.errorMsg != "" {
				require.ErrorContains(t, err, tt.errorMsg)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, res)
			}
		})
	}
}

func TestCreateSpansTableTemplate(t *testing.T) {
	t.Run("without TTL", func(t *testing.T) {
		queryWithoutTTL := schemaQuery(t, "spans table")
		assert.NotContains(t, queryWithoutTTL, "TTL start_time")
	})

	t.Run("with TTL", func(t *testing.T) {
		queryWithTTL, err := loadTemplate("test_ttl", sql.CreateSpansTable, schemaTemplateParams{TTLSeconds: 86400, MergeTree: "MergeTree"})
		require.NoError(t, err)
		assert.Contains(t, queryWithTTL, "TTL start_time + INTERVAL 86400 SECOND DELETE")
	})
}

func TestCreateTraceIDTimestampsTableTemplate(t *testing.T) {
	t.Run("without TTL", func(t *testing.T) {
		queryWithoutTTL := schemaQuery(t, "trace id timestamps table")
		assert.NotContains(t, queryWithoutTTL, "TTL end")
	})

	t.Run("with TTL", func(t *testing.T) {
		queryWithTTL, err := loadTemplate("test_ttl_trace", sql.CreateTraceIDTimestampsTable, schemaTemplateParams{TTLSeconds: 86400, AggregatingMergeTree: "AggregatingMergeTree"})
		require.NoError(t, err)
		assert.Contains(t, queryWithTTL, "TTL end + INTERVAL 86400 SECOND DELETE")
	})
}

// TestSchemaBuilder_Engines checks that every table the builder creates carries the ENGINE
// clause the configured TableEngine renders, and that the materialized views carry none.
func TestSchemaBuilder_Engines(t *testing.T) {
	mergeTreeTables := map[string]bool{
		"spans table":        true,
		"dependencies table": true,
	}
	aggregatingTables := map[string]bool{
		"services table":            true,
		"operations table":          true,
		"trace id timestamps table": true,
		"attribute metadata table":  true,
	}
	tests := []struct {
		name            string
		engine          TableEngine
		wantMergeTree   string
		wantAggregating string
	}{
		{
			name:            "merge_tree",
			engine:          localTableEngine,
			wantMergeTree:   "ENGINE = MergeTree",
			wantAggregating: "ENGINE = AggregatingMergeTree",
		},
		{
			name: "replicated with server defaults",
			engine: TableEngine{
				Replicated: configoptional.Some(ReplicatedEngine{}),
			},
			wantMergeTree:   "ENGINE = ReplicatedMergeTree",
			wantAggregating: "ENGINE = ReplicatedAggregatingMergeTree",
		},
		{
			name: "replicated with keeper path and replica name",
			engine: TableEngine{
				Replicated: configoptional.Some(ReplicatedEngine{
					KeeperPath:  "/clickhouse/tables/{shard}/{database}/{table}",
					ReplicaName: "{replica}",
				}),
			},
			wantMergeTree:   "ENGINE = ReplicatedMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')",
			wantAggregating: "ENGINE = ReplicatedAggregatingMergeTree('/clickhouse/tables/{shard}/{database}/{table}', '{replica}')",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			builder, err := newSchemaBuilder(Configuration{CreateSchema: true, TableEngine: tt.engine})
			require.NoError(t, err)
			seen := 0
			for _, stmt := range builder.statements {
				assert.NotContains(t, stmt.query, "{{", stmt.name)
				// The clause must end at whitespace, so that a bare ReplicatedMergeTree
				// is not satisfied by ReplicatedMergeTree('...') and vice versa.
				switch {
				case mergeTreeTables[stmt.name]:
					assert.Regexp(t, regexp.QuoteMeta(tt.wantMergeTree)+`\s`, stmt.query, stmt.name)
					seen++
				case aggregatingTables[stmt.name]:
					assert.Regexp(t, regexp.QuoteMeta(tt.wantAggregating)+`\s`, stmt.query, stmt.name)
					seen++
				default:
					assert.NotContains(t, stmt.query, "ENGINE", stmt.name)
				}
			}
			assert.Equal(t, len(mergeTreeTables)+len(aggregatingTables), seen)
		})
	}

	t.Run("unset engine is refused", func(t *testing.T) {
		_, err := newSchemaBuilder(Configuration{CreateSchema: true})
		require.ErrorContains(t, err, "`create_schema` requires `table_engine`")
	})
}

func TestNewFactory_KeepsExplicitZeroCacheSettings(t *testing.T) {
	srv := clickhousetest.NewServer(clickhousetest.FailureConfig{})
	defer srv.Close()

	cfg := DefaultConfiguration()
	cfg.Protocol = "http"
	cfg.Addresses = []string{srv.Listener.Addr().String()}
	cfg.AttributeMetadataCacheTTL = 0
	cfg.AttributeMetadataCacheMaxSize = 0

	f, err := NewFactory(context.Background(), cfg, telemetry.NoopSettings())
	require.NoError(t, err)
	defer func() { require.NoError(t, f.Close()) }()

	assert.Zero(t, f.config.AttributeMetadataCacheTTL)
	assert.Zero(t, f.config.AttributeMetadataCacheMaxSize)
}

func TestResolveCluster(t *testing.T) {
	tests := []struct {
		name     string
		cfg      Configuration
		expected string
	}{
		{
			name: "replicated with explicit cluster",
			cfg: Configuration{
				TableEngine: TableEngine{
					Replicated: configoptional.Some(ReplicatedEngine{Cluster: "cluster1"}),
				},
				Database: "custom_db",
			},
			expected: "cluster1",
		},
		{
			name: "replicated without cluster falls back to database",
			cfg: Configuration{
				TableEngine: TableEngine{
					Replicated: configoptional.Some(ReplicatedEngine{}),
				},
				Database: "custom_db",
			},
			expected: "custom_db",
		},
		{
			name: "replicated without cluster and empty database falls back to default database",
			cfg: Configuration{
				TableEngine: TableEngine{
					Replicated: configoptional.Some(ReplicatedEngine{}),
				},
			},
			expected: defaultDatabase,
		},
		{
			name: "merge_tree with database falls back to database",
			cfg: Configuration{
				TableEngine: TableEngine{
					MergeTree: configoptional.Some(MergeTreeEngine{}),
				},
				Database: "custom_db",
			},
			expected: "custom_db",
		},
		{
			name: "merge_tree without database falls back to default database",
			cfg: Configuration{
				TableEngine: TableEngine{
					MergeTree: configoptional.Some(MergeTreeEngine{}),
				},
			},
			expected: defaultDatabase,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, resolveCluster(tt.cfg))
		})
	}
}

type mockTestRow struct {
	scanFn func(dest ...any) error
}

func (mockTestRow) Err() error { return nil }
func (m mockTestRow) Scan(dest ...any) error {
	if m.scanFn != nil {
		return m.scanFn(dest...)
	}
	return errors.New("scan error")
}
func (mockTestRow) ScanStruct(_ any) error { return nil }

type mockTestConn struct {
	driver.Conn
	queryRowFn func(ctx context.Context, query string, args ...any) driver.Row
}

func (m mockTestConn) QueryRow(ctx context.Context, query string, args ...any) driver.Row {
	if m.queryRowFn != nil {
		return m.queryRowFn(ctx, query, args...)
	}
	return mockTestRow{}
}

func TestDetectDatabaseCluster(t *testing.T) {
	tests := []struct {
		name        string
		cfg         Configuration
		dbEngine    string
		scanErr     error
		expected    string
		expectedErr string
	}{
		{
			name: "Shared database without configured cluster defaults to 'default'",
			cfg: Configuration{
				Database: "default",
			},
			dbEngine: "Shared",
			expected: "default",
		},
		{
			name: "Shared database with configured cluster returns error",
			cfg: Configuration{
				Database: "default",
				TableEngine: TableEngine{
					Replicated: configoptional.Some(ReplicatedEngine{Cluster: "my_cluster"}),
				},
			},
			dbEngine:    "Shared",
			expectedErr: "cluster set on a Shared database is a configuration error",
		},
		{
			name: "Replicated database without configured cluster defaults to dbName",
			cfg: Configuration{
				Database: "replicated_db",
			},
			dbEngine: "Replicated",
			expected: "replicated_db",
		},
		{
			name: "Replicated database with configured cluster returns error",
			cfg: Configuration{
				Database: "replicated_db",
				TableEngine: TableEngine{
					Replicated: configoptional.Some(ReplicatedEngine{Cluster: "my_cluster"}),
				},
			},
			dbEngine:    "Replicated",
			expectedErr: "cluster set on a Replicated database is a configuration error",
		},
		{
			name: "Atomic database with configured cluster returns configured cluster",
			cfg: Configuration{
				Database: "atomic_db",
				TableEngine: TableEngine{
					Replicated: configoptional.Some(ReplicatedEngine{Cluster: "atomic_cluster"}),
				},
			},
			dbEngine: "Atomic",
			expected: "atomic_cluster",
		},
		{
			name: "Atomic database without configured cluster returns dbName",
			cfg: Configuration{
				Database: "atomic_db",
			},
			dbEngine: "Atomic",
			expected: "atomic_db",
		},
		{
			name: "Other database engine with configured cluster returns configured cluster",
			cfg: Configuration{
				Database: "other_db",
				TableEngine: TableEngine{
					Replicated: configoptional.Some(ReplicatedEngine{Cluster: "other_cluster"}),
				},
			},
			dbEngine: "Ordinary",
			expected: "other_cluster",
		},
		{
			name: "Other database engine without configured cluster returns dbName",
			cfg: Configuration{
				Database: "other_db",
			},
			dbEngine: "Ordinary",
			expected: "other_db",
		},
		{
			name: "Scan error with configured cluster returns configured cluster",
			cfg: Configuration{
				Database: "unknown_db",
				TableEngine: TableEngine{
					Replicated: configoptional.Some(ReplicatedEngine{Cluster: "fallback_cluster"}),
				},
			},
			scanErr:  errors.New("db not found"),
			expected: "fallback_cluster",
		},
		{
			name: "Scan error without configured cluster returns dbName fallback",
			cfg: Configuration{
				Database: "unknown_db",
			},
			scanErr:  errors.New("db not found"),
			expected: "unknown_db",
		},
		{
			name:     "Scan error with empty database returns default database fallback",
			cfg:      Configuration{},
			scanErr:  errors.New("db not found"),
			expected: defaultDatabase,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			conn := mockTestConn{
				queryRowFn: func(_ context.Context, _ string, _ ...any) driver.Row {
					return mockTestRow{
						scanFn: func(dest ...any) error {
							if tt.scanErr != nil {
								return tt.scanErr
							}
							if len(dest) > 0 {
								if ptr, ok := dest[0].(*string); ok {
									*ptr = tt.dbEngine
								}
							}
							return nil
						},
					}
				},
			}
			cluster, err := detectDatabaseCluster(context.Background(), conn, tt.cfg)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, cluster)
			}
		})
	}
}

func TestSchemaBuilder_NonDistributed(t *testing.T) {
	cfg := Configuration{
		CreateSchema: true,
		Distributed:  false,
		TableEngine:  localTableEngine,
		TTL:          24 * time.Hour,
	}
	builder, err := newSchemaBuilder(cfg)
	require.NoError(t, err)
	require.Len(t, builder.statements, 12)

	expectedNames := []string{
		"spans table",
		"services table",
		"services materialized view",
		"operations table",
		"operations materialized view",
		"trace id timestamps table",
		"trace id timestamps materialized view",
		"attribute metadata table",
		"attribute metadata materialized view",
		"event attribute metadata materialized view",
		"link attribute metadata materialized view",
		"dependencies table",
	}

	for i, stmt := range builder.statements {
		assert.Equal(t, expectedNames[i], stmt.name)
		assert.NotContains(t, stmt.query, "_local")
		assert.NotContains(t, stmt.query, "ENGINE = Distributed")
		assert.NotContains(t, stmt.query, "{{")
	}

	// Verify materialized views target public tables and read from spans (not spans_local)
	for _, stmt := range builder.statements {
		if strings.HasSuffix(stmt.name, "materialized view") {
			assert.Contains(t, stmt.query, "spans")
			assert.NotContains(t, stmt.query, "spans_local")
		}
	}
}

func TestSchemaBuilder_Distributed(t *testing.T) {
	cfg := Configuration{
		CreateSchema: true,
		Distributed:  true,
		Database:     "my_jaeger",
		TableEngine: TableEngine{
			Replicated: configoptional.Some(ReplicatedEngine{
				Cluster: "my_cluster",
			}),
		},
		TTL: 3600 * time.Second,
	}

	builder, err := newSchemaBuilder(cfg)
	require.NoError(t, err)
	require.Len(t, builder.statements, 18)

	expectedNames := []string{
		// 6 local tables
		"spans local table",
		"services local table",
		"operations local table",
		"trace id timestamps local table",
		"attribute metadata local table",
		"dependencies local table",
		// 6 distributed tables
		"spans distributed table",
		"services distributed table",
		"operations distributed table",
		"trace id timestamps distributed table",
		"attribute metadata distributed table",
		"dependencies distributed table",
		// 6 materialized views
		"services materialized view",
		"operations materialized view",
		"trace id timestamps materialized view",
		"attribute metadata materialized view",
		"event attribute metadata materialized view",
		"link attribute metadata materialized view",
	}

	for i, stmt := range builder.statements {
		assert.Equal(t, expectedNames[i], stmt.name)
		assert.NotContains(t, stmt.query, "{{", stmt.name)
	}

	stmtMap := make(map[string]string)
	for _, stmt := range builder.statements {
		stmtMap[stmt.name] = stmt.query
	}

	// Local tables create <name>_local
	assert.Contains(t, stmtMap["spans local table"], "spans_local")
	assert.Contains(t, stmtMap["services local table"], "services_local")
	assert.Contains(t, stmtMap["operations local table"], "operations_local")
	assert.Contains(t, stmtMap["trace id timestamps local table"], "trace_id_timestamps_local")
	assert.Contains(t, stmtMap["attribute metadata local table"], "attribute_metadata_local")
	assert.Contains(t, stmtMap["dependencies local table"], "dependencies_local")

	// TTL applies to spans_local and trace_id_timestamps_local
	assert.Contains(t, stmtMap["spans local table"], "TTL start_time + INTERVAL 3600 SECOND DELETE")
	assert.Contains(t, stmtMap["trace id timestamps local table"], "TTL end + INTERVAL 3600 SECOND DELETE")

	// Distributed tables:
	// spans and trace_id_timestamps sharded by cityHash64(trace_id)
	assert.Equal(t,
		"CREATE TABLE IF NOT EXISTS spans AS spans_local ENGINE = Distributed(my_cluster, my_jaeger, spans_local, cityHash64(trace_id))\n",
		stmtMap["spans distributed table"],
	)
	assert.Equal(t,
		"CREATE TABLE IF NOT EXISTS trace_id_timestamps AS trace_id_timestamps_local ENGINE = Distributed(my_cluster, my_jaeger, trace_id_timestamps_local, cityHash64(trace_id))\n",
		stmtMap["trace id timestamps distributed table"],
	)

	// dependencies sharded by cityHash64(timestamp)
	assert.Equal(t,
		"CREATE TABLE IF NOT EXISTS dependencies AS dependencies_local ENGINE = Distributed(my_cluster, my_jaeger, dependencies_local, cityHash64(timestamp))\n",
		stmtMap["dependencies distributed table"],
	)

	// services, operations, attribute_metadata have no sharding key
	assert.Equal(t,
		"CREATE TABLE IF NOT EXISTS services AS services_local ENGINE = Distributed(my_cluster, my_jaeger, services_local)\n",
		stmtMap["services distributed table"],
	)
	assert.Equal(t,
		"CREATE TABLE IF NOT EXISTS operations AS operations_local ENGINE = Distributed(my_cluster, my_jaeger, operations_local)\n",
		stmtMap["operations distributed table"],
	)
	assert.Equal(t,
		"CREATE TABLE IF NOT EXISTS attribute_metadata AS attribute_metadata_local ENGINE = Distributed(my_cluster, my_jaeger, attribute_metadata_local)\n",
		stmtMap["attribute metadata distributed table"],
	)

	// Materialized views:
	assert.Contains(t, stmtMap["services materialized view"], "TO services_local")
	assert.Contains(t, stmtMap["services materialized view"], "spans_local")

	assert.Contains(t, stmtMap["operations materialized view"], "TO operations_local")
	assert.Contains(t, stmtMap["operations materialized view"], "spans_local")

	assert.Contains(t, stmtMap["trace id timestamps materialized view"], "TO trace_id_timestamps_local")
	assert.Contains(t, stmtMap["trace id timestamps materialized view"], "spans_local")

	assert.Contains(t, stmtMap["attribute metadata materialized view"], "TO attribute_metadata_local")
	assert.Contains(t, stmtMap["attribute metadata materialized view"], "spans_local")

	assert.Contains(t, stmtMap["event attribute metadata materialized view"], "TO attribute_metadata_local")
	assert.Contains(t, stmtMap["event attribute metadata materialized view"], "spans_local")

	assert.Contains(t, stmtMap["link attribute metadata materialized view"], "TO attribute_metadata_local")
	assert.Contains(t, stmtMap["link attribute metadata materialized view"], "spans_local")
}

func distributedSchemaQuery(t *testing.T, name string) string {
	builder, err := newSchemaBuilder(Configuration{
		CreateSchema: true,
		Distributed:  true,
		TableEngine:  localTableEngine,
	})
	require.NoError(t, err)
	for _, stmt := range builder.statements {
		if stmt.name == name {
			return stmt.query
		}
	}
	require.Failf(t, "unknown distributed schema statement", "%s", name)
	return ""
}

func TestNewFactory_Distributed_Success(t *testing.T) {
	srv := clickhousetest.NewServer(clickhousetest.FailureConfig{})
	defer srv.Close()

	cfg := Configuration{
		Protocol: "http",
		Addresses: []string{
			srv.Listener.Addr().String(),
		},
		Database:     "default",
		CreateSchema: true,
		Distributed:  true,
		TableEngine:  localTableEngine,
	}

	f, err := NewFactory(context.Background(), cfg, telemetry.NoopSettings())
	require.NoError(t, err)
	require.NotNil(t, f)
	require.NoError(t, f.Close())
}

func TestNewFactory_Distributed_SharedDatabase(t *testing.T) {
	srv := clickhousetest.NewServer(clickhousetest.FailureConfig{})
	defer srv.Close()

	cfg := Configuration{
		Protocol: "http",
		Addresses: []string{
			srv.Listener.Addr().String(),
		},
		Database:     "shared_db",
		CreateSchema: true,
		Distributed:  true,
		TableEngine:  localTableEngine,
	}

	// Server reports shared_db has Shared engine -> detected cluster is "default" != "shared_db"
	f, err := NewFactory(context.Background(), cfg, telemetry.NoopSettings())
	require.NoError(t, err)
	require.NotNil(t, f)
	require.NoError(t, f.Close())
}

func TestNewFactory_Distributed_SharedDatabase_ClusterError(t *testing.T) {
	srv := clickhousetest.NewServer(clickhousetest.FailureConfig{})
	defer srv.Close()

	cfg := Configuration{
		Protocol: "http",
		Addresses: []string{
			srv.Listener.Addr().String(),
		},
		Database:     "shared_db",
		CreateSchema: true,
		Distributed:  true,
		TableEngine: TableEngine{
			Replicated: configoptional.Some(ReplicatedEngine{
				Cluster: "configured_cluster",
			}),
		},
	}

	f, err := NewFactory(context.Background(), cfg, telemetry.NoopSettings())
	require.ErrorContains(t, err, "cluster set on a Shared database is a configuration error")
	require.Nil(t, f)
}

func TestNewFactory_Distributed_Errors(t *testing.T) {
	tests := []struct {
		name          string
		failureConfig clickhousetest.FailureConfig
		expectedError string
	}{
		{
			name: "spans local table error",
			failureConfig: clickhousetest.FailureConfig{
				distributedSchemaQuery(t, "spans local table"): assert.AnError,
			},
			expectedError: "failed to create spans local table",
		},
		{
			name: "spans distributed table error",
			failureConfig: clickhousetest.FailureConfig{
				distributedSchemaQuery(t, "spans distributed table"): assert.AnError,
			},
			expectedError: "failed to create spans distributed table",
		},
		{
			name: "services distributed table error",
			failureConfig: clickhousetest.FailureConfig{
				distributedSchemaQuery(t, "services distributed table"): assert.AnError,
			},
			expectedError: "failed to create services distributed table",
		},
		{
			name: "operations distributed table error",
			failureConfig: clickhousetest.FailureConfig{
				distributedSchemaQuery(t, "operations distributed table"): assert.AnError,
			},
			expectedError: "failed to create operations distributed table",
		},
		{
			name: "trace id timestamps distributed table error",
			failureConfig: clickhousetest.FailureConfig{
				distributedSchemaQuery(t, "trace id timestamps distributed table"): assert.AnError,
			},
			expectedError: "failed to create trace id timestamps distributed table",
		},
		{
			name: "attribute metadata distributed table error",
			failureConfig: clickhousetest.FailureConfig{
				distributedSchemaQuery(t, "attribute metadata distributed table"): assert.AnError,
			},
			expectedError: "failed to create attribute metadata distributed table",
		},
		{
			name: "dependencies distributed table error",
			failureConfig: clickhousetest.FailureConfig{
				distributedSchemaQuery(t, "dependencies distributed table"): assert.AnError,
			},
			expectedError: "failed to create dependencies distributed table",
		},
		{
			name: "services materialized view error",
			failureConfig: clickhousetest.FailureConfig{
				distributedSchemaQuery(t, "services materialized view"): assert.AnError,
			},
			expectedError: "failed to create services materialized view",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := clickhousetest.NewServer(tt.failureConfig)
			defer srv.Close()

			cfg := Configuration{
				Protocol: "http",
				Addresses: []string{
					srv.Listener.Addr().String(),
				},
				DialTimeout:  1 * time.Second,
				CreateSchema: true,
				Distributed:  true,
				TableEngine:  localTableEngine,
			}

			f, err := NewFactory(context.Background(), cfg, telemetry.NoopSettings())
			require.ErrorContains(t, err, tt.expectedError)
			require.Nil(t, f)
		})
	}
}
