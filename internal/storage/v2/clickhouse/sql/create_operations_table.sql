CREATE TABLE IF NOT EXISTS
    {{ if .TableName }}{{ .TableName }}{{ else }}operations{{ end }} (
        service_name String,
        name String,
        span_kind String
    ) ENGINE = {{ .AggregatingMergeTree }}
ORDER BY
    (service_name, span_kind);
