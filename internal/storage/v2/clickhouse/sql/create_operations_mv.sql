CREATE MATERIALIZED VIEW IF NOT EXISTS operations_mv TO {{ if .TargetTable }}{{ .TargetTable }}{{ else }}operations{{ end }} AS
SELECT
    name,
    kind AS span_kind,
    service_name
FROM
    {{ if .SourceTable }}{{ .SourceTable }}{{ else }}spans{{ end }};
