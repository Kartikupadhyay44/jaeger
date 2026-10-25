CREATE MATERIALIZED VIEW IF NOT EXISTS services_mv TO {{ if .TargetTable }}{{ .TargetTable }}{{ else }}services{{ end }} AS
SELECT
    service_name AS name
FROM
    {{ if .SourceTable }}{{ .SourceTable }}{{ else }}spans{{ end }}
GROUP BY
    service_name;
