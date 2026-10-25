CREATE MATERIALIZED VIEW IF NOT EXISTS trace_id_timestamps_mv
TO {{ if .TargetTable }}{{ .TargetTable }}{{ else }}trace_id_timestamps{{ end }}
AS
SELECT
    trace_id,
    min(start_time) AS start,
    max(start_time) AS end
FROM {{ if .SourceTable }}{{ .SourceTable }}{{ else }}spans{{ end }}
GROUP BY trace_id;
