CREATE TABLE IF NOT EXISTS {{ if .TableName }}{{ .TableName }}{{ else }}trace_id_timestamps{{ end }}
(
    trace_id String,
    start SimpleAggregateFunction(min, DateTime64(9)),
    end SimpleAggregateFunction(max, DateTime64(9))
)
ENGINE = {{ .AggregatingMergeTree }}
ORDER BY (trace_id)
{{- if gt .TTLSeconds 0 }}
TTL end + INTERVAL {{ .TTLSeconds }} SECOND DELETE
{{- end }}
