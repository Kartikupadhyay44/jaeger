CREATE TABLE
    IF NOT EXISTS {{ if .TableName }}{{ .TableName }}{{ else }}dependencies{{ end }} (
        timestamp DateTime64 (9),
        dependencies_json String
    ) ENGINE = {{ .MergeTree }}
PARTITION BY
    toDate(timestamp)
ORDER BY
    (timestamp);
