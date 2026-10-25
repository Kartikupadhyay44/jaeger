CREATE TABLE
    IF NOT EXISTS {{ if .TableName }}{{ .TableName }}{{ else }}services{{ end }} (name String) ENGINE = {{ .AggregatingMergeTree }}
ORDER BY
    (name);
