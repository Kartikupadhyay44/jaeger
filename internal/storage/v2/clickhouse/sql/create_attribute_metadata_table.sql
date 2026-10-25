CREATE TABLE
    IF NOT EXISTS {{ if .TableName }}{{ .TableName }}{{ else }}attribute_metadata{{ end }} (
        attribute_key String,
        type String,  -- 'bool', 'double', 'int', 'str', 'bytes', 'map', 'slice'
        level String  -- 'resource', 'scope', 'span'
    ) ENGINE = {{ .AggregatingMergeTree }}
ORDER BY (attribute_key, type, level)
