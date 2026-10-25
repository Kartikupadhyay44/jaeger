CREATE TABLE IF NOT EXISTS {{ .TableName }} AS {{ .LocalTableName }} ENGINE = Distributed({{ .Cluster }}, {{ .Database }}, {{ .LocalTableName }}{{ if .ShardingKey }}, {{ .ShardingKey }}{{ end }})
