package db

import "embed"

var (
	//go:embed migration/*.sql
	MigrationFS embed.FS
)
