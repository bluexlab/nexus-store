package test_migrations

import "embed"

var (
	//go:embed migration/*.sql
	MigrationFS embed.FS
)
