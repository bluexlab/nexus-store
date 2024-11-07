package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	nexusstore "gitlab.com/navyx/nexus/nexus-store"
	"gitlab.com/navyx/nexus/nexus-store/db"
	"gitlab.com/navyx/nexus/nexus-store/pkg/migrate"
)

func main() {
	// Load environment variables from .env file if exists
	if _, err := os.Stat(".env"); err == nil {
		godotenv.Load(".env")
	}

	logger := initLogger()

	if len(os.Args) > 1 && os.Args[1] == "migrate" {
		migrateUp(logger)
		os.Exit(0)
		return
	}

	app := &App{}
	app.Run()
}

func initLogger() *slog.Logger {
	// Get log level from environment variable, default to "info"
	logLevel := strings.ToLower(os.Getenv("LOG_LEVEL"))
	level := slog.LevelDebug
	switch logLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}

	// Create Logger based on environment
	var logger *slog.Logger
	if os.Getenv("DEV") != "" {
		handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: level})
		logger = slog.New(handler)
	} else {
		opts := slog.HandlerOptions{
			AddSource:   true,
			Level:       level,
			ReplaceAttr: unixTimestampHandler,
		}
		handler := slog.NewJSONHandler(os.Stdout, &opts)
		logger = slog.New(handler).With(
			"service", nexusstore.ServiceName,
			"version", nexusstore.GetVersion(),
		)
	}

	// Set as default logger
	slog.SetDefault(logger)

	return logger
}

func migrateUp(logger *slog.Logger) {
	logger.Info("Running migrations up")
	ctx := context.Background()

	var databaseUrl string
	if databaseUrl = os.Getenv("DATABASE_URL"); databaseUrl == "" {
		host := os.Getenv("DATABASE_HOST")
		port := os.Getenv("DATABASE_PORT")
		user := os.Getenv("DATABASE_USER")
		password := os.Getenv("DATABASE_PASSWORD")
		dbName := os.Getenv("DATABASE_NAME")

		if host == "" || port == "" || user == "" || password == "" || dbName == "" {
			logger.Error("Database connection details are not fully specified in environment variables")
			os.Exit(1)
		}

		databaseUrl = fmt.Sprintf("postgres://%s:%s@%s:%s/%s", user, password, host, port, dbName)
	}

	dbConfig, err := pgxpool.ParseConfig(databaseUrl)
	if err != nil {
		logger.Error("Failed to parse connection string", "err", err)
		os.Exit(1)
	}

	logger.Info("Connecting to database", "url", databaseUrl)

	pool, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		logger.Error("Failed to connect to database", "err", err)
		os.Exit(1)
	}

	defer pool.Close()

	if _, err := migrate.New(pool, migrate.Config{Migrations: db.MigrationFS}).Migrate(ctx, migrate.DirectionUp, &migrate.MigrateOpts{}); err != nil {
		logger.Error("Failed to migrate database", "error", err.Error())
		os.Exit(2)
	}

	logger.Info("maos-core Database migrated")
}

func unixTimestampHandler(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey {
		return slog.Int64("ts", a.Value.Time().UnixNano()/1e6) // millisecond precision
	}
	return a
}
