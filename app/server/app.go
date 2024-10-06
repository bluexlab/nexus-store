package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/go-playground/validator"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kelseyhightower/envconfig"
)

const appName string = "nexus-store"
const bootstrapApiToken string = "bootstrap-token"

type LoggingQueryTracer struct {
	logger *slog.Logger
}

func (t *LoggingQueryTracer) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	traceId := ctx.Value("TRACEID")
	if strings.Contains(data.SQL, "api_tokens") {
		t.logger.Debug("Query started", "sql", data.SQL, "TraceId", traceId)
	} else {
		t.logger.Debug("Query started", "sql", data.SQL, "args", data.Args, "TraceId", traceId)
	}
	return ctx
}

func (t *LoggingQueryTracer) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	traceId := ctx.Value("TRACEID")
	t.logger.Debug("Query ended", "CommandTag", data.CommandTag, "duration", data.CommandTag, "TraceId", traceId)
}

type App struct {
	logger *slog.Logger
}

func (a *App) Run() {
	ctx := context.Background()

	config := a.loadConfig()

	// Connect to the database and create a new accessor
	dbConfig, err := pgxpool.ParseConfig(config.DatabaseUrl)
	if err != nil {
		a.logger.Error("Failed to parse connection string", "err", err)
		os.Exit(1)
	}

	dbConfig.ConnConfig.Tracer = &LoggingQueryTracer{a.logger}
	pool, err := pgxpool.NewWithConfig(ctx, dbConfig)
	if err != nil {
		a.logger.Error("Failed to connect to database", "err", err)
		os.Exit(1)
	}
	defer pool.Close()
}

func (a *App) loadConfig() Config {
	// Load environment variables into the struct
	var config Config
	if err := envconfig.Process("", &config); err != nil {
		a.logger.Error("Failed to process environment variables.", "err", err)
		os.Exit(1)
	}

	// Validate the struct
	validate := validator.New()
	if err := validate.Struct(config); err != nil {
		a.logger.Error("Validation failed", "err", err)
		os.Exit(1)
	}

	// construct database url from the environment variables
	if config.DatabaseUrl == "" {
		config.DatabaseUrl = fmt.Sprintf(
			"postgres://%s:%s@%s:%s/%s",
			config.DatabaseUser,
			config.DatabasePassword,
			config.DatabaseHost,
			config.DatabasePort,
			config.DatabaseName)
	}

	return config
}
