package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"gitlab.com/navyx/nexus/nexus-store/db"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess"
	"gitlab.com/navyx/nexus/nexus-store/pkg/migrate"
)

func main() {
	loadEnv()

	logger := setupLogger()

	migrateCmd := os.Args[1]

	step, dryRun, help := parseFlags()

	if help {
		printHelp()
		os.Exit(0)
	}

	switch migrateCmd {
	case "up":
		migrateUp(logger, dryRun, step)
	case "down":
		migrateDown(logger, dryRun, step)
	default:
		printHelp()
		os.Exit(0)
	}
}

func loadEnv() {
	if _, err := os.Stat(".env"); err == nil {
		godotenv.Load(".env")
	}
}

func setupLogger() *slog.Logger {
	handler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{})
	logger := slog.New(handler)
	slog.SetDefault(logger)
	return logger
}

func parseFlags() (int, bool, bool) {
	var step int
	var help bool
	dryRun := false

	fs := flag.NewFlagSet("ExampleBoolFunc", flag.ContinueOnError)
	fs.IntVar(&step, "s", 0, "Number of migrations to run")
	fs.BoolVar(&dryRun, "d", false, "Run migrations in dry run mode")
	fs.BoolVar(&help, "h", false, "Display help message")
	fs.Parse(os.Args[2:])

	return step, dryRun, help
}

func printHelp() {
	helpMessage := `
Usage: migrate [command] [options]

Commands:
up       Run migrations up
down     Run migrations down

Options:
-s    Number of migrations to run. Default is runs all migrations for up, and 1 for down.
-d    Run migrations in dry run mode. Default is false. Used for testing migrations.
-h    Display this help message
`
	os.Stderr.WriteString(helpMessage)
}

func connectToDatabase(ctx context.Context, logger *slog.Logger) *pgxpool.Pool {
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

	return pool
}

func migrateUp(logger *slog.Logger, dryRun bool, step int) {
	logger.Info("Running migrations up", "steps", step, "dryRun", dryRun)
	ctx := context.Background()

	pool := connectToDatabase(ctx, logger)
	defer pool.Close()

	if err := runMigration(ctx, pool, migrate.DirectionUp, step, dryRun); err != nil {
		logger.Error("Failed to migrate database", "error", err.Error())
		os.Exit(2)
	}

	logger.Info("Database migrated")
}

func migrateDown(logger *slog.Logger, dryRun bool, step int) {
	logger.Info("Running migrations down", "steps", step, "dryRun", dryRun)
	ctx := context.Background()

	pool := connectToDatabase(ctx, logger)
	defer pool.Close()

	if err := runMigration(ctx, pool, migrate.DirectionDown, step, dryRun); err != nil {
		logger.Error("Failed to migrate database", "error", err.Error())
		os.Exit(2)
	}

	logger.Info("Database migrated")
}

func runMigration(ctx context.Context, dataSource dbaccess.DataSource, direction migrate.Direction, step int, dryRun bool) error {
	_, err := migrate.New(dataSource, migrate.Config{
		Migrations: db.MigrationFS,
	}).Migrate(ctx, direction, &migrate.MigrateOpts{MaxSteps: step, DryRun: dryRun})
	return err
}
