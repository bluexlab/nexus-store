package testdb

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.com/navyx/nexus/nexus-store/db"
	"gitlab.com/navyx/nexus/nexus-store/pkg/migrate"
)

// Manager handles isolated test database environments.
//
// It provides:
// - Creation and management of separate schemas for parallel testing
// - Connection pool management
// - Automatic resource cleanup
// - Database migration for each new schema
// - Methods for acquiring and releasing database connections
//
// This setup allows easy, fully isolated parallel tests.
type Manager struct {
	logger       *slog.Logger
	mu           sync.Mutex // protects nextDbNum
	mainPool     *pgxpool.Pool
	schemas      []string
	activePools  map[*pgxpool.Pool]bool
	closed       atomic.Bool
	migrationsFS fs.FS
}

// NewManager creates a new Manager instance.
func NewManager(ctx context.Context, migrationsFS fs.FS) (*Manager, error) {
	poolConfig, err := pgxpool.ParseConfig(getTestDatabaseURL())
	if err != nil {
		return nil, err
	}

	conn, err := pgxpool.NewWithConfig(ctx, poolConfig)
	if err != nil {
		return nil, err
	}
	if migrationsFS == nil {
		migrationsFS = &db.MigrationFS
	}

	return &Manager{
		logger:       slog.New(slog.NewTextHandler(os.Stdout, nil)),
		mainPool:     conn,
		activePools:  make(map[*pgxpool.Pool]bool),
		closed:       atomic.Bool{},
		migrationsFS: migrationsFS,
	}, nil
}

// Acquire returns a DBWithPool containing a pgxpool.Pool. It must be released after use.
func (m *Manager) Acquire(ctx context.Context) (*pgxpool.Pool, error) {
	schemaName := fmt.Sprintf("test-%s", getRandomSuffix())

	m.logger.Info("Creating schema", "schema", schemaName, "closed", m.closed.Load())
	_, err := m.mainPool.Exec(ctx, fmt.Sprintf(`CREATE SCHEMA "%s";`, schemaName))
	if err != nil {
		m.logger.Error("Failed to create schema", "schema", schemaName, "error", err)
		return nil, err
	}

	url := getTestDatabaseURL()
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		m.logger.Error("Failed to parse testing db url", "url", url, "error", err)
		return nil, err
	}

	config.ConnConfig.RuntimeParams["search_path"] = schemaName
	config.MaxConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		m.logger.Error("Failed to create pool for testing db url", "url", url, "error", err)
		return nil, err
	}
	m.addSchemasAndPool(schemaName, pool)

	_, err = migrate.New(pool, migrate.Config{
		Logger:     m.logger,
		Migrations: m.migrationsFS,
	}).Migrate(ctx, migrate.DirectionUp, &migrate.MigrateOpts{})
	if err != nil {
		m.logger.Error("Failed to migrate testing db url", "url", url, "error", err)
		return nil, err
	}

	return pool, err
}

func (m *Manager) Release(pool *pgxpool.Pool) {
	m.releasePool(pool)
}

// Close shuts down the Manager and all its databases and pools.
// It blocks until all resources are released and closed.
func (m *Manager) Close(ctx context.Context) {
	if !m.closed.CompareAndSwap(false, true) {
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	for pool := range m.activePools {
		pool.Close()
		delete(m.activePools, pool)
	}

	backgroundCtx := context.Background()
	for _, schemaName := range m.schemas {
		_, err := m.mainPool.Exec(backgroundCtx, fmt.Sprintf(`DROP SCHEMA "%s" CASCADE;`, schemaName))
		if err != nil {
			m.logger.Error("Failed to drop schema", "schema", schemaName, "error", err)
		}
	}
	m.schemas = []string{}

	m.mainPool.Close()
}

func (m *Manager) addSchemasAndPool(schemaName string, pool *pgxpool.Pool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.schemas = append(m.schemas, schemaName)
	m.activePools[pool] = true
}

func (m *Manager) releasePool(pool *pgxpool.Pool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	pool.Close()
	delete(m.activePools, pool)
}

func getTestDatabaseURL() string {
	if envURL := os.Getenv("TEST_DATABASE_URL"); envURL != "" {
		return envURL
	}
	return "postgres://localhost/nexus-store-test?sslmode=disable"
}

func getRandomSuffix() string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"
	var sb strings.Builder
	for i := 0; i < 8; i++ {
		randomIndex := rand.Intn(len(charset))
		sb.WriteByte(charset[randomIndex])
	}
	return sb.String()
}
