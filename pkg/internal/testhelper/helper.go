// Package testhelper contains shared testing utilities for tests
// throughout the rest of the project.
package testhelper

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"gitlab.com/navyx/nexus/nexus-store/db"
	"gitlab.com/navyx/nexus/nexus-store/pkg/internal/testdb"
	"go.uber.org/goleak"
	//nolint:depguard
)

var (
	dbManager *testdb.Manager //nolint:gochecknoglobals

	// Maximum connections for the pool.
	// If changed, also update the number of databases in `testdbman`.
	dbPoolMaxConns = int32(max(4, runtime.NumCPU())) //nolint:gochecknoglobals
)

func Logger(tb testing.TB) *slog.Logger {
	tb.Helper()

	if os.Getenv("DEBUG") == "1" || os.Getenv("DEBUG") == "true" {
		return NewLogger(tb, &slog.HandlerOptions{Level: slog.LevelDebug})
	}

	return NewLogger(tb, nil)
}

func LoggerWarn(tb testing.TB) *slog.Logger {
	tb.Helper()
	return NewLogger(tb, &slog.HandlerOptions{Level: slog.LevelWarn})
}

// TestDB acquires a dedicated test database for the test duration.
// If an error occurs, the test fails.
// The test database is returned to the pool at the end, and the pgxpool is closed.
func TestDB(ctx context.Context, tb testing.TB) *pgxpool.Pool {
	tb.Helper()

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	testPool, err := dbManager.Acquire(ctx)
	if err != nil {
		tb.Fatalf("Failed to acquire pool for test DB: %v", err)
	}
	tb.Cleanup(func() {
		dbManager.Release(testPool)
	})

	return testPool
}

var ignoredKnownGoroutineLeaks = []goleak.Option{ //nolint:gochecknoglobals
	// This goroutine has a 500 ms uninterruptable sleep that may still be running
	// when the test suite finishes, causing a failure. This might be a pgx issue:
	// https://github.com/jackc/pgx/issues/1641
	goleak.IgnoreTopFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).backgroundHealthCheck"),

	// Similar to the above, it may be in a sleep when the program finishes, and there's little we can do about it.
	goleak.IgnoreAnyFunction("github.com/jackc/pgx/v5/pgxpool.(*Pool).triggerHealthCheck.func1"),

	// This goroutine is started by the glog package and is not stopped by the time the test suite finishes.
	goleak.IgnoreAnyFunction("github.com/golang/glog.(*loggingT).flushDaemon"),
}

// WrapTestMain performs common setup and teardown tasks for all packages.
// It configures a test database manager on setup
// and checks for goroutine leaks on teardown.
func WrapTestMain(m *testing.M, migrationsFS fs.FS) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := loadEnvFile()
	if err != nil {
		panic(err)
	}

	if migrationsFS == nil {
		migrationsFS = db.MigrationFS
	}

	manager, err := testdb.NewManager(ctx, migrationsFS)
	if err != nil {
		panic(err)
	}

	dbManager = manager
	status := m.Run()

	dbManager.Close(ctx)

	if status == 0 {
		if err := goleak.Find(ignoredKnownGoroutineLeaks...); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: Errors on successful test run: %v\n", err)
			status = 1
		}
	}

	os.Exit(status)
}

func WrapTestMainWithoutDB(m *testing.M) {
	err := loadEnvFile()
	if err != nil {
		panic(err)
	}

	status := m.Run()

	if status == 0 {
		if err := goleak.Find(ignoredKnownGoroutineLeaks...); err != nil {
			fmt.Fprintf(os.Stderr, "goleak: Errors on successful test run: %v\n", err)
			status = 1
		}
	}

	os.Exit(status)
}

func loadEnvFile() error {
	// Start from the directory of the test file
	_, b, _, _ := runtime.Caller(0)
	basepath := filepath.Dir(b)

	// Look for a go.mod file to identify the project root
	for {
		// Check for .env file
		envPath := filepath.Join(basepath, ".env")
		if _, err := os.Stat(envPath); err == nil {
			return godotenv.Load(envPath)
		}

		// Check for go.mod to identify project root
		if _, err := os.Stat(filepath.Join(basepath, "go.mod")); err == nil {
			// We've reached the project root without finding .env
			return nil
		}

		parent := filepath.Dir(basepath)
		if parent == basepath {
			// We've reached the filesystem root without finding go.mod or .env
			return nil
		}
		basepath = parent
	}
}

// A pool and mutex protected, lazily initialized by TestTx.
// Once open, the pool is never explicitly closed; it closes implicitly as the package tests finish.
var (
	dbPool   *pgxpool.Pool //nolint:gochecknoglobals
	dbPoolMu sync.RWMutex  //nolint:gochecknoglobals
)
