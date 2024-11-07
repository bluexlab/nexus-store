package testdb_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.com/navyx/nexus/nexus-store/db"
	"gitlab.com/navyx/nexus/nexus-store/pkg/internal/testdb"
)

func TestManager_AcquireMultiple(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	manager, err := testdb.NewManager(ctx, db.MigrationFS)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(ctx)

	pool0, err := manager.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Release(pool0)

	checkSchemaForPool(ctx, t, pool0, "test-")

	pool1, err := manager.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Release(pool1)

	checkSchemaForPool(ctx, t, pool1, "test-")
	manager.Release(pool0)

	//  ensure we get db 0 back on subsequent acquire since it was released to the pool:
	pool0Again, err := manager.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Release(pool0Again)

	checkSchemaForPool(ctx, t, pool0Again, "test-")
}

func TestManager_ReleaseTwice(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	manager, err := testdb.NewManager(ctx, db.MigrationFS)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(ctx)

	tdb0, err := manager.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Release(tdb0)

	selectOne(ctx, t, tdb0)

	// explicitly close p0's pgxpool.Pool before Release to ensure it can be fully
	// reused after release:
	manager.Release(tdb0)
	t.Log("RELEASING P0")

	// Calling release twice should be a no-op:
	t.Log("RELEASING P0 AGAIN")
	manager.Release(tdb0)

	//  acquire a new pool and ensure it can be used:
	t.Log("ACQUIRING P1")
	tdb1, err := manager.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Release(tdb1)

	selectOne(ctx, t, tdb1)
	manager.Release(tdb1)

	t.Log("CALLING MANAGER CLOSE MANUALLY")
	manager.Close(ctx)
}

func checkSchemaForPool(ctx context.Context, t *testing.T, p *pgxpool.Pool, expectedPrefix string) {
	t.Helper()

	var currentSchema string
	if err := p.QueryRow(ctx, "SELECT current_schema()").Scan(&currentSchema); err != nil {
		t.Fatal(err)
	}

	if !strings.HasPrefix(currentSchema, expectedPrefix) {
		t.Errorf("expected database name to begin with %q, got %q", expectedPrefix, currentSchema)
	}
}

func selectOne(ctx context.Context, t *testing.T, p *pgxpool.Pool) {
	t.Helper()

	var one int
	if err := p.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatal(err)
	}

	if one != 1 {
		t.Errorf("expected 1, got %d", one)
	}
}
