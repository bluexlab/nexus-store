package migrate_test

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"testing"

	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess/dbsqlc"
	"gitlab.com/navyx/nexus/nexus-store/pkg/internal/testhelper"
	"gitlab.com/navyx/nexus/nexus-store/pkg/migrate"
	"gitlab.com/navyx/nexus/nexus-store/pkg/migrate/test_migrations"
	"gitlab.com/navyx/nexus/nexus-store/pkg/util"
)

//nolint:gochecknoglobals
const (
	DirectionDown = migrate.DirectionDown
	DirectionUp   = migrate.DirectionUp
)

var (
	querier = dbsqlc.New()

	migrations = migrate.GetMigrations(test_migrations.MigrationFS)

	migrationsMaxVersion = migrations[len(migrations)-1].Version

	testVersions = []*migrate.Migration{
		{
			Version: migrationsMaxVersion + 1,
			SQLUp:   "CREATE TABLE test_table(id bigserial PRIMARY KEY);",
			SQLDown: "DROP TABLE test_table;",
		},
		{
			Version: migrationsMaxVersion + 2,
			SQLUp:   "ALTER TABLE test_table ADD COLUMN name varchar(200); CREATE INDEX idx_test_table_name ON test_table(name);",
			SQLDown: "DROP INDEX idx_test_table_name; ALTER TABLE test_table DROP COLUMN name;",
		},
	}

	migrationsWithtestVersionsMap        = migrate.ValidateAndInit(append(migrations, testVersions...))
	migrationsWithTestVersionsMaxVersion = migrationsMaxVersion + len(testVersions)
)

type MigrateOpts = migrate.MigrateOpts

func TestMigrator(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	type testBundle struct {
		dbPool     *pgxpool.Pool
		dataSource dbaccess.DataSource
		logger     *slog.Logger
	}

	setup := func(t *testing.T) (*migrate.Migrator, *testBundle) {
		t.Helper()

		dbPool := testhelper.TestDB(ctx, t)
		bundle := &testBundle{
			dbPool:     dbPool,
			dataSource: dbPool,
			logger:     testhelper.Logger(t),
		}

		migrator := migrate.New(
			bundle.dataSource,
			migrate.Config{
				Logger:     bundle.logger,
				Migrations: test_migrations.MigrationFS,
			},
		)

		migrator.SetMigrations(migrationsWithtestVersionsMap)

		return migrator, bundle
	}

	t.Run("AllVersions", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		migrations := migrator.AllVersions()
		require.Equal(t, seqOneTo(migrationsWithTestVersionsMaxVersion), util.MapSlice(migrations, migrationToInt))
	})

	t.Run("MigrateDownDefault", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		// update to latest version
		migrator.Migrate(ctx, migrate.DirectionUp, &migrate.MigrateOpts{})

		// Defaults to one step when migrating down.
		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{})
		require.NoError(t, err)
		require.Equal(t, DirectionDown, res.Direction)
		t.Logf("versions: %v", res.Versions)
		require.Equal(t, []int{migrationsMaxVersion + 2}, util.MapSlice(res.Versions, migrateVersionToInt))

		_, err = bundle.dataSource.Exec(ctx, "SELECT * FROM test_table")
		require.NoError(t, err)

		// Run once more to go down one more step

		res, err = migrator.Migrate(ctx, DirectionDown, &MigrateOpts{})
		require.NoError(t, err)
		require.Equal(t, DirectionDown, res.Direction)
		require.Equal(t, []int{migrationsMaxVersion + 1}, util.MapSlice(res.Versions, migrateVersionToInt))

		_, err = bundle.dataSource.Exec(ctx, "SELECT name FROM test_table")
		require.Error(t, err)

	})

	t.Run("MigrateDownAfterUp", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsWithTestVersionsMaxVersion}, util.MapSlice(res.Versions, migrateVersionToInt))
	})

	t.Run("MigrateDownWithMaxSteps", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{MaxSteps: 2})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsWithTestVersionsMaxVersion, migrationsWithTestVersionsMaxVersion - 1},
			util.MapSlice(res.Versions, migrateVersionToInt))

		migrations, err := querier.MigrationGetAll(ctx, bundle.dataSource)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsWithTestVersionsMaxVersion-2),
			util.MapSlice(migrations, driverMigrationToInt))

		_, err = bundle.dataSource.Exec(ctx, "SELECT name FROM test_table")
		require.Error(t, err)
	})

	t.Run("MigrateDownWithPool", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		// We don't actually migrate anything (max steps = -1) because doing so
		// would mess with the test database, but this still runs most code to
		// check that the function generally works.
		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{MaxSteps: -1})
		require.NoError(t, err)
		require.Equal(t, []int{}, util.MapSlice(res.Versions, migrateVersionToInt))

		migrations, err := querier.MigrationGetAll(ctx, bundle.dataSource)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsMaxVersion),
			util.MapSlice(migrations, driverMigrationToInt))
	})

	t.Run("MigrateDownWithTargetVersion", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{TargetVersion: 4})
		require.NoError(t, err)
		require.Equal(t,
			seqBetween(migrationsWithTestVersionsMaxVersion, 4+1),
			util.MapSlice(res.Versions, migrateVersionToInt))

		migrations, err := querier.MigrationGetAll(ctx, bundle.dataSource)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(4),
			util.MapSlice(migrations, driverMigrationToInt))

		_, err = bundle.dataSource.Exec(ctx, "SELECT name FROM test_table")
		require.Error(t, err)
	})

	t.Run("MigrateDownWithTargetVersionMinusOne", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{TargetVersion: -1})
		require.NoError(t, err)
		require.Equal(t, seqToOne(migrationsWithTestVersionsMaxVersion),
			util.MapSlice(res.Versions, migrateVersionToInt))

		_, err = bundle.dataSource.Exec(ctx, "SELECT name FROM _migrate")
		require.Error(t, err)
	})

	t.Run("MigrateDownWithTargetVersionInvalid", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		// migration doesn't exist
		{
			_, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{TargetVersion: 77})
			require.EqualError(t, err, "version 77 is not a valid migration version")
		}

		// migration exists but already applied
		{
			_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{TargetVersion: 3})
			require.EqualError(t, err, "version 3 is not in target list of valid migrations to apply")
		}
	})

	t.Run("MigrateDownDryRun", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
		require.NoError(t, err)

		res, err := migrator.Migrate(ctx, DirectionDown, &MigrateOpts{DryRun: true})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsWithTestVersionsMaxVersion}, util.MapSlice(res.Versions, migrateVersionToInt))

		// Migrate down returned a result above for a migration that was
		// removed, but because we're in a dry run, the database still shows
		// this version.
		migrations, err := querier.MigrationGetAll(ctx, bundle.dataSource)

		require.NoError(t, err)
		require.Equal(t,
			seqOneTo(migrationsWithTestVersionsMaxVersion),
			util.MapSlice(migrations, driverMigrationToInt))
	})

	t.Run("GetVersion", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		{
			migrateVersion, err := migrator.GetVersion(migrationsWithTestVersionsMaxVersion)
			require.NoError(t, err)
			require.Equal(t, migrationsWithTestVersionsMaxVersion, migrateVersion.Version)
		}

		{
			_, err := migrator.GetVersion(99_999)
			availableVersions := seqOneTo(migrationsWithTestVersionsMaxVersion)
			require.EqualError(t, err, fmt.Sprintf("migration %d not found (available versions: %v)", 99_999, availableVersions))
		}
	})

	t.Run("MigrateNilOpts", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		res, err := migrator.Migrate(ctx, DirectionUp, nil)
		require.NoError(t, err)
		require.Equal(t,
			seqBetween(migrationsMaxVersion+1, migrationsWithTestVersionsMaxVersion),
			util.MapSlice(res.Versions, migrateVersionToInt))
	})

	t.Run("MigrateUpDefault", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		// Run an initial time
		{
			res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
			require.NoError(t, err)
			require.Equal(t, DirectionUp, res.Direction)
			require.Equal(t, []int{migrationsWithTestVersionsMaxVersion - 1, migrationsWithTestVersionsMaxVersion},
				util.MapSlice(res.Versions, migrateVersionToInt))

			migrations, err := querier.MigrationGetAll(ctx, bundle.dataSource)

			require.NoError(t, err)
			require.Equal(t, seqOneTo(migrationsWithTestVersionsMaxVersion),
				util.MapSlice(migrations, driverMigrationToInt))

			_, err = bundle.dataSource.Exec(ctx, "SELECT * FROM test_table")
			require.NoError(t, err)
		}

		// Run once more to verify idempotency
		{
			res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{})
			require.NoError(t, err)
			require.Equal(t, DirectionUp, res.Direction)
			require.Equal(t, []int{}, util.MapSlice(res.Versions, migrateVersionToInt))

			migrations, err := querier.MigrationGetAll(ctx, bundle.dataSource)
			require.NoError(t, err)
			require.Equal(t, seqOneTo(migrationsWithTestVersionsMaxVersion),
				util.MapSlice(migrations, driverMigrationToInt))

			_, err = bundle.dataSource.Exec(ctx, "SELECT * FROM test_table")
			require.NoError(t, err)
		}
	})

	t.Run("MigrateUpWithMaxSteps", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: 1})
		require.NoError(t, err)
		require.Equal(t, []int{migrationsWithTestVersionsMaxVersion - 1},
			util.MapSlice(res.Versions, migrateVersionToInt))

		migrations, err := querier.MigrationGetAll(ctx, bundle.dataSource)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsWithTestVersionsMaxVersion-1),
			util.MapSlice(migrations, driverMigrationToInt))

		// Column `name` is only added in the second test version.
		_, err = bundle.dataSource.Exec(ctx, "SELECT name FROM test_table")
		require.Error(t, err)

		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, pgerrcode.UndefinedColumn, pgErr.Code)
	})

	t.Run("MigrateUpWithPool", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)

		// We don't actually migrate anything (max steps = -1) because doing so
		// would mess with the test database, but this still runs most code to
		// check that the function generally works.
		res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{MaxSteps: -1})
		require.NoError(t, err)
		require.Equal(t, []int{}, util.MapSlice(res.Versions, migrateVersionToInt))

		migrations, err := querier.MigrationGetAll(ctx, bundle.dataSource)

		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsMaxVersion),
			util.MapSlice(migrations, driverMigrationToInt))
	})

	t.Run("MigrateUpWithTargetVersion", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{TargetVersion: migrationsWithTestVersionsMaxVersion})
		require.NoError(t, err)
		require.Equal(t,
			seqBetween(migrationsMaxVersion+1, migrationsWithTestVersionsMaxVersion),
			util.MapSlice(res.Versions, migrateVersionToInt))

		migrations, err := querier.MigrationGetAll(ctx, bundle.dataSource)

		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsWithTestVersionsMaxVersion), util.MapSlice(migrations, driverMigrationToInt))
	})

	t.Run("MigrateUpWithTargetVersionInvalid", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		// migration doesn't exist
		{
			_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{TargetVersion: 77})
			require.EqualError(t, err, "version 77 is not a valid migration version")
		}

		// migration exists but already applied
		{
			_, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{TargetVersion: 3})
			require.EqualError(t, err, "version 3 is not in target list of valid migrations to apply")
		}
	})

	t.Run("MigrateUpDryRun", func(t *testing.T) {
		t.Parallel()

		migrator, bundle := setup(t)
		defer bundle.dbPool.Close()

		res, err := migrator.Migrate(ctx, DirectionUp, &MigrateOpts{DryRun: true})
		require.NoError(t, err)
		require.Equal(t, DirectionUp, res.Direction)
		require.Equal(t, []int{migrationsWithTestVersionsMaxVersion - 1, migrationsWithTestVersionsMaxVersion},
			util.MapSlice(res.Versions, migrateVersionToInt))

		// Migrate up returned a result above for migrations that were applied,
		// but because we're in a dry run, the database still shows the test
		// migration versions not applied.
		migrations, err := querier.MigrationGetAll(ctx, bundle.dataSource)
		require.NoError(t, err)
		require.Equal(t, seqOneTo(migrationsMaxVersion),
			util.MapSlice(migrations, driverMigrationToInt))
	})
}

// A command returning an error aborts the transaction. This is a shortcut to
// execute a command in a subtransaction so that we can verify an error, but
// continue to use the original transaction.
// func dbExecError(ctx context.Context, tx pgx.Tx, sql string) error {
// 	return dbaccess.WithTx(ctx, dbaccess.New(tx), func(ctx context.Context, exec dbaccess.TxAccessor) error {
// 		_, err := exec.Exec(ctx, sql)
// 		return err
// 	})
// }

func migrateVersionToInt(version migrate.MigrateVersion) int { return version.Version }
func driverMigrationToInt(r *dbsqlc.Migration) int           { return int(r.Version) }
func migrationToInt(migration migrate.Migration) int         { return migration.Version }

func seqBetween(from int, to int) []int {
	if to < from {
		seq := seqBetween(to, from)
		slices.Reverse(seq)
		return seq
	}

	count := to - from + 1
	seq := make([]int, count)

	for i := 0; i < count; i++ {
		seq[i] = i + from
	}

	return seq
}

func seqOneTo(max int) []int {
	return seqBetween(1, max)
}

func seqToOne(max int) []int {
	return seqBetween(max, 1)
}
