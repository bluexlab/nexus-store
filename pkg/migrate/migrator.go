package migrate

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/samber/lo"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess/dbsqlc"
	"gitlab.com/navyx/nexus/nexus-store/pkg/util"
)

var querier = dbsqlc.New()

type Migration struct {
	// SQLDown is the s SQL for the migration's down direction.
	SQLDown string

	// SQLUp is the s SQL for the migration's up direction.
	SQLUp string

	// Version is the integer version number of this migration.
	Version int
}

func GetMigrations(migrations fs.FS) []*Migration {
	return mustMigrationsFromFS(migrations)
}

type Config struct {
	// STDOUT by default
	Logger     *slog.Logger
	Migrations fs.FS
}

// Migrator is a database migration tool which can run up or down migrations in order to establish the schema.
type Migrator struct {
	logger     *slog.Logger
	dataSource dbaccess.DataSource
	migrations map[int]*Migration // allows us to inject test migrations
}

func New(ds dbaccess.DataSource, config Config) *Migrator {
	logger := config.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
			Level: slog.LevelWarn,
		}))
	}

	return &Migrator{
		logger:     logger,
		dataSource: ds,
		migrations: ValidateAndInit(mustMigrationsFromFS(config.Migrations)),
	}
}

// MigrateOpts are options for a migrate operation.
type MigrateOpts struct {
	DryRun bool

	// MaxSteps sets the maximum number of migrations.
	// Up migrations are unlimited by default.
	// Down migrations default to one step (use TargetVersion -1 for unlimited).
	// Set to -1 to skip migrations (useful for testing).
	MaxSteps int

	// TargetVersion sets the migration version to apply.
	// For up migrations, it includes the target version (e.g., 0 to 3 applies 1, 2, and 3).
	// For down migrations, it excludes the target version (e.g., 5 to 3 applies 5 and 4, leaving 3).
	// Set to -1 for all down migrations (removes the schema).
	TargetVersion int
}

type Direction string

const (
	DirectionDown Direction = "down"
	DirectionUp   Direction = "up"
)

type MigrateResult struct {
	// up or down
	Direction Direction

	// Versions added (up) or removed (down) in this run.
	Versions []MigrateVersion
}

// MigrateVersion is the result for a single applied migration.
type MigrateVersion struct {
	// Duration is the time taken to apply the migration.
	Duration time.Duration

	// SQL is the applied migration SQL.
	SQL string

	// Version is the applied migration version.
	Version int
}

// AllVersions gets information on all known migration versions.
func (m *Migrator) AllVersions() []Migration {
	migrations := lo.Values(m.migrations)
	slices.SortFunc(migrations, func(v1, v2 *Migration) int { return v1.Version - v2.Version })
	return util.MapSlice(migrations, func(m *Migration) Migration { return *m })
}

// SetMigrations is a test helper to inject a custom set of migrations.
func (m *Migrator) SetMigrations(migrations map[int]*Migration) {
	m.migrations = migrations
}

// GetVersion retrieves information about a specific migration version.
// Returns an error if the version does not exist.
func (m *Migrator) GetVersion(version int) (Migration, error) {
	migration, ok := m.migrations[version]
	if !ok {
		availableVersions := lo.Keys(m.migrations)
		slices.Sort(availableVersions)
		return Migration{}, fmt.Errorf("migration %d not found (available versions: %v)", version, availableVersions)
	}

	return *migration, nil
}

// Migrate updates the database up or down. The opts parameter is optional.
//
// Defaults: applies all up migrations and one down migration.
// Use MigrateOpts.MaxSteps or MigrateOpts.TargetVersion to change this.
// Setting MigrateOpts.TargetVersion to -1 removes all down migrations.
//
// Example:
//
//	res, err := migrator.Migrate(ctx, migrate.DirectionUp, nil)
func (m *Migrator) Migrate(ctx context.Context, direction Direction, opts *MigrateOpts) (*MigrateResult, error) {
	switch direction {
	case DirectionDown:
		return m.migrateDown(ctx, direction, opts)
	case DirectionUp:
		return m.migrateUp(ctx, direction, opts)
	}

	panic("invalid direction: " + direction)
}

// migrateDown runs down migrations.
func (m *Migrator) migrateDown(ctx context.Context, direction Direction, opts *MigrateOpts) (*MigrateResult, error) {
	existingMigrations, err := m.existingMigrations(ctx)
	if err != nil {
		return nil, err
	}
	existingMigrationsMap := lo.Associate(existingMigrations,
		func(m *dbsqlc.Migration) (int, struct{}) { return int(m.Version), struct{}{} })

	targetMigrations := maps.Clone(m.migrations)
	for version := range targetMigrations {
		if _, ok := existingMigrationsMap[version]; !ok {
			delete(targetMigrations, version)
		}
	}

	sortedTargetMigrations := lo.Values(targetMigrations)
	slices.SortFunc(sortedTargetMigrations, func(a, b *Migration) int { return b.Version - a.Version }) // reverse order

	res, err := m.applyMigrations(ctx, direction, opts, sortedTargetMigrations)
	if err != nil {
		return nil, err
	}

	// If no work was done, exit early.
	// This prevents errors when a zero-migrated database is no-op downmigrated.
	if len(res.Versions) < 1 {
		return res, nil
	}

	// Migration version 1 is special-cased.
	// If downmigrated, the `_migration` table is gone, so there's nothing to delete.
	if slices.ContainsFunc(res.Versions, func(v MigrateVersion) bool { return v.Version == 1 }) {
		return res, nil
	}

	if !opts.DryRun {
		if _, err := querier.MigrationDeleteByVersionMany(ctx, m.dataSource, util.MapSlice(res.Versions, migrateVersionToInt64)); err != nil {
			return nil, fmt.Errorf("error deleting migration rows for versions %+v: %w", res.Versions, err)
		}
	}

	return res, nil
}

// migrateUp runs up migrations.
func (m *Migrator) migrateUp(ctx context.Context, direction Direction, opts *MigrateOpts) (*MigrateResult, error) {
	existingMigrations, err := m.existingMigrations(ctx)
	if err != nil {
		return nil, err
	}

	targetMigrations := maps.Clone(m.migrations)
	for _, migrateRow := range existingMigrations {
		delete(targetMigrations, int(migrateRow.Version))
	}

	sortedTargetMigrations := lo.Values(targetMigrations)
	slices.SortFunc(sortedTargetMigrations, func(a, b *Migration) int { return a.Version - b.Version })

	res, err := m.applyMigrations(ctx, direction, opts, sortedTargetMigrations)
	if err != nil {
		return nil, err
	}

	if opts == nil || !opts.DryRun {
		if _, err := querier.MigrationInsertMany(ctx, m.dataSource, util.MapSlice(res.Versions, migrateVersionToInt64)); err != nil {
			return nil, fmt.Errorf("error inserting migration rows for versions %+v: %w", res.Versions, err)
		}
	}

	return res, nil
}

// Applies migration directions, walking through and applying each target migration.
func (m *Migrator) applyMigrations(ctx context.Context, direction Direction, opts *MigrateOpts, sortedTargetMigrations []*Migration) (*MigrateResult, error) {
	if opts == nil {
		opts = &MigrateOpts{}
	}

	var maxSteps int
	switch {
	case opts.MaxSteps != 0:
		maxSteps = opts.MaxSteps
	case direction == DirectionDown && opts.TargetVersion == 0:
		maxSteps = 1
	}

	switch {
	case maxSteps < 0:
		sortedTargetMigrations = []*Migration{}
	case maxSteps > 0:
		sortedTargetMigrations = sortedTargetMigrations[0:min(maxSteps, len(sortedTargetMigrations))]
	}

	if opts.TargetVersion > 0 {
		if _, ok := m.migrations[opts.TargetVersion]; !ok {
			return nil, fmt.Errorf("version %d is not a valid migration version", opts.TargetVersion)
		}

		targetIndex := slices.IndexFunc(sortedTargetMigrations, func(b *Migration) bool { return b.Version == opts.TargetVersion })
		if targetIndex == -1 {
			return nil, fmt.Errorf("version %d is not in target list of valid migrations to apply", opts.TargetVersion)
		}

		// Replace target list with migrations up to target index.
		// The list is sorted by migration direction.
		// For down migrations, the list is already reversed and this truncates it to include the most recent migration down to the target.
		sortedTargetMigrations = sortedTargetMigrations[0 : targetIndex+1]

		if direction == DirectionDown && len(sortedTargetMigrations) > 0 {
			sortedTargetMigrations = sortedTargetMigrations[0 : len(sortedTargetMigrations)-1]
		}
	}

	res := &MigrateResult{Direction: direction, Versions: make([]MigrateVersion, 0, len(sortedTargetMigrations))}

	// Exit early if there's nothing to do.
	if len(sortedTargetMigrations) < 1 {
		m.logger.InfoContext(ctx, "No migrations to apply")
		return res, nil
	}

	for _, versionBundle := range sortedTargetMigrations {
		var sql string
		switch direction {
		case DirectionDown:
			sql = versionBundle.SQLDown
		case DirectionUp:
			sql = versionBundle.SQLUp
		}

		var duration time.Duration

		if !opts.DryRun {
			start := time.Now()
			_, err := m.dataSource.Exec(ctx, sql)
			if err != nil {
				return nil, fmt.Errorf("error applying version %03d [%s]: %w",
					versionBundle.Version, strings.ToUpper(string(direction)), err)
			}
			duration = time.Since(start)
		}

		m.logger.InfoContext(ctx, "Applied migration",
			slog.String("direction", string(direction)),
			slog.Bool("dry_run", opts.DryRun),
			slog.Duration("duration", duration),
			slog.Int("version", versionBundle.Version),
		)

		res.Versions = append(res.Versions, MigrateVersion{Duration: duration, SQL: sql, Version: versionBundle.Version})
	}

	return res, nil
}

// Get migrations already run in the database.
// Uses a subtransaction to handle the case when the `_migration` table doesn't exist.
// This prevents the main transaction from being aborted on an unsuccessful check.
func (m *Migrator) existingMigrations(ctx context.Context) ([]*dbsqlc.Migration, error) {
	exists, err := querier.TableExists(ctx, m.dataSource, "_migration")
	if err != nil {
		return nil, fmt.Errorf("error checking if `%s` exists: %w", "_migration", err)
	}
	if !exists {
		return nil, nil
	}

	migrations, err := querier.MigrationGetAll(ctx, m.dataSource)
	if err != nil {
		return nil, fmt.Errorf("error getting existing migrations: %w", err)
	}

	return migrations, nil
}

// Reads migration bundles from the file system.
// Typically reads from the embedded FS in the `migration/` subdirectory.
func migrationsFromFS(migrationFS fs.FS) ([]*Migration, error) {
	const subdir = "migration"

	var (
		bundles    []*Migration
		lastBundle *Migration
	)

	err := fs.WalkDir(migrationFS, subdir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return fmt.Errorf("error walking FS: %w", err)
		}

		// Gets called one with the name of the subdirectory. Continue.
		if path == subdir {
			return nil
		}

		// Invoked with the full path name.
		// Strips `migration/` from the front to get a parsable name.
		if !strings.HasPrefix(path, subdir) {
			return fmt.Errorf("expected path %q to start with subdir %q", path, subdir)
		}
		name := path[len(subdir)+1:]

		versionStr, _, _ := strings.Cut(name, "_")

		version, err := strconv.Atoi(versionStr)
		if err != nil {
			return fmt.Errorf("error parsing version %q: %w", versionStr, err)
		}

		// This works because `fs.WalkDir` guarantees lexical order,
		// ensuring all 001* files appear before 002* files, and so on.
		if lastBundle == nil || lastBundle.Version != version {
			lastBundle = &Migration{Version: version}
			bundles = append(bundles, lastBundle)
		}

		file, err := migrationFS.Open(path)
		if err != nil {
			return fmt.Errorf("error opening file %q: %w", path, err)
		}

		contents, err := io.ReadAll(file)
		if err != nil {
			return fmt.Errorf("error reading file %q: %w", path, err)
		}

		switch {
		case strings.HasSuffix(name, ".down.sql"):
			lastBundle.SQLDown = string(contents)
		case strings.HasSuffix(name, ".up.sql"):
			lastBundle.SQLUp = string(contents)
		default:
			return fmt.Errorf("file %q should end with either '.down.sql' or '.up.sql'", name)
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return bundles, nil
}

func mustMigrationsFromFS(migrationFS fs.FS) []*Migration {
	bundles, err := migrationsFromFS(migrationFS)
	if err != nil {
		panic(err)
	}
	return bundles
}

// Validates and fully initializes a set of migrations and checks for missing fields or duplicated version numbers.
func ValidateAndInit(versions []*Migration) map[int]*Migration {
	lastVersion := 0
	migrations := make(map[int]*Migration, len(versions))

	for _, versionBundle := range versions {
		if versionBundle.SQLDown == "" {
			panic(fmt.Sprintf("version bundle should specify Down: %+v", versionBundle))
		}
		if versionBundle.SQLUp == "" {
			panic(fmt.Sprintf("version bundle should specify Up: %+v", versionBundle))
		}
		if versionBundle.Version == 0 {
			panic(fmt.Sprintf("version bundle should specify Version: %+v", versionBundle))
		}

		if _, ok := migrations[versionBundle.Version]; ok {
			panic(fmt.Sprintf("duplicate version: %03d", versionBundle.Version))
		}
		if versionBundle.Version <= lastVersion {
			panic(fmt.Sprintf("versions should be ascending; current: %03d, last: %03d", versionBundle.Version, lastVersion))
		}
		if versionBundle.Version > lastVersion+1 {
			panic(fmt.Sprintf("versions shouldn't skip a sequence number; current: %03d, last: %03d", versionBundle.Version, lastVersion))
		}

		lastVersion = versionBundle.Version
		migrations[versionBundle.Version] = versionBundle
	}

	return migrations
}

func migrateVersionToInt64(version MigrateVersion) int64 { return int64(version.Version) }
