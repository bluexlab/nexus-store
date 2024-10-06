package migrate_test

import (
	"testing"

	"gitlab.com/navyx/nexus/nexus-store/pkg/internal/testhelper"
	"gitlab.com/navyx/nexus/nexus-store/pkg/migrate/test_migrations"
)

func TestMain(m *testing.M) {
	testhelper.WrapTestMain(m, test_migrations.MigrationFS)
}
