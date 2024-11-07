package grpc

import (
	"testing"

	"gitlab.com/navyx/nexus/nexus-store/db"
	"gitlab.com/navyx/nexus/nexus-store/pkg/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.WrapTestMain(m, db.MigrationFS)
}
