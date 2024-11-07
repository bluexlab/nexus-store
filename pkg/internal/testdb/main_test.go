package testdb_test

import (
	"testing"

	"gitlab.com/navyx/nexus/nexus-store/pkg/internal/testhelper"
)

func TestMain(m *testing.M) {
	testhelper.WrapTestMainWithoutDB(m)
}
