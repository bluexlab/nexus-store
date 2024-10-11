package fixture

import (
	"context"
	"testing"

	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess/dbsqlc"
)

// InsertS3Object inserts a new S3 object into the database and returns it
func InsertS3Object(t *testing.T, ctx context.Context, querier dbsqlc.Querier, source dbaccess.DataSource) *dbsqlc.S3Object {
	t.Helper()

	s3Object, err := querier.S3ObjectInsert(ctx, source)

	if err != nil {
		t.Fatalf("Failed to insert S3 object: %v", err)
	}

	return s3Object
}
