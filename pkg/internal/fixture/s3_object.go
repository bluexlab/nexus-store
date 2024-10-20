package fixture

import (
	"context"
	"testing"

	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess/dbsqlc"
	"gitlab.com/navyx/nexus/nexus-store/pkg/util"
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

func InsertS3ObjectWithId(t *testing.T, ctx context.Context, querier dbsqlc.Querier, source dbaccess.DataSource, id string) *dbsqlc.S3Object {
	t.Helper()

	uuid, err := util.StringToPgtypeUUID(id)
	if err != nil {
		t.Fatalf("Failed to parse UUID: %v", err)
	}

	rec, err := source.Query(ctx, "INSERT INTO s3_objects (id) VALUES ($1) RETURNING id, created_at", uuid)
	if err != nil {
		t.Fatalf("Failed to insert S3 object: %v", err)
	}
	defer rec.Close()

	if err != nil {
		t.Fatalf("Failed to insert S3 object: %v", err)
	}

	rec.Next()

	var s3Object dbsqlc.S3Object
	err = rec.Scan(&s3Object.ID, &s3Object.CreatedAt)
	if err != nil {
		t.Fatalf("Failed to scan S3 object: %v", err)
	}

	return &s3Object
}
