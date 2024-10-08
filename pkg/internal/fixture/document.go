package fixture

import (
	"context"
	"testing"

	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess/dbsqlc"
)

// InsertDocument inserts a new document into the database and returns it
func InsertDocument(t *testing.T, ctx context.Context, querier dbsqlc.Querier, source dbaccess.DataSource, content string) *dbsqlc.Document {
	t.Helper()

	document, err := querier.DocumentInsert(ctx, source, []byte(content))

	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}

	return document
}
