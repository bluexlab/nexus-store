package fixture

import (
	"context"
	"testing"

	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess/dbsqlc"
	"gitlab.com/navyx/nexus/nexus-store/pkg/util"
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

func InsertDocumentWithId(t *testing.T, ctx context.Context, querier dbsqlc.Querier, source dbaccess.DataSource, id string, content string) *dbsqlc.Document {
	t.Helper()

	uuid, err := util.StringToPgtypeUUID(id)
	if err != nil {
		t.Fatalf("Failed to parse UUID: %v", err)
	}

	rec, err := source.Query(ctx, "INSERT INTO documents (id, content) VALUES ($1, $2) RETURNING id, content, created_at", uuid, content)
	if err != nil {
		t.Fatalf("Failed to insert document: %v", err)
	}
	defer rec.Close()

	rec.Next()

	var document dbsqlc.Document
	err = rec.Scan(&document.ID, &document.Content, &document.CreatedAt)
	if err != nil {
		t.Fatalf("Failed to scan document: %v", err)
	}

	return &document
}
