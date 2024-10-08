package fixture

import (
	"context"
	"testing"

	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess/dbsqlc"
)

func InsertMetadata(t *testing.T, ctx context.Context, querier dbsqlc.Querier, source dbaccess.DataSource, params *dbsqlc.MetadataInsertBatchParams) {
	t.Helper()

	err := querier.MetadataInsertBatch(ctx, source, params)
	if err != nil {
		t.Fatalf("Failed to insert metadata: %v", err)
	}
}
