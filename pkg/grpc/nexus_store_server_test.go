package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess/dbsqlc"
	"gitlab.com/navyx/nexus/nexus-store/pkg/internal/fixture"
	"gitlab.com/navyx/nexus/nexus-store/pkg/internal/testhelper"
	pb "gitlab.com/navyx/nexus/nexus-store/pkg/proto/nexus_store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestAddMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	type testBundle struct {
		dbPool     *pgxpool.Pool
		dataSource dbaccess.DataSource
		logger     *slog.Logger
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		dbPool := testhelper.TestDB(ctx, t)
		bundle := &testBundle{
			dbPool:     dbPool,
			dataSource: dbPool,
			logger:     testhelper.Logger(t),
		}

		return bundle
	}

	t.Run("Successful add metadata with S3 object", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		defer bundle.dbPool.Close()

		objectKey := "test-file.txt"
		bucket := "test-bucket"
		s3Object := fixture.InsertS3Object(t, ctx, querier, bundle.dbPool, bucket, objectKey)

		id, err := s3Object.ID.Value()
		require.NoError(t, err)

		req := &pb.AddMetadataRequest{
			Key: fmt.Sprintf("object-%v", id),
			NewMetadata: []*pb.MetadataEntry{
				{
					Key:   "SourceSystem",
					Value: "Test System",
				},
				{
					Key:   "CreatorEmail",
					Value: "test@example.com",
				},
				{
					Key:   "CreatorName",
					Value: "Test Creator",
				},
				{
					Key:   "test key",
					Value: "test value",
				},
			},
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		response, err := server.AddMetadata(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, response)
	})

	t.Run("Successful add metadata with document", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		defer bundle.dbPool.Close()

		jsonContent := `{"test": "content"}`
		document := fixture.InsertDocument(t, ctx, querier, bundle.dbPool, jsonContent)

		id, err := document.ID.Value()
		require.NoError(t, err)

		req := &pb.AddMetadataRequest{
			Key: fmt.Sprintf("document-%v", id),
			NewMetadata: []*pb.MetadataEntry{
				{
					Key:   "SourceSystem",
					Value: "Test System",
				},
				{
					Key:   "CreatorEmail",
					Value: "test@example.com",
				},
				{
					Key:   "CreatorName",
					Value: "Test Creator",
				},
				{
					Key:   "test key",
					Value: "test value",
				},
			},
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		response, err := server.AddMetadata(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, response)
	})

	t.Run("Duplicate metadata", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		defer bundle.dbPool.Close()

		objectKey := "test-file.txt"
		bucket := "test-bucket"
		s3Object := fixture.InsertS3Object(t, ctx, querier, bundle.dbPool, bucket, objectKey)
		id, err := s3Object.ID.Value()
		require.NoError(t, err)

		var pgUUID pgtype.UUID
		pgUUID.Bytes = s3Object.ID.Bytes
		pgUUID.Valid = true

		fixture.InsertMetadata(t, ctx, querier, bundle.dbPool, &dbsqlc.MetadataInsertBatchParams{
			ObjectIds: []pgtype.UUID{pgUUID},
			Keys:      []string{"ExistingKey"},
			Values:    []string{"ExistingValue"},
		})

		req := &pb.AddMetadataRequest{
			Key: fmt.Sprintf("object-%v", id),
			NewMetadata: []*pb.MetadataEntry{
				{
					Key:   "SourceSystem",
					Value: "Test System",
				},
				{
					Key:   "CreatorEmail",
					Value: "test@example.com",
				},
				{
					Key:   "CreatorName",
					Value: "Test Creator",
				},
				{
					Key:   "test key",
					Value: "test value",
				},
				{
					Key:   "ExistingKey",
					Value: "ExistingValue",
				},
			},
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		response, err := server.AddMetadata(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, response)

	})

	t.Run("Invalid request - no key provided", func(t *testing.T) {
		t.Parallel()
		bundle := setup(t)
		defer bundle.dbPool.Close()

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))

		req := &pb.AddMetadataRequest{
			NewMetadata: []*pb.MetadataEntry{
				{
					Key:   "SourceSystem",
					Value: "Test System",
				},
			},
		}

		_, err := server.AddMetadata(ctx, req)

		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Contains(t, err.Error(), "Key is required")
	})

	t.Run("Invalid request - invalid key prefix", func(t *testing.T) {
		t.Parallel()
		bundle := setup(t)
		defer bundle.dbPool.Close()

		req := &pb.AddMetadataRequest{
			Key: "invalid-key",
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		_, err := server.AddMetadata(ctx, req)

		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Contains(t, err.Error(), "Invalid key prefix")
	})

	t.Run("Invalid request - object not found", func(t *testing.T) {
		t.Parallel()
		bundle := setup(t)
		defer bundle.dbPool.Close()

		id := uuid.New()
		req := &pb.AddMetadataRequest{
			Key: fmt.Sprintf("object-%v", id),
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		_, err := server.AddMetadata(ctx, req)

		require.Error(t, err)
		require.Equal(t, codes.Internal, status.Code(err))
		require.Contains(t, err.Error(), "Failed to find object")
	})

	t.Run("Invalid request - document not found", func(t *testing.T) {
		t.Parallel()
		bundle := setup(t)
		defer bundle.dbPool.Close()

		id := uuid.New()
		req := &pb.AddMetadataRequest{
			Key: fmt.Sprintf("document-%v", id),
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		_, err := server.AddMetadata(ctx, req)

		require.Error(t, err)
		require.Equal(t, codes.Internal, status.Code(err))
		require.Contains(t, err.Error(), "Failed to find document")
	})
}
