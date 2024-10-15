package grpc

import (
	"context"
	"fmt"
	"log/slog"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/samber/lo"
	"github.com/stretchr/testify/require"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess/dbsqlc"
	"gitlab.com/navyx/nexus/nexus-store/pkg/internal/fixture"
	"gitlab.com/navyx/nexus/nexus-store/pkg/internal/testhelper"
	pb "gitlab.com/navyx/nexus/nexus-store/pkg/proto/nexus_store"
	"gitlab.com/navyx/nexus/nexus-store/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestUploadDocument(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	type testBundle struct {
		dbPool     *pgxpool.Pool
		dataSource dbaccess.DataSource
		logger     *slog.Logger
		server     *_NexusStoreServer
		s3Storage  *storage.S3Storage
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		dbPool := testhelper.TestDB(ctx, t)
		s3Storage, err := storage.NewS3InMemoryStorage(ctx, "test-bucket")
		require.NoError(t, err)

		bundle := &testBundle{
			dbPool:     dbPool,
			dataSource: dbPool,
			logger:     testhelper.Logger(t),
			s3Storage:  s3Storage,
		}

		bundle.server = NewNexusStoreServer(
			WithDataSource(bundle.dataSource),
			WithStorage(s3Storage),
		)

		return bundle
	}

	t.Run("Successful upload document", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		defer bundle.dbPool.Close()

		req := &pb.UploadDocumentRequest{
			Data: `{"test": "content"}`,
			Metadata: []*pb.MetadataEntry{
				{Key: "SourceSystem", Value: "Test System"},
				{Key: "CreatorEmail", Value: "test@example.com"},
				{Key: "CreatorName", Value: "Test Creator"},
				{Key: "CustomerEmail", Value: "customer@example.com"},
				{Key: "CustomerName", Value: "Test Customer"},
				{Key: "Purpose", Value: "Testing"},
			},
		}

		resp, err := bundle.server.UploadDocument(ctx, req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Id)

		// Verify the document was inserted
		documentUUID, err := uuid.Parse(resp.Id)
		require.NoError(t, err)

		pgUUID := pgtype.UUID{Bytes: documentUUID, Valid: true}
		document, err := querier.DocumentFindById(ctx, bundle.dataSource, pgUUID)
		require.NoError(t, err)
		require.Equal(t, req.Data, string(document.Content))

		// Verify metadata was inserted
		recs, err := querier.MetadataFindByDocumentId(ctx, bundle.dataSource, pgUUID)
		metadata := lo.Reduce(recs, func(agg map[string]string, item *dbsqlc.MetadataFindByDocumentIdRow, _ int) map[string]string {
			agg[item.Key] = item.Value
			return agg
		}, map[string]string{})
		require.NoError(t, err)
		require.Len(t, metadata, len(req.Metadata))
		require.Equal(t, lo.Reduce(req.Metadata, func(agg map[string]string, item *pb.MetadataEntry, _ int) map[string]string {
			agg[item.Key] = item.Value
			return agg
		}, map[string]string{}), metadata)

		// Verify S3 storage was not used
		require.Empty(t, bundle.s3Storage.Client().(*storage.MockS3Client).Objects)
	})

	t.Run("Upload document with missing required metadata", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		defer bundle.dbPool.Close()

		req := &pb.UploadDocumentRequest{
			Data: "Test document content",
			Metadata: []*pb.MetadataEntry{
				{Key: "SourceSystem", Value: "Test System"},
				// Missing other required fields
			},
		}

		_, err := bundle.server.UploadDocument(ctx, req)
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
		require.Contains(t, st.Message(), "Missing required field for metadata")

		// Verify S3 storage was not used
		require.Empty(t, bundle.s3Storage.Client().(*storage.MockS3Client).Objects)
	})

	t.Run("Upload document with empty data", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)
		defer bundle.dbPool.Close()

		req := &pb.UploadDocumentRequest{
			Data: "",
			Metadata: []*pb.MetadataEntry{
				{Key: "SourceSystem", Value: "Test System"},
				{Key: "CreatorEmail", Value: "test@example.com"},
				{Key: "CreatorName", Value: "Test Creator"},
				{Key: "CustomerEmail", Value: "customer@example.com"},
				{Key: "CustomerName", Value: "Test Customer"},
				{Key: "Purpose", Value: "Testing"},
			},
		}

		_, err := bundle.server.UploadDocument(ctx, req)
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
		require.Contains(t, st.Message(), "Data is required")

		// Verify S3 storage was not used
		require.Empty(t, bundle.s3Storage.Client().(*storage.MockS3Client).Objects)
	})
}

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

		s3Object := fixture.InsertS3Object(t, ctx, querier, bundle.dbPool)

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

		s3Object := fixture.InsertS3Object(t, ctx, querier, bundle.dbPool)
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
