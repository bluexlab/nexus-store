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
	nexus "gitlab.com/navyx/nexus/nexus-store/pkg/proto/nexus_store"
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

		req := &pb.UploadDocumentRequest{
			Data: `{"test": "content"}`,
			Metadata: map[string]string{
				"Source":       "Test System",
				"ContentType":  "application/json",
				"CreatorEmail": "test@example.com",
				"CustomerName": "Test Customer",
				"Purpose":      "Testing",
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
		require.NoError(t, err)
		metadata := lo.Reduce(recs, func(agg map[string]string, item *dbsqlc.MetadataFindByDocumentIdRow, _ int) map[string]string {
			agg[item.Key] = item.Value
			return agg
		}, map[string]string{})
		require.Equal(t, req.Metadata, metadata)

		// Verify S3 storage was not used
		require.Empty(t, bundle.s3Storage.Client().(*storage.MockS3Client).Objects)
	})

	t.Run("Upload document with missing required metadata", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		req := &pb.UploadDocumentRequest{
			Data: "Test document content",
			Metadata: map[string]string{
				"Source": "Test System",
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

		req := &pb.UploadDocumentRequest{
			Data: "",
			Metadata: map[string]string{
				"Source":       "Test System",
				"ContentType":  "application/json",
				"CreatorEmail": "test@example.com",
				"CustomerName": "Test Customer",
				"Purpose":      "Testing",
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

func TestUploadFile(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	type testBundle struct {
		server    *_NexusStoreServer
		dbPool    *pgxpool.Pool
		s3Storage *storage.S3Storage
		logger    *slog.Logger
	}

	setup := func(t *testing.T) *testBundle {
		t.Helper()

		dbPool := testhelper.TestDB(ctx, t)
		s3Storage, err := storage.NewS3InMemoryStorage(ctx, "test-bucket")
		require.NoError(t, err)

		server := NewNexusStoreServer(
			WithDataSource(dbPool),
			WithStorage(s3Storage),
		)

		bundle := &testBundle{
			server:    server,
			dbPool:    dbPool,
			s3Storage: s3Storage,
			logger:    testhelper.Logger(t),
		}

		return bundle
	}

	t.Run("Successful upload file", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		req := &pb.UploadFileRequest{
			FileName:   "test.txt",
			Data:       []byte("Test file content"),
			AutoExpire: true,
			Metadata: map[string]string{
				"Source":       "Test System",
				"ContentType":  "text/plain",
				"CreatorEmail": "test@example.com",
				"CustomerName": "Test Customer",
				"Purpose":      "Testing",
			},
		}

		resp, err := bundle.server.UploadFile(ctx, req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Key)

		// Verify S3 storage was used
		mockS3Client := bundle.s3Storage.Client().(*storage.MockS3Client)
		require.Len(t, mockS3Client.Objects, 1)
		require.Equal(t, []byte("Test file content"), mockS3Client.Objects[resp.Key])

		// Verify metadata was stored
		metadata, err := bundle.s3Storage.GetMetadata(ctx, resp.Key)
		require.NoError(t, err)
		require.Equal(t, req.Metadata, metadata)
		require.Equal(t, "test.txt", metadata["FileName"])

		// Verify auto-expire tag
		require.Equal(t, map[string]string{storage.TagAutoExpire: "1"}, mockS3Client.Tags[resp.Key])
	})

	t.Run("Upload file with missing metadata", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		req := &pb.UploadFileRequest{
			FileName: "test.txt",
			Data:     []byte("Test file content"),
			Metadata: map[string]string{
				"Source": "Test System",
				// Missing other required fields
			},
		}

		_, err := bundle.server.UploadFile(ctx, req)
		require.Error(t, err)
		st, ok := status.FromError(err)
		require.True(t, ok)
		require.Equal(t, codes.InvalidArgument, st.Code())
		require.Contains(t, st.Message(), "Missing required field for metadata")

		// Verify S3 storage was not used
		require.Empty(t, bundle.s3Storage.Client().(*storage.MockS3Client).Objects)
	})

	t.Run("Upload file with empty data", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		req := &pb.UploadFileRequest{
			FileName: "test.txt",
			Data:     []byte{},
			Metadata: map[string]string{
				"Source":       "Test System",
				"ContentType":  "text/plain",
				"CreatorEmail": "test@example.com",
				"CustomerName": "Test Customer",
				"Purpose":      "Testing",
			},
		}

		_, err := bundle.server.UploadFile(ctx, req)
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
		bundle := setup(t)

		s3Object := fixture.InsertS3Object(t, ctx, querier, bundle.dbPool)

		id, err := s3Object.ID.Value()
		require.NoError(t, err)

		req := &pb.AddMetadataRequest{
			Key: fmt.Sprintf("object-%v", id),
			NewMetadata: map[string]string{
				"Source":       "Test System",
				"CreatorEmail": "test@example.com",
				"test key":     "test value",
			},
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		response, err := server.AddMetadata(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, response)
	})

	t.Run("Successful add metadata with document", func(t *testing.T) {
		bundle := setup(t)

		jsonContent := `{"test": "content"}`
		document := fixture.InsertDocument(t, ctx, querier, bundle.dbPool, jsonContent)

		id, err := document.ID.Value()
		require.NoError(t, err)

		req := &pb.AddMetadataRequest{
			Key: fmt.Sprintf("document-%v", id),
			NewMetadata: map[string]string{
				"Source":       "Test System",
				"CreatorEmail": "test@example.com",
				"test key":     "test value",
			},
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		response, err := server.AddMetadata(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, response)
	})

	t.Run("Duplicate metadata", func(t *testing.T) {
		bundle := setup(t)

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
			NewMetadata: map[string]string{
				"Source":       "Test System",
				"CreatorEmail": "test@example.com",
				"test key":     "test value",
				"ExistingKey":  "ExistingValue",
			},
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		response, err := server.AddMetadata(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, response)
	})

	t.Run("Invalid request - no key provided", func(t *testing.T) {
		bundle := setup(t)

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))

		req := &pb.AddMetadataRequest{
			NewMetadata: map[string]string{
				"Source": "Test System",
			},
		}

		_, err := server.AddMetadata(ctx, req)

		require.Error(t, err)
		require.Equal(t, codes.InvalidArgument, status.Code(err))
		require.Contains(t, err.Error(), "Key is required")
	})

	t.Run("Invalid request - invalid key prefix", func(t *testing.T) {
		bundle := setup(t)

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
		bundle := setup(t)

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
		bundle := setup(t)

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

func TestList(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	type testBundle struct {
		dbPool     *pgxpool.Pool
		dataSource dbaccess.DataSource
		logger     *slog.Logger
		server     *_NexusStoreServer
	}

	setup := func(t *testing.T) (*testBundle, []pgtype.UUID) {
		t.Helper()

		dbPool := testhelper.TestDB(ctx, t)
		bundle := &testBundle{
			dbPool:     dbPool,
			dataSource: dbPool,
			logger:     testhelper.Logger(t),
		}

		bundle.server = NewNexusStoreServer(
			WithDataSource(bundle.dataSource),
		)

		// Insert test data
		document1 := fixture.InsertDocument(t, ctx, querier, bundle.dbPool, `{"test": "content1"}`)
		document2 := fixture.InsertDocument(t, ctx, querier, bundle.dbPool, `{"test": "content2"}`)
		s3Object1 := fixture.InsertS3Object(t, ctx, querier, bundle.dbPool)
		s3Object2 := fixture.InsertS3Object(t, ctx, querier, bundle.dbPool)

		// Insert metadata for documents and objects
		fixture.InsertMetadata(t, ctx, querier, bundle.dbPool, &dbsqlc.MetadataInsertBatchParams{
			DocumentIds: []pgtype.UUID{
				document1.ID,
				document2.ID,
				document1.ID,
				document2.ID,
				{Valid: false},
				{Valid: false},
				{Valid: false},
				{Valid: false},
			},
			ObjectIds: []pgtype.UUID{
				{Valid: false},
				{Valid: false},
				{Valid: false},
				{Valid: false},
				s3Object1.ID,
				s3Object2.ID,
				s3Object1.ID,
				s3Object2.ID,
			},
			Keys:   []string{"Source", "Source", "Purpose", "Purpose", "Source", "Source", "Purpose", "Purpose"},
			Values: []string{"System1", "System1", "Purpose1", "Purpose2", "System1", "System2", "Purpose1", "Purpose2"},
		})

		return bundle, []pgtype.UUID{document1.ID, document2.ID, s3Object1.ID, s3Object2.ID}
	}

	t.Run("List documents and objects with metadata filter", func(t *testing.T) {
		bundle, oids := setup(t)

		// Create a filter to match documents and objects with Source = System1
		filter := &pb.ListFilter{
			Type: pb.ListFilter_EQUAL,
			Entries: map[string]string{
				"Source": "System1",
			},
		}

		resp, err := bundle.server.List(ctx, &pb.ListRequest{Filter: filter})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Error)

		// Check that we got the correct number of results
		require.Len(t, resp.DocumentIds, 2)
		require.Len(t, resp.FileIds, 1)
		require.Len(t, resp.Documents, 0)

		// Convert UUIDs to strings for comparison
		doc1ID, err := pgtypeUUIDToString(oids[0])
		require.NoError(t, err)
		obj1ID, err := pgtypeUUIDToString(oids[2])
		require.NoError(t, err)

		// Check that we got the correct document and object
		require.Contains(t, resp.DocumentIds, doc1ID)
		require.Contains(t, resp.FileIds, obj1ID)
	})

	t.Run("List documents and objects with two entries in filter", func(t *testing.T) {
		bundle, oids := setup(t)
		filter := &pb.ListFilter{
			Type: pb.ListFilter_EQUAL,
			Entries: map[string]string{
				"Source":  "System1",
				"Purpose": "Purpose1",
			},
		}

		resp, err := bundle.server.List(ctx, &pb.ListRequest{Filter: filter})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Error)

		// Check that we got the correct number of results
		require.Len(t, resp.DocumentIds, 1)
		require.Len(t, resp.FileIds, 1)
		require.Len(t, resp.Documents, 0)

		// Check that we got the correct document and object
		doc1ID, _ := pgtypeUUIDToString(oids[0])
		obj1ID, _ := pgtypeUUIDToString(oids[2])
		require.Contains(t, resp.DocumentIds, doc1ID)
		require.Contains(t, resp.FileIds, obj1ID)
	})

	t.Run("List documents and objects with nested AND filter", func(t *testing.T) {
		bundle, oids := setup(t)
		filter := &pb.ListFilter{
			Type: pb.ListFilter_AND_GROUP,
			SubFilters: []*pb.ListFilter{
				{
					Type: pb.ListFilter_EQUAL,
					Entries: map[string]string{
						"Source": "System1",
					},
				},
				{
					Type: pb.ListFilter_EQUAL,
					Entries: map[string]string{
						"Purpose": "Purpose1",
					},
				},
			},
		}

		resp, err := bundle.server.List(ctx, &pb.ListRequest{Filter: filter})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Error)

		// Check that we got the correct number of results
		require.Len(t, resp.DocumentIds, 1)
		require.Len(t, resp.FileIds, 1)
		require.Len(t, resp.Documents, 0)

		// Check that we got the correct document and object
		doc1ID, _ := pgtypeUUIDToString(oids[0])
		obj1ID, _ := pgtypeUUIDToString(oids[2])
		require.Contains(t, resp.DocumentIds, doc1ID)
		require.Contains(t, resp.FileIds, obj1ID)
	})

	t.Run("List documents and objects with AND and Contains filter", func(t *testing.T) {
		bundle, oids := setup(t)
		filter := &pb.ListFilter{
			Type: pb.ListFilter_AND_GROUP,
			SubFilters: []*pb.ListFilter{
				{
					Type: pb.ListFilter_EQUAL,
					Entries: map[string]string{
						"Source": "System1",
					},
				},
				{
					Type: pb.ListFilter_CONTAINS,
					Entries: map[string]string{
						"Purpose": "ose1",
					},
				},
			},
		}

		resp, err := bundle.server.List(ctx, &pb.ListRequest{Filter: filter})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Error)

		// Check that we got the correct number of results
		require.Len(t, resp.DocumentIds, 1)
		require.Len(t, resp.FileIds, 1)
		require.Len(t, resp.Documents, 0)

		// Check that we got the correct document and object
		doc1ID, _ := pgtypeUUIDToString(oids[0])
		obj1ID, _ := pgtypeUUIDToString(oids[2])
		require.Contains(t, resp.DocumentIds, doc1ID)
		require.Contains(t, resp.FileIds, obj1ID)
	})

	t.Run("List documents and objects with AND and NOT filter", func(t *testing.T) {
		bundle, oids := setup(t)
		filter := &pb.ListFilter{
			Type: pb.ListFilter_AND_GROUP,
			SubFilters: []*pb.ListFilter{
				{
					Type: pb.ListFilter_EQUAL,
					Entries: map[string]string{
						"Source": "System1",
					},
				},
				{
					Type: pb.ListFilter_NOT_EQUAL,
					Entries: map[string]string{
						"Purpose": "Purpose2",
					},
				},
			},
		}

		resp, err := bundle.server.List(ctx, &pb.ListRequest{Filter: filter})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Error)

		// Check that we got the correct number of results
		require.Len(t, resp.DocumentIds, 1)
		require.Len(t, resp.FileIds, 1)
		require.Len(t, resp.Documents, 0)

		// Check that we got the correct document and object
		doc1ID, _ := pgtypeUUIDToString(oids[0])
		obj1ID, _ := pgtypeUUIDToString(oids[2])
		require.Contains(t, resp.DocumentIds, doc1ID)
		require.Contains(t, resp.FileIds, obj1ID)
	})

	t.Run("List documents and objects with nested AND and OR filter", func(t *testing.T) {
		bundle, oids := setup(t)
		filter := &pb.ListFilter{
			Type: pb.ListFilter_OR_GROUP,
			SubFilters: []*pb.ListFilter{
				{
					Type: pb.ListFilter_AND_GROUP,
					SubFilters: []*pb.ListFilter{
						{
							Type: pb.ListFilter_EQUAL,
							Entries: map[string]string{
								"Source": "System1",
							},
						},
						{
							Type: pb.ListFilter_EQUAL,
							Entries: map[string]string{
								"Purpose": "Purpose1",
							},
						},
					},
				},
				{
					Type: pb.ListFilter_AND_GROUP,
					SubFilters: []*pb.ListFilter{
						{
							Type: pb.ListFilter_EQUAL,
							Entries: map[string]string{
								"Source": "System2",
							},
						},
						{
							Type: pb.ListFilter_EQUAL,
							Entries: map[string]string{
								"Purpose": "Purpose2",
							},
						},
					},
				},
			},
		}

		resp, err := bundle.server.List(ctx, &pb.ListRequest{Filter: filter})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Error)

		// Check that we got the correct number of results
		require.Len(t, resp.DocumentIds, 1)
		require.Len(t, resp.FileIds, 2)
		require.Len(t, resp.Documents, 0)

		// Check that we got the correct document and object
		doc1ID, _ := pgtypeUUIDToString(oids[0])
		obj1ID, _ := pgtypeUUIDToString(oids[2])
		obj2ID, _ := pgtypeUUIDToString(oids[3])
		require.Equal(t, resp.DocumentIds, []string{doc1ID})
		require.ElementsMatch(t, resp.FileIds, []string{obj1ID, obj2ID})
	})

	t.Run("List with invalid filter", func(t *testing.T) {
		bundle, _ := setup(t)

		filter := &pb.ListFilter{
			Type: pb.ListFilter_AND_GROUP,
		}

		resp, err := bundle.server.List(ctx, &pb.ListRequest{Filter: filter})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.NotNil(t, resp.Error)
		require.Equal(t, nexus.Error_NEXUS_STORE_INVALID_PARAMETER, resp.Error.Code)
	})

	t.Run("List with no results", func(t *testing.T) {
		bundle, _ := setup(t)

		// Create a filter that won't match any documents or objects
		filter := &pb.ListFilter{
			Type: pb.ListFilter_EQUAL,
			Entries: map[string]string{
				"Source": "NonExistentSystem",
			},
		}

		req := &pb.ListRequest{
			Filter: filter,
		}

		resp, err := bundle.server.List(ctx, req)
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Error)
		require.Empty(t, resp.DocumentIds)
		require.Empty(t, resp.FileIds)
		require.Empty(t, resp.Documents)
	})
}
