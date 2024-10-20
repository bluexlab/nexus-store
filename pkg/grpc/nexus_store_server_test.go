package grpc

import (
	"context"
	"encoding/json"
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
	"gitlab.com/navyx/nexus/nexus-store/pkg/util"
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

		req := &pb.AddDocumentRequest{
			Data: `{"test": "content"}`,
			Metadata: map[string]string{
				"Source":       "Test System",
				"ContentType":  "application/json",
				"CreatorEmail": "test@example.com",
				"CustomerName": "Test Customer",
				"Purpose":      "Testing",
			},
		}

		resp, err := bundle.server.AddDocument(ctx, req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Key)
		require.Empty(t, resp.Error)

		// Verify the document was inserted
		documentUUID, err := uuid.Parse(resp.Key)
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

		req := &pb.AddDocumentRequest{
			Data: "Test document content",
			Metadata: map[string]string{
				"Source": "Test System",
				// Missing other required fields
			},
		}

		resp, err := bundle.server.AddDocument(ctx, req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Error)
		require.Equal(t, nexus.Error_NEXUS_STORE_INVALID_PARAMETER, resp.Error.Code)

		// Verify S3 storage was not used
		require.Empty(t, bundle.s3Storage.Client().(*storage.MockS3Client).Objects)
	})

	t.Run("Upload document with empty data", func(t *testing.T) {
		t.Parallel()

		bundle := setup(t)

		req := &pb.AddDocumentRequest{
			Data: "",
			Metadata: map[string]string{
				"Source":       "Test System",
				"ContentType":  "application/json",
				"CreatorEmail": "test@example.com",
				"CustomerName": "Test Customer",
				"Purpose":      "Testing",
			},
		}

		resp, err := bundle.server.AddDocument(ctx, req)
		require.NoError(t, err)
		require.NotEmpty(t, resp.Error)
		require.Equal(t, nexus.Error_NEXUS_STORE_INVALID_PARAMETER, resp.Error.Code)
		require.Contains(t, resp.Error.Message, "Data is required")

		// Verify S3 storage was not used
		require.Empty(t, bundle.s3Storage.Client().(*storage.MockS3Client).Objects)
	})
}

func TestAddFile(t *testing.T) {
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

		req := &pb.AddFileRequest{
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

		resp, err := bundle.server.AddFile(ctx, req)
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

		req := &pb.AddFileRequest{
			FileName: "test.txt",
			Data:     []byte("Test file content"),
			Metadata: map[string]string{
				"Source": "Test System",
				// Missing other required fields
			},
		}

		_, err := bundle.server.AddFile(ctx, req)
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

		req := &pb.AddFileRequest{
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

		_, err := bundle.server.AddFile(ctx, req)
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
			Key: fmt.Sprintf("%v", id),
			Metadata: map[string]string{
				"Source":       "Test System",
				"CreatorEmail": "test@example.com",
				"test key":     "test value",
			},
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		response, err := server.AddMetadata(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, response)

		res, err := querier.MetadataFindByObjectIds(ctx, bundle.dataSource, []pgtype.UUID{s3Object.ID})
		require.NoError(t, err)
		require.Len(t, res, 1)
		var metadata map[string]string
		err = json.Unmarshal(res[0].Metadata, &metadata)
		require.NoError(t, err)
		require.Len(t, metadata, 3)
		require.Equal(t, req.Metadata, metadata)
	})

	t.Run("Successful add metadata with document", func(t *testing.T) {
		bundle := setup(t)

		jsonContent := `{"test": "content"}`
		document := fixture.InsertDocument(t, ctx, querier, bundle.dbPool, jsonContent)

		id, err := document.ID.Value()
		require.NoError(t, err)

		req := &pb.AddMetadataRequest{
			Key: fmt.Sprintf("%v", id),
			Metadata: map[string]string{
				"Source":       "Test System",
				"CreatorEmail": "test@example.com",
				"test key":     "test value",
			},
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		response, err := server.AddMetadata(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, response)

		res, err := querier.MetadataFindByDocumentIds(ctx, bundle.dataSource, []pgtype.UUID{document.ID})
		require.NoError(t, err)
		require.Len(t, res, 1)

		var metadata map[string]string
		err = json.Unmarshal(res[0].Metadata, &metadata)
		require.NoError(t, err)
		require.Len(t, metadata, 3)
		require.Equal(t, req.Metadata, metadata)
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
			ID:     pgUUID,
			Keys:   []string{"ExistingKey", "ImmutableKey"},
			Values: []string{"ExistingValue", "ImmutableValue"},
		})

		req := &pb.AddMetadataRequest{
			Key: fmt.Sprintf("%v", id),
			Metadata: map[string]string{
				"Source":       "Test System",
				"CreatorEmail": "test@example.com",
				"test key":     "test value",
				"ExistingKey":  "ExistingValue",
			},
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		response, err := server.AddMetadata(ctx, req)

		require.NoError(t, err)
		require.Empty(t, response.Error)

		res, err := querier.MetadataFindByObjectIds(ctx, bundle.dataSource, []pgtype.UUID{s3Object.ID})
		require.NoError(t, err)
		require.Len(t, res, 1)
		var metadata map[string]string
		err = json.Unmarshal(res[0].Metadata, &metadata)
		require.NoError(t, err)
		require.Len(t, metadata, 5)
		require.Equal(t, map[string]string{
			"Source":       "Test System",
			"CreatorEmail": "test@example.com",
			"test key":     "test value",
			"ExistingKey":  "ExistingValue",
			"ImmutableKey": "ImmutableValue",
		}, metadata)
	})

	t.Run("Invalid request - no key provided", func(t *testing.T) {
		bundle := setup(t)

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))

		req := &pb.AddMetadataRequest{
			Metadata: map[string]string{
				"Source": "Test System",
			},
		}

		resp, err := server.AddMetadata(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp.Error)
		require.Equal(t, nexus.Error_NEXUS_STORE_INVALID_PARAMETER, resp.Error.Code)
		require.Contains(t, resp.Error.Message, "Key is required")
	})

	t.Run("Invalid request - invalid key prefix", func(t *testing.T) {
		bundle := setup(t)

		req := &pb.AddMetadataRequest{
			Key: "invalid-key",
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		resp, err := server.AddMetadata(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp.Error)
		require.Equal(t, nexus.Error_NEXUS_STORE_INVALID_PARAMETER, resp.Error.Code)
		require.Contains(t, resp.Error.Message, "Invalid UUID")
	})

	t.Run("Invalid request - object not found", func(t *testing.T) {
		bundle := setup(t)

		id := uuid.New()
		req := &pb.AddMetadataRequest{
			Key: fmt.Sprintf("%v", id),
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		resp, err := server.AddMetadata(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp.Error)
		require.Equal(t, nexus.Error_NEXUS_STORE_INVALID_PARAMETER, resp.Error.Code)
		require.Contains(t, resp.Error.Message, "Object or document not found")
	})

	t.Run("Invalid request - document not found", func(t *testing.T) {
		bundle := setup(t)

		id := uuid.New()
		req := &pb.AddMetadataRequest{
			Key: fmt.Sprintf("%v", id),
		}

		server := NewNexusStoreServer(WithDataSource(bundle.dbPool))
		resp, err := server.AddMetadata(ctx, req)

		require.NoError(t, err)
		require.NotNil(t, resp.Error)
		require.Equal(t, nexus.Error_NEXUS_STORE_INVALID_PARAMETER, resp.Error.Code)
		require.Contains(t, resp.Error.Message, "Object or document not found")
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
		document1 := fixture.InsertDocumentWithId(t, ctx, querier, bundle.dbPool, "11111111-a176-48e8-8cea-0c8e6dee4de2", `{"test": "content1"}`)
		document2 := fixture.InsertDocumentWithId(t, ctx, querier, bundle.dbPool, "22222222-a176-48e8-8cea-0c8e6dee4de2", `{"test": "content2"}`)
		s3Object1 := fixture.InsertS3ObjectWithId(t, ctx, querier, bundle.dbPool, "33333333-a176-48e8-8cea-0c8e6dee4de2")
		s3Object2 := fixture.InsertS3ObjectWithId(t, ctx, querier, bundle.dbPool, "44444444-a176-48e8-8cea-0c8e6dee4de2")

		// Insert metadata for documents and objects
		fixture.InsertMetadata(t, ctx, querier, bundle.dbPool, &dbsqlc.MetadataInsertBatchParams{
			ID:     document1.ID,
			Keys:   []string{"Source", "Purpose", "Unique"},
			Values: []string{"System1", "Purpose1", "true"},
		})

		// Insert metadata for document2
		fixture.InsertMetadata(t, ctx, querier, bundle.dbPool, &dbsqlc.MetadataInsertBatchParams{
			ID:     document2.ID,
			Keys:   []string{"Source", "Purpose"},
			Values: []string{"System1", "Purpose2"},
		})

		// Insert metadata for s3Object1
		fixture.InsertMetadata(t, ctx, querier, bundle.dbPool, &dbsqlc.MetadataInsertBatchParams{
			ID:     s3Object1.ID,
			Keys:   []string{"Source", "Purpose"},
			Values: []string{"System1", "Purpose1"},
		})

		// Insert metadata for s3Object2
		fixture.InsertMetadata(t, ctx, querier, bundle.dbPool, &dbsqlc.MetadataInsertBatchParams{
			ID:     s3Object2.ID,
			Keys:   []string{"Source", "Purpose"},
			Values: []string{"System2", "Purpose2"},
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
		require.Len(t, resp.Documents, 2)
		require.Len(t, resp.Files, 1)

		// Convert UUIDs to strings for comparison
		doc1ID, err := util.PgtypeUUIDToString(oids[0])
		require.NoError(t, err)
		obj1ID, err := util.PgtypeUUIDToString(oids[2])
		require.NoError(t, err)

		// Check that we got the correct document and object
		require.Contains(t, lo.Map(resp.Documents, func(item *pb.ListItem, _ int) string { return item.Key }), doc1ID)
		require.Contains(t, lo.Map(resp.Files, func(item *pb.ListItem, _ int) string { return item.Key }), obj1ID)
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
		require.Len(t, resp.Documents, 1)
		require.Len(t, resp.Files, 1)

		// Check that we got the correct document and object
		doc1ID, _ := util.PgtypeUUIDToString(oids[0])
		obj1ID, _ := util.PgtypeUUIDToString(oids[2])
		require.Contains(t, lo.Map(resp.Documents, func(item *pb.ListItem, _ int) string { return item.Key }), doc1ID)
		require.Contains(t, lo.Map(resp.Files, func(item *pb.ListItem, _ int) string { return item.Key }), obj1ID)
		require.Empty(t, resp.Documents[0].Metadata)
		require.Empty(t, resp.Documents[0].Data)
		require.Empty(t, resp.Files[0].Metadata)
	})

	t.Run("List with two entries in filter including content and metadata", func(t *testing.T) {
		bundle, oids := setup(t)
		filter := &pb.ListFilter{
			Type: pb.ListFilter_EQUAL,
			Entries: map[string]string{
				"Source":  "System1",
				"Purpose": "Purpose1",
			},
		}

		resp, err := bundle.server.List(ctx, &pb.ListRequest{Filter: filter, ReturnDocumentContent: true, ReturnMetadata: true})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Error)

		// Check that we got the correct number of results
		require.Len(t, resp.Documents, 1)
		require.Len(t, resp.Files, 1)

		// Check that we got the correct document and object
		doc1ID, _ := util.PgtypeUUIDToString(oids[0])
		obj1ID, _ := util.PgtypeUUIDToString(oids[2])
		require.Contains(t, lo.Map(resp.Documents, func(item *pb.ListItem, _ int) string { return item.Key }), doc1ID)
		require.Contains(t, lo.Map(resp.Files, func(item *pb.ListItem, _ int) string { return item.Key }), obj1ID)
		require.Equal(t, resp.Documents[0].Metadata, map[string]string{"Source": "System1", "Purpose": "Purpose1", "Unique": "true"})
		require.Equal(t, resp.Documents[0].Data, `{"test": "content1"}`)
		require.Equal(t, resp.Files[0].Metadata, map[string]string{"Source": "System1", "Purpose": "Purpose1"})
		require.Empty(t, resp.Files[0].Data)
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
		require.Len(t, resp.Documents, 1)
		require.Len(t, resp.Files, 1)

		// Check that we got the correct document and object
		doc1ID, _ := util.PgtypeUUIDToString(oids[0])
		obj1ID, _ := util.PgtypeUUIDToString(oids[2])
		require.Contains(t, lo.Map(resp.Documents, func(item *pb.ListItem, _ int) string { return item.Key }), doc1ID)
		require.Contains(t, lo.Map(resp.Files, func(item *pb.ListItem, _ int) string { return item.Key }), obj1ID)
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
		require.Len(t, resp.Documents, 1)
		require.Len(t, resp.Files, 1)

		// Check that we got the correct document and object
		doc1ID, _ := util.PgtypeUUIDToString(oids[0])
		obj1ID, _ := util.PgtypeUUIDToString(oids[2])
		require.Contains(t, lo.Map(resp.Documents, func(item *pb.ListItem, _ int) string { return item.Key }), doc1ID)
		require.Contains(t, lo.Map(resp.Files, func(item *pb.ListItem, _ int) string { return item.Key }), obj1ID)
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
		require.Len(t, resp.Documents, 1)
		require.Len(t, resp.Files, 1)

		// Check that we got the correct document and object
		doc1ID, _ := util.PgtypeUUIDToString(oids[0])
		obj1ID, _ := util.PgtypeUUIDToString(oids[2])
		require.Contains(t, lo.Map(resp.Documents, func(item *pb.ListItem, _ int) string { return item.Key }), doc1ID)
		require.Contains(t, lo.Map(resp.Files, func(item *pb.ListItem, _ int) string { return item.Key }), obj1ID)
	})

	t.Run("List documents and objects with AND and HAS_KEYS filter", func(t *testing.T) {
		bundle, oids := setup(t)
		filter := &pb.ListFilter{
			Type: pb.ListFilter_AND_GROUP,
			SubFilters: []*pb.ListFilter{
				{
					Type: pb.ListFilter_HAS_KEYS,
					Entries: map[string]string{
						"Unique": "",
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
		require.Len(t, resp.Documents, 1)
		require.Len(t, resp.Files, 0)

		// Check that we got the correct document and object
		doc1ID, _ := util.PgtypeUUIDToString(oids[0])
		require.Contains(t, lo.Map(resp.Documents, func(item *pb.ListItem, _ int) string { return item.Key }), doc1ID)
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
		require.Len(t, resp.Documents, 1)
		require.Len(t, resp.Files, 2)

		// Check that we got the correct document and object
		doc1ID, _ := util.PgtypeUUIDToString(oids[0])
		obj1ID, _ := util.PgtypeUUIDToString(oids[2])
		obj2ID, _ := util.PgtypeUUIDToString(oids[3])
		require.Contains(t, lo.Map(resp.Documents, func(item *pb.ListItem, _ int) string { return item.Key }), doc1ID)
		require.ElementsMatch(t, lo.Map(resp.Files, func(item *pb.ListItem, _ int) string { return item.Key }), []string{obj1ID, obj2ID})
	})

	t.Run("List documents and objects with inclusion and exclusion filters", func(t *testing.T) {
		bundle, oids := setup(t)
		filter := &pb.ListFilter{
			Type: pb.ListFilter_OR_GROUP,
			SubFilters: []*pb.ListFilter{
				{
					Type:    pb.ListFilter_EQUAL,
					Entries: map[string]string{"Purpose": "Purpose1"},
				},
				{
					Type:    pb.ListFilter_EQUAL,
					Entries: map[string]string{"Purpose": "Purpose2"},
				},
			},
		}
		exclusionFilter := &pb.ListFilter{
			Type: pb.ListFilter_HAS_KEYS,
			Entries: map[string]string{
				"Unique": "",
			},
		}

		resp, err := bundle.server.List(ctx, &pb.ListRequest{Filter: filter, ExclusionFilter: exclusionFilter})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Error)

		// Check that we got the correct number of results
		require.Len(t, resp.Documents, 1)
		require.Len(t, resp.Files, 2)

		// Check that we got the correct document and object
		doc2ID, _ := util.PgtypeUUIDToString(oids[1])
		obj1ID, _ := util.PgtypeUUIDToString(oids[2])
		obj2ID, _ := util.PgtypeUUIDToString(oids[3])
		require.Contains(t, lo.Map(resp.Documents, func(item *pb.ListItem, _ int) string { return item.Key }), doc2ID)
		require.Contains(t, lo.Map(resp.Files, func(item *pb.ListItem, _ int) string { return item.Key }), obj1ID)
		require.Contains(t, lo.Map(resp.Files, func(item *pb.ListItem, _ int) string { return item.Key }), obj2ID)
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
		require.Empty(t, resp.Documents)
		require.Empty(t, resp.Files)
	})
}

func TestUpdateMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	type testBundle struct {
		dbPool     *pgxpool.Pool
		dataSource dbaccess.DataSource
		logger     *slog.Logger
		server     *_NexusStoreServer
	}

	setup := func(t *testing.T) *testBundle {
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

		return bundle
	}

	t.Run("Successful update metadata for document", func(t *testing.T) {
		bundle := setup(t)

		// Insert a document
		document := fixture.InsertDocument(t, ctx, querier, bundle.dbPool, `{"test": "content"}`)

		// Insert initial metadata
		initialMetadata := map[string]string{
			"Source":       "Initial System",
			"CreatorEmail": "initial@example.com",
			"CustomerName": "Initial Customer",
			"Purpose":      "Initial Purpose",
			"ContentType":  "application/json",
		}
		err := updateMetadata(ctx, bundle.dbPool, document.ID, initialMetadata)
		require.NoError(t, err)

		// Prepare update request
		id, err := util.PgtypeUUIDToString(document.ID)
		require.NoError(t, err)

		updateReq := &pb.UpdateMetadataRequest{
			Key: id,
			Metadata: map[string]string{
				"Source":       "Updated System",
				"CreatorEmail": "updated@example.com",
				"NewField":     "New Value",
			},
		}

		// Perform update
		resp, err := bundle.server.UpdateMetadata(ctx, updateReq)
		require.NoError(t, err)
		require.Nil(t, resp.Error)

		// Verify updated metadata
		updatedMetadata, err := querier.MetadataFindByDocumentIds(ctx, bundle.dbPool, []pgtype.UUID{document.ID})
		require.NoError(t, err)
		require.Len(t, updatedMetadata, 1)

		var metadataMap map[string]string
		err = json.Unmarshal(updatedMetadata[0].Metadata, &metadataMap)
		require.NoError(t, err)

		require.Equal(t, "Updated System", metadataMap["Source"])
		require.Equal(t, "updated@example.com", metadataMap["CreatorEmail"])
		require.Equal(t, "New Value", metadataMap["NewField"])
		require.Equal(t, "", metadataMap["CustomerName"])
		require.Equal(t, "", metadataMap["Purpose"])
		require.Equal(t, "", metadataMap["ContentType"])
	})

	t.Run("Successful update metadata for S3 object", func(t *testing.T) {
		bundle := setup(t)

		// Insert an S3 object
		s3Object := fixture.InsertS3Object(t, ctx, querier, bundle.dbPool)

		// Insert initial metadata
		initialMetadata := map[string]string{
			"Source":       "Initial System",
			"CreatorEmail": "initial@example.com",
			"CustomerName": "Initial Customer",
			"Purpose":      "Initial Purpose",
			"ContentType":  "application/octet-stream",
		}
		err := updateMetadata(ctx, bundle.dbPool, s3Object.ID, initialMetadata)
		require.NoError(t, err)

		// Prepare update request
		id, err := util.PgtypeUUIDToString(s3Object.ID)
		require.NoError(t, err)

		updateReq := &pb.UpdateMetadataRequest{
			Key: id,
			Metadata: map[string]string{
				"Source":       "Updated System",
				"CreatorEmail": "updated@example.com",
				"NewField":     "New Value",
			},
		}

		// Perform update
		resp, err := bundle.server.UpdateMetadata(ctx, updateReq)
		require.NoError(t, err)
		require.Nil(t, resp.Error)

		// Verify updated metadata
		updatedMetadata, err := querier.MetadataFindByObjectIds(ctx, bundle.dbPool, []pgtype.UUID{s3Object.ID})
		require.NoError(t, err)
		require.Len(t, updatedMetadata, 1)

		var metadataMap map[string]string
		err = json.Unmarshal(updatedMetadata[0].Metadata, &metadataMap)
		require.NoError(t, err)

		require.Equal(t, "Updated System", metadataMap["Source"])
		require.Equal(t, "updated@example.com", metadataMap["CreatorEmail"])
		require.Equal(t, "New Value", metadataMap["NewField"])
		require.Equal(t, "", metadataMap["CustomerName"])
		require.Equal(t, "", metadataMap["Purpose"])
		require.Equal(t, "", metadataMap["ContentType"])
	})

	t.Run("Update metadata for non-existent object", func(t *testing.T) {
		bundle := setup(t)

		nonExistentID := uuid.New().String()
		updateReq := &pb.UpdateMetadataRequest{
			Key: nonExistentID,
			Metadata: map[string]string{
				"Source": "Updated System",
			},
		}

		resp, err := bundle.server.UpdateMetadata(ctx, updateReq)
		require.NoError(t, err)
		require.NotNil(t, resp.Error)
		require.Equal(t, nexus.Error_NEXUS_STORE_INVALID_PARAMETER, resp.Error.Code)
		require.Contains(t, resp.Error.Message, "Object or document not found")
	})

	t.Run("Update metadata with invalid UUID", func(t *testing.T) {
		bundle := setup(t)

		updateReq := &pb.UpdateMetadataRequest{
			Key: "invalid-uuid",
			Metadata: map[string]string{
				"Source": "Updated System",
			},
		}

		resp, err := bundle.server.UpdateMetadata(ctx, updateReq)
		require.NoError(t, err)
		require.NotNil(t, resp.Error)
		require.Equal(t, nexus.Error_NEXUS_STORE_INVALID_PARAMETER, resp.Error.Code)
		require.Contains(t, resp.Error.Message, "Invalid UUID")
	})

	t.Run("Update metadata with empty key", func(t *testing.T) {
		bundle := setup(t)

		updateReq := &pb.UpdateMetadataRequest{
			Key: "",
			Metadata: map[string]string{
				"Source": "Updated System",
			},
		}

		resp, err := bundle.server.UpdateMetadata(ctx, updateReq)
		require.NoError(t, err)
		require.NotNil(t, resp.Error)
		require.Equal(t, nexus.Error_NEXUS_STORE_INVALID_PARAMETER, resp.Error.Code)
		require.Contains(t, resp.Error.Message, "Key is required")
	})
}
