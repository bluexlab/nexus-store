package grpc

import (
	"context"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess/dbsqlc"
	nexus "gitlab.com/navyx/nexus/nexus-store/pkg/proto/nexus_store"
	pb "gitlab.com/navyx/nexus/nexus-store/pkg/proto/nexus_store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	MetadataKeyPrefixObject   = "object"
	MetadataKeyPrefixDocument = "document"
)

var (
	querier = dbsqlc.New()
)

type _NexusStoreServer struct {
	dataSource dbaccess.DataSource
	nexus.UnimplementedNexusStoreServer
}

type NexusStoreServerOption func(*_NexusStoreServer)

func WithDataSource(dataSource dbaccess.DataSource) NexusStoreServerOption {
	return func(s *_NexusStoreServer) {
		s.dataSource = dataSource
	}
}

func NewNexusStoreServer(opts ...NexusStoreServerOption) *_NexusStoreServer {
	server := &_NexusStoreServer{}
	for _, opt := range opts {
		opt(server)
	}

	return server
}

func (s *_NexusStoreServer) GetDownloadURL(ctx context.Context, req *pb.GetDownloadURLRequest) (*pb.GetDownloadURLResponse, error) {
	return nil, nil
}

func (s *_NexusStoreServer) UploadFile(ctx context.Context, req *pb.UploadFileRequest) (*pb.UploadFileResponse, error) {
	// TODO: validate required fields are provided when uploading file
	// requiredFields := []string{"SourceSystem", "CreatorEmail", "CreatorName", "CustomerEmail", "CustomerName", "Purpose"}
	return nil, nil
}

func (s *_NexusStoreServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	return nil, nil
}

func (s *_NexusStoreServer) TagAutoExpire(ctx context.Context, req *pb.TagAutoExpireRequest) (*pb.TagAutoExpireResponse, error) {
	return nil, nil
}

func (s *_NexusStoreServer) UntagAutoExpire(ctx context.Context, req *pb.UntagAutoExpireRequest) (*pb.UntagAutoExpireResponse, error) {
	return nil, nil
}

func (s *_NexusStoreServer) AddMetadata(ctx context.Context, req *pb.AddMetadataRequest) (*pb.AddMetadataResponse, error) {
	if req.Key == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Key is required")
	}

	parts := strings.SplitN(req.Key, "-", 2)
	prefix, id := parts[0], parts[1]

	validPrefixes := []string{MetadataKeyPrefixObject, MetadataKeyPrefixDocument}
	if !slices.Contains(validPrefixes, prefix) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid key prefix")
	}

	pgUuid, err := stringToPgtypeUUID(id)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid UUID: %v", err)
	}

	// Verify existence of the object or document
	if prefix == MetadataKeyPrefixObject {
		if _, err := querier.S3ObjectFindById(ctx, s.dataSource, pgUuid); err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to find object: %v", err)
		}
	} else {
		if _, err := querier.DocumentFindById(ctx, s.dataSource, pgUuid); err != nil {
			return nil, status.Errorf(codes.Internal, "Failed to find document: %v", err)
		}
	}

	var keys, values []string
	var objectIDs, documentIDs []pgtype.UUID

	// Handle new_metadata entries
	for _, entry := range req.NewMetadata {
		keys = append(keys, entry.Key)
		values = append(values, entry.Value)
		appendIdentifier(&objectIDs, &documentIDs, prefix, pgUuid)
	}

	// Ensure slices are aligned
	if len(keys) != len(values) || len(keys) != len(objectIDs) || len(keys) != len(documentIDs) {
		return nil, status.Errorf(codes.Internal, "Mismatch in slice lengths")
	}

	// Perform batch insert
	err = querier.MetadataInsertBatch(ctx, s.dataSource, &dbsqlc.MetadataInsertBatchParams{
		ObjectIds:   objectIDs,
		DocumentIds: documentIDs,
		Keys:        keys,
		Values:      values,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "Failed to add metadata: %v", err)
	}

	return &pb.AddMetadataResponse{}, nil
}

func appendIdentifier(objectIDs, documentIDs *[]pgtype.UUID, prefix string, id pgtype.UUID) {
	if prefix == MetadataKeyPrefixObject {
		*objectIDs = append(*objectIDs, id)
		*documentIDs = append(*documentIDs, pgtype.UUID{Valid: false})
	} else {
		*objectIDs = append(*objectIDs, pgtype.UUID{Valid: false})
		*documentIDs = append(*documentIDs, id)
	}
}

func stringToPgtypeUUID(uuidStr string) (pgtype.UUID, error) {
	// Parse the string into a uuid.UUID
	parsedUUID, err := uuid.Parse(uuidStr)
	if err != nil {
		return pgtype.UUID{}, err
	}

	// Create a pgtype.UUID and set the Bytes and Status
	var pgUUID pgtype.UUID
	pgUUID.Bytes = parsedUUID
	pgUUID.Valid = true

	return pgUUID, nil
}
