package grpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/samber/lo"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess/dbsqlc"
	"gitlab.com/navyx/nexus/nexus-store/pkg/model"
	nexus "gitlab.com/navyx/nexus/nexus-store/pkg/proto/nexus_store"
	pb "gitlab.com/navyx/nexus/nexus-store/pkg/proto/nexus_store"
	"gitlab.com/navyx/nexus/nexus-store/pkg/storage"
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
	storage    storage.Storage
	nexus.UnimplementedNexusStoreServer
}

type NexusStoreServerOption func(*_NexusStoreServer)

func WithDataSource(dataSource dbaccess.DataSource) NexusStoreServerOption {
	return func(s *_NexusStoreServer) {
		s.dataSource = dataSource
	}
}

func WithStorage(storage storage.Storage) NexusStoreServerOption {
	return func(s *_NexusStoreServer) {
		s.storage = storage
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
	slog.Debug("NexusStoreServer::GetDownloadURL() invoked", "req", req)

	downloadURL, err := s.storage.GetDownloadURL(ctx, req.GetKey(), req.GetLiveTime())
	if err != nil {
		slog.Error("NexusStoreServer::GetDownloadURL() fails to GetDownloadURL().", "err", err)

		var noSuchKey *types.NoSuchKey
		if errors.As(err, &noSuchKey) {
			return &pb.GetDownloadURLResponse{
				Error: &nexus.Error{
					Code: nexus.Error_NEXUS_STORE_KEY_NOT_EXIST,
				},
			}, nil
		}

		return nil, status.Errorf(codes.Internal, err.Error())
	}

	response := &pb.GetDownloadURLResponse{
		Url: downloadURL,
	}
	return response, nil
}

func (s *_NexusStoreServer) UploadDocument(ctx context.Context, req *pb.UploadDocumentRequest) (*pb.UploadDocumentResponse, error) {
	slog.Debug("NexusStoreServer::UploadDocument() invoked", "req", req)

	data := req.GetData()
	if len(data) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Data is required")
	}

	metadata := req.GetMetadata()
	err := s.checkMetadataRequiredFields(metadata)
	if err != nil {
		slog.Error("NexusStoreServer::UploadDocument() fails to check metadata required fields.", "err", err)
		return nil, err
	}

	var documentId string
	err = dbaccess.WithTx(ctx, s.dataSource, func(ctx context.Context, tx dbaccess.DataSource) error {
		// Insert data into document table
		document, err := querier.DocumentInsert(ctx, tx, []byte(data))
		if err != nil {
			slog.Error("NexusStoreServer::UploadDocument() fails to insert into document table.", "err", err)
			return status.Errorf(codes.Internal, "Failed to insert into document table: %v", err)
		}

		documentId, err = pgtypeUUIDToString(document.ID)
		if err != nil {
			slog.Error("NexusStoreServer::UploadDocument() fails to convert UUID to string.", "err", err)
			return status.Errorf(codes.Internal, "Failed to convert UUID to string: %v", err)
		}

		if err := s.insertMetadata(ctx, tx, MetadataKeyPrefixDocument, document.ID, metadata); err != nil {
			slog.Error("NexusStoreServer::UploadDocument() fails to insert metadata.", "err", err)
			return err
		}

		return nil
	})
	if err != nil {
		slog.Error("NexusStoreServer::UploadDocument() fails to execute transaction.", "err", err)
		return nil, err
	}

	return &pb.UploadDocumentResponse{
		Id: documentId,
	}, nil
}

func (s *_NexusStoreServer) UploadFile(ctx context.Context, req *pb.UploadFileRequest) (*pb.UploadFileResponse, error) {
	slog.Debug("NexusStoreServer::UploadFile() invoked", "FileName", req.GetFileName(), "AutoExpire", req.GetAutoExpire())

	if len(req.GetData()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Data is required")
	}

	metadata := req.GetMetadata()
	err := s.checkMetadataRequiredFields(metadata)
	if err != nil {
		slog.Error("NexusStoreServer::UploadFile() fails to check metadata required fields.", "err", err)
		return nil, err
	}
	metadata["FileName"] = req.GetFileName()

	var objectId string

	err = dbaccess.WithTx(ctx, s.dataSource, func(ctx context.Context, tx dbaccess.DataSource) error {
		// Add a record to the s3Object table
		s3Object, err := querier.S3ObjectInsert(ctx, tx)
		if err != nil {
			slog.Error("NexusStoreServer::UploadFile() fails to insert into s3Object table.", "err", err)
			return status.Errorf(codes.Internal, "Failed to insert into s3Object table: %v", err)
		}

		objectId, err = pgtypeUUIDToString(s3Object.ID)
		if err != nil {
			slog.Error("NexusStoreServer::UploadFile() fails to convert UUID to string.", "err", err)
			return status.Errorf(codes.Internal, "Failed to convert UUID to string: %v", err)
		}

		// Upload file to S3 storage
		_, err = s.storage.AddDocument(ctx, objectId, req.GetData(), req.GetAutoExpire(), metadata)
		if err != nil {
			slog.Error("FileServer::UploadFile() fails to AddDocument().", "err", err)
			return status.Errorf(codes.Internal, err.Error())
		}

		if err := s.insertMetadata(ctx, tx, MetadataKeyPrefixObject, s3Object.ID, metadata); err != nil {
			slog.Error("NexusStoreServer::UploadFile() fails to insert metadata.", "err", err)
			return err
		}

		return nil
	})
	if err != nil {
		slog.Error("NexusStoreServer::UploadFile() fails to execute transaction.", "err", err)
		return nil, err
	}

	response := &pb.UploadFileResponse{
		Key: objectId,
	}
	return response, nil
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
	slog.Debug("NexusStoreServer::AddMetadata() invoked", "req", req)
	key := req.GetKey()
	if len(key) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Key is required")
	}

	parts := strings.SplitN(key, "-", 2)
	prefix, id := parts[0], parts[1]

	validPrefixes := []string{MetadataKeyPrefixObject, MetadataKeyPrefixDocument}
	if !slices.Contains(validPrefixes, prefix) {
		slog.Error("NexusStoreServer::AddMetadata() fails to check metadata required fields.", "prefix", prefix)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid key prefix")
	}

	pgUuid, err := stringToPgtypeUUID(id)
	if err != nil {
		slog.Error("NexusStoreServer::AddMetadata() fails to convert UUID to string.", "err", err)
		return nil, status.Errorf(codes.InvalidArgument, "Invalid UUID: %v", err)
	}

	// Verify existence of the object or document
	if prefix == MetadataKeyPrefixObject {
		if _, err := querier.S3ObjectFindById(ctx, s.dataSource, pgUuid); err != nil {
			slog.Error("NexusStoreServer::AddMetadata() fails to find object.", "err", err)
			return nil, status.Errorf(codes.Internal, "Failed to find object: %v", err)
		}
	} else {
		if _, err := querier.DocumentFindById(ctx, s.dataSource, pgUuid); err != nil {
			slog.Error("NexusStoreServer::AddMetadata() fails to find document.", "err", err)
			return nil, status.Errorf(codes.Internal, "Failed to find document: %v", err)
		}
	}

	if err := s.insertMetadata(ctx, s.dataSource, prefix, pgUuid, req.GetNewMetadata()); err != nil {
		slog.Error("NexusStoreServer::AddMetadata() fails to insert metadata.", "err", err)
		return nil, err
	}

	return &pb.AddMetadataResponse{}, nil
}

func (s *_NexusStoreServer) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	slog.Debug("NexusStoreServer::List() invoked", "req", req)

	filter, err := model.CreateListFilter(req.GetFilter())
	if err != nil {
		return s.errorResponse(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "fails to create list filter", err)
	}

	builder, err := filter.SqlQuery()
	if err != nil {
		return s.errorResponse(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "fails to create list filter", err)
	}
	query := s.buildQuery(builder)
	slog.Info("NexusStoreServer::List() query", "query", query, "whereArgs", builder.WhereArgs, "havingArgs", builder.HavingArgs)

	args := append(builder.WhereArgs, builder.HavingArgs...)
	rows, err := s.dataSource.Query(ctx, query, args...)
	if err != nil {
		return s.errorResponse(nexus.Error_NEXUS_STORE_QUERY_FAILED, "fails to execute query", err)
	}
	defer rows.Close()

	documentIds, objectIds := s.scanRows(rows)
	slog.Info("NexusStoreServer::List() documentIds", "documentIds", documentIds, "objectIds", objectIds)

	return &pb.ListResponse{
		DocumentIds: documentIds,
		FileIds:     objectIds,
	}, nil
}

func (s *_NexusStoreServer) errorResponse(code nexus.Error_ErrorCode, msg string, err error) (*pb.ListResponse, error) {
	slog.Error(fmt.Sprintf("NexusStoreServer::List() %s", msg), "err", err)
	return &pb.ListResponse{
		Error: &nexus.Error{
			Code: code,
		},
	}, nil
}

func (s *_NexusStoreServer) buildQuery(builder *model.SqlQueryBuilder) string {
	sqlQuery := fmt.Sprintf(
		"SELECT object_id, document_id FROM metadatas WHERE %s GROUP BY object_id, document_id HAVING %s;",
		builder.WhereClause,
		builder.HavingClause,
	)

	paramCount := 1
	sqlQuery = regexp.MustCompile(`\?`).ReplaceAllStringFunc(sqlQuery, func(string) string {
		placeholder := fmt.Sprintf("$%d", paramCount)
		paramCount++
		return placeholder
	})
	return sqlQuery
}

func (s *_NexusStoreServer) scanRows(rows pgx.Rows) ([]string, []string) {
	var documentIds, objectIds []string
	for rows.Next() {
		var documentId, objectId *string
		if err := rows.Scan(&objectId, &documentId); err != nil {
			slog.Error("NexusStoreServer::List() fails to scan row", "err", err)
			continue
		}
		slog.Info("scan documentId", "documentId", lo.FromPtr(documentId), "objectId", lo.FromPtr(objectId))
		if documentId != nil {
			documentIds = append(documentIds, *documentId)
		}
		if objectId != nil {
			objectIds = append(objectIds, *objectId)
		}
	}
	return documentIds, objectIds
}

func (s *_NexusStoreServer) checkMetadataRequiredFields(entries map[string]string) error {
	requiredFields := []string{"Source", "CreatorEmail", "CustomerName", "Purpose", "ContentType"}
	for _, field := range requiredFields {
		if _, ok := entries[field]; !ok {
			return status.Errorf(codes.InvalidArgument, "Missing required field for metadata: %s", field)
		}
	}
	return nil
}

func (s *_NexusStoreServer) insertMetadata(ctx context.Context, tx dbaccess.DataSource, prefix string, uuid pgtype.UUID, metadata map[string]string) error {
	var keys, values []string
	var objectIDs, documentIDs []pgtype.UUID

	for key, value := range metadata {
		keys = append(keys, key)
		values = append(values, value)
		appendIdentifier(&objectIDs, &documentIDs, prefix, uuid)
	}

	// Ensure slices are aligned
	if len(keys) != len(values) || len(keys) != len(objectIDs) || len(keys) != len(documentIDs) {
		return status.Errorf(codes.Internal, "Mismatch in slice lengths")
	}

	// Perform batch insert
	err := querier.MetadataInsertBatch(ctx, tx, &dbsqlc.MetadataInsertBatchParams{
		ObjectIds:   objectIDs,
		DocumentIds: documentIDs,
		Keys:        keys,
		Values:      values,
	})
	if err != nil {
		return status.Errorf(codes.Internal, "Failed to add metadata: %v", err)
	}

	return nil
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

func pgtypeUUIDToString(uuid pgtype.UUID) (string, error) {
	if !uuid.Valid {
		return "", errors.New("invalid UUID")
	}

	s := fmt.Sprintf("%x-%x-%x-%x-%x", uuid.Bytes[0:4], uuid.Bytes[4:6], uuid.Bytes[6:8], uuid.Bytes[8:10], uuid.Bytes[10:16])
	return s, nil
}
