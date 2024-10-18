package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"regexp"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/samber/lo"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess"
	"gitlab.com/navyx/nexus/nexus-store/pkg/dbaccess/dbsqlc"
	"gitlab.com/navyx/nexus/nexus-store/pkg/model"
	nexus "gitlab.com/navyx/nexus/nexus-store/pkg/proto/nexus_store"
	pb "gitlab.com/navyx/nexus/nexus-store/pkg/proto/nexus_store"
	"gitlab.com/navyx/nexus/nexus-store/pkg/storage"
	"gitlab.com/navyx/nexus/nexus-store/pkg/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ObjectType int

const (
	ObjectTypeFile ObjectType = iota
	ObjectTypeDocument
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

func (s *_NexusStoreServer) AddDocument(ctx context.Context, req *pb.AddDocumentRequest) (*pb.AddDocumentResponse, error) {
	slog.Debug("NexusStoreServer::AddDocument() invoked", "req", req)

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

		documentId, err = util.PgtypeUUIDToString(document.ID)
		if err != nil {
			slog.Error("NexusStoreServer::UploadDocument() fails to convert UUID to string.", "err", err)
			return status.Errorf(codes.Internal, "Failed to convert UUID to string: %v", err)
		}

		if err := s.insertMetadata(ctx, tx, ObjectTypeDocument, document.ID, metadata); err != nil {
			slog.Error("NexusStoreServer::UploadDocument() fails to insert metadata.", "err", err)
			return err
		}

		return nil
	})
	if err != nil {
		slog.Error("NexusStoreServer::UploadDocument() fails to execute transaction.", "err", err)
		return nil, err
	}

	return &pb.AddDocumentResponse{
		Id: documentId,
	}, nil
}

func (s *_NexusStoreServer) AddFile(ctx context.Context, req *pb.AddFileRequest) (*pb.AddFileResponse, error) {
	slog.Debug("NexusStoreServer::AddFile() invoked", "FileName", req.GetFileName(), "AutoExpire", req.GetAutoExpire())

	if len(req.GetData()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Data is required")
	}

	metadata := req.GetMetadata()
	err := s.checkMetadataRequiredFields(metadata)
	if err != nil {
		slog.Error("NexusStoreServer::AddFile() fails to check metadata required fields.", "err", err)
		return nil, err
	}
	metadata["FileName"] = req.GetFileName()

	var objectId string

	err = dbaccess.WithTx(ctx, s.dataSource, func(ctx context.Context, tx dbaccess.DataSource) error {
		// Add a record to the s3Object table
		s3Object, err := querier.S3ObjectInsert(ctx, tx)
		if err != nil {
			slog.Error("NexusStoreServer::AddFile() fails to insert into s3Object table.", "err", err)
			return status.Errorf(codes.Internal, "Failed to insert into s3Object table: %v", err)
		}

		objectId, err = util.PgtypeUUIDToString(s3Object.ID)
		if err != nil {
			slog.Error("NexusStoreServer::AddFile() fails to convert UUID to string.", "err", err)
			return status.Errorf(codes.Internal, "Failed to convert UUID to string: %v", err)
		}

		// Upload file to S3 storage
		_, err = s.storage.AddDocument(ctx, objectId, req.GetData(), req.GetAutoExpire(), metadata)
		if err != nil {
			slog.Error("FileServer::AddFile() fails to AddDocument().", "err", err)
			return status.Errorf(codes.Internal, err.Error())
		}

		if err := s.insertMetadata(ctx, tx, ObjectTypeFile, s3Object.ID, metadata); err != nil {
			slog.Error("NexusStoreServer::AddFile() fails to insert metadata.", "err", err)
			return err
		}

		return nil
	})
	if err != nil {
		slog.Error("NexusStoreServer::AddFile() fails to execute transaction.", "err", err)
		return nil, err
	}

	response := &pb.AddFileResponse{
		Key: objectId,
	}
	return response, nil
}

func (s *_NexusStoreServer) RemoveFile(ctx context.Context, req *pb.RemoveFileRequest) (*pb.RemoveFileResponse, error) {
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
	keyUuid, err := util.StringToPgtypeUUID(key)
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid UUID: %v", err)
	}

	tx, err := s.dataSource.Begin(ctx)
	if err != nil {
		slog.Error("NexusStoreServer::AddMetadata() fails to begin transaction.", "err", err)
		return nil, status.Errorf(codes.Internal, "Failed to begin transaction: %v", err)
	}
	defer tx.Rollback(ctx)

	recs, err := querier.DocumentOrObjectById(ctx, tx, keyUuid)
	if err != nil {
		slog.Error("NexusStoreServer::AddMetadata() fails to find object or document.", "err", err)
		return nil, status.Errorf(codes.Internal, "Failed to find object or document: %v", err)
	}
	if len(recs) == 0 {
		slog.Error("NexusStoreServer::AddMetadata() object or document not found", "key", key)
		return nil, status.Errorf(codes.NotFound, "Object or document not found")
	}

	objectType := lo.Ternary(recs[0].DocumentID.Valid, ObjectTypeDocument, ObjectTypeFile)
	if err := s.insertMetadata(ctx, tx, objectType, keyUuid, req.GetNewMetadata()); err != nil {
		slog.Error("NexusStoreServer::AddMetadata() fails to insert metadata.", "err", err)
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		slog.Error("NexusStoreServer::AddMetadata() fails to commit transaction.", "err", err)
		return nil, status.Errorf(codes.Internal, "Failed to commit transaction: %v", err)
	}

	return &pb.AddMetadataResponse{}, nil
}

func (s *_NexusStoreServer) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	slog.Debug("NexusStoreServer::List() invoked", "req", req)

	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Request is required")
	}

	filter, err := model.CreateListFilter(req.GetFilter())
	if err != nil {
		return s.errorResponse(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "fails to create list filter", err), nil
	}

	builder, err := filter.SqlQuery()
	if err != nil {
		return s.errorResponse(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "fails to create list filter", err), nil
	}
	query := s.buildQuery(builder)
	slog.Info("NexusStoreServer::List() query", "query", query, "whereArgs", builder.WhereArgs, "havingArgs", builder.HavingArgs)

	args := append(builder.WhereArgs, builder.HavingArgs...)
	rows, err := s.dataSource.Query(ctx, query, args...)
	if err != nil {
		return s.errorResponse(nexus.Error_NEXUS_STORE_QUERY_FAILED, "fails to execute query", err), nil
	}
	defer rows.Close()

	documentIds, objectIds := s.scanRows(rows)
	slog.Debug("NexusStoreServer::List() documentIds", "documentIds", documentIds, "objectIds", objectIds)

	documents, errResp := s.processDocuments(ctx, documentIds, req.GetIncludeDocuments(), req.GetIncludeMetadata())
	if errResp != nil {
		return errResp, nil
	}

	files, errResp := s.processFiles(ctx, objectIds, req.GetIncludeMetadata())
	if errResp != nil {
		return errResp, nil
	}

	return &pb.ListResponse{
		Documents: lo.Values(documents),
		Files:     lo.Values(files),
	}, nil
}

func (s *_NexusStoreServer) processDocuments(
	ctx context.Context,
	documentIds []pgtype.UUID,
	includeDocuments,
	includeMetadata bool,
) (map[pgtype.UUID]*pb.ListItem, *pb.ListResponse) {
	documents := map[pgtype.UUID]*pb.ListItem{}

	if len(documentIds) <= 0 {
		return documents, nil
	}

	for _, id := range documentIds {
		if !id.Valid {
			continue
		}
		documents[id] = &pb.ListItem{Id: util.MustPgtypeUUIDToString(id)}
	}

	if includeDocuments {
		records, err := querier.DocumentFindByIds(ctx, s.dataSource, documentIds)
		if err != nil {
			return nil, s.errorResponse(nexus.Error_NEXUS_STORE_QUERY_FAILED, "fails to find documents", err)
		}
		for _, record := range records {
			if doc, ok := documents[record.ID]; ok {
				doc.Data = string(record.Content)
				doc.CreatedAt = record.CreatedAt
			}
		}
	}

	if includeMetadata {
		metadataRecords, err := querier.MetadataFindByDocumentIds(ctx, s.dataSource, documentIds)
		if err != nil {
			return nil, s.errorResponse(nexus.Error_NEXUS_STORE_QUERY_FAILED, "fails to find document metadata", err)
		}
		for _, metadata := range metadataRecords {
			if !metadata.DocumentID.Valid {
				continue
			}
			if doc, ok := documents[metadata.DocumentID]; ok {
				if doc.Metadata == nil {
					doc.Metadata = make(map[string]string)
				}
				err := json.Unmarshal(metadata.Metadata, &doc.Metadata)
				if err != nil {
					return nil, s.errorResponse(nexus.Error_NEXUS_STORE_QUERY_FAILED, "fails to unmarshal document metadata", err)
				}
			}
		}
	}

	return documents, nil
}

func (s *_NexusStoreServer) processFiles(
	ctx context.Context,
	objectIds []pgtype.UUID,
	includeMetadata bool,
) (map[pgtype.UUID]*pb.ListItem, *pb.ListResponse) {
	files := map[pgtype.UUID]*pb.ListItem{}

	if len(objectIds) <= 0 {
		return files, nil
	}

	for _, id := range objectIds {
		if !id.Valid {
			continue
		}
		files[id] = &pb.ListItem{Id: util.MustPgtypeUUIDToString(id)}
	}

	if includeMetadata {
		metadataRecords, err := querier.MetadataFindByObjectIds(ctx, s.dataSource, objectIds)
		if err != nil {
			return nil, s.errorResponse(nexus.Error_NEXUS_STORE_QUERY_FAILED, "fails to find file metadata", err)
		}
		for _, metadata := range metadataRecords {
			if !metadata.ObjectID.Valid {
				continue
			}
			if file, ok := files[metadata.ObjectID]; ok {
				if file.Metadata == nil {
					file.Metadata = make(map[string]string)
				}
				err := json.Unmarshal(metadata.Metadata, &file.Metadata)
				if err != nil {
					return nil, s.errorResponse(nexus.Error_NEXUS_STORE_QUERY_FAILED, "fails to unmarshal file metadata", err)
				}
			}
		}
	}

	return files, nil
}

func (s *_NexusStoreServer) errorResponse(code nexus.Error_ErrorCode, msg string, err error) *pb.ListResponse {
	slog.Error(fmt.Sprintf("NexusStoreServer::List() %s", msg), "err", err)
	return &pb.ListResponse{
		Error: &nexus.Error{
			Code: code,
		},
	}
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

func (s *_NexusStoreServer) scanRows(rows pgx.Rows) ([]pgtype.UUID, []pgtype.UUID) {
	var documentIds, objectIds []pgtype.UUID
	for rows.Next() {
		var documentId, objectId *pgtype.UUID
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

func (s *_NexusStoreServer) insertMetadata(ctx context.Context, tx dbaccess.DataSource, objectType ObjectType, uuid pgtype.UUID, metadata map[string]string) error {
	var keys, values []string
	var objectIDs, documentIDs []pgtype.UUID

	for key, value := range metadata {
		keys = append(keys, key)
		values = append(values, value)
		if objectType == ObjectTypeFile {
			objectIDs = append(objectIDs, uuid)
			documentIDs = append(documentIDs, pgtype.UUID{Valid: false})
		} else {
			objectIDs = append(objectIDs, pgtype.UUID{Valid: false})
			documentIDs = append(documentIDs, uuid)
		}
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
