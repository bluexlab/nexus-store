package grpc

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"

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
	slog.Debug("GetDownloadURL() invoked", "req", req)

	downloadURL, err := s.storage.GetDownloadURL(ctx, req.GetKey(), req.GetLiveTime())
	if err != nil {
		slog.Error("GetDownloadURL() fails to GetDownloadURL().", "err", err)

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
	slog.Debug("AddDocument() invoked", "req", req)

	data := req.GetData()
	if len(data) == 0 {
		return reportAddDocumentError(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "Data is required", nil)
	}

	metadata := req.GetMetadata()
	err := checkMetadataRequiredFields(metadata)
	if err != nil {
		return reportAddDocumentError(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, err.Error(), err)
	}

	documentId, err := dbaccess.WithTxV(ctx, s.dataSource, func(ctx context.Context, tx dbaccess.DataSource) (string, error) {
		document, err := querier.DocumentInsert(ctx, tx, []byte(data))
		if err != nil {
			return "", status.Errorf(codes.Internal, "Failed to insert into document table: %v", err)
		}

		documentId, err := util.PgtypeUUIDToString(document.ID)
		if err != nil {
			return "", status.Errorf(codes.Internal, "Failed to convert UUID to string: %v", err)
		}

		if err := updateMetadata(ctx, tx, document.ID, metadata); err != nil {
			return "", err
		}

		return documentId, nil
	})
	if err != nil {
		return reportAddDocumentError(nexus.Error_NEXUS_STORE_INTERNAL_ERROR, "fails to execute transaction", err)
	}

	return &pb.AddDocumentResponse{
		Key: documentId,
	}, nil
}

func (s *_NexusStoreServer) UpdateDocument(ctx context.Context, req *pb.UpdateDocumentRequest) (*pb.UpdateDocumentResponse, error) {
	slog.Debug("UpdateDocument() invoked", "req", req)

	data := req.GetData()
	if len(data) == 0 {
		return reportUpdateDocumentError(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "Data is required", nil)
	}

	key := req.GetKey()
	if len(key) == 0 {
		return reportUpdateDocumentError(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "Key is required", nil)
	}

	keyUuid, err := util.StringToPgtypeUUID(key)
	if err != nil {
		return reportUpdateDocumentError(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "Invalid UUID", err)
	}

	documentId, err := querier.DocumentUpdate(ctx, s.dataSource, &dbsqlc.DocumentUpdateParams{
		ID:      keyUuid,
		Content: []byte(data),
	})

	if err != nil {
		return reportUpdateDocumentError(nexus.Error_NEXUS_STORE_INTERNAL_ERROR, "Failed to update document: "+err.Error(), err)
	}
	if !documentId.Valid {
		return reportUpdateDocumentError(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "Document not found", nil)
	}

	return &pb.UpdateDocumentResponse{}, nil
}

func (s *_NexusStoreServer) AddFile(ctx context.Context, req *pb.AddFileRequest) (*pb.AddFileResponse, error) {
	slog.Debug("AddFile() invoked", "FileName", req.GetFileName(), "AutoExpire", req.GetAutoExpire())

	if len(req.GetData()) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Data is required")
	}

	metadata := req.GetMetadata()
	err := checkMetadataRequiredFields(metadata)
	if err != nil {
		slog.Error("AddFile() fails to check metadata required fields.", "err", err)
		return nil, err
	}
	metadata["FileName"] = req.GetFileName()

	var objectId string

	err = dbaccess.WithTx(ctx, s.dataSource, func(ctx context.Context, tx dbaccess.DataSource) error {
		// Add a record to the s3Object table
		s3Object, err := querier.S3ObjectInsert(ctx, tx)
		if err != nil {
			slog.Error("AddFile() fails to insert into s3Object table.", "err", err)
			return status.Errorf(codes.Internal, "Failed to insert into s3Object table: %v", err)
		}

		objectId, err = util.PgtypeUUIDToString(s3Object.ID)
		if err != nil {
			slog.Error("AddFile() fails to convert UUID to string.", "err", err)
			return status.Errorf(codes.Internal, "Failed to convert UUID to string: %v", err)
		}

		// Upload file to S3 storage
		_, err = s.storage.AddDocument(ctx, objectId, req.GetData(), req.GetAutoExpire(), metadata)
		if err != nil {
			slog.Error("FileServer::AddFile() fails to AddDocument().", "err", err)
			return status.Errorf(codes.Internal, err.Error())
		}

		if err := updateMetadata(ctx, tx, s3Object.ID, metadata); err != nil {
			slog.Error("AddFile() fails to insert metadata.", "err", err)
			return err
		}

		return nil
	})
	if err != nil {
		slog.Error("AddFile() fails to execute transaction.", "err", err)
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
	slog.Debug("AddMetadata() invoked", "key", req.GetKey(), "metadata", req.GetMetadata())
	key := req.GetKey()
	if len(key) == 0 {
		return reportAddMetadataError(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "Key is required", nil)
	}
	keyUuid, err := util.StringToPgtypeUUID(key)
	if err != nil {
		return reportAddMetadataError(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "Invalid UUID", err)
	}

	if err := addMetadata(ctx, s.dataSource, keyUuid, req.GetMetadata()); err != nil {
		if err == pgx.ErrNoRows {
			return reportAddMetadataError(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "Object or document not found", err)
		}
		return reportAddMetadataError(nexus.Error_NEXUS_STORE_INTERNAL_ERROR, "Failed to update metadata", err)
	}

	return &pb.AddMetadataResponse{}, nil
}

func (s *_NexusStoreServer) List(ctx context.Context, req *pb.ListRequest) (*pb.ListResponse, error) {
	slog.Debug("List() invoked", "req", req)

	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "Request is required")
	}

	query, args, err := model.BuildListQuery(req.GetFilter(), req.GetExclusionFilter())
	if err != nil {
		return reportListError(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "fails to create list filter", err), nil
	}
	slog.Info("List() query", "query", query, "whereArgs", args)

	rows, err := s.dataSource.Query(ctx, query, args...)
	if err != nil {
		return reportListError(nexus.Error_NEXUS_STORE_QUERY_FAILED, "fails to execute query", err), nil
	}
	defer rows.Close()

	documentIds, objectIds := scanRows(rows)
	slog.Debug("List() documentIds", "documentIds", documentIds, "objectIds", objectIds)

	documents, errResp := processDocuments(ctx, s.dataSource, documentIds, req.GetReturnDocumentContent(), req.GetReturnMetadata())
	if errResp != nil {
		return errResp, nil
	}

	files, errResp := processFiles(ctx, s.dataSource, objectIds, req.GetReturnMetadata())
	if errResp != nil {
		return errResp, nil
	}

	return &pb.ListResponse{
		Documents: lo.Values(documents),
		Files:     lo.Values(files),
	}, nil
}

func processDocuments(
	ctx context.Context,
	dataSource dbaccess.DataSource,
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
		documents[id] = &pb.ListItem{Key: util.MustPgtypeUUIDToString(id)}
	}

	if includeDocuments {
		records, err := querier.DocumentFindByIds(ctx, dataSource, documentIds)
		if err != nil {
			return nil, reportListError(nexus.Error_NEXUS_STORE_QUERY_FAILED, "fails to find documents", err)
		}
		for _, record := range records {
			if doc, ok := documents[record.ID]; ok {
				doc.Data = string(record.Content)
				doc.CreatedAt = record.CreatedAt
			}
		}
	}

	if includeMetadata {
		metadataRecords, err := querier.MetadataFindByDocumentIds(ctx, dataSource, documentIds)
		if err != nil {
			return nil, reportListError(nexus.Error_NEXUS_STORE_QUERY_FAILED, "fails to find document metadata", err)
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
					return nil, reportListError(nexus.Error_NEXUS_STORE_QUERY_FAILED, "fails to unmarshal document metadata", err)
				}
			}
		}
	}

	return documents, nil
}

func processFiles(
	ctx context.Context,
	dataSource dbaccess.DataSource,
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
		files[id] = &pb.ListItem{Key: util.MustPgtypeUUIDToString(id)}
	}

	if includeMetadata {
		metadataRecords, err := querier.MetadataFindByObjectIds(ctx, dataSource, objectIds)
		if err != nil {
			return nil, reportListError(nexus.Error_NEXUS_STORE_QUERY_FAILED, "fails to find file metadata", err)
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
					return nil, reportListError(nexus.Error_NEXUS_STORE_QUERY_FAILED, "fails to unmarshal file metadata", err)
				}
			}
		}
	}

	return files, nil
}

func scanRows(rows pgx.Rows) ([]pgtype.UUID, []pgtype.UUID) {
	var documentIds, objectIds []pgtype.UUID
	for rows.Next() {
		var documentId, objectId *pgtype.UUID
		if err := rows.Scan(&objectId, &documentId); err != nil {
			slog.Error("List() fails to scan row", "err", err)
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

func addMetadata(ctx context.Context, tx dbaccess.DataSource, uuid pgtype.UUID, metadata map[string]string) error {
	var keys, values []string
	for key, value := range metadata {
		keys = append(keys, key)
		values = append(values, value)
	}

	// Ensure slices are aligned
	if len(keys) != len(values) {
		return status.Errorf(codes.Internal, "Mismatch in slice lengths")
	}

	_, err := querier.MetadataInsertBatch(ctx, tx, &dbsqlc.MetadataInsertBatchParams{
		ID:     uuid,
		Keys:   keys,
		Values: values,
	})
	return err
}

func updateMetadata(ctx context.Context, tx dbaccess.DataSource, uuid pgtype.UUID, metadata map[string]string) error {
	var keys, values []string
	for key, value := range metadata {
		keys = append(keys, key)
		values = append(values, value)
	}

	// Ensure slices are aligned
	if len(keys) != len(values) {
		return status.Errorf(codes.Internal, "Mismatch in slice lengths")
	}

	_, err := querier.MetadataUpdateBatch(ctx, tx, &dbsqlc.MetadataUpdateBatchParams{
		ID:     uuid,
		Keys:   keys,
		Values: values,
	})
	return err
}

func checkMetadataRequiredFields(entries map[string]string) error {
	requiredFields := []string{"Source", "CreatorEmail", "CustomerName", "Purpose", "ContentType"}
	for _, field := range requiredFields {
		if _, ok := entries[field]; !ok {
			return status.Errorf(codes.InvalidArgument, "Missing required field for metadata: %s", field)
		}
	}
	return nil
}

func reportAddDocumentError(code nexus.Error_ErrorCode, message string, err error) (*pb.AddDocumentResponse, error) {
	slog.Error("AddDocument()", "code", code, "message", message, "err", err)
	return &pb.AddDocumentResponse{
		Error: &nexus.Error{Code: code, Message: message},
	}, nil
}

func reportUpdateDocumentError(code nexus.Error_ErrorCode, message string, err error) (*pb.UpdateDocumentResponse, error) {
	slog.Error("UpdateDocument()", "code", code, "message", message, "err", err)
	return &pb.UpdateDocumentResponse{
		Error: &nexus.Error{Code: code, Message: message},
	}, nil
}

func reportAddMetadataError(code nexus.Error_ErrorCode, msg string, err error) (*pb.AddMetadataResponse, error) {
	slog.Error("AddMetadata()", "code", code, "message", msg, "err", err)
	return &pb.AddMetadataResponse{
		Error: &nexus.Error{Code: code, Message: msg},
	}, nil
}

func reportListError(code nexus.Error_ErrorCode, msg string, err error) *pb.ListResponse {
	slog.Error("List()", "code", code, "message", msg, "err", err)
	return &pb.ListResponse{
		Error: &nexus.Error{Code: code, Message: msg},
	}
}

func (s *_NexusStoreServer) UpdateMetadata(ctx context.Context, req *pb.UpdateMetadataRequest) (*pb.UpdateMetadataResponse, error) {
	slog.Debug("UpdateMetadata() invoked", "req", req)
	key := req.GetKey()
	if len(key) == 0 {
		return reportUpdateMetadataError(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "Key is required", nil)
	}
	keyUuid, err := util.StringToPgtypeUUID(key)
	if err != nil {
		return reportUpdateMetadataError(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "Invalid UUID", err)
	}

	if err := updateMetadata(ctx, s.dataSource, keyUuid, req.GetMetadata()); err != nil {
		if err == pgx.ErrNoRows {
			return reportUpdateMetadataError(nexus.Error_NEXUS_STORE_INVALID_PARAMETER, "Object or document not found", err)
		}
		return reportUpdateMetadataError(nexus.Error_NEXUS_STORE_INTERNAL_ERROR, "Failed to update metadata", err)
	}

	return &pb.UpdateMetadataResponse{}, nil
}

func reportUpdateMetadataError(code nexus.Error_ErrorCode, msg string, err error) (*pb.UpdateMetadataResponse, error) {
	slog.Error("UpdateMetadata()", "code", code, "message", msg, "err", err)
	return &pb.UpdateMetadataResponse{
		Error: &nexus.Error{Code: code, Message: msg},
	}, nil
}
