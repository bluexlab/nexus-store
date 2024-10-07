package grpc

import (
	"context"

	pb "gitlab.com/navyx/nexus/nexus-store/pkg/proto/nexus_store"
)

type _NexusStoreServer struct {
	pb.UnimplementedNexusStoreServer
}

type NexusStoreServerOption func(*_NexusStoreServer)

func NewNexusStoreServer(opts ...NexusStoreServerOption) pb.NexusStoreServer {
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
	return nil, nil
}

func (s *_NexusStoreServer) DeleteFile(ctx context.Context, req *pb.DeleteFileRequest) (*pb.DeleteFileResponse, error) {
	return nil, nil
}

func (s *_NexusStoreServer) TagAutoExpire(ctx context.Context, req *pb.TagAutoExpireRequest) (*pb.TagAutoExpireResponse, error) {
	return nil, nil
}
