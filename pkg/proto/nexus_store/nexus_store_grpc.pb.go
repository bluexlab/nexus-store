// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v5.27.3
// source: nexus_store/nexus_store.proto

package bluex_payment

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// NexusStoreClient is the client API for NexusStore service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type NexusStoreClient interface {
	// API for unstructure data
	GetDownloadURL(ctx context.Context, in *GetDownloadURLRequest, opts ...grpc.CallOption) (*GetDownloadURLResponse, error)
	UploadFile(ctx context.Context, in *UploadFileRequest, opts ...grpc.CallOption) (*UploadFileResponse, error)
	DeleteFile(ctx context.Context, in *DeleteFileRequest, opts ...grpc.CallOption) (*DeleteFileResponse, error)
	TagAutoExpire(ctx context.Context, in *TagAutoExpireRequest, opts ...grpc.CallOption) (*TagAutoExpireResponse, error)
	UntagAutoExpire(ctx context.Context, in *UntagAutoExpireRequest, opts ...grpc.CallOption) (*UntagAutoExpireResponse, error)
	// API for both unstructure and semi-structure data
	AddMetadata(ctx context.Context, in *AddMetadataRequest, opts ...grpc.CallOption) (*AddMetadataResponse, error)
}

type nexusStoreClient struct {
	cc grpc.ClientConnInterface
}

func NewNexusStoreClient(cc grpc.ClientConnInterface) NexusStoreClient {
	return &nexusStoreClient{cc}
}

func (c *nexusStoreClient) GetDownloadURL(ctx context.Context, in *GetDownloadURLRequest, opts ...grpc.CallOption) (*GetDownloadURLResponse, error) {
	out := new(GetDownloadURLResponse)
	err := c.cc.Invoke(ctx, "/NexusStore/GetDownloadURL", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nexusStoreClient) UploadFile(ctx context.Context, in *UploadFileRequest, opts ...grpc.CallOption) (*UploadFileResponse, error) {
	out := new(UploadFileResponse)
	err := c.cc.Invoke(ctx, "/NexusStore/UploadFile", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nexusStoreClient) DeleteFile(ctx context.Context, in *DeleteFileRequest, opts ...grpc.CallOption) (*DeleteFileResponse, error) {
	out := new(DeleteFileResponse)
	err := c.cc.Invoke(ctx, "/NexusStore/DeleteFile", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nexusStoreClient) TagAutoExpire(ctx context.Context, in *TagAutoExpireRequest, opts ...grpc.CallOption) (*TagAutoExpireResponse, error) {
	out := new(TagAutoExpireResponse)
	err := c.cc.Invoke(ctx, "/NexusStore/TagAutoExpire", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nexusStoreClient) UntagAutoExpire(ctx context.Context, in *UntagAutoExpireRequest, opts ...grpc.CallOption) (*UntagAutoExpireResponse, error) {
	out := new(UntagAutoExpireResponse)
	err := c.cc.Invoke(ctx, "/NexusStore/UntagAutoExpire", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *nexusStoreClient) AddMetadata(ctx context.Context, in *AddMetadataRequest, opts ...grpc.CallOption) (*AddMetadataResponse, error) {
	out := new(AddMetadataResponse)
	err := c.cc.Invoke(ctx, "/NexusStore/AddMetadata", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// NexusStoreServer is the server API for NexusStore service.
// All implementations must embed UnimplementedNexusStoreServer
// for forward compatibility
type NexusStoreServer interface {
	// API for unstructure data
	GetDownloadURL(context.Context, *GetDownloadURLRequest) (*GetDownloadURLResponse, error)
	UploadFile(context.Context, *UploadFileRequest) (*UploadFileResponse, error)
	DeleteFile(context.Context, *DeleteFileRequest) (*DeleteFileResponse, error)
	TagAutoExpire(context.Context, *TagAutoExpireRequest) (*TagAutoExpireResponse, error)
	UntagAutoExpire(context.Context, *UntagAutoExpireRequest) (*UntagAutoExpireResponse, error)
	// API for both unstructure and semi-structure data
	AddMetadata(context.Context, *AddMetadataRequest) (*AddMetadataResponse, error)
	mustEmbedUnimplementedNexusStoreServer()
}

// UnimplementedNexusStoreServer must be embedded to have forward compatible implementations.
type UnimplementedNexusStoreServer struct {
}

func (UnimplementedNexusStoreServer) GetDownloadURL(context.Context, *GetDownloadURLRequest) (*GetDownloadURLResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method GetDownloadURL not implemented")
}
func (UnimplementedNexusStoreServer) UploadFile(context.Context, *UploadFileRequest) (*UploadFileResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UploadFile not implemented")
}
func (UnimplementedNexusStoreServer) DeleteFile(context.Context, *DeleteFileRequest) (*DeleteFileResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DeleteFile not implemented")
}
func (UnimplementedNexusStoreServer) TagAutoExpire(context.Context, *TagAutoExpireRequest) (*TagAutoExpireResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method TagAutoExpire not implemented")
}
func (UnimplementedNexusStoreServer) UntagAutoExpire(context.Context, *UntagAutoExpireRequest) (*UntagAutoExpireResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method UntagAutoExpire not implemented")
}
func (UnimplementedNexusStoreServer) AddMetadata(context.Context, *AddMetadataRequest) (*AddMetadataResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "method AddMetadata not implemented")
}
func (UnimplementedNexusStoreServer) mustEmbedUnimplementedNexusStoreServer() {}

// UnsafeNexusStoreServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to NexusStoreServer will
// result in compilation errors.
type UnsafeNexusStoreServer interface {
	mustEmbedUnimplementedNexusStoreServer()
}

func RegisterNexusStoreServer(s grpc.ServiceRegistrar, srv NexusStoreServer) {
	s.RegisterService(&NexusStore_ServiceDesc, srv)
}

func _NexusStore_GetDownloadURL_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetDownloadURLRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NexusStoreServer).GetDownloadURL(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/NexusStore/GetDownloadURL",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NexusStoreServer).GetDownloadURL(ctx, req.(*GetDownloadURLRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NexusStore_UploadFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UploadFileRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NexusStoreServer).UploadFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/NexusStore/UploadFile",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NexusStoreServer).UploadFile(ctx, req.(*UploadFileRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NexusStore_DeleteFile_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(DeleteFileRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NexusStoreServer).DeleteFile(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/NexusStore/DeleteFile",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NexusStoreServer).DeleteFile(ctx, req.(*DeleteFileRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NexusStore_TagAutoExpire_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(TagAutoExpireRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NexusStoreServer).TagAutoExpire(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/NexusStore/TagAutoExpire",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NexusStoreServer).TagAutoExpire(ctx, req.(*TagAutoExpireRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NexusStore_UntagAutoExpire_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(UntagAutoExpireRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NexusStoreServer).UntagAutoExpire(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/NexusStore/UntagAutoExpire",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NexusStoreServer).UntagAutoExpire(ctx, req.(*UntagAutoExpireRequest))
	}
	return interceptor(ctx, in, info, handler)
}

func _NexusStore_AddMetadata_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(AddMetadataRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(NexusStoreServer).AddMetadata(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/NexusStore/AddMetadata",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(NexusStoreServer).AddMetadata(ctx, req.(*AddMetadataRequest))
	}
	return interceptor(ctx, in, info, handler)
}

// NexusStore_ServiceDesc is the grpc.ServiceDesc for NexusStore service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var NexusStore_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "NexusStore",
	HandlerType: (*NexusStoreServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetDownloadURL",
			Handler:    _NexusStore_GetDownloadURL_Handler,
		},
		{
			MethodName: "UploadFile",
			Handler:    _NexusStore_UploadFile_Handler,
		},
		{
			MethodName: "DeleteFile",
			Handler:    _NexusStore_DeleteFile_Handler,
		},
		{
			MethodName: "TagAutoExpire",
			Handler:    _NexusStore_TagAutoExpire_Handler,
		},
		{
			MethodName: "UntagAutoExpire",
			Handler:    _NexusStore_UntagAutoExpire_Handler,
		},
		{
			MethodName: "AddMetadata",
			Handler:    _NexusStore_AddMetadata_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "nexus_store/nexus_store.proto",
}
