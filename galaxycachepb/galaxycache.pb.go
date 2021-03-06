// Code generated by protoc-gen-go. DO NOT EDIT.
// source: galaxycache.proto

package galaxycachepb

import (
	context "context"
	fmt "fmt"
	proto "github.com/golang/protobuf/proto"
	grpc "google.golang.org/grpc"
	math "math"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.ProtoPackageIsVersion3 // please upgrade the proto package

type GetRequest struct {
	Galaxy               string   `protobuf:"bytes,1,opt,name=galaxy,proto3" json:"galaxy,omitempty"`
	Key                  string   `protobuf:"bytes,2,opt,name=key,proto3" json:"key,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *GetRequest) Reset()         { *m = GetRequest{} }
func (m *GetRequest) String() string { return proto.CompactTextString(m) }
func (*GetRequest) ProtoMessage()    {}
func (*GetRequest) Descriptor() ([]byte, []int) {
	return fileDescriptor_23bd509ca7b74957, []int{0}
}

func (m *GetRequest) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetRequest.Unmarshal(m, b)
}
func (m *GetRequest) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetRequest.Marshal(b, m, deterministic)
}
func (m *GetRequest) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetRequest.Merge(m, src)
}
func (m *GetRequest) XXX_Size() int {
	return xxx_messageInfo_GetRequest.Size(m)
}
func (m *GetRequest) XXX_DiscardUnknown() {
	xxx_messageInfo_GetRequest.DiscardUnknown(m)
}

var xxx_messageInfo_GetRequest proto.InternalMessageInfo

func (m *GetRequest) GetGalaxy() string {
	if m != nil {
		return m.Galaxy
	}
	return ""
}

func (m *GetRequest) GetKey() string {
	if m != nil {
		return m.Key
	}
	return ""
}

type GetResponse struct {
	Value                []byte   `protobuf:"bytes,1,opt,name=value,proto3" json:"value,omitempty"`
	MinuteQps            float64  `protobuf:"fixed64,2,opt,name=minute_qps,json=minuteQps,proto3" json:"minute_qps,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *GetResponse) Reset()         { *m = GetResponse{} }
func (m *GetResponse) String() string { return proto.CompactTextString(m) }
func (*GetResponse) ProtoMessage()    {}
func (*GetResponse) Descriptor() ([]byte, []int) {
	return fileDescriptor_23bd509ca7b74957, []int{1}
}

func (m *GetResponse) XXX_Unmarshal(b []byte) error {
	return xxx_messageInfo_GetResponse.Unmarshal(m, b)
}
func (m *GetResponse) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	return xxx_messageInfo_GetResponse.Marshal(b, m, deterministic)
}
func (m *GetResponse) XXX_Merge(src proto.Message) {
	xxx_messageInfo_GetResponse.Merge(m, src)
}
func (m *GetResponse) XXX_Size() int {
	return xxx_messageInfo_GetResponse.Size(m)
}
func (m *GetResponse) XXX_DiscardUnknown() {
	xxx_messageInfo_GetResponse.DiscardUnknown(m)
}

var xxx_messageInfo_GetResponse proto.InternalMessageInfo

func (m *GetResponse) GetValue() []byte {
	if m != nil {
		return m.Value
	}
	return nil
}

func (m *GetResponse) GetMinuteQps() float64 {
	if m != nil {
		return m.MinuteQps
	}
	return 0
}

func init() {
	proto.RegisterType((*GetRequest)(nil), "galaxycachepb.GetRequest")
	proto.RegisterType((*GetResponse)(nil), "galaxycachepb.GetResponse")
}

func init() { proto.RegisterFile("galaxycache.proto", fileDescriptor_23bd509ca7b74957) }

var fileDescriptor_23bd509ca7b74957 = []byte{
	// 186 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0x12, 0x4c, 0x4f, 0xcc, 0x49,
	0xac, 0xa8, 0x4c, 0x4e, 0x4c, 0xce, 0x48, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0xe2, 0x45,
	0x12, 0x2a, 0x48, 0x52, 0x32, 0xe3, 0xe2, 0x72, 0x4f, 0x2d, 0x09, 0x4a, 0x2d, 0x2c, 0x4d, 0x2d,
	0x2e, 0x11, 0x12, 0xe3, 0x62, 0x83, 0x48, 0x4b, 0x30, 0x2a, 0x30, 0x6a, 0x70, 0x06, 0x41, 0x79,
	0x42, 0x02, 0x5c, 0xcc, 0xd9, 0xa9, 0x95, 0x12, 0x4c, 0x60, 0x41, 0x10, 0x53, 0xc9, 0x89, 0x8b,
	0x1b, 0xac, 0xaf, 0xb8, 0x20, 0x3f, 0xaf, 0x38, 0x55, 0x48, 0x84, 0x8b, 0xb5, 0x2c, 0x31, 0xa7,
	0x34, 0x15, 0xac, 0x8f, 0x27, 0x08, 0xc2, 0x11, 0x92, 0xe5, 0xe2, 0xca, 0xcd, 0xcc, 0x2b, 0x2d,
	0x49, 0x8d, 0x2f, 0x2c, 0x28, 0x06, 0xeb, 0x66, 0x0c, 0xe2, 0x84, 0x88, 0x04, 0x16, 0x14, 0x1b,
	0x85, 0x72, 0x71, 0xbb, 0x83, 0xcd, 0x77, 0x06, 0x39, 0x46, 0xc8, 0x0d, 0x6c, 0xa4, 0x5b, 0x51,
	0x7e, 0x6e, 0x40, 0x6a, 0x6a, 0x91, 0x90, 0xa4, 0x1e, 0x8a, 0x4b, 0xf5, 0x10, 0xce, 0x94, 0x92,
	0xc2, 0x26, 0x05, 0x71, 0x89, 0x12, 0x43, 0x12, 0x1b, 0xd8, 0xa3, 0xc6, 0x80, 0x00, 0x00, 0x00,
	0xff, 0xff, 0x84, 0x48, 0x7a, 0xc8, 0xfd, 0x00, 0x00, 0x00,
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
const _ = grpc.SupportPackageIsVersion4

// GalaxyCacheClient is the client API for GalaxyCache service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://godoc.org/google.golang.org/grpc#ClientConn.NewStream.
type GalaxyCacheClient interface {
	GetFromPeer(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error)
}

type galaxyCacheClient struct {
	cc *grpc.ClientConn
}

func NewGalaxyCacheClient(cc *grpc.ClientConn) GalaxyCacheClient {
	return &galaxyCacheClient{cc}
}

func (c *galaxyCacheClient) GetFromPeer(ctx context.Context, in *GetRequest, opts ...grpc.CallOption) (*GetResponse, error) {
	out := new(GetResponse)
	err := c.cc.Invoke(ctx, "/galaxycachepb.GalaxyCache/GetFromPeer", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// GalaxyCacheServer is the server API for GalaxyCache service.
type GalaxyCacheServer interface {
	GetFromPeer(context.Context, *GetRequest) (*GetResponse, error)
}

func RegisterGalaxyCacheServer(s *grpc.Server, srv GalaxyCacheServer) {
	s.RegisterService(&_GalaxyCache_serviceDesc, srv)
}

func _GalaxyCache_GetFromPeer_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(GetRequest)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(GalaxyCacheServer).GetFromPeer(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/galaxycachepb.GalaxyCache/GetFromPeer",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(GalaxyCacheServer).GetFromPeer(ctx, req.(*GetRequest))
	}
	return interceptor(ctx, in, info, handler)
}

var _GalaxyCache_serviceDesc = grpc.ServiceDesc{
	ServiceName: "galaxycachepb.GalaxyCache",
	HandlerType: (*GalaxyCacheServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "GetFromPeer",
			Handler:    _GalaxyCache_GetFromPeer_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "galaxycache.proto",
}
