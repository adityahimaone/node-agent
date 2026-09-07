package transport

import (
	"context"
	"io"

	"google.golang.org/grpc"
)

const GRPCServiceName = "nodeagent.v1.NodeAgentService"

type RegisterFrame struct {
	NodeID     string            `json:"node_id"`
	Hostname   string            `json:"hostname"`
	Version    string            `json:"version"`
	Workspaces []string          `json:"workspaces"`
	Executors  []string          `json:"executors,omitempty"`
	Versions   map[string]string `json:"versions,omitempty"`
	Transports []string          `json:"transports,omitempty"`
}
type HeartbeatFrame struct {
	NodeID string `json:"node_id"`
	Status string `json:"status"`
}
type JobAck struct {
	DeliveryID string `json:"delivery_id"`
	Accepted   bool   `json:"accepted"`
	Reason     string `json:"reason,omitempty"`
}
type JobProgress struct {
	DeliveryID string `json:"delivery_id"`
	Phase      string `json:"phase"`
	Message    string `json:"message"`
}
type JobResult struct {
	DeliveryID string `json:"delivery_id"`
	TaskID     string `json:"task_id"`
	Success    bool   `json:"success"`
	Output     string `json:"output"`
	Error      string `json:"error,omitempty"`
	DurationMs int64  `json:"duration_ms"`
}
type WorkerFrame struct {
	Register    *RegisterFrame  `json:"register,omitempty"`
	Heartbeat   *HeartbeatFrame `json:"heartbeat,omitempty"`
	JobAck      *JobAck         `json:"job_ack,omitempty"`
	JobProgress *JobProgress    `json:"job_progress,omitempty"`
	JobResult   *JobResult      `json:"job_result,omitempty"`
}
type RegisterAck struct {
	SessionID           string `json:"session_id"`
	HeartbeatIntervalMs int64  `json:"heartbeat_interval_ms"`
}
type DispatchJob struct {
	DeliveryID           string `json:"delivery_id"`
	Attempt              uint32 `json:"attempt"`
	TaskID               string `json:"task_id"`
	Board                string `json:"board"`
	Message              string `json:"message"`
	Workspace            string `json:"workspace"`
	Executor             string `json:"executor"`
	Command              string `json:"command"`
	Model                string `json:"model"`
	Provider             string `json:"provider"`
	PrequestNote         string `json:"prequest_note"`
	LeaseExpiresAtUnixMs int64  `json:"lease_expires_at_unix_ms"`
}
type ResultAck struct {
	DeliveryID string `json:"delivery_id"`
	Accepted   bool   `json:"accepted"`
}
type ServerNotice struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}
type ServerFrame struct {
	RegisterAck *RegisterAck  `json:"register_ack,omitempty"`
	DispatchJob *DispatchJob  `json:"dispatch_job,omitempty"`
	ResultAck   *ResultAck    `json:"result_ack,omitempty"`
	Notice      *ServerNotice `json:"notice,omitempty"`
}

type NodeAgentServiceClient interface {
	Connect(context.Context, ...grpc.CallOption) (NodeAgentService_ConnectClient, error)
}
type nodeAgentClient struct{ cc grpc.ClientConnInterface }
type NodeAgentService_ConnectClient interface {
	grpc.ClientStream
	Send(*WorkerFrame) error
	Recv() (*ServerFrame, error)
}
type connectClient struct{ grpc.ClientStream }

func (c *connectClient) Send(v *WorkerFrame) error { return c.SendMsg(v) }
func (c *connectClient) Recv() (*ServerFrame, error) {
	v := new(ServerFrame)
	if err := c.RecvMsg(v); err != nil {
		return nil, err
	}
	return v, nil
}
func NewNodeAgentServiceClient(cc grpc.ClientConnInterface) NodeAgentServiceClient {
	return &nodeAgentClient{cc: cc}
}
func (c *nodeAgentClient) Connect(ctx context.Context, opts ...grpc.CallOption) (NodeAgentService_ConnectClient, error) {
	s, err := c.cc.NewStream(ctx, &grpc.StreamDesc{StreamName: "Connect", ServerStreams: true, ClientStreams: true}, "/"+GRPCServiceName+"/Connect", opts...)
	if err != nil {
		return nil, err
	}
	return &connectClient{s}, nil
}

type NodeAgentServiceServer interface {
	Connect(NodeAgentService_ConnectServer) error
}
type NodeAgentService_ConnectServer interface {
	grpc.ServerStream
	Send(*ServerFrame) error
	Recv() (*WorkerFrame, error)
}
type connectServer struct{ grpc.ServerStream }

func (s *connectServer) Send(v *ServerFrame) error { return s.SendMsg(v) }
func (s *connectServer) Recv() (*WorkerFrame, error) {
	v := new(WorkerFrame)
	if err := s.RecvMsg(v); err != nil {
		return nil, err
	}
	return v, nil
}
func RegisterNodeAgentServiceServer(s grpc.ServiceRegistrar, srv NodeAgentServiceServer) {
	s.RegisterService(&grpc.ServiceDesc{ServiceName: GRPCServiceName, HandlerType: (*NodeAgentServiceServer)(nil), Streams: []grpc.StreamDesc{{StreamName: "Connect", Handler: connectHandler, ServerStreams: true, ClientStreams: true}}}, srv)
}
func connectHandler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(NodeAgentServiceServer).Connect(&connectServer{stream})
}

var _ = io.EOF
