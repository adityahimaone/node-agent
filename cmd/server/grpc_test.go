package main

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"node-agent/internal/heartbeat"
	"node-agent/internal/transport"
)

// Tests exercise the full gRPC Connect lifecycle in-process:
// register -> ack -> dispatch -> ack -> result -> results map.

func TestGRPCConnectLifecycle(t *testing.T) {
	reg = heartbeat.New(45 * time.Second)
	currentAuthToken = ""
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	gs := grpc.NewServer(grpc.ForceServerCodec(transport.Codec{}))
	transport.RegisterNodeAgentServiceServer(gs, grpcService{})
	go gs.Serve(lis)
	defer gs.Stop()

	conn, err := grpc.DialContext(context.Background(), lis.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(grpc.ForceCodec(transport.Codec{})),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := transport.NewNodeAgentServiceClient(conn).Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&transport.WorkerFrame{Register: &transport.RegisterFrame{NodeID: "test-node", Hostname: "h", Workspaces: []string{"/ws"}}}); err != nil {
		t.Fatal(err)
	}
	ack, err := stream.Recv()
	if err != nil || ack.RegisterAck == nil {
		t.Fatalf("register ack: %v", err)
	}
	n, ok := reg.Get("test-node")
	if !ok {
		t.Fatal("node not registered in registry")
	}
	if len(n.Transports) == 0 || n.Transports[0] != "grpc" {
		t.Fatalf("node transports = %v", n.Transports)
	}
	if id, ok := dispatchGRPC(transport.DispatchRequest{TaskID: "t1", Board: "b", Workspace: "/ws", Message: "m"}, "test-node"); !ok || id == "" {
		t.Fatalf("dispatchGRPC failed ok=%v id=%q", ok, id)
	}
	job, err := stream.Recv()
	if err != nil || job.DispatchJob == nil {
		t.Fatalf("dispatch job frame: %v", err)
	}
	if job.DispatchJob.TaskID != "t1" {
		t.Fatalf("job task_id = %q", job.DispatchJob.TaskID)
	}
	if err := stream.Send(&transport.WorkerFrame{JobAck: &transport.JobAck{DeliveryID: job.DispatchJob.DeliveryID, Accepted: true}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&transport.WorkerFrame{JobResult: &transport.JobResult{DeliveryID: job.DispatchJob.DeliveryID, TaskID: "t1", Success: true, Output: "done"}}); err != nil {
		t.Fatal(err)
	}
	frame, err := stream.Recv()
	if err != nil || frame.ResultAck == nil {
		t.Fatalf("result ack frame: %v", err)
	}
	if !frame.ResultAck.Accepted {
		t.Fatal("result ack not accepted")
	}
	rmu.Lock()
	sr, ok := results["t1"]
	rmu.Unlock()
	if !ok || !sr.res.Success {
		t.Fatalf("results map missing t1: %+v", sr)
	}
}
