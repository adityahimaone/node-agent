package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"log"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"node-agent/internal/heartbeat"
	"node-agent/internal/session"
	"node-agent/internal/transport"
)

type grpcSession struct {
	nodeID   string
	out      chan *transport.ServerFrame
	done     chan struct{}
	doneOnce sync.Once
}

func (s *grpcSession) closeDone() { s.doneOnce.Do(func() { close(s.done) }) }

var grpcSessions = struct {
	sync.RWMutex
	m map[string]*grpcSession
}{m: map[string]*grpcSession{}}
var deliveries = session.NewManager(660 * time.Second)

func grpcToken(ctx context.Context) bool {
	want := authTokenValue()
	if want == "" {
		return true
	}
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return false
	}
	for _, key := range []string{"x-node-agent-token", "authorization"} {
		for _, v := range md.Get(key) {
			if v == want || v == "Bearer "+want {
				return true
			}
		}
	}
	return false
}
func authTokenValue() string { return currentAuthToken }

var currentAuthToken string

type grpcService struct{}

func (grpcService) Connect(stream transport.NodeAgentService_ConnectServer) error {
	if !grpcToken(stream.Context()) {
		return status.Error(codes.Unauthenticated, "invalid node token")
	}
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	if first.Register == nil || first.Register.NodeID == "" {
		return status.Error(codes.InvalidArgument, "register must be first frame")
	}
	r := first.Register
	reg.Upsert(&heartbeat.Node{NodeID: r.NodeID, Hostname: r.Hostname, Workspaces: r.Workspaces, Executors: r.Executors, Versions: r.Versions, Transports: []string{"grpc", "http"}})
	idBytes := make([]byte, 8)
	_, _ = rand.Read(idBytes)
	sessionID := hex.EncodeToString(idBytes)
	s := &grpcSession{nodeID: r.NodeID, out: make(chan *transport.ServerFrame, 16), done: make(chan struct{})}
	grpcSessions.Lock()
	if old := grpcSessions.m[r.NodeID]; old != nil {
		old.closeDone()
	}
	grpcSessions.m[r.NodeID] = s
	grpcSessions.Unlock()
	defer func() {
		grpcSessions.Lock()
		if grpcSessions.m[r.NodeID] == s {
			delete(grpcSessions.m, r.NodeID)
		}
		grpcSessions.Unlock()
		s.closeDone()
	}()
	if err := stream.Send(&transport.ServerFrame{RegisterAck: &transport.RegisterAck{SessionID: sessionID, HeartbeatIntervalMs: 15000}}); err != nil {
		return err
	}
	errCh := make(chan error, 1)
	go func() {
		for {
			select {
			case f := <-s.out:
				if err := stream.Send(f); err != nil {
					errCh <- err
					return
				}
			case <-s.done:
				return
			}
		}
	}()
	for {
		select {
		case err := <-errCh:
			return err
		default:
		}
		f, err := stream.Recv()
		if err != nil {
			return err
		}
		if f.Heartbeat != nil {
			reg.Heartbeat(r.NodeID, f.Heartbeat.Status)
		}
		if f.JobAck != nil {
			deliveries.AcceptAck(f.JobAck.DeliveryID)
		}
		if f.JobResult != nil {
			accepted := deliveries.AcceptResult(f.JobResult.DeliveryID, session.Result{TaskID: f.JobResult.TaskID, Success: f.JobResult.Success, Output: f.JobResult.Output, Error: f.JobResult.Error})
			if accepted {
				// Mirror into the HTTP results store so GET /api/results/{task_id}
				// (kanban polling) works identically for both lanes.
				rmu.Lock()
				results[f.JobResult.TaskID] = storedResult{res: transport.ResultRequest{TaskID: f.JobResult.TaskID, Success: f.JobResult.Success, Output: f.JobResult.Output, Error: f.JobResult.Error, DurationMs: f.JobResult.DurationMs}, at: time.Now()}
				rmu.Unlock()
				log.Printf("grpc result %s success=%v %dms", f.JobResult.TaskID, f.JobResult.Success, f.JobResult.DurationMs)
			}
			select {
			case s.out <- &transport.ServerFrame{ResultAck: &transport.ResultAck{DeliveryID: f.JobResult.DeliveryID, Accepted: accepted}}:
			default:
			}
		}
	}
}

func dispatchGRPC(req transport.DispatchRequest, nodeID string) (string, bool) {
	grpcSessions.RLock()
	s := grpcSessions.m[nodeID]
	grpcSessions.RUnlock()
	if s == nil {
		return "", false
	}
	d := deliveries.NewDelivery(req.TaskID, req.Board, req.Workspace)
	job := &transport.ServerFrame{DispatchJob: &transport.DispatchJob{DeliveryID: d.ID, Attempt: 1, TaskID: req.TaskID, Board: req.Board, Message: req.Message, Workspace: req.Workspace, Executor: req.Executor, Command: req.Command, Model: req.Model, Provider: req.Provider, PrequestNote: req.PrequestNote, LeaseExpiresAtUnixMs: d.ExpiresAt.UnixMilli()}}
	select {
	case s.out <- job:
		log.Printf("grpc dispatch %s -> %s delivery=%s", req.TaskID, nodeID, d.ID)
		return d.ID, true
	default:
		return "", false
	}
}

var _ = transport.GRPCServiceName
