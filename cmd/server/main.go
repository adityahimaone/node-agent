package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"node-agent/internal/heartbeat"
	"node-agent/internal/transport"
)

var (
	reg      = heartbeat.New(45 * time.Second)
	queues   = map[string]chan transport.DispatchRequest{}
	qmu      sync.Mutex
	results  = map[string]transport.ResultRequest{}
	rmu      sync.Mutex
)

func getQueue(nodeID string) chan transport.DispatchRequest {
	qmu.Lock(); defer qmu.Unlock()
	if ch, ok := queues[nodeID]; ok { return ch }
	ch := make(chan transport.DispatchRequest, 16)
	queues[nodeID]=ch
	return ch
}

func main() {
	addr := os.Getenv("NODE_AGENT_ADDR")
	if addr=="" { addr=":8788" }
	r := chi.NewRouter()

	r.Get("/health", func(w http.ResponseWriter, r *http.Request){
		transport.WriteJSON(w,200,map[string]any{"ok":true,"nodes": reg.List()})
	})
	r.Get("/api/nodes", func(w http.ResponseWriter, r *http.Request){
		transport.WriteJSON(w,200,reg.List())
	})
	r.Post("/api/nodes/register", func(w http.ResponseWriter, r *http.Request){
		var req transport.RegisterRequest
		if err:=transport.ReadJSON(r,&req); err!=nil { http.Error(w,err.Error(),400); return }
		reg.Upsert(&heartbeat.Node{NodeID:req.NodeID, Hostname:req.Hostname, Workspaces:req.Workspaces, Status:"idle"})
		qmu.Lock(); if _,ok:=queues[req.NodeID]; !ok { queues[req.NodeID]=make(chan transport.DispatchRequest,16) }; qmu.Unlock()
		log.Printf("register %s (%s) workspaces=%v", req.NodeID, req.Hostname, req.Workspaces)
		transport.WriteJSON(w,200,map[string]string{"status":"ok"})
	})
	r.Post("/api/nodes/{id}/heartbeat", func(w http.ResponseWriter, r *http.Request){
		id:=chi.URLParam(r,"id")
		var req transport.HeartbeatRequest
		_ = transport.ReadJSON(r,&req)
		if !reg.Heartbeat(id, req.Status) {
			http.Error(w,"unknown node",404); return
		}
		transport.WriteJSON(w,200,map[string]string{"status":"ok"})
	})
	r.Get("/api/nodes/{id}/poll", func(w http.ResponseWriter, r *http.Request){
		id:=chi.URLParam(r,"id")
		if _,ok:=reg.Get(id); !ok { http.Error(w,"unknown node",404); return }
		ch:=getQueue(id)
		// long-poll up to 25s
		ctx, cancel := context.WithTimeout(r.Context(), 25*time.Second)
		defer cancel()
		select {
		case job:=<-ch:
			transport.WriteJSON(w,200,job)
		case <-ctx.Done():
			// 204 no job — client retries immediately
			w.WriteHeader(204)
		case <-r.Context().Done():
			return
		}
	})
	r.Post("/api/nodes/{id}/result", func(w http.ResponseWriter, r *http.Request){
		id:=chi.URLParam(r,"id")
		var req transport.ResultRequest
		if err:=transport.ReadJSON(r,&req); err!=nil { http.Error(w,err.Error(),400); return }
		rmu.Lock(); results[req.TaskID]=req; rmu.Unlock()
		log.Printf("result %s from %s success=%v %dms", req.TaskID, id, req.Success, req.DurationMs)
		transport.WriteJSON(w,200,map[string]string{"status":"ok"})
	})
	r.Post("/api/dispatch", func(w http.ResponseWriter, r *http.Request){
		var req transport.DispatchRequest
		if err:=transport.ReadJSON(r,&req); err!=nil { http.Error(w,err.Error(),400); return }
		// find node by workspace prefix: choose node that owns workspace
		nodeID:=""
		if req.Workspace!="" {
			for _,n:=range reg.List() {
				for _,ws:=range n.Workspaces {
					if req.Workspace==ws || len(req.Workspace)>len(ws) && req.Workspace[:len(ws)]==ws {
						nodeID=n.NodeID; break
					}
				}
				if nodeID!=""{break}
			}
		}
		// fallback: first idle node
		if nodeID=="" {
			for _,n:=range reg.List() { if n.Status!="offline" { nodeID=n.NodeID; break } }
		}
		if nodeID=="" { http.Error(w,"no nodes available",503); return }
		// if DispatchRequest didn't specify node, we route via workspace
		// allow explicit TaskID routing via header/query? For now use routed node
		ch:=getQueue(nodeID)
		select {
		case ch<-req:
			log.Printf("dispatch %s -> %s ws=%s", req.TaskID, nodeID, req.Workspace)
			transport.WriteJSON(w,200,map[string]any{"status":"queued","node_id":nodeID})
		default:
			http.Error(w,"node queue full",503)
		}
	})
	r.Get("/api/results/{task_id}", func(w http.ResponseWriter, r *http.Request){
		id:=chi.URLParam(r,"task_id")
		rmu.Lock(); res,ok:=results[id]; rmu.Unlock()
		if !ok { http.Error(w,"not found",404); return }
		transport.WriteJSON(w,200,res)
	})
	// sf init SF. json. dump. l. for. j. w. /api/workspaces proxy. / l. v u t. project. s. s. d. .. -1
	r.Get("/api/workspaces", func(w http.ResponseWriter, req *http.Request){
		// return server-side workspaces.json + live node status
		f,err:=os.ReadFile(os.ExpandEnv("$HOME/.hermes/workspaces.json"))
		if err!=nil { http.Error(w,err.Error(),500); return }
		var data map[string]any
		_ = json.Unmarshal(f,&data)
		transport.WriteJSON(w,200,map[string]any{"workspaces":data["workspaces"],"nodes":reg.List()})
	})

	log.Printf("node-agent server listening on %s", addr)
	if err:=http.ListenAndServe(addr,r); err!=nil { log.Fatal(err) }
}
