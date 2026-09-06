package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"node-agent/internal/transport"
)

func main() {
	server := os.Getenv("NODE_AGENT_SERVER")
	if server=="" { server="http://100.80.220.71:8788" }
	nodeID := os.Getenv("NODE_AGENT_ID")
	if nodeID=="" {
		h,_:=os.Hostname()
		if h=="" { h="mac" }
		// normalize: adityas-macbook-pro -> mac, aditya-rtx -> windows
		if len(h)>3 && h[:6]=="aditya" { if contains(h,"macbook") { nodeID="mac" } else { nodeID="windows" } } else { nodeID=h }
	}
	fmt.Printf("node-agent %s -> %s\n", nodeID, server)

	// load workspaces
	wsPaths := loadWorkspaces()

	// register
	if err:=postJSON(server+"/api/nodes/register", transport.RegisterRequest{NodeID:nodeID, Hostname:hostname(), Version:"0.1.0", Workspaces:wsPaths}); err!=nil {
		log.Fatalf("register: %v", err)
	}
	log.Printf("registered %s workspaces=%v", nodeID, wsPaths)

	go heartbeatLoop(server, nodeID)

	// poll loop — re-register when the server lost our registration (restart).
	client:=&http.Client{Timeout:30*time.Second}
	register:=func() error {
		return postJSON(server+"/api/nodes/register", transport.RegisterRequest{NodeID:nodeID, Hostname:hostname(), Version:"0.1.0", Workspaces:loadWorkspaces()})
	}
	if err:=register(); err!=nil { log.Printf("register: %v (will retry on poll 404)", err) }
	for {
		req,_:=http.NewRequest("GET", server+"/api/nodes/"+nodeID+"/poll", nil)
		resp, err:=client.Do(req)
		if err!=nil { log.Printf("poll err: %v", err); time.Sleep(2*time.Second); continue }
		if resp.StatusCode==204 { resp.Body.Close(); continue }
		if resp.StatusCode==404 {
			// server restarted / lost state — re-register then keep polling
			resp.Body.Close()
			if rerr:=register(); rerr!=nil { log.Printf("re-register: %v", rerr); time.Sleep(2*time.Second) }
			continue
		}
		if resp.StatusCode!=200 { b:=readAll(resp); log.Printf("poll %d: %s", resp.StatusCode, string(b)); resp.Body.Close(); time.Sleep(2*time.Second); continue }
		var job transport.DispatchRequest
		if err:=json.NewDecoder(resp.Body).Decode(&job); err!=nil { log.Printf("decode: %v",err); resp.Body.Close(); continue }
		resp.Body.Close()
		log.Printf("job %s ws=%s msg=%.80s", job.TaskID, job.Workspace, job.Message)
		// mark busy
		_ = postJSON(server+"/api/nodes/"+nodeID+"/heartbeat", transport.HeartbeatRequest{NodeID:nodeID, Status:"busy"})
		start:=time.Now()
		output, ok, errStr := runJob(job)
		dur:=time.Since(start).Milliseconds()
		res:=transport.ResultRequest{TaskID:job.TaskID, Success:ok, Output:output, Error:errStr, DurationMs:dur}
		if err:=postJSON(server+"/api/nodes/"+nodeID+"/result", res); err!=nil { log.Printf("result post err: %v",err) }
		_ = postJSON(server+"/api/nodes/"+nodeID+"/heartbeat", transport.HeartbeatRequest{NodeID:nodeID, Status:"idle"})
	}
}

func heartbeatLoop(server, nodeID string) {
	for {
		time.Sleep(15 * time.Second)
		_ = postJSON(server+"/api/nodes/"+nodeID+"/heartbeat", transport.HeartbeatRequest{NodeID:nodeID, Status:"idle"})
	}
}

func contains(s, sub string) bool { return bytes.Contains([]byte(s), []byte(sub)) }
func hostname() string { h,_:=os.Hostname(); if h==""{return "unknown"}; return h }
func loadWorkspaces() []string {
	f,err:=os.ReadFile(os.ExpandEnv("$HOME/.hermes/workspaces.json"))
	if err!=nil { return nil }
	var data struct{ Workspaces []struct{Path string `json:"path"`} `json:"workspaces"`}
	if err:=json.Unmarshal(f,&data); err!=nil { return nil }
	out:=make([]string,0,len(data.Workspaces))
	for _,w:=range data.Workspaces { out=append(out,w.Path) }
	return out
}
func postJSON(url string, v any) error {
	b,_:=json.Marshal(v)
	resp,err:=http.Post(url,"application/json", bytes.NewReader(b))
	if err!=nil { return err }
	defer resp.Body.Close()
	if resp.StatusCode>=300 { bb:=readAll(resp); return fmt.Errorf("%d %s", resp.StatusCode, string(bb)) }
	return nil
}
func readAll(r *http.Response) []byte { defer r.Body.Close(); b,_:=bytesReadAll(r); return b }
func bytesReadAll(r *http.Response) ([]byte,error){ buf:=new(bytes.Buffer); _,e:=buf.ReadFrom(r.Body); return buf.Bytes(),e }

func runJob(job transport.DispatchRequest) (output string, ok bool, errStr string) {
	ws:=job.Workspace
	if ws=="" { ws=os.ExpandEnv("$HOME") }
	if _,err:=os.Stat(ws); err!=nil {
		return "", false, fmt.Sprintf("workspace not found: %s", ws)
	}
	tmpDir:=filepath.Join(os.TempDir(), "node-agent-"+job.TaskID)
	_ = os.MkdirAll(tmpDir,0755)
	logFile:=filepath.Join(tmpDir,"run.log")

	// Heuristic: shell meta or common CLI prefix → run shell directly (no LLM).
	// Otherwise treat as hermes/codex task prompt.
	shellPrefixes := []string{"git ", "ls ", "cat ", "echo ", "pwd", "cd ", "grep ", "find ", "head ", "tail ", "curl ", "node ", "npm ", "pnpm ", "python "}
	isShell := bytes.Contains([]byte(job.Message), []byte(";")) || bytes.Contains([]byte(job.Message), []byte("&&")) || bytes.Contains([]byte(job.Message), []byte("|"))
	for _, p := range shellPrefixes {
		if bytes.HasPrefix([]byte(job.Message), []byte(p)) { isShell = true; break }
	}
	var cmd *exec.Cmd
	if isShell {
		cmd=exec.Command("bash","-lc", job.Message)
		cmd.Dir=ws
	} else {
		hermesBin:=findBin("hermes")
		codexBin:=findBin("codex")
		if hermesBin!="" {
			cmd=exec.Command(hermesBin, "chat", "-q", job.Message)
			cmd.Dir=ws
			cmd.Env=append(os.Environ(), "HERMES_WORKSPACE="+ws)
		} else if codexBin!="" {
			cmd=exec.Command(codexBin, "exec", "--full-auto", job.Message)
			cmd.Dir=ws
		} else {
			cmd=exec.Command("bash","-lc", job.Message)
			cmd.Dir=ws
		}
	}
	out, err:=cmd.CombinedOutput()
	_ = os.WriteFile(logFile, out, 0644)
	if err!=nil {
		return string(out), false, err.Error()
	}
	return string(out), true, ""
}
func findBin(name string) string {
	for _,p:=range []string{os.ExpandEnv("$HOME/.local/bin/"+name), "/opt/homebrew/bin/"+name, "/usr/local/bin/"+name} {
		if _,err:=os.Stat(p); err==nil { return p }
	}
	if b,err:=exec.LookPath(name); err==nil { return b }
	return ""
}
