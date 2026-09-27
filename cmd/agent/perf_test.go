package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"
)

// streamCommand posts progress chunks to the control plane. Point it at a local
// stub so the test does not block on an unreachable NODE_AGENT_SERVER
// (t.Setenv restores the previous value automatically).
func stubControlPlane(t *testing.T) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("NODE_AGENT_SERVER", srv.URL)
}

// The old loop exited only when `pending == nil`, which nothing but the 400ms
// ticker could clear. A command that finished instantly still took 400ms.
func TestStreamCommandNoFlushTailStall(t *testing.T) {
	stubControlPlane(t)
	cmd := exec.Command("bash", "-lc", "echo hi")
	start := time.Now()
	out, err := streamCommand(cmd, "perf-flush-tail")
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("streamCommand: %v", err)
	}
	if !strings.Contains(string(out), "hi") {
		t.Fatalf("output = %q, want it to contain 'hi'", out)
	}
	// The 400ms ticker is the only thing this test could be waiting on. Allow a
	// little headroom for a loaded machine, but stay far below a full tick.
	if elapsed >= 350*time.Millisecond {
		t.Fatalf("streamCommand took %v; expected well under the 400ms flush tick", elapsed)
	}
}

// The final chunk must still be delivered: streamCommand's caller posts the
// captured output as the job result, so a dropped tail corrupts the result.
func TestStreamCommandDeliversFinalChunk(t *testing.T) {
	stubControlPlane(t)
	cmd := exec.Command("bash", "-lc", "printf 'tail-marker-line\\n'")
	out, err := streamCommand(cmd, "perf-final-chunk")
	if err != nil {
		t.Fatalf("streamCommand: %v", err)
	}
	if !strings.Contains(string(out), "tail-marker-line") {
		t.Fatalf("final chunk lost: output = %q", out)
	}
}

// Emits must not block the job: each delivery simulates a ~65ms control-plane
// round trip, so a synchronous emitter would dominate the job's runtime.
func TestEventEmitterDoesNotBlockProducer(t *testing.T) {
	delivered := make(chan string, 64)
	e := newEventEmitter(func(chunk string) { delivered <- chunk })

	start := time.Now()
	for i := 0; i < 4; i++ {
		e.Emit("marker")
	}
	enqueue := time.Since(start)
	if enqueue >= 100*time.Millisecond {
		t.Fatalf("emitting 4 markers took %v; Emit must not block on delivery", enqueue)
	}
	e.Close()
}

// The UI replays streamed markers in arrival order, so batching must not
// reorder them.
func TestEventEmitterPreservesOrder(t *testing.T) {
	var mu sync.Mutex
	var got []string
	e := newEventEmitter(func(chunk string) {
		mu.Lock()
		got = append(got, chunk)
		mu.Unlock()
	})
	want := make([]string, 0, 20)
	for i := 0; i < 20; i++ {
		chunk := fmt.Sprintf("marker-%02d", i)
		want = append(want, chunk)
		e.Emit(chunk)
	}
	e.Close()
	mu.Lock()
	defer mu.Unlock()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("delivered out of order:\n got %v\nwant %v", got, want)
	}
}

// Close must not return until everything queued has been delivered, otherwise
// the final markers race the terminal result post.
func TestEventEmitterCloseDrainsQueue(t *testing.T) {
	var mu sync.Mutex
	var count int
	e := newEventEmitter(func(string) {
		mu.Lock()
		count++
		mu.Unlock()
	})
	for i := 0; i < 30; i++ {
		e.Emit("x")
	}
	e.Close()
	mu.Lock()
	defer mu.Unlock()
	if count != 30 {
		t.Fatalf("Close returned with %d of 30 delivered", count)
	}
}

// Regression: the idle branch of drain() used to Unlock the mutex a second
// time, which is a runtime fatal error ("sync: unlock of unlocked mutex") and
// killed the whole agent process. It only triggers when a concurrent Emit takes
// the lock between the two unlocks, so the test drives Emit from another
// goroutine while the drainer is posting slow chunks.
func TestEventEmitterIdleBranchDoesNotDoubleUnlock(t *testing.T) {
	for i := 0; i < 100; i++ {
		e := newEventEmitter(func(string) { time.Sleep(2 * time.Millisecond) })
		stop := make(chan struct{})
		go func() {
			for {
				select {
				case <-stop:
					return
				default:
					e.Emit("x")
					time.Sleep(time.Millisecond)
				}
			}
		}()
		time.Sleep(3 * time.Millisecond)
		e.Close()
		close(stop)
	}
}

func TestEventEmitterConcurrentEmitAndClose(t *testing.T) {
	for i := 0; i < 200; i++ {
		e := newEventEmitter(func(string) {})
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 5; j++ {
				e.Emit("x")
			}
		}()
		wg.Wait()
		e.Close()
	}
}

func TestEventEmitterEmitAfterCloseIsNoop(t *testing.T) {
	var mu sync.Mutex
	var count int
	e := newEventEmitter(func(string) {
		mu.Lock()
		count++
		mu.Unlock()
	})
	e.Emit("a")
	e.Close()
	e.Emit("b")
	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Fatalf("count = %d, want 1 (post-close emit must be dropped)", count)
	}
}
