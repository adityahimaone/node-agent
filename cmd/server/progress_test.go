package main

import (
	"strings"
	"testing"
)

// TestProgressWindowDeliversEveryBytePastTheCap replays the
// failure the old handler had: a run logging more than the
// 512KB cap, with the poller polling after every append. It
// must receive every byte appended — a capped buffer must
// never freeze it out — and end caught up.
func TestProgressWindowDeliversEveryBytePastTheCap(t *testing.T) {
	const cap = 512 * 1024
	chunk := strings.Repeat("x", 300*1024)
	var full string
	base, off := 0, 0
	var received int
	for i := 0; i < 4; i++ {
		full += chunk
		if len(full) > cap {
			dropped := len(full) - cap
			base += dropped
			full = full[dropped:]
		}
		text, next := progressWindow(full, base, off)
		received += len(text)
		off = next
	}
	if want := 4 * 300 * 1024; received != want {
		t.Fatalf("poller received %d bytes, want %d (every byte appended)", received, want)
	}
	if want := base + len(full); off != want {
		t.Fatalf("poller offset = %d, want %d (caught up with the buffer)", off, want)
	}
}

// TestProgressWindowResendsWhenPositionWasDropped covers a
// poller that held offset 0 while the buffer overflowed: its
// position was dropped, so everything retained is new to it,
// and it must be told so in absolute terms.
func TestProgressWindowResendsWhenPositionWasDropped(t *testing.T) {
	const cap = 512 * 1024
	full := strings.Repeat("x", cap)
	base := 400 * 1024 // the first 400KB was dropped by the cap

	text, next := progressWindow(full, base, 0)
	if len(text) != cap {
		t.Fatalf("resend = %d bytes, want the full retained %d", len(text), cap)
	}
	if next != base+cap {
		t.Fatalf("next offset = %d, want %d", next, base+cap)
	}

	// A caught-up poller gets nothing and keeps its position.
	text, next = progressWindow(full, base, base+cap)
	if text != "" || next != base+cap {
		t.Fatalf("caught-up poller = (%q, %d), want (\"\", %d)", text, next, base+cap)
	}
}

// TestProgressWindowStreamsTheMiddle covers the ordinary case:
// the poller is somewhere inside the retained window and must
// get exactly the bytes after it.
func TestProgressWindowStreamsTheMiddle(t *testing.T) {
	full := "hello, world"
	text, next := progressWindow(full, 0, 7)
	if text != "world" || next != len(full) {
		t.Fatalf("window = (%q, %d), want (\"world\", %d)", text, next, len(full))
	}
}
