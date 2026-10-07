package main

// progressWindow returns the progress bytes a poller at absolute
// offset off has not seen yet, and the absolute offset to report
// back to it.
//
// The progress buffer is capped by dropping its oldest bytes, so
// base is the absolute offset of full[0]. A poller whose position
// was dropped (off < base) is behind the cap: everything retained
// is new to it, so the window starts at the oldest retained byte
// rather than freezing at a position that no longer exists — which
// is what a relative-offset reader does once the buffer has been
// truncated under it.
func progressWindow(full string, base, off int) (string, int) {
	switch {
	case off < base:
		// The position was truncated away; resend everything
		// retained. The dropped middle is unrecoverable, but
		// the log continues from the oldest surviving byte.
		return full, base + len(full)
	case off < base+len(full):
		return full[off-base:], base + len(full)
	}
	// Caught up: nothing new, and the poller keeps its position.
	return "", off
}
