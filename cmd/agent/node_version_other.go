//go:build !windows

package main

// The Node preflight is Windows-only: nvm-windows flips a symlink,
// and the stale-node failure mode it fixes was observed on the
// Windows worker. Other platforms keep their existing behavior —
// a genuinely broken node still surfaces the executor's own error.
func ensureNodeForCommandCode() string { return "" }
