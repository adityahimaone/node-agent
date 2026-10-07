package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
)

// probedNodeMajor reports the major node version a Command Code
// run would get. linkDir empty probes `node` through PATH — the
// same resolution the cmdc launcher uses; otherwise the node.exe
// inside the given directory is probed directly.
func probedNodeMajor(linkDir string) (int, bool) {
	var cmd *exec.Cmd
	if linkDir == "" {
		cmd = exec.Command("node", "--version")
	} else {
		cmd = exec.Command(filepath.Join(linkDir, "node.exe"), "--version")
	}
	out, err := cmd.Output()
	if err != nil {
		return 0, false
	}
	return parseNodeMajor(string(out))
}

// ensureNodeForCommandCode is the Command Code Node preflight.
// An old node (an nvm default, a Volta pin) fails every Command
// Code run in ~140ms and leaves the card blocked; when nvm-windows
// has a Node >= 22 installed, this repoints the nodejs symlink to
// it — the same switch `nvm use` performs — so the run proceeds.
// It returns a progress note, or "" when nothing needed changing.
// Every failure path returns a note and lets the run proceed: the
// original version error is the honest outcome when no fix exists.
func ensureNodeForCommandCode() string {
	if major, ok := probedNodeMajor(""); ok && major >= commandCodeMinNodeMajor {
		return ""
	}
	root, linkPath := "", ""
	if content, err := os.ReadFile(filepath.Join(os.Getenv("APPDATA"), "nvm", "settings.txt")); err == nil {
		root, linkPath = parseNvmSettings(string(content))
	}
	if root == "" {
		if appData := os.Getenv("APPDATA"); appData != "" {
			root = filepath.Join(appData, "nvm")
		}
	}
	if linkPath == "" {
		linkPath = `C:\Program Files\nodejs`
	}
	if root == "" {
		return ""
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return ""
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			names = append(names, e.Name())
		}
	}
	picks := pickNodeVersions(names, commandCodeMinNodeMajor)
	if len(picks) == 0 {
		return fmt.Sprintf("Node >= %d required by Command Code; none installed in nvm (%s) — run: nvm install lts", commandCodeMinNodeMajor, root)
	}
	// Only ever repoint a symlink — never delete a real directory.
	if _, err := os.Readlink(linkPath); err != nil {
		return linkPath + " is not a symlink; leaving node alone (run: nvm use " + picks[0] + ")"
	}
	// Walk candidates lowest-first and probe each install's
	// own node.exe before repointing: a broken nvm install
	// (an interrupted download leaves a 0-byte node.exe)
	// must be skipped in favor of the next working one.
	for _, pick := range picks {
		target := filepath.Join(root, pick)
		if major, ok := probedNodeMajor(target); !ok || major < commandCodeMinNodeMajor {
			continue
		}
		if err := os.Remove(linkPath); err != nil {
			return "node switch failed: " + err.Error()
		}
		if err := os.Symlink(target, linkPath); err != nil {
			return "node switch failed: " + err.Error()
		}
		return fmt.Sprintf("Node was older than %d; switched nvm to %s", commandCodeMinNodeMajor, pick)
	}
	return fmt.Sprintf("no working Node >= %d in nvm (%s) — run: nvm install lts", commandCodeMinNodeMajor, root)
}
