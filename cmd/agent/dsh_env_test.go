package main

import (
	"strings"
	"testing"
)

func TestDSHEnvWithPermission(t *testing.T) {
	base := []string{"PATH=/usr/bin"}
	if got := dshEnvWithPermission(base, ""); len(got) != 1 {
		t.Fatalf("empty mode must not add env: %v", got)
	}
	if got := dshEnvWithPermission(base, "  "); len(got) != 1 {
		t.Fatalf("blank mode must not add env: %v", got)
	}
	got := dshEnvWithPermission(base, " danger-full-access ")
	if !strings.Contains(strings.Join(got, " "), "DSH_PERMISSION_MODE=danger-full-access") {
		t.Fatalf("mode not mapped: %v", got)
	}
}
