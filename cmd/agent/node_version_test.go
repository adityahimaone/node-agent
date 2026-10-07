package main

import "testing"

func TestParseNodeMajor(t *testing.T) {
	cases := []struct {
		in    string
		major int
		ok    bool
	}{
		{"v22.8.0\n", 22, true},
		{"v22.22.3", 22, true},
		{"v14.15.1", 14, true},
		{"v24", 24, true},
		{"22.8.0", 22, true},
		{"garbage", 0, false},
		{"", 0, false},
		{"v.x.0", 0, false},
	}
	for _, c := range cases {
		major, ok := parseNodeMajor(c.in)
		if major != c.major || ok != c.ok {
			t.Errorf("parseNodeMajor(%q) = (%d, %v), want (%d, %v)", c.in, major, ok, c.major, c.ok)
		}
	}
}

func TestParseNvmSettings(t *testing.T) {
	content := "root: C:\\Users\\User\\AppData\\Roaming\\nvm\npath: C:\\Program Files\\nodejs\narch: 64\nproxy: none\n"
	root, linkPath := parseNvmSettings(content)
	if root != `C:\Users\User\AppData\Roaming\nvm` {
		t.Errorf("root = %q", root)
	}
	if linkPath != `C:\Program Files\nodejs` {
		t.Errorf("linkPath = %q", linkPath)
	}
	if r, p := parseNvmSettings("arch: 64\n"); r != "" || p != "" {
		t.Errorf("missing lines must parse empty, got %q %q", r, p)
	}
}

func TestPickNodeVersions(t *testing.T) {
	cases := []struct {
		entries  []string
		minMajor int
		want     []string
	}{
		{[]string{"v14.15.1", "v22.8.0", "v24.19.0"}, 22, []string{"v22.8.0", "v24.19.0"}},
		{[]string{"v24.19.0", "v22.8.0", "v14.15.1"}, 22, []string{"v22.8.0", "v24.19.0"}},
		{[]string{"v22.10.0", "v22.8.0"}, 22, []string{"v22.8.0", "v22.10.0"}},
		{[]string{"v14.15.1"}, 22, nil},
		{[]string{"v21.9.0"}, 22, nil},
		{[]string{}, 22, nil},
		{[]string{"not-a-version", "v22.8.0"}, 22, []string{"v22.8.0"}},
	}
	for _, c := range cases {
		got := pickNodeVersions(c.entries, c.minMajor)
		if len(got) != len(c.want) {
			t.Errorf("pickNodeVersions(%v, %d) = %v, want %v", c.entries, c.minMajor, got, c.want)
			continue
		}
		for i := range got {
			if got[i] != c.want[i] {
				t.Errorf("pickNodeVersions(%v, %d) = %v, want %v", c.entries, c.minMajor, got, c.want)
				break
			}
		}
	}
}

func TestNodeVersionTooOldOutput(t *testing.T) {
	real := "Command Code needs Node.js 22 or newer — you're on v14.15.1."
	if !nodeVersionTooOldOutput(real) {
		t.Error("the real Command Code version error must match")
	}
	if nodeVersionTooOldOutput("provenance executor=commandcode\nrun finished") {
		t.Error("normal output must not match")
	}
	if nodeVersionTooOldOutput("") {
		t.Error("empty output must not match")
	}
}
