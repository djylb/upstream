package upstream

import (
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

// TestDependencies checks what the README promises: every package, tests
// included, depends on the standard library and the shared clock. The root
// package does not link net, and upnet does not link net/http.
func TestDependencies(t *testing.T) {
	if runtime.GOOS == "js" || runtime.GOOS == "wasip1" {
		t.Skip("cannot run the go command")
	}
	goCmd, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go command")
	}
	const module = "github.com/djylb/upstream"
	const clock = "github.com/djylb/timeshim"

	cmd := exec.Command(goCmd, "list", "-m", "-f", "{{.Path}}", "all")
	cmd.Env = append(cmd.Environ(), "GOWORK=off")
	mods, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -m: %v", err)
	}
	for mod := range strings.Lines(string(mods)) {
		if name := strings.TrimSpace(mod); name != module && name != clock {
			t.Errorf("unexpected module: %s", name)
		}
	}

	out, err := exec.Command(goCmd, "list", "-deps", "-test", "-f", "{{.ImportPath}} {{.Standard}}", "./...").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for line := range strings.Lines(string(out)) {
		path, std, _ := strings.Cut(strings.TrimSpace(line), " ")
		ours := path == module
		for _, prefix := range []string{module + "/", module + "_test", module + " ", module + ".test"} {
			ours = ours || strings.HasPrefix(path, prefix)
		}
		if std != "true" && !ours && path != clock {
			t.Errorf("unexpected package dependency: %s", path)
		}
	}

	// upnet is a leaf beside uphttp: net and the root package only.
	out, err = exec.Command(goCmd, "list", "-deps", module+"/upnet").Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for line := range strings.Lines(string(out)) {
		switch path := strings.TrimSpace(line); {
		case path == "net/http" || strings.HasPrefix(path, "net/http/"):
			t.Errorf("upnet depends on %s", path)
		case strings.HasPrefix(path, module+"/") && path != module+"/upnet" && path != module+"/internal/dialresult" && path != module+"/internal/latency":
			t.Errorf("upnet depends on %s", path)
		}
	}

	out, err = exec.Command(goCmd, "list", "-deps", module).Output()
	if err != nil {
		t.Fatalf("go list: %v", err)
	}
	for line := range strings.Lines(string(out)) {
		switch path := strings.TrimSpace(line); {
		case path == "net" || strings.HasPrefix(path, "net/"):
			t.Errorf("the root package depends on %s", path)
		case strings.HasPrefix(path, module+"/") && path != module+"/internal/latency":
			t.Errorf("the root package depends on %s", path)
		}
	}
}
