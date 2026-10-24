package upstream

import (
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
)

// TestDependencies checks what the README promises: every package, tests
// included, depends on the standard library and this module alone, and the
// root package does not link net/http or even net.
func TestDependencies(t *testing.T) {
	if runtime.GOOS == "js" || runtime.GOOS == "wasip1" {
		t.Skip("cannot run the go command")
	}
	goCmd, err := exec.LookPath("go")
	if err != nil {
		t.Skip("no go command")
	}
	const module = "github.com/djylb/upstream"

	mod, err := os.ReadFile("go.mod")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(mod), "require") {
		t.Error("go.mod has requirements")
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
		if std != "true" && !ours {
			t.Errorf("dependency outside the standard library: %s", path)
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
		case strings.HasPrefix(path, module+"/"):
			t.Errorf("the root package depends on %s", path)
		}
	}
}
