package upnet

import (
	"testing"

	"github.com/djylb/upstream/internal/leakcheck"
)

func TestMain(m *testing.M) { leakcheck.VerifyTestMain(m) }
