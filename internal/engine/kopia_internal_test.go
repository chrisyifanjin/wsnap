package engine

import (
	"strings"
	"testing"

	"github.com/chrisyifanjin/wsnap/internal/protocol"
)

// TestKopiaCachePerRepository: a sidecar that opens its own repository and
// a fork source must not share a Kopia cache between them.
func TestKopiaCachePerRepository(t *testing.T) {
	k := &Kopia{}
	cacheOf := func(prefix string) string {
		env, err := k.env(Target{StateDir: t.TempDir(), Backend: protocol.Backend{S3: &protocol.S3Backend{Bucket: "b", Prefix: prefix}}})
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range env {
			if v, ok := strings.CutPrefix(e, "KOPIA_CACHE_DIRECTORY="); ok {
				return v[strings.LastIndex(v, "/")+1:]
			}
		}
		t.Fatal("no cache directory set")
		return ""
	}
	if cacheOf("ns/child") == cacheOf("ns/parent") {
		t.Error("two repositories share one cache")
	}
	if first, again := cacheOf("ns/child"), cacheOf("ns/child"); first != again {
		t.Error("the cache of one repository is not stable")
	}
}
