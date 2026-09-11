package api

import (
	"crypto/subtle"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"
)

// APIKeyAuth requires `Authorization: Bearer <key>` where key is one of keys.
//
// An empty key list returns a pass-through, which is what makes auth opt-in:
// an existing deployment, the load tests, and CI keep working unchanged, and
// the server warns at boot that the API is open.
//
// This mounts on the /api/jobs group, never on the root router. /live, /ready,
// /health, and /metrics are registered on the root by cmd/server, and putting
// auth in that chain would 401 the Kubernetes probes and the Prometheus scrape.
// Keeping those endpoints unauthenticated is deliberate, which makes the
// reverse proxy in front responsible for keeping them off the public internet.
// See docs/DEPLOYMENT.md.
//
// One shared key means no per-caller identity: holding it means being able to
// claim, cancel, and read every job in the instance. Per-tenant keys are phase
// 2 in docs/ROADMAP.md.
func APIKeyAuth(keys []string) gin.HandlerFunc {
	if len(keys) == 0 {
		return func(c *gin.Context) { c.Next() }
	}

	// Pre-convert once so the comparison below does no allocation per request.
	expected := make([][]byte, 0, len(keys))
	for _, k := range keys {
		expected = append(expected, []byte(k))
	}

	return func(c *gin.Context) {
		presented := []byte(bearerToken(c.GetHeader("Authorization")))
		if len(presented) == 0 {
			RespondError(c, http.StatusUnauthorized, "unauthorized", "missing bearer token")
			return
		}

		// Constant-time comparison against every key, with no early exit, so
		// response timing does not leak which key matched or how far it matched.
		var ok int
		for _, want := range expected {
			ok |= subtle.ConstantTimeCompare(presented, want)
		}
		if ok != 1 {
			RespondError(c, http.StatusUnauthorized, "unauthorized", "invalid bearer token")
			return
		}

		c.Next()
	}
}

func bearerToken(header string) string {
	const prefix = "Bearer "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}
