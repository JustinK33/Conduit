package api

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"

	"github.com/JustinK33/Conduit/internal/etl"
	"github.com/JustinK33/Conduit/internal/store"
	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

// KeyLookup resolves the keys issued with `conduit keys`. *store.KeyStore is
// the implementation; nil means only CONDUIT_API_KEYS count.
type KeyLookup interface {
	TenantForKey(ctx context.Context, key string) (string, error)
	HasActiveKeys(ctx context.Context) (bool, error)
}

// APIKeyAuth requires `Authorization: Bearer <key>` and records which tenant
// the key acts for. A key from keys acts for the default tenant; anything else
// is looked up in lookup.
//
// Auth is opt-in: with no keys and no live issued key the API is open and
// every request acts for the default tenant, which keeps the quickstart, the
// load tests, and CI working unchanged. The server warns at boot when that is
// the case.
//
// This mounts on the /api groups, never on the root router, so the probes and
// the Prometheus scrape stay reachable. See docs/DEPLOYMENT.md.
func APIKeyAuth(keys []string, lookup KeyLookup) gin.HandlerFunc {
	// Pre-convert once so the comparison below does no allocation per request.
	expected := make([][]byte, 0, len(keys))
	for _, k := range keys {
		expected = append(expected, []byte(k))
	}

	return func(c *gin.Context) {
		ctx := c.Request.Context()
		presented := bearerToken(c.GetHeader("Authorization"))

		if presented != "" {
			// Constant-time comparison against every key, with no early exit, so
			// response timing does not leak which key matched or how far it matched.
			var ok int
			for _, want := range expected {
				ok |= subtle.ConstantTimeCompare([]byte(presented), want)
			}
			if ok == 1 {
				c.Set(tenantKey, models.DefaultTenant)
				c.Next()
				return
			}
			if lookup != nil {
				tenant, err := lookup.TenantForKey(ctx, presented)
				if err == nil {
					c.Set(tenantKey, tenant)
					c.Next()
					return
				}
				if !errors.Is(err, store.ErrKeyNotFound) {
					zerolog.Ctx(ctx).Error().Err(err).Msg("api key lookup failed")
					RespondError(c, http.StatusInternalServerError, "internal_error", "failed to check api key")
					return
				}
			}
		}

		open := len(expected) == 0
		if open && lookup != nil {
			// ponytail: one EXISTS per unauthenticated request, cache it if an
			// open instance ever takes enough traffic to notice.
			active, err := lookup.HasActiveKeys(ctx)
			if err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Msg("api key lookup failed")
				RespondError(c, http.StatusInternalServerError, "internal_error", "failed to check api key")
				return
			}
			open = !active
		}
		if open {
			c.Set(tenantKey, models.DefaultTenant)
			c.Next()
			return
		}

		if presented == "" {
			RespondError(c, http.StatusUnauthorized, "unauthorized", "missing bearer token")
			return
		}
		RespondError(c, http.StatusUnauthorized, "unauthorized", "invalid bearer token")
	}
}

const tenantKey = "conduit.tenant"

// tenantOf is the tenant a request acts for. Auth sets it, and a request on an
// open instance acts for the default tenant.
func tenantOf(c *gin.Context) string {
	if t := c.GetString(tenantKey); t != "" {
		return t
	}
	return models.DefaultTenant
}

func bearerToken(header string) string {
	const prefix = "Bearer "
	if len(header) < len(prefix) || !strings.EqualFold(header[:len(prefix)], prefix) {
		return ""
	}
	return strings.TrimSpace(header[len(prefix):])
}

// taskAllowed keeps sql.etl to the default tenant. It runs SQL against the
// operator's own databases, so a tenant key must not be able to reach it.
func taskAllowed(c *gin.Context, name string) bool {
	if name == etl.TaskName() && tenantOf(c) != models.DefaultTenant {
		RespondError(c, http.StatusForbidden, "forbidden", name+" is only available to the default tenant")
		return false
	}
	return true
}
