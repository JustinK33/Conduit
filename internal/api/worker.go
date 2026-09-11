package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/JustinK33/Conduit/internal/store"
	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

// The pull protocol. A worker claims a job, gets a lease token, and presents
// that token to report the outcome. Every request body here is a narrow DTO:
// a worker can never post a models.Job, because UpdateJob is a full-row
// overwrite whose fencing predicate is disabled by an empty token, and these
// handlers must not be able to express that.
//
// See docs/WORKERS.md for the contract and docs/decisions/0006 for why the
// protocol looks like this.

type ClaimRequest struct {
	// Queues to claim from. Empty means any queue, which is almost never what
	// a remote worker wants, since it will win jobs it has no handler for.
	Queues []string `json:"queues,omitempty"`
	// LeaseSeconds is a request, not a grant. The server clamps it to its own
	// configured maximum.
	LeaseSeconds int `json:"lease_seconds,omitempty"`
}

type ClaimResponse struct {
	Job            models.Job `json:"job"`
	LeaseToken     string     `json:"lease_token"`
	LeaseExpiresAt time.Time  `json:"lease_expires_at"`
}

type HeartbeatRequest struct {
	LeaseToken   string `json:"lease_token"`
	LeaseSeconds int    `json:"lease_seconds,omitempty"`
}

type CompleteRequest struct {
	LeaseToken string            `json:"lease_token"`
	Metadata   map[string]string `json:"metadata,omitempty"`
}

type FailRequest struct {
	LeaseToken string `json:"lease_token"`
	Error      string `json:"error"`
	// Retry is the worker's opinion, and false is binding: it means "do not
	// retry this whatever the policy says". True defers to the server's retry
	// budget, which may still dead-letter the job.
	Retry bool `json:"retry"`
}

// ClaimJob hands the caller the next due job in one of the named queues.
//
// 204 rather than an empty 200 when nothing is due, so a polling client can
// branch on the status line without parsing a body.
//
// ponytail: this is a poll, not a long poll. LISTEN/NOTIFY or a held
// connection is the upgrade when claim QPS starts to matter; until then an
// idle worker costs one indexed query per poll.
func (h *Handler) ClaimJob(c *gin.Context) {
	log := zerolog.Ctx(c.Request.Context())

	var request ClaimRequest
	// An empty body is a valid claim from any queue with the default lease.
	if c.Request.ContentLength > 0 {
		if err := c.ShouldBindJSON(&request); err != nil {
			RespondError(c, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
	}
	if request.LeaseSeconds < 0 {
		RespondError(c, http.StatusBadRequest, "invalid_request", "lease_seconds must not be negative")
		return
	}

	job, err := h.Queue.Claim(c.Request.Context(), request.Queues,
		time.Duration(request.LeaseSeconds)*time.Second)
	if err != nil {
		if errors.Is(err, store.ErrJobNotFound) {
			c.Status(http.StatusNoContent)
			return
		}
		log.Error().Err(err).Msg("claim failed")
		RespondError(c, http.StatusInternalServerError, "internal_error", "failed to claim a job")
		return
	}

	// Same counter the in-process worker increments, so remote execution shows
	// up on the existing dashboards rather than looking like an idle system.
	h.Metrics.JobStarted.Inc()

	// job serialises with LeaseToken elided (json:"-"), so the token appears
	// exactly once in the API surface: here, alongside it.
	c.JSON(http.StatusOK, ClaimResponse{
		Job:            job,
		LeaseToken:     job.LeaseToken,
		LeaseExpiresAt: derefTime(job.LeaseExpiresAt),
	})
}

func (h *Handler) HeartbeatJob(c *gin.Context) {
	log := zerolog.Ctx(c.Request.Context())
	id := c.Param("id")

	var request HeartbeatRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		RespondError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if request.LeaseToken == "" {
		RespondError(c, http.StatusBadRequest, "invalid_request", "lease_token is required")
		return
	}

	expires, err := h.Queue.Heartbeat(c.Request.Context(), id, request.LeaseToken,
		time.Duration(request.LeaseSeconds)*time.Second)
	if err != nil {
		if respondLeaseLost(c, err) {
			return
		}
		log.Error().Err(err).Str("job_id", id).Msg("heartbeat failed")
		RespondError(c, http.StatusInternalServerError, "internal_error", "failed to renew lease")
		return
	}

	c.JSON(http.StatusOK, gin.H{"lease_expires_at": expires})
}

func (h *Handler) CompleteJob(c *gin.Context) {
	log := zerolog.Ctx(c.Request.Context())
	id := c.Param("id")

	var request CompleteRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		RespondError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if request.LeaseToken == "" {
		RespondError(c, http.StatusBadRequest, "invalid_request", "lease_token is required")
		return
	}

	if err := h.Queue.Complete(c.Request.Context(), id, request.LeaseToken, request.Metadata); err != nil {
		if respondLeaseLost(c, err) {
			return
		}
		log.Error().Err(err).Str("job_id", id).Msg("complete failed")
		RespondError(c, http.StatusInternalServerError, "internal_error", "failed to complete job")
		return
	}

	h.Metrics.JobCompleted.Inc()
	c.JSON(http.StatusOK, gin.H{"state": "COMPLETED"})
}

func (h *Handler) FailJob(c *gin.Context) {
	log := zerolog.Ctx(c.Request.Context())
	id := c.Param("id")

	var request FailRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		RespondError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if request.LeaseToken == "" {
		RespondError(c, http.StatusBadRequest, "invalid_request", "lease_token is required")
		return
	}
	if request.Error == "" {
		request.Error = "worker reported failure without a message"
	}

	job, err := h.Queue.Fail(c.Request.Context(), id, request.LeaseToken, request.Error, !request.Retry)
	if err != nil {
		if respondLeaseLost(c, err) {
			return
		}
		if errors.Is(err, store.ErrJobNotFound) {
			RespondError(c, http.StatusNotFound, "not_found", "job not found")
			return
		}
		log.Error().Err(err).Str("job_id", id).Msg("fail failed")
		RespondError(c, http.StatusInternalServerError, "internal_error", "failed to record job failure")
		return
	}

	h.Metrics.JobFailed.Inc()
	c.JSON(http.StatusOK, gin.H{
		"state":        job.State,
		"attempt":      job.Attempt,
		"scheduled_at": job.ScheduledAt,
	})
}

// respondLeaseLost maps a lost lease to 409 lease_lost, distinct from
// 409 invalid_state, so a worker can tell "someone else owns this now" from
// "you asked for something the state machine forbids". It is the expected
// outcome of a worker that stalled past its lease and was requeued, so it is
// not logged as an error.
func respondLeaseLost(c *gin.Context, err error) bool {
	if !errors.Is(err, store.ErrLeaseLost) {
		return false
	}
	RespondError(c, http.StatusConflict, "lease_lost",
		"the lease on this job is no longer held; stop working on it and claim another")
	return true
}

func derefTime(t *time.Time) time.Time {
	if t == nil {
		return time.Time{}
	}
	return *t
}
