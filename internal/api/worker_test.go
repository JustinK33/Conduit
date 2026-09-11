package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/JustinK33/Conduit/internal/store"
	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

// workerQueue records what the handlers passed down and returns canned results.
type workerQueue struct {
	claimJob    models.Job
	claimErr    error
	claimQueues []string
	claimLease  time.Duration

	heartbeatExpires time.Time
	heartbeatErr     error

	completeErr  error
	completeMeta map[string]string

	failJob       models.Job
	failErr       error
	failPermanent bool
	failMsg       string
	failToken     string
}

func (w *workerQueue) Enqueue(context.Context, models.Job) (string, error) { return "", nil }
func (w *workerQueue) Cancel(context.Context, string) error                { return nil }

func (w *workerQueue) Claim(_ context.Context, queues []string, lease time.Duration) (models.Job, error) {
	w.claimQueues = queues
	w.claimLease = lease
	return w.claimJob, w.claimErr
}

func (w *workerQueue) Heartbeat(context.Context, string, string, time.Duration) (time.Time, error) {
	return w.heartbeatExpires, w.heartbeatErr
}

func (w *workerQueue) Complete(_ context.Context, _, _ string, meta map[string]string) error {
	w.completeMeta = meta
	return w.completeErr
}

func (w *workerQueue) Fail(_ context.Context, _, token, errMsg string, permanent bool) (models.Job, error) {
	w.failToken = token
	w.failMsg = errMsg
	w.failPermanent = permanent
	return w.failJob, w.failErr
}

// newWorkerRouter mounts the real routes so the tests exercise routing and
// middleware, not just the handler functions.
func newWorkerRouter(q Queue, keys []string) *gin.Engine {
	router := gin.New()
	NewHandler(q, mockStore{}, zerolog.Nop(), testRegistry(), keys).RegisterRoutes(router)
	return router
}

func do(router *gin.Engine, method, path, body, bearer string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	if bearer != "" {
		req.Header.Set("Authorization", "Bearer "+bearer)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func TestClaimJob(t *testing.T) {
	leased := time.Now().UTC().Add(time.Minute)
	job := models.Job{
		ID:             "job-1",
		State:          models.JobStateRunning,
		LeaseToken:     "lease-tok",
		LeaseExpiresAt: &leased,
		Task:           models.Task{Name: "remote.render"},
	}

	t.Run("returns the job, its token, and its expiry", func(t *testing.T) {
		q := &workerQueue{claimJob: job}
		rec := do(newWorkerRouter(q, nil), http.MethodPost, "/api/jobs/claim",
			`{"queues":["remote"],"lease_seconds":30}`, "")

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
		var got ClaimResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if got.Job.ID != "job-1" {
			t.Errorf("job id = %q, want job-1", got.Job.ID)
		}
		if got.LeaseToken != "lease-tok" {
			t.Errorf("lease_token = %q, want lease-tok", got.LeaseToken)
		}
		if got.LeaseExpiresAt.IsZero() {
			t.Error("lease_expires_at is zero")
		}
		if len(q.claimQueues) != 1 || q.claimQueues[0] != "remote" {
			t.Errorf("queues = %v, want [remote]", q.claimQueues)
		}
		if q.claimLease != 30*time.Second {
			t.Errorf("lease = %v, want 30s", q.claimLease)
		}
	})

	t.Run("the token appears only beside the job, never inside it", func(t *testing.T) {
		q := &workerQueue{claimJob: job}
		rec := do(newWorkerRouter(q, nil), http.MethodPost, "/api/jobs/claim", "", "")

		var envelope struct {
			Job map[string]any `json:"job"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &envelope); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if _, present := envelope.Job["lease_token"]; present {
			t.Error("job object serialised its lease token; it must only appear as the sibling field")
		}
	})

	t.Run("an empty body claims from any queue with the default lease", func(t *testing.T) {
		q := &workerQueue{claimJob: job}
		rec := do(newWorkerRouter(q, nil), http.MethodPost, "/api/jobs/claim", "", "")

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
		if q.claimQueues != nil {
			t.Errorf("queues = %v, want nil", q.claimQueues)
		}
		if q.claimLease != 0 {
			t.Errorf("lease = %v, want 0 so the service applies its default", q.claimLease)
		}
	})

	t.Run("an empty queue is 204 with no body", func(t *testing.T) {
		q := &workerQueue{claimErr: store.ErrJobNotFound}
		rec := do(newWorkerRouter(q, nil), http.MethodPost, "/api/jobs/claim", "", "")

		if rec.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want 204", rec.Code)
		}
		if rec.Body.Len() != 0 {
			t.Errorf("body = %q, want empty", rec.Body.String())
		}
	})

	t.Run("claim does not collide with the :id routes", func(t *testing.T) {
		q := &workerQueue{claimJob: job}
		router := newWorkerRouter(q, nil)

		rec := do(router, http.MethodPost, "/api/jobs/claim", "", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("claim status = %d, want 200", rec.Code)
		}
		// A job id in the same position must still reach the cancel route.
		rec = do(router, http.MethodPost, "/api/jobs/job-1/cancel", "", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("cancel status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("a negative lease is rejected", func(t *testing.T) {
		rec := do(newWorkerRouter(&workerQueue{}, nil), http.MethodPost, "/api/jobs/claim",
			`{"lease_seconds":-1}`, "")
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("status = %d, want 400", rec.Code)
		}
	})
}

func TestWorkerEndpointsRequireALeaseToken(t *testing.T) {
	paths := []struct {
		name string
		path string
		body string
	}{
		{name: "heartbeat", path: "/api/jobs/job-1/heartbeat", body: `{}`},
		{name: "complete", path: "/api/jobs/job-1/complete", body: `{}`},
		{name: "fail", path: "/api/jobs/job-1/fail", body: `{"error":"boom"}`},
	}

	for _, tc := range paths {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(newWorkerRouter(&workerQueue{}, nil), http.MethodPost, tc.path, tc.body, "")
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400: %s", rec.Code, rec.Body.String())
			}
		})
	}
}

func TestWorkerEndpointsMapALostLeaseTo409(t *testing.T) {
	tests := []struct {
		name string
		path string
		body string
		q    *workerQueue
	}{
		{
			name: "heartbeat",
			path: "/api/jobs/job-1/heartbeat",
			body: `{"lease_token":"stale"}`,
			q:    &workerQueue{heartbeatErr: store.ErrLeaseLost},
		},
		{
			name: "complete",
			path: "/api/jobs/job-1/complete",
			body: `{"lease_token":"stale"}`,
			q:    &workerQueue{completeErr: store.ErrLeaseLost},
		},
		{
			name: "fail",
			path: "/api/jobs/job-1/fail",
			body: `{"lease_token":"stale","error":"boom"}`,
			q:    &workerQueue{failErr: store.ErrLeaseLost},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rec := do(newWorkerRouter(tc.q, nil), http.MethodPost, tc.path, tc.body, "")
			if rec.Code != http.StatusConflict {
				t.Fatalf("status = %d, want 409: %s", rec.Code, rec.Body.String())
			}
			// lease_lost has to be distinguishable from invalid_state, because a
			// worker recovers from one by claiming again and from the other not
			// at all.
			var body struct {
				Error ErrorBody `json:"error"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if body.Error.Code != "lease_lost" {
				t.Errorf("code = %q, want lease_lost", body.Error.Code)
			}
		})
	}
}

func TestCompleteJob(t *testing.T) {
	t.Run("passes metadata through and reports COMPLETED", func(t *testing.T) {
		q := &workerQueue{}
		rec := do(newWorkerRouter(q, nil), http.MethodPost, "/api/jobs/job-1/complete",
			`{"lease_token":"tok","metadata":{"worker":"gpu-3"}}`, "")

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
		if q.completeMeta["worker"] != "gpu-3" {
			t.Errorf("metadata = %v, want worker=gpu-3", q.completeMeta)
		}
	})
}

func TestFailJob(t *testing.T) {
	t.Run("retry true defers to the server policy", func(t *testing.T) {
		next := time.Now().UTC().Add(time.Minute)
		q := &workerQueue{failJob: models.Job{
			State: models.JobStatePending, Attempt: 2, ScheduledAt: &next,
		}}
		rec := do(newWorkerRouter(q, nil), http.MethodPost, "/api/jobs/job-1/fail",
			`{"lease_token":"tok","error":"upstream 503","retry":true}`, "")

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
		if q.failPermanent {
			t.Error("retry:true must not be reported as a permanent failure")
		}
		if q.failMsg != "upstream 503" {
			t.Errorf("error = %q, want %q", q.failMsg, "upstream 503")
		}
		if q.failToken != "tok" {
			t.Errorf("token = %q, want tok", q.failToken)
		}

		var body struct {
			State       string     `json:"state"`
			Attempt     int        `json:"attempt"`
			ScheduledAt *time.Time `json:"scheduled_at"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if body.State != string(models.JobStatePending) {
			t.Errorf("state = %q, want PENDING", body.State)
		}
		if body.Attempt != 2 {
			t.Errorf("attempt = %d, want 2", body.Attempt)
		}
		if body.ScheduledAt == nil || !body.ScheduledAt.After(time.Now().UTC()) {
			t.Errorf("scheduled_at = %v, want a future time", body.ScheduledAt)
		}
	})

	t.Run("retry false is binding", func(t *testing.T) {
		q := &workerQueue{failJob: models.Job{State: models.JobStateDead}}
		rec := do(newWorkerRouter(q, nil), http.MethodPost, "/api/jobs/job-1/fail",
			`{"lease_token":"tok","error":"bad input","retry":false}`, "")

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200: %s", rec.Code, rec.Body.String())
		}
		if !q.failPermanent {
			t.Error("retry:false must be reported as permanent")
		}
	})

	t.Run("an absent retry field defaults to permanent", func(t *testing.T) {
		q := &workerQueue{failJob: models.Job{State: models.JobStateDead}}
		do(newWorkerRouter(q, nil), http.MethodPost, "/api/jobs/job-1/fail",
			`{"lease_token":"tok","error":"boom"}`, "")

		if !q.failPermanent {
			t.Error("a worker that does not ask for a retry does not get one")
		}
	})
}

func TestAPIKeyAuth(t *testing.T) {
	const key = "0123456789abcdef0123456789abcdef"

	tests := []struct {
		name       string
		keys       []string
		bearer     string
		header     string
		wantStatus int
	}{
		{name: "no keys configured lets everything through", keys: nil, wantStatus: http.StatusNoContent},
		{name: "the right key is accepted", keys: []string{key}, bearer: key, wantStatus: http.StatusNoContent},
		{name: "a wrong key is rejected", keys: []string{key}, bearer: "wrong", wantStatus: http.StatusUnauthorized},
		{name: "a missing key is rejected", keys: []string{key}, wantStatus: http.StatusUnauthorized},
		{
			name:       "any configured key works, so rotation is possible",
			keys:       []string{"old-key-old-key-old-key", key},
			bearer:     key,
			wantStatus: http.StatusNoContent,
		},
		{name: "a bare token without the Bearer scheme is rejected", keys: []string{key}, header: key, wantStatus: http.StatusUnauthorized},
		{name: "the scheme is case-insensitive", keys: []string{key}, header: "bearer " + key, wantStatus: http.StatusNoContent},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			router := newWorkerRouter(&workerQueue{claimErr: store.ErrJobNotFound}, tc.keys)

			req := httptest.NewRequest(http.MethodPost, "/api/jobs/claim", bytes.NewReader(nil))
			switch {
			case tc.header != "":
				req.Header.Set("Authorization", tc.header)
			case tc.bearer != "":
				req.Header.Set("Authorization", "Bearer "+tc.bearer)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)

			if rec.Code != tc.wantStatus {
				t.Fatalf("status = %d, want %d: %s", rec.Code, tc.wantStatus, rec.Body.String())
			}
		})
	}
}

// The probe and scrape endpoints are registered on the root router by
// cmd/server, so auth mounted on the group must not touch them. This asserts
// the property directly rather than trusting the mounting point.
func TestAPIKeyAuthDoesNotCoverRootRoutes(t *testing.T) {
	router := gin.New()
	NewHandler(&workerQueue{}, mockStore{}, zerolog.Nop(), testRegistry(),
		[]string{"0123456789abcdef0123456789abcdef"}).RegisterRoutes(router)
	router.GET("/live", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	router.GET("/metrics", func(c *gin.Context) { c.String(http.StatusOK, "# metrics") })

	for _, path := range []string{"/live", "/metrics"} {
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Errorf("%s status = %d, want 200; auth must not be on the root chain", path, rec.Code)
		}
	}

	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/api/jobs", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("/api/jobs status = %d, want 401", rec.Code)
	}
}
