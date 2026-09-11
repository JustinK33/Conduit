package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/example/conduit/internal/metrics"
	"github.com/example/conduit/internal/store"
	"github.com/example/conduit/pkg/models"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog"
)

// testRegistry returns a fresh metrics registry on an isolated Prometheus registry
// so parallel tests don't conflict on duplicate metric registration.
func testRegistry() *metrics.Registry {
	reg := metrics.NewRegistry("test", "api")
	_ = reg.Register(prometheus.NewRegistry())
	return reg
}

func init() {
	gin.SetMode(gin.TestMode)
}

// mockQueue satisfies api.Queue with controllable responses.
type mockQueue struct {
	enqueueID  string
	enqueueErr error
	cancelErr  error
}

func (m mockQueue) Enqueue(context.Context, models.Job) (string, error) {
	return m.enqueueID, m.enqueueErr
}
func (m mockQueue) Cancel(context.Context, string) error { return m.cancelErr }
func (m mockQueue) Claim(context.Context, []string, time.Duration) (models.Job, error) {
	return models.Job{}, store.ErrJobNotFound
}
func (m mockQueue) Heartbeat(context.Context, string, string, time.Duration) (time.Time, error) {
	return time.Time{}, nil
}
func (m mockQueue) Complete(context.Context, string, string, map[string]string) error { return nil }
func (m mockQueue) Fail(context.Context, string, string, string, bool) (models.Job, error) {
	return models.Job{}, nil
}

type spyQueue struct {
	enqueueID string
	job       models.Job
}

func (s *spyQueue) Enqueue(_ context.Context, job models.Job) (string, error) {
	s.job = job
	return s.enqueueID, nil
}

func (s *spyQueue) Cancel(context.Context, string) error { return nil }
func (s *spyQueue) Claim(context.Context, []string, time.Duration) (models.Job, error) {
	return models.Job{}, store.ErrJobNotFound
}
func (s *spyQueue) Heartbeat(context.Context, string, string, time.Duration) (time.Time, error) {
	return time.Time{}, nil
}
func (s *spyQueue) Complete(context.Context, string, string, map[string]string) error { return nil }
func (s *spyQueue) Fail(context.Context, string, string, string, bool) (models.Job, error) {
	return models.Job{}, nil
}

// mockStore satisfies store.JobStore with controllable responses.
type mockStore struct {
	job               models.Job
	getErr            error
	idempotencyJob    models.Job
	idempotencyGetErr error
}

func (m mockStore) CreateJob(context.Context, models.Job) error { return nil }
func (m mockStore) UpdateJob(context.Context, models.Job) error { return nil }
func (m mockStore) GetJob(_ context.Context, _ string) (models.Job, error) {
	return m.job, m.getErr
}
func (m mockStore) GetJobByIdempotencyKey(context.Context, string) (models.Job, error) {
	return m.idempotencyJob, m.idempotencyGetErr
}
func (m mockStore) CancelJob(context.Context, string) error { return nil }
func (m mockStore) ClaimNextJob(context.Context, time.Duration, []string) (models.Job, error) {
	return models.Job{}, nil
}
func (m mockStore) RenewLease(context.Context, models.Job, time.Duration) error { return nil }
func (m mockStore) RequeueExpiredRunning(context.Context, int) (int, error)     { return 0, nil }
func (m mockStore) ReleaseClaim(context.Context, models.Job, string) error      { return nil }
func (m mockStore) CompleteClaimedJob(context.Context, string, string, map[string]string) error {
	return nil
}
func (m mockStore) FailClaimedJob(context.Context, string, string, string, *time.Time) error {
	return nil
}
func (m mockStore) ListJobs(context.Context, store.ListFilter) ([]models.Job, string, error) {
	return nil, "", nil
}

func TestNewHandler(t *testing.T) {
	t.Run("wires queue and store dependencies", func(t *testing.T) {
		q := mockQueue{enqueueID: "job-1"}
		s := mockStore{}
		h := NewHandler(q, s, zerolog.Logger{}, testRegistry(), nil)
		if h == nil {
			t.Fatal("NewHandler returned nil")
		}
		if h.Queue == nil {
			t.Error("Handler.Queue is nil")
		}
		if h.Store == nil {
			t.Error("Handler.Store is nil")
		}
	})
}

func TestHandlerRegisterRoutes(t *testing.T) {
	t.Run("mounts enqueue, status, and cancel routes", func(t *testing.T) {
		h := NewHandler(mockQueue{enqueueID: "job-1"}, mockStore{}, zerolog.Logger{}, testRegistry(), nil)
		if h == nil {
			t.Skip("NewHandler not yet implemented")
		}

		router := gin.New()
		h.RegisterRoutes(router)

		// Verify POST /api/jobs is registered (not 404 or 405).
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewBufferString(`{}`))
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, req)

		if w.Code == http.StatusNotFound {
			t.Error("POST /api/jobs returned 404: route was not registered")
		}
	})
}

func TestEnqueueJob(t *testing.T) {
	tests := []struct {
		name       string
		body       string
		queue      mockQueue
		wantStatus int
	}{
		{
			name:       "valid request returns created status",
			body:       `{"id":"job-1","task":{"name":"send-email","queue":"default"}}`,
			queue:      mockQueue{enqueueID: "job-1"},
			wantStatus: http.StatusCreated,
		},
		{
			name:       "invalid JSON returns bad request",
			body:       `not json`,
			queue:      mockQueue{},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(tc.queue, mockStore{}, zerolog.Logger{}, testRegistry(), nil)
			if h == nil {
				t.Skip("NewHandler not yet implemented")
			}
			router := gin.New()
			h.RegisterRoutes(router)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewBufferString(tc.body))
			req.Header.Set("Content-Type", "application/json")
			router.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
		})
	}
}

func TestEnqueueJobIgnoresServerOwnedFields(t *testing.T) {
	queue := &spyQueue{enqueueID: "created-job"}
	h := NewHandler(queue, mockStore{}, zerolog.Logger{}, testRegistry(), nil)
	router := gin.New()
	h.RegisterRoutes(router)

	body := `{
		"id":"caller-job",
		"state":"COMPLETED",
		"attempt":99,
		"started_at":"2026-01-01T00:00:00Z",
		"idempotency_key":"request-1",
		"task":{"name":"send-email","queue":"default"},
		"metadata":{"source":"test"}
	}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/api/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)

	if w.Code != http.StatusCreated {
		t.Fatalf("status = %d, want %d (body: %s)", w.Code, http.StatusCreated, w.Body.String())
	}
	if queue.job.ID != "" {
		t.Fatalf("job ID reached queue = %q, want empty", queue.job.ID)
	}
	if queue.job.State != "" {
		t.Fatalf("job state reached queue = %q, want empty", queue.job.State)
	}
	if queue.job.Attempt != 0 {
		t.Fatalf("job attempt reached queue = %d, want 0", queue.job.Attempt)
	}
	if queue.job.StartedAt != nil {
		t.Fatal("started_at reached queue, want nil")
	}
	if queue.job.IdempotencyKey != "request-1" {
		t.Fatalf("idempotency key = %q, want request-1", queue.job.IdempotencyKey)
	}
	if queue.job.Metadata["source"] != "test" {
		t.Fatalf("metadata not preserved: %#v", queue.job.Metadata)
	}
}

func TestGetJobStatus(t *testing.T) {
	tests := []struct {
		name       string
		jobID      string
		store      mockStore
		wantStatus int
	}{
		{
			name:       "found job returns 200",
			jobID:      "job-1",
			store:      mockStore{job: models.Job{ID: "job-1", State: models.JobStatePending}},
			wantStatus: http.StatusOK,
		},
		{
			name:       "missing job returns 404",
			jobID:      "missing",
			store:      mockStore{getErr: store.ErrJobNotFound},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(mockQueue{}, tc.store, zerolog.Logger{}, testRegistry(), nil)
			if h == nil {
				t.Skip("NewHandler not yet implemented")
			}
			router := gin.New()
			h.RegisterRoutes(router)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/jobs/"+tc.jobID, nil)
			router.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
		})
	}
}

func TestGetJobByIdempotencyKey(t *testing.T) {
	tests := []struct {
		name       string
		key        string
		store      mockStore
		wantStatus int
	}{
		{
			name: "found job returns 200",
			key:  "request-1",
			store: mockStore{
				idempotencyJob: models.Job{
					ID:             "job-1",
					IdempotencyKey: "request-1",
					State:          models.JobStatePending,
				},
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "missing job returns 404",
			key:  "missing",
			store: mockStore{
				idempotencyGetErr: store.ErrJobNotFound,
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(mockQueue{}, tc.store, zerolog.Logger{}, testRegistry(), nil)
			router := gin.New()
			h.RegisterRoutes(router)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/api/jobs/by-idempotency-key/"+tc.key, nil)
			router.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
		})
	}
}

func TestCancelJob(t *testing.T) {
	tests := []struct {
		name       string
		jobID      string
		queue      mockQueue
		wantStatus int
	}{
		{
			name:       "successful cancellation returns 200",
			jobID:      "job-1",
			queue:      mockQueue{},
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHandler(tc.queue, mockStore{}, zerolog.Logger{}, testRegistry(), nil)
			if h == nil {
				t.Skip("NewHandler not yet implemented")
			}
			router := gin.New()
			h.RegisterRoutes(router)

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/jobs/"+tc.jobID+"/cancel", nil)
			router.ServeHTTP(w, req)

			if w.Code != tc.wantStatus {
				t.Errorf("status = %d, want %d (body: %s)", w.Code, tc.wantStatus, w.Body.String())
			}
		})
	}
}
