package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/JustinK33/Conduit/internal/store"
	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

type mockScheduleStore struct {
	created   models.Schedule
	createErr error
	list      []models.Schedule
	listErr   error
	deletedID string
	deleteErr error
}

func (m *mockScheduleStore) CreateSchedule(_ context.Context, sched models.Schedule) (models.Schedule, error) {
	m.created = sched
	if m.createErr != nil {
		return models.Schedule{}, m.createErr
	}
	sched.ID = "sch-1"
	return sched, nil
}

func (m *mockScheduleStore) ListSchedules(context.Context) ([]models.Schedule, error) {
	return m.list, m.listErr
}

func (m *mockScheduleStore) DeleteSchedule(_ context.Context, id string) error {
	m.deletedID = id
	return m.deleteErr
}

func scheduleRouter(s ScheduleStore) *gin.Engine {
	router := gin.New()
	NewScheduleHandler(s, zerolog.Nop(), nil).RegisterRoutes(router)
	return router
}

func TestCreateSchedule(t *testing.T) {
	t.Run("a valid cron returns 201 with the stored schedule", func(t *testing.T) {
		schedules := &mockScheduleStore{}
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/schedules",
			bytes.NewBufferString(`{"name":"nightly","cron":"0 3 * * *","task":{"name":"sql.etl","queue":"default"}}`))
		req.Header.Set("Content-Type", "application/json")
		scheduleRouter(schedules).ServeHTTP(w, req)

		if w.Code != http.StatusCreated {
			t.Fatalf("status = %d, want 201 (body: %s)", w.Code, w.Body.String())
		}
		var got models.Schedule
		if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
			t.Fatalf("decode response: %v", err)
		}
		if got.ID != "sch-1" {
			t.Errorf("id = %q, want the stored one", got.ID)
		}
		// The first fire instant comes from the cron expression, never from the
		// caller, so the server has to have computed one.
		if schedules.created.NextRunAt.IsZero() {
			t.Error("stored schedule has no next_run_at")
		}
		if !schedules.created.Enabled {
			t.Error("a new schedule should be enabled")
		}
		if schedules.created.Task.Name != "sql.etl" {
			t.Errorf("task name = %q, want sql.etl", schedules.created.Task.Name)
		}
	})

	// An expression that cannot be parsed is a schedule that logs an error every
	// tick forever and never runs, so it has to be refused at create time.
	tests := []struct {
		name string
		body string
	}{
		{name: "unparseable cron", body: `{"name":"broken","cron":"not a cron","task":{"name":"webhook"}}`},
		{name: "too few fields", body: `{"name":"broken","cron":"* * *","task":{"name":"webhook"}}`},
		{name: "minute out of range", body: `{"name":"broken","cron":"99 * * * *","task":{"name":"webhook"}}`},
		{name: "missing name", body: `{"cron":"0 3 * * *","task":{"name":"webhook"}}`},
		{name: "missing task name", body: `{"name":"nightly","cron":"0 3 * * *","task":{}}`},
		{name: "invalid JSON", body: `not json`},
	}
	for _, tc := range tests {
		t.Run(tc.name+" returns 400", func(t *testing.T) {
			schedules := &mockScheduleStore{}
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodPost, "/api/schedules", bytes.NewBufferString(tc.body))
			req.Header.Set("Content-Type", "application/json")
			scheduleRouter(schedules).ServeHTTP(w, req)

			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want 400 (body: %s)", w.Code, w.Body.String())
			}
			if schedules.created.Name != "" {
				t.Errorf("rejected request still reached the store: %#v", schedules.created)
			}
		})
	}

	t.Run("a duplicate name returns 409", func(t *testing.T) {
		schedules := &mockScheduleStore{createErr: store.ErrDuplicateSchedule}
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/api/schedules",
			bytes.NewBufferString(`{"name":"nightly","cron":"0 3 * * *","task":{"name":"sql.etl"}}`))
		req.Header.Set("Content-Type", "application/json")
		scheduleRouter(schedules).ServeHTTP(w, req)

		if w.Code != http.StatusConflict {
			t.Fatalf("status = %d, want 409 (body: %s)", w.Code, w.Body.String())
		}
	})
}

func TestListSchedules(t *testing.T) {
	schedules := &mockScheduleStore{list: []models.Schedule{
		{ID: "sch-1", Name: "nightly", Cron: "0 3 * * *"},
	}}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/schedules", nil)
	scheduleRouter(schedules).ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200 (body: %s)", w.Code, w.Body.String())
	}
	var got struct {
		Schedules []models.Schedule `json:"schedules"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(got.Schedules) != 1 || got.Schedules[0].Name != "nightly" {
		t.Errorf("schedules = %#v, want the one from the store", got.Schedules)
	}
}

func TestDeleteSchedule(t *testing.T) {
	t.Run("returns 204 and passes the id through", func(t *testing.T) {
		schedules := &mockScheduleStore{}
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodDelete, "/api/schedules/sch-1", nil)
		scheduleRouter(schedules).ServeHTTP(w, req)

		if w.Code != http.StatusNoContent {
			t.Fatalf("status = %d, want 204 (body: %s)", w.Code, w.Body.String())
		}
		if schedules.deletedID != "sch-1" {
			t.Errorf("deleted %q, want sch-1", schedules.deletedID)
		}
	})

	t.Run("an unknown id returns 404", func(t *testing.T) {
		schedules := &mockScheduleStore{deleteErr: store.ErrScheduleNotFound}
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodDelete, "/api/schedules/missing", nil)
		scheduleRouter(schedules).ServeHTTP(w, req)

		if w.Code != http.StatusNotFound {
			t.Fatalf("status = %d, want 404 (body: %s)", w.Code, w.Body.String())
		}
	})
}

// The routes sit behind the same APIKeyAuth as /api/jobs. Creating a schedule is
// creating unbounded future work, so an open one is worse than an open enqueue.
func TestScheduleRoutesRequireTheAPIKey(t *testing.T) {
	router := gin.New()
	NewScheduleHandler(&mockScheduleStore{}, zerolog.Nop(), []string{"secret"}).RegisterRoutes(router)

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/api/schedules", nil)
	router.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want 401 (body: %s)", w.Code, w.Body.String())
	}
}
