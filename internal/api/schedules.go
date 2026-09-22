package api

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/JustinK33/Conduit/internal/scheduler"
	"github.com/JustinK33/Conduit/internal/store"
	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

// ScheduleStore is the subset of *store.ScheduleStore these three routes need.
type ScheduleStore interface {
	CreateSchedule(ctx context.Context, sched models.Schedule) (models.Schedule, error)
	ListSchedules(ctx context.Context) ([]models.Schedule, error)
	DeleteSchedule(ctx context.Context, id string) error
}

// ScheduleHandler serves /api/schedules.
//
// Its own handler rather than more fields on Handler, because NewHandler has
// fourteen call sites and none of them care about schedules. It mounts on the
// same router, so it inherits the request id, the request log, the panic
// recovery, and the same API key auth.
type ScheduleHandler struct {
	Store   ScheduleStore
	Logger  zerolog.Logger
	APIKeys []string
}

func NewScheduleHandler(schedules ScheduleStore, logger zerolog.Logger, apiKeys []string) *ScheduleHandler {
	return &ScheduleHandler{Store: schedules, Logger: logger, APIKeys: apiKeys}
}

func (h *ScheduleHandler) RegisterRoutes(router gin.IRouter) {
	g := router.Group("/api/schedules", APIKeyAuth(h.APIKeys))
	g.POST("", h.CreateSchedule)
	g.GET("", h.ListSchedules)
	g.DELETE("/:id", h.DeleteSchedule)
}

// CreateScheduleRequest is a task template plus the cron expression that decides
// when to enqueue it.
//
// There is no enabled field and no next_run_at: a schedule is created enabled
// and its first fire instant is computed from the cron expression, because a
// caller-supplied fire instant is a way to ask for a fire the cron never named.
type CreateScheduleRequest struct {
	Name string      `json:"name"`
	Cron string      `json:"cron"`
	Task models.Task `json:"task"`
}

func (h *ScheduleHandler) CreateSchedule(c *gin.Context) {
	log := zerolog.Ctx(c.Request.Context())

	var request CreateScheduleRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		RespondError(c, http.StatusBadRequest, "invalid_request", err.Error())
		return
	}
	if request.Name == "" {
		RespondError(c, http.StatusBadRequest, "invalid_request", "name is required")
		return
	}
	if request.Task.Name == "" {
		RespondError(c, http.StatusBadRequest, "invalid_request", "task.name is required")
		return
	}

	// Parse now, not at fire time. An expression that cannot be parsed is a
	// schedule that would log an error every tick forever and never run.
	cron, err := scheduler.Parse(request.Cron)
	if err != nil {
		RespondError(c, http.StatusBadRequest, "invalid_request", "cron: "+err.Error())
		return
	}

	now := time.Now().UTC()
	next := cron.Next(now)
	if next.IsZero() {
		RespondError(c, http.StatusBadRequest, "invalid_request", "cron has no occurrence in the next four years")
		return
	}

	sched, err := h.Store.CreateSchedule(c.Request.Context(), models.Schedule{
		Name:      request.Name,
		Cron:      request.Cron,
		Task:      request.Task,
		Enabled:   true,
		NextRunAt: next,
	})
	if err != nil {
		if errors.Is(err, store.ErrDuplicateSchedule) {
			RespondError(c, http.StatusConflict, "duplicate_name", "a schedule with that name already exists")
			return
		}
		log.Error().Err(err).Str("schedule", request.Name).Msg("create schedule failed")
		RespondError(c, http.StatusInternalServerError, "internal_error", "failed to create schedule")
		return
	}

	log.Info().Str("schedule_id", sched.ID).Str("schedule", sched.Name).
		Str("cron", sched.Cron).Time("next_run_at", next).Msg("schedule created")
	c.JSON(http.StatusCreated, sched)
}

func (h *ScheduleHandler) ListSchedules(c *gin.Context) {
	log := zerolog.Ctx(c.Request.Context())

	schedules, err := h.Store.ListSchedules(c.Request.Context())
	if err != nil {
		log.Error().Err(err).Msg("list schedules failed")
		RespondError(c, http.StatusInternalServerError, "internal_error", "failed to list schedules")
		return
	}
	c.JSON(http.StatusOK, gin.H{"schedules": schedules})
}

// DeleteSchedule removes a schedule. Jobs it already enqueued are untouched:
// they are ordinary jobs and finish or fail on their own terms.
//
// Delete is also how you pause one, which is why there is no PATCH. A pause that
// is really a delete and a re-create loses nothing but last_run_at, and one
// column plus one route is a cheap thing to add when somebody asks for it.
func (h *ScheduleHandler) DeleteSchedule(c *gin.Context) {
	log := zerolog.Ctx(c.Request.Context())
	id := c.Param("id")

	if err := h.Store.DeleteSchedule(c.Request.Context(), id); err != nil {
		if errors.Is(err, store.ErrScheduleNotFound) {
			RespondError(c, http.StatusNotFound, "not_found", "schedule not found")
			return
		}
		log.Error().Err(err).Str("schedule_id", id).Msg("delete schedule failed")
		RespondError(c, http.StatusInternalServerError, "internal_error", "failed to delete schedule")
		return
	}

	log.Info().Str("schedule_id", id).Msg("schedule deleted")
	c.Status(http.StatusNoContent)
}
