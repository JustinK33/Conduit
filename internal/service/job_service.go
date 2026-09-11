package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/example/conduit/internal/retry"
	"github.com/example/conduit/internal/store"
	"github.com/example/conduit/pkg/models"
	"github.com/rs/zerolog"
)

// Publisher is the subset of queue.KafkaClient that JobService actually needs.
type Publisher interface {
	Publish(context.Context, string, models.Job) error
}

type JobService struct {
	kafka Publisher
	store store.JobStore
	retry *retry.Engine
	topic string
	lease time.Duration
	log   zerolog.Logger
}

func NewJobService(kafka Publisher, s store.JobStore, r *retry.Engine, topic string, lease time.Duration, log zerolog.Logger) *JobService {
	if lease <= 0 {
		lease = 5 * time.Minute
	}
	return &JobService{kafka: kafka, store: s, retry: r, topic: topic, lease: lease, log: log}
}

func (s *JobService) Enqueue(ctx context.Context, job models.Job) (string, error) {
	if job.IdempotencyKey != "" {
		existing, err := s.store.GetJobByIdempotencyKey(ctx, job.IdempotencyKey)
		if err == nil {
			return existing.ID, nil
		}
		if !errors.Is(err, store.ErrJobNotFound) {
			return "", fmt.Errorf("service: lookup idempotency key: %w", err)
		}
	}

	if job.ID == "" {
		id, err := newJobID()
		if err != nil {
			return "", fmt.Errorf("service: generate job id: %w", err)
		}
		job.ID = id
	}
	if job.State == "" {
		job.State = models.JobStatePending
	}
	now := time.Now().UTC()
	if job.ScheduledAt == nil {
		job.ScheduledAt = &now
	}

	if err := s.store.CreateJob(ctx, job); err != nil {
		if errors.Is(err, store.ErrDuplicateIdempotencyKey) && job.IdempotencyKey != "" {
			existing, lookupErr := s.store.GetJobByIdempotencyKey(ctx, job.IdempotencyKey)
			if lookupErr != nil {
				return "", fmt.Errorf("service: lookup duplicate idempotency key: %w", lookupErr)
			}
			return existing.ID, nil
		}
		return "", fmt.Errorf("service: persist job: %w", err)
	}

	if shouldPublishImmediately(job, now) {
		// Postgres is source of truth. Kafka is best-effort; the reconciler
		// picks up anything that never got consumed.
		go func() {
			if err := s.kafka.Publish(context.Background(), s.topic, job); err != nil {
				s.log.Warn().Err(err).Str("job_id", job.ID).
					Msg("service: async kafka publish failed; job remains PENDING for reconciliation")
			}
		}()
	}

	return job.ID, nil
}

func (s *JobService) Cancel(ctx context.Context, id string) error {
	if err := s.store.CancelJob(ctx, id); err != nil {
		return fmt.Errorf("service: cancel job %s: %w", id, err)
	}
	return nil
}

// Claim hands the next due job in one of the named queues to a caller and
// returns it with its lease token. An empty queues slice claims from any queue.
// lease is the caller's request, clamped to the server's configured maximum so
// a worker cannot park a job for a week.
func (s *JobService) Claim(ctx context.Context, queues []string, lease time.Duration) (models.Job, error) {
	job, err := s.store.ClaimNextJob(ctx, s.clampLease(lease), queues)
	if err != nil {
		if errors.Is(err, store.ErrJobNotFound) {
			return models.Job{}, err
		}
		return models.Job{}, fmt.Errorf("service: claim job: %w", err)
	}
	return job, nil
}

// Heartbeat extends the lease on a job the caller still holds. It never reads
// the job: RenewLease is already fenced on state and token, so a stale token
// matches zero rows.
func (s *JobService) Heartbeat(ctx context.Context, id, leaseToken string, lease time.Duration) (time.Time, error) {
	d := s.clampLease(lease)
	if err := s.store.RenewLease(ctx, models.Job{ID: id, LeaseToken: leaseToken}, d); err != nil {
		if errors.Is(err, store.ErrInvalidTransition) {
			return time.Time{}, store.ErrLeaseLost
		}
		return time.Time{}, fmt.Errorf("service: renew lease for job %s: %w", id, err)
	}
	return time.Now().UTC().Add(d), nil
}

// Complete marks a job COMPLETED, merging meta into the job's metadata.
func (s *JobService) Complete(ctx context.Context, id, leaseToken string, meta map[string]string) error {
	if err := s.store.CompleteClaimedJob(ctx, id, leaseToken, meta); err != nil {
		if errors.Is(err, store.ErrLeaseLost) {
			return err
		}
		return fmt.Errorf("service: complete job %s: %w", id, err)
	}
	return nil
}

// Fail applies the retry policy to a failed job and writes the outcome in a
// single statement: PENDING with a future scheduled_at if it should be retried,
// DEAD otherwise. It takes an id and a token rather than a models.Job so that
// the in-process worker and the HTTP endpoint cannot drift apart, at the cost of
// one extra SELECT on the failure path.
//
// permanent is the caller saying "do not retry this whatever the policy says",
// which is what retry.ErrNoRetry means in-process.
func (s *JobService) Fail(ctx context.Context, id, leaseToken, errMsg string, permanent bool) (models.Job, error) {
	job, err := s.store.GetJob(ctx, id)
	if err != nil {
		return models.Job{}, fmt.Errorf("service: load job %s: %w", id, err)
	}

	var nextRun *time.Time
	var delay time.Duration
	if !permanent && s.shouldRetry(job) {
		delay = s.retry.Delay(job.Attempt)
		t := time.Now().UTC().Add(delay)
		nextRun = &t
	}

	if err := s.store.FailClaimedJob(ctx, id, leaseToken, errMsg, nextRun); err != nil {
		if errors.Is(err, store.ErrLeaseLost) {
			return models.Job{}, err
		}
		return models.Job{}, fmt.Errorf("service: fail job %s: %w", id, err)
	}

	// Reflect the write back to the caller without a second read. The job goes
	// to PENDING for a retry rather than through FAILED, because the two-write
	// version could crash in between and leave a row nothing recovered.
	job.LastError = errMsg
	job.StartedAt = nil
	job.LeaseExpiresAt = nil
	job.LeaseToken = ""
	if nextRun != nil {
		job.State = models.JobStatePending
		job.ScheduledAt = nextRun
		s.log.Warn().Str("job_id", id).Int("attempt", job.Attempt).Dur("retry_in", delay).Msg("scheduled retry")
	} else {
		job.State = models.JobStateDead
		s.log.Error().Str("job_id", id).Int("attempts", job.Attempt).Str("last_error", errMsg).Msg("job dead-lettered")
	}
	return job, nil
}

// shouldRetry prefers the job's own MaxRetries over the engine's global budget,
// which is what jobWorker did before this moved.
func (s *JobService) shouldRetry(job models.Job) bool {
	if job.Task.MaxRetries > 0 {
		return job.Attempt < job.Task.MaxRetries
	}
	return s.retry.ShouldRetry(job.Attempt, nil)
}

func (s *JobService) clampLease(requested time.Duration) time.Duration {
	if requested <= 0 || requested > s.lease {
		return s.lease
	}
	return requested
}

func newJobID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	// UUID v4 encoding
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return fmt.Sprintf("%s-%s-%s-%s-%s", h[0:8], h[8:12], h[12:16], h[16:20], h[20:]), nil
}

func shouldPublishImmediately(job models.Job, now time.Time) bool {
	return job.ScheduledAt == nil || !job.ScheduledAt.After(now)
}
