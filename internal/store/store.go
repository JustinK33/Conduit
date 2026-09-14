package store

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/JustinK33/Conduit/pkg/models"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog"
)

// JobStore is the durable state machine contract for jobs.
type JobStore interface {
	CreateJob(context.Context, models.Job) error
	UpdateJob(context.Context, models.Job) error
	GetJob(context.Context, string) (models.Job, error)
	GetJobByIdempotencyKey(context.Context, string) (models.Job, error)
	CancelJob(context.Context, string) error
	ClaimNextJob(context.Context, time.Duration, []string) (models.Job, error)
	RenewLease(context.Context, models.Job, time.Duration) error
	RequeueExpiredRunning(context.Context, int) (int, error)
	ReleaseClaim(context.Context, models.Job, string) error
	CompleteClaimedJob(context.Context, string, string, map[string]string) error
	FailClaimedJob(context.Context, string, string, string, *time.Time) error
	ListJobs(context.Context, ListFilter) ([]models.Job, string, error)
}

// ListFilter narrows a ListJobs call. State is optional - empty string means
// any state. Cursor is the opaque token returned by the previous page; pass
// empty string for the first page. Limit is capped server-side.
type ListFilter struct {
	State  models.JobState
	Limit  int
	Cursor string
}

type PostgresStore struct {
	Pool      *pgxpool.Pool
	TableName string
}

func NewPostgresStore(pool *pgxpool.Pool, tableName string) *PostgresStore {
	return &PostgresStore{Pool: pool, TableName: tableName}
}

func (s *PostgresStore) CreateJob(ctx context.Context, job models.Job) error {
	taskMetaBytes, err := json.Marshal(job.Task.Metadata)
	if err != nil {
		return fmt.Errorf("store: marshal task metadata: %w", err)
	}
	metaBytes, err := json.Marshal(job.Metadata)
	if err != nil {
		return fmt.Errorf("store: marshal metadata: %w", err)
	}

	now := time.Now().UTC()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now

	query := fmt.Sprintf(`
		INSERT INTO %s (
			id, idempotency_key, task_id, task_name, task_payload,
			task_retry_count, task_max_retries, task_timeout_ns,
			task_cron_expr, task_queue, task_metadata,
			state, attempt, last_error,
			scheduled_at, started_at, lease_expires_at, lease_token, completed_at,
			created_at, updated_at, metadata
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
			$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22
		)`, s.TableName)

	_, err = s.Pool.Exec(ctx, query,
		job.ID,
		nullableString(job.IdempotencyKey),
		job.Task.ID,
		job.Task.Name,
		[]byte(job.Task.Payload),
		job.Task.RetryCount,
		job.Task.MaxRetries,
		time.Duration(job.Task.Timeout).Nanoseconds(),
		job.Task.CronExpression,
		job.Task.Queue,
		taskMetaBytes,
		string(job.State),
		job.Attempt,
		job.LastError,
		job.ScheduledAt,
		job.StartedAt,
		job.LeaseExpiresAt,
		nullableString(job.LeaseToken),
		job.CompletedAt,
		job.CreatedAt,
		job.UpdatedAt,
		metaBytes,
	)
	if isIdempotencyUniqueViolation(err) {
		return ErrDuplicateIdempotencyKey
	}
	return err
}

func (s *PostgresStore) GetJob(ctx context.Context, id string) (models.Job, error) {
	query := fmt.Sprintf(`
		SELECT
			id, idempotency_key, task_id, task_name, task_payload,
			task_retry_count, task_max_retries, task_timeout_ns,
			task_cron_expr, task_queue, task_metadata,
			state, attempt, last_error,
			scheduled_at, started_at, lease_expires_at, lease_token, completed_at,
			created_at, updated_at, metadata
		FROM %s WHERE id = $1`, s.TableName)

	job, err := scanJob(ctx, s.Pool.QueryRow(ctx, query, id))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.Job{}, ErrJobNotFound
		}
		return models.Job{}, err
	}
	return job, nil
}

func (s *PostgresStore) GetJobByIdempotencyKey(ctx context.Context, key string) (models.Job, error) {
	query := fmt.Sprintf(`
		SELECT
			id, idempotency_key, task_id, task_name, task_payload,
			task_retry_count, task_max_retries, task_timeout_ns,
			task_cron_expr, task_queue, task_metadata,
			state, attempt, last_error,
			scheduled_at, started_at, lease_expires_at, lease_token, completed_at,
			created_at, updated_at, metadata
		FROM %s WHERE idempotency_key = $1`, s.TableName)

	job, err := scanJob(ctx, s.Pool.QueryRow(ctx, query, key))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.Job{}, ErrJobNotFound
		}
		return models.Job{}, err
	}
	return job, nil
}

// UpdateJob validates the state transition before persisting.
func (s *PostgresStore) UpdateJob(ctx context.Context, job models.Job) error {
	existing, err := s.GetJob(ctx, job.ID)
	if err != nil {
		return err
	}
	if !CanTransition(existing.State, job.State) {
		return ErrInvalidTransition
	}

	metaBytes, err := json.Marshal(job.Metadata)
	if err != nil {
		return fmt.Errorf("store: marshal metadata: %w", err)
	}
	taskMetaBytes, err := json.Marshal(job.Task.Metadata)
	if err != nil {
		return fmt.Errorf("store: marshal task metadata: %w", err)
	}
	job.UpdatedAt = time.Now().UTC()

	query := fmt.Sprintf(`
		UPDATE %s SET
			state            = $1,
			attempt          = $2,
			last_error       = $3,
			scheduled_at     = $4,
			started_at       = $5,
			lease_expires_at = $6,
			lease_token      = CASE
				WHEN $1 IN ('COMPLETED', 'DEAD', 'PENDING') THEN NULL
				ELSE $7
			END,
			completed_at     = $8,
			updated_at       = $9,
			metadata         = $10,
			task_metadata    = $11,
			task_retry_count = $12
		WHERE id = $13
		  AND (
			$14 = ''
			OR lease_token = $14
			OR (state = 'PENDING' AND $1 = 'RUNNING' AND lease_token IS NULL)
		  )`, s.TableName)

	tag, err := s.Pool.Exec(ctx, query,
		string(job.State),
		job.Attempt,
		job.LastError,
		job.ScheduledAt,
		job.StartedAt,
		job.LeaseExpiresAt,
		nullableString(job.LeaseToken),
		job.CompletedAt,
		job.UpdatedAt,
		metaBytes,
		taskMetaBytes,
		job.Task.RetryCount,
		job.ID,
		job.LeaseToken,
	)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrInvalidTransition
	}
	return nil
}

// CancelJob transitions a job to DEAD if the state machine allows it.
func (s *PostgresStore) CancelJob(ctx context.Context, id string) error {
	job, err := s.GetJob(ctx, id)
	if err != nil {
		return err
	}
	if !CanTransition(job.State, models.JobStateDead) {
		return ErrInvalidTransition
	}

	query := fmt.Sprintf(
		`UPDATE %s SET state = $1, updated_at = $2 WHERE id = $3`,
		s.TableName,
	)
	_, err = s.Pool.Exec(ctx, query, string(models.JobStateDead), time.Now().UTC(), id)
	return err
}

// ClaimNextJob atomically selects the next due PENDING job and marks it RUNNING,
// using SELECT FOR UPDATE SKIP LOCKED so concurrent workers don't double-claim.
func (s *PostgresStore) ClaimNextJob(ctx context.Context, leaseDuration time.Duration, queues []string) (models.Job, error) {
	now := time.Now().UTC()
	leaseExpiresAt := now.Add(leaseDuration)
	leaseToken, err := newLeaseToken()
	if err != nil {
		return models.Job{}, fmt.Errorf("store: generate lease token: %w", err)
	}

	var queueParam any
	if len(queues) == 0 {
		queueParam = nil
	} else {
		queueParam = queues
	}

	query := fmt.Sprintf(`
		WITH selected AS (
			SELECT id
			FROM %s
			WHERE state = 'PENDING'
			  AND (scheduled_at IS NULL OR scheduled_at <= NOW())
			  AND ($5::text[] IS NULL OR task_queue = ANY($5))
			ORDER BY scheduled_at ASC NULLS LAST
			LIMIT 1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE %s jobs
		SET state = 'RUNNING',
			attempt = jobs.attempt + 1,
			started_at = $1,
			lease_expires_at = $2,
			lease_token = $3,
			updated_at = $4
		FROM selected
		WHERE jobs.id = selected.id
		RETURNING
			jobs.id, jobs.idempotency_key, jobs.task_id, jobs.task_name, jobs.task_payload,
			jobs.task_retry_count, jobs.task_max_retries, jobs.task_timeout_ns,
			jobs.task_cron_expr, jobs.task_queue, jobs.task_metadata,
			jobs.state, jobs.attempt, jobs.last_error,
			jobs.scheduled_at, jobs.started_at, jobs.lease_expires_at, jobs.lease_token, jobs.completed_at,
			jobs.created_at, jobs.updated_at, jobs.metadata`, s.TableName, s.TableName)

	job, err := scanJob(ctx, s.Pool.QueryRow(ctx, query, now, leaseExpiresAt, leaseToken, now, queueParam))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return models.Job{}, ErrJobNotFound
		}
		return models.Job{}, err
	}
	return job, nil
}

func (s *PostgresStore) RenewLease(ctx context.Context, job models.Job, leaseDuration time.Duration) error {
	if job.LeaseToken == "" {
		return ErrInvalidTransition
	}
	query := fmt.Sprintf(`
		UPDATE %s
		SET lease_expires_at = $1,
			updated_at = NOW()
		WHERE id = $2
		  AND state = 'RUNNING'
		  AND lease_token = $3`, s.TableName)

	tag, err := s.Pool.Exec(ctx, query, time.Now().UTC().Add(leaseDuration), job.ID, job.LeaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrInvalidTransition
	}
	return nil
}

func (s *PostgresStore) RequeueExpiredRunning(ctx context.Context, limit int) (int, error) {
	if limit <= 0 {
		return 0, nil
	}
	query := fmt.Sprintf(`
		WITH expired AS (
			SELECT id, attempt, task_max_retries
			FROM %s
			WHERE state = 'RUNNING'
			  AND lease_expires_at IS NOT NULL
			  AND lease_expires_at <= NOW()
			ORDER BY lease_expires_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)
		UPDATE %s jobs
		SET state = CASE
				WHEN expired.task_max_retries > 0 AND expired.attempt >= expired.task_max_retries THEN 'DEAD'
				ELSE 'PENDING'
			END,
			last_error = 'job lease expired before completion',
			scheduled_at = CASE
				WHEN expired.task_max_retries > 0 AND expired.attempt >= expired.task_max_retries THEN scheduled_at
				ELSE NOW()
			END,
			started_at = NULL,
			lease_expires_at = NULL,
			lease_token = NULL,
			updated_at = NOW()
		FROM expired
		WHERE jobs.id = expired.id`, s.TableName, s.TableName)

	tag, err := s.Pool.Exec(ctx, query, limit)
	if err != nil {
		return 0, err
	}
	return int(tag.RowsAffected()), nil
}

func (s *PostgresStore) ReleaseClaim(ctx context.Context, job models.Job, reason string) error {
	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'PENDING',
			last_error = $1,
			started_at = NULL,
			lease_expires_at = NULL,
			lease_token = NULL,
			updated_at = NOW()
		WHERE id = $2
		  AND state = 'RUNNING'
		  AND lease_token = $3`, s.TableName)

	tag, err := s.Pool.Exec(ctx, query, reason, job.ID, job.LeaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrInvalidTransition
	}
	return nil
}

func (s *PostgresStore) CompleteClaimedJob(ctx context.Context, id, leaseToken string, meta map[string]string) error {
	if leaseToken == "" {
		return ErrInvalidTransition
	}

	metaBytes := []byte("{}")
	if len(meta) > 0 {
		var err error
		metaBytes, err = json.Marshal(meta)
		if err != nil {
			return fmt.Errorf("store: marshal metadata: %w", err)
		}
	}

	query := fmt.Sprintf(`
		UPDATE %s
		SET state = 'COMPLETED',
			completed_at = NOW(),
			started_at = NULL,
			lease_expires_at = NULL,
			lease_token = NULL,
			updated_at = NOW(),
			-- A job enqueued without metadata stores JSON null, not SQL NULL,
			-- because CreateJob marshals a nil map. jsonb's || then treats the
			-- two sides as arrays and produces [null, {...}], which scanJob
			-- cannot read into a map: the worker's result would be silently
			-- lost behind one warn line. Anything that is not an object starts
			-- from {} instead.
			metadata = CASE WHEN jsonb_typeof(metadata) = 'object'
				THEN metadata ELSE '{}'::jsonb END || $1::jsonb
		WHERE id = $2
		  AND state = 'RUNNING'
		  AND lease_token = $3`, s.TableName)

	tag, err := s.Pool.Exec(ctx, query, metaBytes, id, leaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (s *PostgresStore) FailClaimedJob(ctx context.Context, id, leaseToken, errMsg string, nextRun *time.Time) error {
	if leaseToken == "" {
		return ErrInvalidTransition
	}

	query := fmt.Sprintf(`
		UPDATE %s
		SET state = CASE
				WHEN $1::timestamptz IS NOT NULL THEN 'PENDING'
				ELSE 'DEAD'
			END,
			last_error = $2,
			scheduled_at = CASE
				WHEN $1::timestamptz IS NOT NULL THEN $1
				ELSE scheduled_at
			END,
			completed_at = CASE
				WHEN $1::timestamptz IS NULL THEN NOW()
				ELSE completed_at
			END,
			started_at = NULL,
			lease_expires_at = NULL,
			lease_token = NULL,
			updated_at = NOW()
		WHERE id = $3
		  AND state = 'RUNNING'
		  AND lease_token = $4`, s.TableName)

	tag, err := s.Pool.Exec(ctx, query, nextRun, errMsg, id, leaseToken)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// ListJobs returns up to filter.Limit jobs ordered by created_at DESC, id DESC,
// optionally filtered by state. Pagination is keyset-based: callers pass the
// `nextCursor` returned by the previous call to fetch the next page. An empty
// returned cursor means "no more results."
func (s *PostgresStore) ListJobs(ctx context.Context, filter ListFilter) ([]models.Job, string, error) {
	const (
		defaultLimit = 50
		maxLimit     = 500
	)
	limit := filter.Limit
	if limit <= 0 {
		limit = defaultLimit
	}
	if limit > maxLimit {
		limit = maxLimit
	}

	args := []any{}
	where := []string{"1=1"}
	if filter.State != "" {
		args = append(args, string(filter.State))
		where = append(where, fmt.Sprintf("state = $%d", len(args)))
	}
	if filter.Cursor != "" {
		ts, id, err := decodeCursor(filter.Cursor)
		if err != nil {
			return nil, "", fmt.Errorf("store: invalid cursor: %w", err)
		}
		args = append(args, ts, id)
		// Standard keyset pagination: rows strictly after the cursor in our
		// (created_at DESC, id DESC) ordering.
		where = append(where, fmt.Sprintf("(created_at, id) < ($%d, $%d)", len(args)-1, len(args)))
	}
	args = append(args, limit+1) // fetch one extra to detect "has next"

	query := fmt.Sprintf(`
		SELECT
			id, idempotency_key, task_id, task_name, task_payload,
			task_retry_count, task_max_retries, task_timeout_ns,
			task_cron_expr, task_queue, task_metadata,
			state, attempt, last_error,
			scheduled_at, started_at, lease_expires_at, lease_token, completed_at,
			created_at, updated_at, metadata
		FROM %s
		WHERE %s
		ORDER BY created_at DESC, id DESC
		LIMIT $%d`, s.TableName, strings.Join(where, " AND "), len(args))

	rows, err := s.Pool.Query(ctx, query, args...)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()

	jobs := make([]models.Job, 0, limit)
	for rows.Next() {
		job, err := scanJob(ctx, rows)
		if err != nil {
			return nil, "", err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}

	var nextCursor string
	if len(jobs) > limit {
		last := jobs[limit-1]
		nextCursor = encodeCursor(last.CreatedAt, last.ID)
		jobs = jobs[:limit]
	}
	return jobs, nextCursor, nil
}

// scanJob reads one row from a pgx.Rows or pgx.Row into a models.Job. The
// column order must match the SELECT in GetJob and ListJobs.
func scanJob(ctx context.Context, row pgx.Row) (models.Job, error) {
	var (
		job          models.Job
		taskMetaJSON []byte
		metaJSON     []byte
		timeoutNS    int64
		// payload is scanned as a plain []byte and converted after: pgx maps
		// BYTEA to []byte, not to a named slice type over it.
		payload []byte
		state   string
		// idempotency_key and lease_token are written with nullableString, so
		// they arrive as NULL for any job without a key or an active lease.
		// Scanning those straight into a string fails with "cannot scan NULL
		// into *string", so take them through pointers and flatten to "".
		idempotencyKey *string
		leaseToken     *string
	)

	err := row.Scan(
		&job.ID,
		&idempotencyKey,
		&job.Task.ID,
		&job.Task.Name,
		&payload,
		&job.Task.RetryCount,
		&job.Task.MaxRetries,
		&timeoutNS,
		&job.Task.CronExpression,
		&job.Task.Queue,
		&taskMetaJSON,
		&state,
		&job.Attempt,
		&job.LastError,
		&job.ScheduledAt,
		&job.StartedAt,
		&job.LeaseExpiresAt,
		&leaseToken,
		&job.CompletedAt,
		&job.CreatedAt,
		&job.UpdatedAt,
		&metaJSON,
	)
	if err != nil {
		return models.Job{}, err
	}

	if idempotencyKey != nil {
		job.IdempotencyKey = *idempotencyKey
	}
	if leaseToken != nil {
		job.LeaseToken = *leaseToken
	}
	job.State = models.JobState(state)
	job.Task.Timeout = models.Duration(timeoutNS)
	job.Task.Payload = payload

	if len(taskMetaJSON) > 0 {
		if err := json.Unmarshal(taskMetaJSON, &job.Task.Metadata); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Str("job_id", job.ID).Msg("store: failed to unmarshal task metadata")
		}
	}
	if len(metaJSON) > 0 {
		if err := json.Unmarshal(metaJSON, &job.Metadata); err != nil {
			zerolog.Ctx(ctx).Warn().Err(err).Str("job_id", job.ID).Msg("store: failed to unmarshal job metadata")
		}
	}
	return job, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func newLeaseToken() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}

func isIdempotencyUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) &&
		pgErr.Code == "23505" &&
		pgErr.ConstraintName == "jobs_idempotency_key_idx"
}

func encodeCursor(t time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf("%d|%s", t.UnixMicro(), id)))
}

func decodeCursor(c string) (time.Time, string, error) {
	raw, err := base64.RawURLEncoding.DecodeString(c)
	if err != nil {
		return time.Time{}, "", err
	}
	parts := strings.SplitN(string(raw), "|", 2)
	if len(parts) != 2 {
		return time.Time{}, "", fmt.Errorf("malformed cursor")
	}
	micros, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return time.Time{}, "", err
	}
	return time.UnixMicro(micros).UTC(), parts[1], nil
}

// CanTransition reports whether a job may move from one state to another.
//
// Valid transitions:
//
//	PENDING  → RUNNING
//	RUNNING  → COMPLETED | FAILED | DEAD
//	FAILED   → PENDING  (retry re-enqueue)
//	Any      → DEAD     (dead-letter escalation)
func CanTransition(from, to models.JobState) bool {
	if from == models.JobStateCompleted || from == models.JobStateDead {
		return false
	}
	switch from {
	case models.JobStatePending:
		return to == models.JobStateRunning || to == models.JobStateDead
	case models.JobStateRunning:
		return to == models.JobStateCompleted || to == models.JobStateFailed || to == models.JobStateDead
	case models.JobStateFailed:
		return to == models.JobStatePending || to == models.JobStateDead
	default:
		return false
	}
}
