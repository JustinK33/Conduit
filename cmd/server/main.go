package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/IBM/sarama"
	"github.com/example/conduit/internal/api"
	"github.com/example/conduit/internal/circuitbreaker"
	"github.com/example/conduit/internal/config"
	"github.com/example/conduit/internal/etl"
	"github.com/example/conduit/internal/lock"
	"github.com/example/conduit/internal/logger"
	"github.com/example/conduit/internal/metrics"
	"github.com/example/conduit/internal/queue"
	"github.com/example/conduit/internal/reconciler"
	"github.com/example/conduit/internal/retry"
	"github.com/example/conduit/internal/scheduler"
	"github.com/example/conduit/internal/service"
	"github.com/example/conduit/internal/store"
	"github.com/example/conduit/internal/webhook"
	"github.com/example/conduit/internal/worker"
	"github.com/example/conduit/pkg/models"
	"github.com/gin-gonic/gin"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"github.com/rs/zerolog"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "conduit: %v\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	log, err := logger.New(logger.Config{
		Level:       cfg.Logger.Level,
		Format:      cfg.Logger.Format,
		ServiceName: cfg.Logger.ServiceName,
		Environment: cfg.Logger.Environment,
		Pretty:      cfg.Logger.Pretty,
		AddCaller:   cfg.Logger.AddCaller,
	})
	if err != nil {
		return fmt.Errorf("logger: %w", err)
	}
	logger.ConfigureGlobal(log)

	reg := metrics.NewRegistry(cfg.Metrics.Namespace, cfg.Metrics.Subsystem)
	if err := reg.Register(prometheus.DefaultRegisterer); err != nil {
		log.Warn().Err(err).Msg("metrics already registered")
	}

	pgCfg, err := pgxpool.ParseConfig(cfg.Postgres.DSN)
	if err != nil {
		return fmt.Errorf("postgres config: %w", err)
	}
	pgCfg.MaxConns = cfg.Postgres.MaxConns
	pgCfg.MinConns = cfg.Postgres.MinConns
	pgCfg.MaxConnLifetime = cfg.Postgres.MaxConnLifetime
	pgCfg.MaxConnIdleTime = cfg.Postgres.MaxConnIdleTime
	pgCfg.HealthCheckPeriod = cfg.Postgres.HealthCheckPeriod

	pgPool, err := pgxpool.NewWithConfig(ctx, pgCfg)
	if err != nil {
		return fmt.Errorf("postgres: %w", err)
	}
	defer pgPool.Close()

	jobStore := store.NewPostgresStore(pgPool, "jobs")

	var redisClients []redis.UniversalClient
	for _, addr := range cfg.Redis.Addresses {
		redisClients = append(redisClients, redis.NewUniversalClient(&redis.UniversalOptions{
			Addrs:    []string{addr},
			Username: cfg.Redis.Username,
			Password: cfg.Redis.Password,
			DB:       cfg.Redis.Database,
			PoolSize: cfg.Redis.PoolSize,
		}))
	}
	defer func() {
		for _, c := range redisClients {
			c.Close()
		}
	}()

	// One lock manager shared across workers prevents duplicate execution of the same
	// job ID across multiple service instances.
	lockMgr := lock.NewManager(redisClients, lock.Config{
		TTL:         30 * time.Second,
		RetryCount:  3,
		RetryDelay:  100 * time.Millisecond,
		DriftFactor: 0.01,
	})

	kafkaClient, err := queue.NewKafkaClient(cfg.Kafka)
	if err != nil {
		return fmt.Errorf("kafka: %w", err)
	}
	defer kafkaClient.Close()

	jobSvc := service.NewJobService(kafkaClient, jobStore, cfg.Kafka.Topic, logger.WithComponent(log, "service"))

	breaker := circuitbreaker.New(circuitbreaker.Config{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		OpenTimeout:      30 * time.Second,
		HalfOpenRequests: 1,
	})

	retryEngine := retry.NewEngine(retry.Config{
		BaseDelay:   time.Second,
		MaxDelay:    30 * time.Second,
		Multiplier:  2,
		MaxAttempts: 5,
		Jitter:      0.1,
	})

	runner := &jobWorker{
		store:    jobStore,
		lockMgr:  lockMgr,
		breaker:  breaker,
		retry:    retryEngine,
		metrics:  reg,
		log:      logger.WithComponent(log, "worker"),
		handlers: defaultHandlers(cfg.Webhook, pgPool),
		lease:    cfg.Reconciler.RunningLease,
	}
	workerPool := worker.NewPool(worker.Config{
		Concurrency:     cfg.Worker.Concurrency,
		QueueSize:       cfg.Worker.QueueSize,
		ShutdownTimeout: cfg.Worker.ShutdownTimeout,
	}, runner)
	workerPool.Start(ctx)

	jobReconciler := reconciler.New(reconciler.Config{
		Interval:     cfg.Reconciler.Interval,
		IdleInterval: cfg.Reconciler.IdleInterval,
		BatchSize:    cfg.Reconciler.BatchSize,
		RunningLease: cfg.Reconciler.RunningLease,
	}, jobStore, workerPool, logger.WithComponent(log, "reconciler"))
	if cfg.Reconciler.Enabled {
		jobReconciler.Start(ctx)
	}
	defer jobReconciler.Stop()

	consumerLog := logger.WithComponent(log, "consumer")
	go func() {
		h := &kafkaJobHandler{pool: workerPool, log: consumerLog}
		if err := kafkaClient.Consume(ctx, []string{cfg.Kafka.Topic}, h); err != nil && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Msg("kafka consumer stopped")
		}
	}()
	// Consumer.Return.Errors is on, so sarama pushes consume and rebalance
	// failures onto this channel. Nothing read it before, which is why a
	// consumer that stopped delivering jobs looked exactly like an idle one.
	go func() {
		for err := range kafkaClient.ConsumerGroup.Errors() {
			consumerLog.Error().Err(err).Msg("kafka consumer group error")
		}
	}()

	sched := scheduler.New(cfg.Scheduler.TickInterval)
	if cfg.Scheduler.Enabled {
		sched.Start(ctx)
	}
	defer sched.Stop()

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	// Recovery is installed by api.RegisterRoutes so it can use the same
	// request-scoped logger as the other middlewares.

	handler := api.NewHandler(jobSvc, jobStore, logger.WithComponent(log, "api"), reg)
	handler.RegisterRoutes(router)
	router.GET("/metrics", gin.WrapH(reg.Handler()))
	router.GET("/live", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})
	router.GET("/ready", readinessHandler(pgPool, redisClients, log))
	// /health kept for backward compat - same as /live.
	router.GET("/health", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{"status": "ok"})
	})

	srv := &http.Server{
		Addr:         cfg.HTTP.Address,
		Handler:      router,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
		IdleTimeout:  cfg.HTTP.IdleTimeout,
	}

	go func() {
		log.Info().Str("addr", cfg.HTTP.Address).Msg("server listening")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Msg("listen error")
		}
	}()

	<-ctx.Done()
	log.Info().Msg("shutting down")

	shutCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutCtx); err != nil {
		log.Warn().Err(err).Msg("HTTP server shutdown")
	}
	jobReconciler.Stop()
	if err := workerPool.Stop(shutCtx); err != nil {
		log.Warn().Err(err).Msg("worker pool drain timeout")
	}

	return nil
}

// TaskHandler is the application-side function executed for a job.
type TaskHandler func(context.Context, models.Job) error

// defaultHandlers returns the registry of task handlers. Add new entries here
// to wire up real task logic; jobs whose Task.Name is unregistered fail with
// retry.ErrNoRetry so they go straight to DEAD instead of looping forever.
func defaultHandlers(cfg models.WebhookConfig, pgPool *pgxpool.Pool) map[string]TaskHandler {
	webhookExecutor := webhook.NewExecutorWithConfig(webhook.Config{
		Timeout:              cfg.Timeout,
		MaxRedirects:         cfg.MaxRedirects,
		AllowPrivateNetworks: cfg.AllowPrivateNetworks,
	})
	etlExecutor := etl.NewExecutor(pgPool)
	return map[string]TaskHandler{
		webhook.TaskName(): webhookExecutor.Handler,
		etl.TaskName():     etlExecutor.Handler,
	}
}

// jobWorker implements worker.JobRunner. It wraps execution with a distributed lock,
// circuit breaker, retry policy, and per-job timeout.
type jobWorker struct {
	store    store.JobStore
	lockMgr  *lock.Manager
	breaker  *circuitbreaker.Breaker
	retry    *retry.Engine
	metrics  *metrics.Registry
	log      zerolog.Logger
	handlers map[string]TaskHandler
	lease    time.Duration
}

// maxInWorkerSleep caps how long a worker will block waiting for scheduled_at.
// Longer waits are left in Postgres for the reconciler instead of holding a slot.
const maxInWorkerSleep = 60 * time.Second

func (jw *jobWorker) Run(ctx context.Context, job models.Job) error {
	// Honor scheduled_at so retry backoff actually delays execution.
	if job.ScheduledAt != nil {
		if wait := time.Until(*job.ScheduledAt); wait > 0 {
			if wait > maxInWorkerSleep {
				jw.log.Debug().Str("job_id", job.ID).Dur("wait", wait).Msg("scheduled too far ahead, leaving for reconciler")
				return nil
			}
			select {
			case <-time.After(wait):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	if !jw.breaker.Allow() {
		jw.log.Warn().Str("job_id", job.ID).Msg("circuit open, skipping execution")
		jw.releaseClaim(ctx, job, "circuit open")
		return nil
	}

	// Distributed lock prevents the same job from executing twice across
	// multiple instances (e.g. if it was re-enqueued before the first run commits).
	lck, err := jw.lockMgr.Acquire(ctx, "job:exec:"+job.ID)
	if err != nil {
		jw.log.Warn().Err(err).Str("job_id", job.ID).Msg("could not acquire execution lock")
		jw.releaseClaim(ctx, job, "could not acquire execution lock")
		return nil
	}
	defer jw.lockMgr.Release(ctx, lck) //nolint:errcheck

	if job.State != models.JobStateRunning {
		// Kafka-delivered jobs arrive as PENDING and are claimed here. The
		// reconciler path already claims them in Postgres before submitting.
		now := time.Now().UTC()
		leaseToken, err := newWorkerLeaseToken()
		if err != nil {
			return fmt.Errorf("worker: generate lease token: %w", err)
		}
		leaseExpiresAt := now.Add(jw.effectiveLease())
		job.Attempt++
		job.State = models.JobStateRunning
		job.StartedAt = &now
		job.LeaseExpiresAt = &leaseExpiresAt
		job.LeaseToken = leaseToken
		if err := jw.store.UpdateJob(ctx, job); err != nil {
			jw.log.Error().Err(err).Str("job_id", job.ID).Msg("failed to mark job running")
			return err
		}
	}

	jw.metrics.WorkerInFlight.Inc()
	start := time.Now()
	defer func() {
		jw.metrics.WorkerInFlight.Dec()
		jw.metrics.JobDuration.Observe(time.Since(start).Seconds())
	}()

	jw.metrics.JobStarted.Inc()

	stopRenewLease := jw.startLeaseRenewal(ctx, job)
	defer stopRenewLease()

	if err := jw.execute(ctx, job); err != nil {
		jw.breaker.RecordFailure()
		jw.metrics.JobFailed.Inc()

		if jw.shouldRetry(job, err) {
			return jw.scheduleRetry(ctx, job, err)
		}
		return jw.deadLetter(ctx, job, err)
	}

	jw.breaker.RecordSuccess()
	jw.metrics.JobCompleted.Inc()

	completedAt := time.Now().UTC()
	job.State = models.JobStateCompleted
	job.CompletedAt = &completedAt
	job.LeaseExpiresAt = nil
	if err := jw.store.UpdateJob(ctx, job); err != nil {
		jw.log.Error().Err(err).Str("job_id", job.ID).Msg("failed to mark job complete")
		return err
	}
	return nil
}

func (jw *jobWorker) startLeaseRenewal(ctx context.Context, job models.Job) func() {
	if job.State != models.JobStateRunning || job.LeaseToken == "" {
		return func() {}
	}

	lease := jw.effectiveLease()
	interval := lease / 2
	if interval < time.Second {
		interval = time.Second
	}

	renewCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		timer := time.NewTimer(interval)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				if err := jw.store.RenewLease(renewCtx, job, lease); err != nil {
					jw.log.Warn().Err(err).Str("job_id", job.ID).Msg("failed to renew job lease")
					return
				}
				timer.Reset(interval)
			case <-renewCtx.Done():
				return
			}
		}
	}()

	return func() {
		cancel()
		<-done
	}
}

func (jw *jobWorker) effectiveLease() time.Duration {
	if jw.lease > 0 {
		return jw.lease
	}
	return 5 * time.Minute
}

func (jw *jobWorker) releaseClaim(ctx context.Context, job models.Job, reason string) {
	if job.State != models.JobStateRunning || job.LeaseToken == "" {
		return
	}
	if err := jw.store.ReleaseClaim(ctx, job, reason); err != nil {
		jw.log.Warn().Err(err).Str("job_id", job.ID).Msg("failed to release claimed job")
	}
}

// execute looks up a registered handler for job.Task.Name and runs it under
// the per-task timeout if one is set. Unknown task names short-circuit with
// retry.ErrNoRetry so they don't burn through the retry budget.
func (jw *jobWorker) execute(ctx context.Context, job models.Job) error {
	handler, ok := jw.handlers[job.Task.Name]
	if !ok {
		return fmt.Errorf("worker: no handler registered for task %q: %w", job.Task.Name, retry.ErrNoRetry)
	}

	if job.Task.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, job.Task.Timeout)
		defer cancel()
	}
	return handler(ctx, job)
}

func (jw *jobWorker) shouldRetry(job models.Job, err error) bool {
	if job.Task.MaxRetries > 0 {
		return !errors.Is(err, retry.ErrNoRetry) && job.Attempt < job.Task.MaxRetries
	}
	return jw.retry.ShouldRetry(job.Attempt, err)
}

// scheduleRetry transitions RUNNING to FAILED to PENDING and stamps scheduled_at
// with the next attempt's backoff. The reconciler dispatches the job when it
// becomes due.
func (jw *jobWorker) scheduleRetry(ctx context.Context, job models.Job, runErr error) error {
	delay := jw.retry.Delay(job.Attempt)
	nextRun := time.Now().UTC().Add(delay)

	// RUNNING to FAILED.
	job.State = models.JobStateFailed
	job.LastError = runErr.Error()
	if err := jw.store.UpdateJob(ctx, job); err != nil {
		jw.log.Error().Err(err).Str("job_id", job.ID).Msg("failed to record retry state")
		return runErr
	}

	// FAILED to PENDING with future scheduled_at.
	job.State = models.JobStatePending
	job.ScheduledAt = &nextRun
	job.StartedAt = nil
	job.LeaseExpiresAt = nil
	if err := jw.store.UpdateJob(ctx, job); err != nil {
		jw.log.Error().Err(err).Str("job_id", job.ID).Msg("failed to reschedule retry")
		return runErr
	}

	jw.log.Warn().
		Err(runErr).
		Str("job_id", job.ID).
		Int("attempt", job.Attempt).
		Dur("retry_in", delay).
		Msg("scheduled retry")
	return runErr
}

// deadLetter transitions RUNNING → DEAD when retries are exhausted or the
// error is permanent (errors.Is(err, retry.ErrNoRetry)).
func (jw *jobWorker) deadLetter(ctx context.Context, job models.Job, runErr error) error {
	job.State = models.JobStateDead
	job.LastError = runErr.Error()
	job.LeaseExpiresAt = nil
	if err := jw.store.UpdateJob(ctx, job); err != nil {
		jw.log.Error().Err(err).Str("job_id", job.ID).Msg("failed to dead-letter job")
	}
	jw.log.Error().Err(runErr).Str("job_id", job.ID).Int("attempts", job.Attempt).Msg("job exhausted retries")
	return runErr
}

// readinessHandler verifies the service can actually serve traffic by pinging
// each downstream. Postgres is required; Redis is required to quorum (>= N/2+1
// nodes responding) since the lock manager depends on it.
const readinessCacheTTL = 2 * time.Second

type readinessSnapshot struct {
	status  int
	body    gin.H
	expires time.Time
}

func readinessHandler(pg *pgxpool.Pool, redisClients []redis.UniversalClient, log zerolog.Logger) gin.HandlerFunc {
	var mu sync.Mutex
	var snapshot readinessSnapshot

	return func(c *gin.Context) {
		now := time.Now()
		mu.Lock()
		if !snapshot.expires.IsZero() && now.Before(snapshot.expires) {
			status := snapshot.status
			body := snapshot.body
			mu.Unlock()
			c.JSON(status, body)
			return
		}
		mu.Unlock()

		ctx, cancel := context.WithTimeout(c.Request.Context(), 2*time.Second)
		defer cancel()

		checks := gin.H{}
		ok := true

		if err := pg.Ping(ctx); err != nil {
			checks["postgres"] = err.Error()
			ok = false
		} else {
			checks["postgres"] = "ok"
		}

		alive := 0
		for i, client := range redisClients {
			if err := client.Ping(ctx).Err(); err != nil {
				checks[fmt.Sprintf("redis[%d]", i)] = err.Error()
				continue
			}
			checks[fmt.Sprintf("redis[%d]", i)] = "ok"
			alive++
		}
		quorum := len(redisClients)/2 + 1
		if alive < quorum {
			ok = false
			checks["redis_quorum"] = fmt.Sprintf("%d/%d (need %d)", alive, len(redisClients), quorum)
		}

		status := http.StatusOK
		if !ok {
			status = http.StatusServiceUnavailable
			log.Warn().Interface("checks", checks).Msg("readiness probe failed")
		}
		body := gin.H{"status": map[bool]string{true: "ready", false: "not_ready"}[ok], "checks": checks}
		mu.Lock()
		snapshot = readinessSnapshot{
			status:  status,
			body:    body,
			expires: now.Add(readinessCacheTTL),
		}
		mu.Unlock()
		c.JSON(status, body)
	}
}

// kafkaJobHandler bridges Kafka messages to the worker pool.
type kafkaJobHandler struct {
	pool *worker.Pool
	log  zerolog.Logger
}

func (h *kafkaJobHandler) Handle(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var job models.Job
	if err := json.Unmarshal(msg.Value, &job); err != nil {
		h.log.Error().Err(err).Msg("failed to unmarshal kafka message")
		return nil
	}
	if !h.pool.Submit(ctx, job) {
		// Returning an error skips MarkMessage, but that does not get the job
		// redelivered: sarama commits the highest marked offset per partition,
		// so the next message that succeeds commits past this one. The job stays
		// PENDING in Postgres and the reconciler is what actually recovers it.
		h.log.Warn().Str("job_id", job.ID).Msg("worker pool full, leaving job for the reconciler")
		return fmt.Errorf("worker pool full")
	}
	return nil
}

func newWorkerLeaseToken() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
