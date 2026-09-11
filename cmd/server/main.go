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
	"github.com/JustinK33/Conduit/internal/api"
	"github.com/JustinK33/Conduit/internal/circuitbreaker"
	"github.com/JustinK33/Conduit/internal/config"
	"github.com/JustinK33/Conduit/internal/etl"
	"github.com/JustinK33/Conduit/internal/lock"
	"github.com/JustinK33/Conduit/internal/logger"
	"github.com/JustinK33/Conduit/internal/metrics"
	"github.com/JustinK33/Conduit/internal/queue"
	"github.com/JustinK33/Conduit/internal/reconciler"
	"github.com/JustinK33/Conduit/internal/retry"
	"github.com/JustinK33/Conduit/internal/scheduler"
	"github.com/JustinK33/Conduit/internal/service"
	"github.com/JustinK33/Conduit/internal/store"
	"github.com/JustinK33/Conduit/internal/webhook"
	"github.com/JustinK33/Conduit/internal/worker"
	"github.com/JustinK33/Conduit/pkg/models"
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

	jobSvc := service.NewJobService(kafkaClient, jobStore, retryEngine, cfg.Kafka.Topic,
		cfg.Reconciler.RunningLease, logger.WithComponent(log, "service"))

	runner := &jobWorker{
		store:    jobStore,
		outcome:  jobSvc,
		lockMgr:  lockMgr,
		breaker:  breaker,
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
		// The reconciler claims on behalf of the in-process pool, so it must
		// respect the same queue filter. Without this it would win jobs meant
		// for remote workers and dead-letter them for having no handler.
		Queues: cfg.Worker.Queues,
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

	// gin trusts every proxy by default, which makes the client_ip in the
	// request log forgeable by anyone sending X-Forwarded-For. Empty means
	// trust nothing and use the peer address, which is correct when nothing is
	// in front; set TRUSTED_PROXIES to the proxy's CIDR when something is.
	if err := router.SetTrustedProxies(cfg.HTTP.TrustedProxies); err != nil {
		return fmt.Errorf("trusted proxies: %w", err)
	}

	if len(cfg.HTTP.APIKeys) == 0 {
		log.Warn().Msg("API_KEYS is empty: /api/jobs is unauthenticated, so anyone who can reach this port can enqueue, claim, and cancel work")
	} else {
		// The process cannot see what is in front of it, so this is
		// unconditional rather than clever.
		log.Warn().Msg("API keys are bearer tokens and this server speaks plain HTTP: terminate TLS in front of it or the keys transit in clear (see docs/DEPLOYMENT.md)")
	}

	handler := api.NewHandler(jobSvc, jobStore, logger.WithComponent(log, "api"), reg, cfg.HTTP.APIKeys)
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

// jobOutcome is the terminal write path, shared with the HTTP worker endpoints
// so that a job executed in this process and a job executed by a remote worker
// produce identical state transitions. The retry decision lives behind Fail.
type jobOutcome interface {
	Complete(ctx context.Context, id, leaseToken string, meta map[string]string) error
	Fail(ctx context.Context, id, leaseToken, errMsg string, permanent bool) (models.Job, error)
}

// jobLocker is lock.Manager narrowed to what execution needs, so the worker's
// own logic is testable without a live Redis quorum.
type jobLocker interface {
	Acquire(ctx context.Context, resource string) (lock.Lock, error)
	Release(ctx context.Context, l lock.Lock) error
}

// jobWorker implements worker.JobRunner. It wraps execution with a distributed lock,
// circuit breaker, retry policy, and per-job timeout.
type jobWorker struct {
	store    store.JobStore
	outcome  jobOutcome
	lockMgr  jobLocker
	breaker  *circuitbreaker.Breaker
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

	if runErr := jw.execute(ctx, job); runErr != nil {
		jw.breaker.RecordFailure()
		jw.metrics.JobFailed.Inc()

		// The retry policy lives in the service layer so that this path and the
		// HTTP fail endpoint cannot drift. ErrNoRetry is how an in-process
		// handler says "permanent" - the same thing a remote worker says by
		// posting retry:false.
		permanent := errors.Is(runErr, retry.ErrNoRetry)
		if _, err := jw.outcome.Fail(ctx, job.ID, job.LeaseToken, runErr.Error(), permanent); err != nil {
			jw.log.Error().Err(err).Str("job_id", job.ID).Msg("failed to record job failure")
		}
		return runErr
	}

	jw.breaker.RecordSuccess()
	jw.metrics.JobCompleted.Inc()

	if err := jw.outcome.Complete(ctx, job.ID, job.LeaseToken, nil); err != nil {
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
