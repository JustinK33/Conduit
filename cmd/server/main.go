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

	// `conduit migrate` applies the schema and exits; anything else serves.
	// Migrations used to be the Postgres entrypoint's job, which runs exactly
	// once, on first boot, on an empty volume - so no schema change ever reached
	// a database that already existed.
	command := "serve"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}

	var err error
	switch command {
	case "serve":
		err = run(ctx)
	case "migrate":
		err = runMigrations(ctx)
	default:
		fmt.Fprintf(os.Stderr, "conduit: unknown command %q (want serve or migrate)\n", command)
		os.Exit(2)
	}

	if err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "conduit: %v\n", err)
		os.Exit(1)
	}
}

// bootstrap does the three things both commands need: read the environment,
// build the logger, and open the connection pool.
func bootstrap(ctx context.Context) (models.Config, zerolog.Logger, *pgxpool.Pool, error) {
	var nolog zerolog.Logger

	cfg, err := config.Load()
	if err != nil {
		return cfg, nolog, nil, fmt.Errorf("config: %w", err)
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
		return cfg, nolog, nil, fmt.Errorf("logger: %w", err)
	}
	logger.ConfigureGlobal(log)

	pgCfg, err := pgxpool.ParseConfig(cfg.Postgres.DSN)
	if err != nil {
		return cfg, log, nil, fmt.Errorf("postgres config: %w", err)
	}
	pgCfg.MaxConns = cfg.Postgres.MaxConns
	pgCfg.MinConns = cfg.Postgres.MinConns
	pgCfg.MaxConnLifetime = cfg.Postgres.MaxConnLifetime
	pgCfg.MaxConnIdleTime = cfg.Postgres.MaxConnIdleTime
	pgCfg.HealthCheckPeriod = cfg.Postgres.HealthCheckPeriod

	pgPool, err := pgxpool.NewWithConfig(ctx, pgCfg)
	if err != nil {
		return cfg, log, nil, fmt.Errorf("postgres: %w", err)
	}
	return cfg, log, pgPool, nil
}

func runMigrations(ctx context.Context) error {
	cfg, log, pgPool, err := bootstrap(ctx)
	if err != nil {
		return err
	}
	defer pgPool.Close()

	return store.Migrate(ctx, pgPool, cfg.Postgres.MigrationsPath, logger.WithComponent(log, "migrate"))
}

func run(ctx context.Context) error {
	cfg, log, pgPool, err := bootstrap(ctx)
	if err != nil {
		return err
	}
	defer pgPool.Close()

	reg := metrics.NewRegistry(cfg.Metrics.Namespace, cfg.Metrics.Subsystem)
	if err := reg.Register(prometheus.DefaultRegisterer); err != nil {
		log.Warn().Err(err).Msg("metrics already registered")
	}

	log.Info().Str("transport", cfg.Transport).Str("lock", cfg.Lock).Msg("dispatch configured")

	jobStore := store.NewPostgresStore(pgPool, "jobs")
	scheduleStore := store.NewScheduleStore(pgPool, store.DefaultSchedulesTable)

	// Redis exists for one reason, Redlock, so it is only dialled when Redlock is
	// what was asked for. An advisory lock is a Postgres session lock, and it
	// dies with its connection instead of outliving a killed process the way a
	// Redlock key does.
	var redisClients []redis.UniversalClient
	var lockMgr jobLocker
	switch cfg.Lock {
	case models.LockRedlock:
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
		lockMgr = lock.NewManager(redisClients, lock.Config{
			TTL:         30 * time.Second,
			RetryCount:  3,
			RetryDelay:  100 * time.Millisecond,
			DriftFactor: 0.01,
		})
	case models.LockAdvisory:
		advisory := lock.NewAdvisory(pgPool, logger.WithComponent(log, "lock"))
		defer advisory.Close()
		lockMgr = advisory
	default:
		// The Postgres transport dispatches only through ClaimNextJob, whose
		// FOR UPDATE SKIP LOCKED claim is already atomic across instances, so
		// nothing here needs a second guard. With the Kafka transport it does:
		// a job arrives PENDING from the topic and two instances can both take it.
		if cfg.Transport == models.TransportKafka {
			log.Warn().Msg("CONDUIT_LOCK=none with CONDUIT_TRANSPORT=kafka: two instances consuming one topic can execute the same job twice")
		}
		lockMgr = lock.NoOp{}
	}

	// Kafka's client is only built when Kafka is the transport, which is what
	// makes the broker optional. Construction still fails hard when it is
	// selected and unreachable; Compose's restart: on-failure covers the startup
	// race, and the reconciler covers a broker that dies later.
	var publisher service.Publisher
	var kafkaClient *queue.KafkaClient
	dispatchTarget := queue.NotifyChannel
	if cfg.Transport == models.TransportKafka {
		kafkaClient, err = queue.NewKafkaClient(cfg.Kafka)
		if err != nil {
			return fmt.Errorf("kafka: %w", err)
		}
		defer kafkaClient.Close()
		publisher = kafkaClient
		dispatchTarget = cfg.Kafka.Topic
	} else {
		publisher = queue.NewPostgresNotifier(pgPool)
	}

	breakerCfg := circuitbreaker.Config{
		FailureThreshold: 5,
		SuccessThreshold: 2,
		OpenTimeout:      30 * time.Second,
		HalfOpenRequests: 1,
	}
	handlers := defaultHandlers(cfg.Webhook, pgPool)

	// Bound the task label on every job metric to the handler table, the same
	// set newTaskBreakers is built from. Task names come from callers, so an
	// unfiltered label is unbounded cardinality on untrusted input; anything
	// outside this set reports as "other".
	knownTasks := make([]string, 0, len(handlers))
	for name := range handlers {
		knownTasks = append(knownTasks, name)
	}
	reg.SetKnownTasks(knownTasks)

	retryEngine := retry.NewEngine(retry.Config{
		BaseDelay:   time.Second,
		MaxDelay:    30 * time.Second,
		Multiplier:  2,
		MaxAttempts: 5,
		Jitter:      0.1,
	})

	jobSvc := service.NewJobService(publisher, jobStore, retryEngine, dispatchTarget,
		cfg.Reconciler.RunningLease, logger.WithComponent(log, "service"))

	runner := &jobWorker{
		store:    jobStore,
		outcome:  jobSvc,
		lockMgr:  lockMgr,
		breakers: newTaskBreakers(breakerCfg, handlers),
		metrics:  reg,
		log:      logger.WithComponent(log, "worker"),
		handlers: handlers,
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
		Retention: reconciler.RetentionConfig{
			Completed: cfg.Retention.Completed,
			Dead:      cfg.Retention.Dead,
			Interval:  cfg.Retention.Interval,
			BatchSize: cfg.Retention.BatchSize,
		},
		BacklogInterval: cfg.Reconciler.BacklogInterval,
		Metrics:         reg,
	}, jobStore, workerPool, logger.WithComponent(log, "reconciler"))
	if cfg.Reconciler.Enabled {
		jobReconciler.Start(ctx)
	}
	defer jobReconciler.Stop()

	switch {
	case kafkaClient != nil:
		consumerLog := logger.WithComponent(log, "consumer")
		go func() {
			h := &kafkaJobHandler{pool: workerPool, queues: cfg.Worker.Queues, log: consumerLog}
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

	case cfg.Reconciler.Enabled:
		// The Postgres transport's consume side. A notification only pokes the
		// reconciler, which then claims through the same path it always uses, so
		// the transport cannot double-dispatch and needs no queue filter of its
		// own. Without this an enqueue onto an idle queue waits out
		// CONDUIT_RECONCILER_IDLE_INTERVAL.
		listener := queue.NewPostgresListener(cfg.Postgres.DSN, queue.NotifyChannel, logger.WithComponent(log, "notify"))
		go listener.Run(ctx, jobReconciler.Wake)

	default:
		// Nothing consumes: no broker, and the reconciler that would have claimed
		// the work is switched off. Correct for an API-only instance whose jobs
		// are executed by remote workers over the pull API, and a silent black
		// hole otherwise.
		log.Warn().Msg("CONDUIT_RECONCILER_ENABLED=false with the postgres transport: this instance dispatches nothing to its own worker pool")
	}

	sched := scheduler.New(scheduler.Config{
		TickInterval: cfg.Scheduler.TickInterval,
		FireBudget:   cfg.Scheduler.MaxConcurrentRuns,
	}, scheduleStore, jobSvc, logger.WithComponent(log, "scheduler"))
	if cfg.Scheduler.Enabled {
		sched.Start(ctx)
	} else {
		// Schedules are rows, so they survive this being off. Nothing fires them
		// until an instance with the scheduler enabled is running, and then it
		// skips what it missed rather than replaying it.
		log.Warn().Msg("CONDUIT_SCHEDULER_ENABLED=false: this instance fires no schedules")
	}
	defer sched.Stop()

	gin.SetMode(gin.ReleaseMode)
	router := gin.New()
	// Recovery is installed by api.RegisterRoutes so it can use the same
	// request-scoped logger as the other middlewares.

	// gin trusts every proxy by default, which makes the client_ip in the
	// request log forgeable by anyone sending X-Forwarded-For. Empty means
	// trust nothing and use the peer address, which is correct when nothing is
	// in front; set CONDUIT_TRUSTED_PROXIES to the proxy's CIDR when something is.
	if err := router.SetTrustedProxies(cfg.HTTP.TrustedProxies); err != nil {
		return fmt.Errorf("trusted proxies: %w", err)
	}

	if len(cfg.HTTP.APIKeys) == 0 {
		log.Warn().Msg("CONDUIT_API_KEYS is empty: /api/jobs is unauthenticated, so anyone who can reach this port can enqueue, claim, and cancel work")
	} else {
		// The process cannot see what is in front of it, so this is
		// unconditional rather than clever.
		log.Warn().Msg("API keys are bearer tokens and this server speaks plain HTTP: terminate TLS in front of it or the keys transit in clear (see docs/DEPLOYMENT.md)")
	}

	handler := api.NewHandler(jobSvc, jobStore, logger.WithComponent(log, "api"), reg, cfg.HTTP.APIKeys)
	handler.RegisterRoutes(router)
	// Its own handler with its own routes, mounted on the same router so it
	// inherits the middleware stack and the same API key auth.
	api.NewScheduleHandler(scheduleStore, logger.WithComponent(log, "api"), cfg.HTTP.APIKeys).RegisterRoutes(router)
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
	Complete(ctx context.Context, id, leaseToken string, meta map[string]string) (models.Job, error)
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
	breakers *taskBreakers
	metrics  *metrics.Registry
	log      zerolog.Logger
	handlers map[string]TaskHandler
	lease    time.Duration
}

// taskBreakers holds one circuit breaker per registered task name. There used to
// be a single process-global breaker, which meant one unreachable webhook
// endpoint opened the circuit for sql.etl and for every other task at once: five
// failures anywhere stopped everything for the open timeout.
//
// The map is built once from the handler table, which is fixed at startup, so
// there is nothing to lock and nothing that grows. Keying a lazily-populated map
// on job.Task.Name would be the obvious alternative and is worse, because task
// names come from callers and an unbounded map keyed on caller input is a way to
// spend memory on a typo.
type taskBreakers struct {
	byTask map[string]*circuitbreaker.Breaker
	// unregistered names share one breaker. They fail on every attempt, since
	// execute has no handler for them and returns ErrNoRetry, and there is no
	// endpoint behind them whose health is worth tracking separately.
	unknown *circuitbreaker.Breaker
	// openTimeout is how long a release-on-open should defer the job for. Kept
	// here so it cannot drift from the value the breakers were built with.
	openTimeout time.Duration
}

func newTaskBreakers(cfg circuitbreaker.Config, handlers map[string]TaskHandler) *taskBreakers {
	byTask := make(map[string]*circuitbreaker.Breaker, len(handlers))
	for name := range handlers {
		byTask[name] = circuitbreaker.New(cfg)
	}
	return &taskBreakers{
		byTask:      byTask,
		unknown:     circuitbreaker.New(cfg),
		openTimeout: cfg.OpenTimeout,
	}
}

func (b *taskBreakers) get(taskName string) *circuitbreaker.Breaker {
	if breaker, ok := b.byTask[taskName]; ok {
		return breaker
	}
	return b.unknown
}

// lockContentionBackoff defers a job whose execution lock is held elsewhere.
// Short, because the other holder is running the job right now: by the time this
// elapses the job is either terminal or the lock is free. Non-zero all the same,
// so two instances cannot trade the same job back and forth at reconciler speed.
const lockContentionBackoff = 5 * time.Second

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

	breaker := jw.breakers.get(job.Task.Name)
	if !breaker.Allow() {
		// Wait out the open timeout before this job is eligible again. Releasing
		// with no delay put it back in front of the reconciler on the next tick,
		// which re-claimed it, which released it, for as long as the circuit
		// stayed open.
		jw.log.Warn().Str("job_id", job.ID).Str("task", job.Task.Name).Msg("circuit open, skipping execution")
		jw.releaseClaim(ctx, job, "circuit open", jw.breakers.openTimeout)
		return nil
	}

	// Distributed lock prevents the same job from executing twice across
	// multiple instances (e.g. if it was re-enqueued before the first run commits).
	lck, err := jw.lockMgr.Acquire(ctx, "job:exec:"+job.ID)
	if err != nil {
		jw.log.Warn().Err(err).Str("job_id", job.ID).Msg("could not acquire execution lock")
		// Another instance holds the lock, so it is executing this job right now.
		// Short delay: by the time it clears, the job is either terminal or the
		// lock is free.
		jw.releaseClaim(ctx, job, "could not acquire execution lock", lockContentionBackoff)
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
		jw.metrics.ObserveJobDuration(job.Task.Name, time.Since(start).Seconds())
	}()

	jw.metrics.IncJobStarted(job.Task.Name)

	stopRenewLease := jw.startLeaseRenewal(ctx, job)
	defer stopRenewLease()

	if runErr := jw.execute(ctx, job); runErr != nil {
		breaker.RecordFailure()
		jw.metrics.IncJobFailed(job.Task.Name)

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

	breaker.RecordSuccess()
	jw.metrics.IncJobCompleted(job.Task.Name)

	if _, err := jw.outcome.Complete(ctx, job.ID, job.LeaseToken, nil); err != nil {
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

func (jw *jobWorker) releaseClaim(ctx context.Context, job models.Job, reason string, retryAfter time.Duration) {
	if job.State != models.JobStateRunning || job.LeaseToken == "" {
		return
	}
	if err := jw.store.ReleaseClaim(ctx, job, reason, retryAfter); err != nil {
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

	if timeout := time.Duration(job.Task.Timeout); timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
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

		// No clients means Redlock was not selected, so there is nothing to be
		// ready for. Checking anyway would compute a quorum of 1 out of 0 nodes
		// and report every Postgres-only deployment as permanently not ready.
		if len(redisClients) > 0 {
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
	// queues is the same filter the reconciler applies. The topic carries every
	// job regardless of queue, so without this the in-process pool would run
	// jobs meant for remote workers - and dead-letter them, since it has no
	// handler for them - before the remote worker ever polled. Empty means all
	// queues, matching CONDUIT_WORKER_QUEUES everywhere else.
	queues []string
	log    zerolog.Logger
}

func (h *kafkaJobHandler) Handle(ctx context.Context, msg *sarama.ConsumerMessage) error {
	var job models.Job
	if err := json.Unmarshal(msg.Value, &job); err != nil {
		h.log.Error().Err(err).Msg("failed to unmarshal kafka message")
		return nil
	}
	if !h.claims(job.Task.Queue) {
		// Not ours. The job stays PENDING for whoever claims that queue, and
		// returning nil marks the offset so the topic does not stall on it.
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

func (h *kafkaJobHandler) claims(queue string) bool {
	if len(h.queues) == 0 {
		return true
	}
	if queue == "" {
		// Pre-normalisation rows and anything enqueued by an older build.
		queue = models.DefaultQueue
	}
	for _, q := range h.queues {
		if q == queue {
			return true
		}
	}
	return false
}

func newWorkerLeaseToken() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes), nil
}
