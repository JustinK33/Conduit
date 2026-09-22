package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/JustinK33/Conduit/pkg/models"
)

// Environment abstracts environment lookups so config loading can be tested
// without process state.
type Environment interface {
	LookupEnv(key string) (string, bool)
}

type osEnv struct{}

func (o osEnv) LookupEnv(key string) (string, bool) { return os.LookupEnv(key) }

func Default() models.Config {
	return models.Config{
		// Postgres-only by default, so the quickstart is one container plus a
		// database. Kafka buys dispatch latency and Redlock buys nothing
		// measurable; neither is worth being mandatory.
		Transport: models.TransportPostgres,
		Lock:      models.LockAdvisory,
		HTTP: models.HTTPConfig{
			Address:      ":8080",
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
			IdleTimeout:  60 * time.Second,
		},
		Kafka: models.KafkaConfig{
			Brokers:           []string{"localhost:9092"},
			Topic:             "conduit.jobs",
			ConsumerGroup:     "conduit-workers",
			ClientID:          "conduit",
			RequiredAcks:      1,
			CompressionCodec:  2, // snappy
			FlushFrequencyMs:  5,
			FlushBytes:        1048576,
			ChannelBufferSize: 256,
		},
		Redis: models.RedisConfig{
			Addresses: []string{"localhost:6379", "localhost:6380", "localhost:6381"},
			Database:  0,
			PoolSize:  10,
		},
		Postgres: models.PostgresConfig{
			DSN:               "postgres://postgres:postgres@localhost:5432/conduit?sslmode=disable",
			MaxConns:          10,
			MinConns:          1,
			MigrationsPath:    "migrations",
			MaxConnLifetime:   30 * time.Minute,
			MaxConnIdleTime:   5 * time.Minute,
			HealthCheckPeriod: time.Minute,
		},
		Worker: models.WorkerConfig{
			Concurrency:     8,
			QueueSize:       256,
			ShutdownTimeout: 30 * time.Second,
		},
		Scheduler: models.SchedulerConfig{
			Enabled: true,
			// Lateness is bounded by one tick, and cron granularity is a minute, so
			// a minute-long tick could put a fire a full minute late. Thirty seconds
			// halves that for one extra indexed query per instance per minute.
			TickInterval: 30 * time.Second,
			// MaxConcurrentRuns is the per-tick fire budget: how many due schedules
			// one tick enqueues. Anything over budget is still due next tick, since
			// DueSchedules orders by next_run_at.
			MaxConcurrentRuns: 5,
		},
		Reconciler: models.ReconcilerConfig{
			Enabled:      true,
			Interval:     time.Second,
			IdleInterval: 15 * time.Second,
			// BatchSize is the one knob that moves drain throughput on the default
			// transport, where the reconciler is the only claim path there is:
			// CONDUIT_WORKER_QUEUE_SIZE and CONDUIT_WORKER_CONCURRENCY measured as
			// noise, alone and together. It is a per-tick claim budget rather than a
			// buffer, and ClaimNextJob is submitted with SubmitBlocking, so the loop
			// stops on a full pool by itself and a larger budget costs no memory.
			//
			// 500 over 100 is a tail-latency argument more than a throughput one: in
			// six back-to-back pairs the throughput margins were mostly inside the
			// noise band, but the worst e2e p95 was 4.45s at 100 against 1.68s at
			// 500. A 2,000-job burst needs twenty ticks at 100 per second.
			// The sweep flattens by 250 and 500 to 2000 are indistinguishable, so
			// this is the low end of the plateau: past the knee, and short enough
			// that a claim loop does not delay the next RequeueExpiredRunning pass.
			BatchSize:    500,
			RunningLease: 5 * time.Minute,
			// One sample per instance per ten seconds, against three indexed
			// counts. Prometheus scrapes on a similar timescale, so sampling faster
			// would only produce numbers nobody reads.
			BacklogInterval: 10 * time.Second,
		},
		Retention: models.RetentionConfig{
			// Seven days of completed history is generous for a queue and thirty
			// days of it is a disk bill. A DEAD job is the one somebody wants to
			// investigate, so nothing deletes those until asked: zero means forever.
			Completed: 168 * time.Hour,
			Dead:      0,
			Interval:  time.Hour,
			BatchSize: 1000,
		},
		Metrics: models.MetricsConfig{
			Namespace:     "conduit",
			Subsystem:     "server",
			ListenAddress: ":9090",
		},
		Logger: models.LoggerConfig{
			Level:       "info",
			Format:      "json",
			ServiceName: "conduit",
			Environment: "development",
		},
		Webhook: models.WebhookConfig{
			Timeout:              10 * time.Second,
			MaxRedirects:         0,
			AllowPrivateNetworks: false,
		},
	}
}

func Load() (models.Config, error) {
	return LoadFromEnvironment(osEnv{})
}

// envPrefix namespaces every variable Conduit reads, so the names in
// LoadFromEnvironment are written bare and prefixed in exactly one place:
// HTTP_ADDRESS below is CONDUIT_HTTP_ADDRESS in the environment.
//
// API_KEYS and POSTGRES_DSN are the reason it exists. Both are generic enough to
// already mean something else in a shared .env, docker-compose.yml sets
// POSTGRES_DB, POSTGRES_USER, and POSTGRES_PASSWORD for the database service,
// and a silent collision on the key list is one that hands out a credential.
const envPrefix = "CONDUIT_"

// LoadFromEnvironment merges defaults with values pulled from env. Empty or
// unparseable values fall through to the default rather than erroring - only
// Validate enforces required fields.
func LoadFromEnvironment(env Environment) (models.Config, error) {
	cfg := Default()
	p := envParser{env: env}

	p.str("TRANSPORT", &cfg.Transport)
	p.str("LOCK", &cfg.Lock)

	p.str("HTTP_ADDRESS", &cfg.HTTP.Address)
	p.dur("HTTP_READ_TIMEOUT", &cfg.HTTP.ReadTimeout)
	p.dur("HTTP_WRITE_TIMEOUT", &cfg.HTTP.WriteTimeout)
	p.dur("HTTP_IDLE_TIMEOUT", &cfg.HTTP.IdleTimeout)
	p.strs("API_KEYS", &cfg.HTTP.APIKeys)
	p.strs("TRUSTED_PROXIES", &cfg.HTTP.TrustedProxies)

	p.strs("KAFKA_BROKERS", &cfg.Kafka.Brokers)
	p.str("KAFKA_TOPIC", &cfg.Kafka.Topic)
	p.str("KAFKA_CONSUMER_GROUP", &cfg.Kafka.ConsumerGroup)
	p.str("KAFKA_CLIENT_ID", &cfg.Kafka.ClientID)
	p.int16("KAFKA_REQUIRED_ACKS", &cfg.Kafka.RequiredAcks)
	p.intv("KAFKA_COMPRESSION_CODEC", &cfg.Kafka.CompressionCodec)
	p.intv("KAFKA_FLUSH_FREQUENCY_MS", &cfg.Kafka.FlushFrequencyMs)
	p.intv("KAFKA_FLUSH_BYTES", &cfg.Kafka.FlushBytes)
	p.intv("KAFKA_CHANNEL_BUFFER_SIZE", &cfg.Kafka.ChannelBufferSize)

	p.strs("REDIS_ADDRESSES", &cfg.Redis.Addresses)
	p.str("REDIS_USERNAME", &cfg.Redis.Username)
	p.str("REDIS_PASSWORD", &cfg.Redis.Password)
	p.intv("REDIS_DATABASE", &cfg.Redis.Database)
	p.intv("REDIS_POOL_SIZE", &cfg.Redis.PoolSize)

	p.str("POSTGRES_DSN", &cfg.Postgres.DSN)
	p.int32("POSTGRES_MAX_CONNS", &cfg.Postgres.MaxConns)
	p.int32("POSTGRES_MIN_CONNS", &cfg.Postgres.MinConns)
	p.str("POSTGRES_MIGRATIONS_PATH", &cfg.Postgres.MigrationsPath)
	p.dur("POSTGRES_MAX_CONN_LIFETIME", &cfg.Postgres.MaxConnLifetime)
	p.dur("POSTGRES_MAX_CONN_IDLE_TIME", &cfg.Postgres.MaxConnIdleTime)
	p.dur("POSTGRES_HEALTH_CHECK_PERIOD", &cfg.Postgres.HealthCheckPeriod)

	p.intv("WORKER_CONCURRENCY", &cfg.Worker.Concurrency)
	p.intv("WORKER_QUEUE_SIZE", &cfg.Worker.QueueSize)
	p.dur("WORKER_SHUTDOWN_TIMEOUT", &cfg.Worker.ShutdownTimeout)
	p.strs("WORKER_QUEUES", &cfg.Worker.Queues)

	p.boolv("SCHEDULER_ENABLED", &cfg.Scheduler.Enabled)
	p.dur("SCHEDULER_TICK_INTERVAL", &cfg.Scheduler.TickInterval)
	p.intv("SCHEDULER_MAX_CONCURRENT_RUNS", &cfg.Scheduler.MaxConcurrentRuns)

	p.boolv("RECONCILER_ENABLED", &cfg.Reconciler.Enabled)
	p.dur("RECONCILER_INTERVAL", &cfg.Reconciler.Interval)
	p.dur("RECONCILER_IDLE_INTERVAL", &cfg.Reconciler.IdleInterval)
	p.intv("RECONCILER_BATCH_SIZE", &cfg.Reconciler.BatchSize)
	p.dur("RECONCILER_RUNNING_LEASE", &cfg.Reconciler.RunningLease)
	p.dur("RECONCILER_BACKLOG_INTERVAL", &cfg.Reconciler.BacklogInterval)

	p.dur("RETENTION_COMPLETED", &cfg.Retention.Completed)
	p.dur("RETENTION_DEAD", &cfg.Retention.Dead)
	p.dur("RETENTION_INTERVAL", &cfg.Retention.Interval)
	p.intv("RETENTION_BATCH_SIZE", &cfg.Retention.BatchSize)

	p.str("METRICS_NAMESPACE", &cfg.Metrics.Namespace)
	p.str("METRICS_SUBSYSTEM", &cfg.Metrics.Subsystem)
	p.str("METRICS_LISTEN_ADDRESS", &cfg.Metrics.ListenAddress)

	p.str("LOG_LEVEL", &cfg.Logger.Level)
	p.str("LOG_FORMAT", &cfg.Logger.Format)
	p.str("LOG_SERVICE_NAME", &cfg.Logger.ServiceName)
	p.str("LOG_ENVIRONMENT", &cfg.Logger.Environment)
	p.boolv("LOG_PRETTY", &cfg.Logger.Pretty)
	p.boolv("LOG_ADD_CALLER", &cfg.Logger.AddCaller)

	p.dur("WEBHOOK_TIMEOUT", &cfg.Webhook.Timeout)
	p.intv("WEBHOOK_MAX_REDIRECTS", &cfg.Webhook.MaxRedirects)
	p.boolv("WEBHOOK_ALLOW_PRIVATE_NETWORKS", &cfg.Webhook.AllowPrivateNetworks)

	return cfg, Validate(cfg)
}

func Validate(cfg models.Config) error {
	if cfg.Postgres.DSN == "" {
		return fmt.Errorf("config: CONDUIT_POSTGRES_DSN is required")
	}

	// An unrecognised value is rejected rather than falling back to the default.
	// Silently reading CONDUIT_LOCK=redlok as "none" would turn a typo into a
	// missing guard, and CONDUIT_TRANSPORT=kafla into a broker nobody notices is
	// unused.
	switch cfg.Transport {
	case models.TransportPostgres:
	case models.TransportKafka:
		// Only required for the Kafka transport: with the Postgres transport no
		// client is constructed, so a blank broker list is not a misconfiguration.
		if len(cfg.Kafka.Brokers) == 0 {
			return fmt.Errorf("config: CONDUIT_KAFKA_BROKERS is required for CONDUIT_TRANSPORT=kafka")
		}
		if cfg.Kafka.Topic == "" {
			return fmt.Errorf("config: CONDUIT_KAFKA_TOPIC is required for CONDUIT_TRANSPORT=kafka")
		}
	default:
		return fmt.Errorf("config: CONDUIT_TRANSPORT must be %q or %q, got %q",
			models.TransportPostgres, models.TransportKafka, cfg.Transport)
	}

	switch cfg.Lock {
	case models.LockNone, models.LockAdvisory:
	case models.LockRedlock:
		if len(cfg.Redis.Addresses) == 0 {
			return fmt.Errorf("config: CONDUIT_REDIS_ADDRESSES is required for CONDUIT_LOCK=redlock")
		}
	default:
		return fmt.Errorf("config: CONDUIT_LOCK must be %q, %q, or %q, got %q",
			models.LockNone, models.LockAdvisory, models.LockRedlock, cfg.Lock)
	}

	if cfg.Worker.Concurrency <= 0 {
		return fmt.Errorf("config: CONDUIT_WORKER_CONCURRENCY must be > 0, got %d", cfg.Worker.Concurrency)
	}
	if cfg.Webhook.Timeout <= 0 {
		return fmt.Errorf("config: CONDUIT_WEBHOOK_TIMEOUT must be > 0, got %s", cfg.Webhook.Timeout)
	}
	if cfg.Webhook.MaxRedirects < 0 {
		return fmt.Errorf("config: CONDUIT_WEBHOOK_MAX_REDIRECTS must be >= 0, got %d", cfg.Webhook.MaxRedirects)
	}
	for _, key := range cfg.HTTP.APIKeys {
		if len(key) < 16 {
			return fmt.Errorf("config: CONDUIT_API_KEYS must be at least 16 characters each, got one with length %d", len(key))
		}
	}
	return nil
}

type envParser struct {
	env Environment
}

func (p envParser) lookup(key string) (string, bool) {
	v, ok := p.env.LookupEnv(envPrefix + key)
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

func (p envParser) str(key string, dest *string) {
	if v, ok := p.lookup(key); ok {
		*dest = v
	}
}

func (p envParser) strs(key string, dest *[]string) {
	if v, ok := p.lookup(key); ok {
		parts := strings.Split(v, ",")
		var result []string
		for _, part := range parts {
			trimmed := strings.TrimSpace(part)
			if trimmed != "" {
				result = append(result, trimmed)
			}
		}
		*dest = result
	}
}

func (p envParser) dur(key string, dest *time.Duration) {
	if v, ok := p.lookup(key); ok {
		if d, err := time.ParseDuration(v); err == nil {
			*dest = d
		}
	}
}

func (p envParser) intv(key string, dest *int) {
	if v, ok := p.lookup(key); ok {
		if n, err := strconv.Atoi(v); err == nil {
			*dest = n
		}
	}
}

func (p envParser) int32(key string, dest *int32) {
	if v, ok := p.lookup(key); ok {
		if n, err := strconv.ParseInt(v, 10, 32); err == nil {
			*dest = int32(n)
		}
	}
}

func (p envParser) int16(key string, dest *int16) {
	if v, ok := p.lookup(key); ok {
		if n, err := strconv.ParseInt(v, 10, 16); err == nil {
			*dest = int16(n)
		}
	}
}

func (p envParser) boolv(key string, dest *bool) {
	if v, ok := p.lookup(key); ok {
		if b, err := strconv.ParseBool(v); err == nil {
			*dest = b
		}
	}
}
