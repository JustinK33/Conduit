package models

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"
)

// JobState is the durable lifecycle stage of a queued task.
type JobState string

const (
	JobStatePending   JobState = "PENDING"
	JobStateRunning   JobState = "RUNNING"
	JobStateCompleted JobState = "COMPLETED"
	JobStateFailed    JobState = "FAILED"
	JobStateDead      JobState = "DEAD"
)

// Duration is a time.Duration that crosses the wire as "15s" rather than as
// 15000000000. time.Duration's own JSON form is integer nanoseconds, which is
// unreadable in a curl command and easy to get wrong by three orders of
// magnitude.
type Duration time.Duration

func (d Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(time.Duration(d).String())
}

// UnmarshalJSON accepts a duration string only. The integer-nanosecond form
// that Conduit accepted before v0.2.0 is rejected rather than guessed at: a
// caller who means 15 seconds and writes 15 should hear about it.
func (d *Duration) UnmarshalJSON(b []byte) error {
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return fmt.Errorf("timeout must be a duration string like \"15s\", got %s", b)
	}
	parsed, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("timeout %q: %w", s, err)
	}
	*d = Duration(parsed)
	return nil
}

// Payload is the caller's task body, carried inline as JSON rather than
// base64. Conduit does not interpret it, so any valid JSON value is accepted
// and stored byte for byte.
type Payload []byte

func (p Payload) MarshalJSON() ([]byte, error) {
	if len(p) == 0 {
		return []byte("null"), nil
	}
	if json.Valid(p) {
		return p, nil
	}
	// ponytail: task_payload is BYTEA and held arbitrary bytes before v0.2.0,
	// so returning those verbatim would put invalid JSON in a response body.
	// Base64 keeps them readable. Drop this branch once no such row survives.
	return json.Marshal(base64.StdEncoding.EncodeToString(p))
}

func (p *Payload) UnmarshalJSON(b []byte) error {
	*p = append((*p)[:0], b...)
	return nil
}

// Task describes the payload and execution metadata for a job.
type Task struct {
	ID             string   `json:"id"`
	Name           string   `json:"name"`
	Payload        Payload  `json:"payload,omitempty"`
	RetryCount     int      `json:"retry_count"`
	MaxRetries     int      `json:"max_retries"`
	Timeout        Duration `json:"timeout"`
	CronExpression string   `json:"cron_expression,omitempty"`
	// Queue is the routing key: a worker claims from the queues it names, so a
	// remote worker never wins a job it has no code for. Empty is normalised to
	// DefaultQueue on enqueue.
	Queue    string            `json:"queue,omitempty"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// DefaultQueue is where a job goes when it names no queue. It matches the
// task_queue column's own default in migrations/001.
const DefaultQueue = "default"

// Job is the durable execution record that moves through the state machine.
type Job struct {
	ID             string     `json:"id"`
	IdempotencyKey string     `json:"idempotency_key,omitempty"`
	Task           Task       `json:"task"`
	State          JobState   `json:"state"`
	Attempt        int        `json:"attempt"`
	ScheduledAt    *time.Time `json:"scheduled_at,omitempty"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	LeaseExpiresAt *time.Time `json:"lease_expires_at,omitempty"`
	// LeaseToken is a fencing token used by workers to prove they still hold the
	// lease when completing or failing a job. It is never serialized to JSON
	// because only the claim response should include it, not GET /api/jobs/:id.
	LeaseToken  string            `json:"-"`
	CompletedAt *time.Time        `json:"completed_at,omitempty"`
	LastError   string            `json:"last_error,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
}

// Schedule is a recurring job definition: a task template plus a cron
// expression. Firing one enqueues an ordinary Job, so a scheduled run gets the
// same retries, leases, and state machine as anything else in the queue.
//
// NextRunAt is both the fire instant and the optimistic-concurrency token that
// keeps replicas from firing the same instant twice. See
// store.ScheduleStore.AdvanceSchedule.
type Schedule struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Cron    string `json:"cron"`
	Task    Task   `json:"task"`
	Enabled bool   `json:"enabled"`

	NextRunAt time.Time  `json:"next_run_at"`
	LastRunAt *time.Time `json:"last_run_at,omitempty"`
	LastJobID string     `json:"last_job_id,omitempty"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
}

type HTTPConfig struct {
	Address      string
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	IdleTimeout  time.Duration
	APIKeys      []string
	// TrustedProxies is a CIDR list. Empty means trust nothing, so gin uses the
	// direct peer address and a forged X-Forwarded-For cannot poison the audit
	// log. Set it to the reverse proxy's network to get real client addresses.
	TrustedProxies []string
}

type KafkaConfig struct {
	Brokers           []string
	Topic             string
	ConsumerGroup     string
	ClientID          string
	RequiredAcks      int16
	CompressionCodec  int // 0=none 1=gzip 2=snappy 3=lz4 4=zstd
	FlushFrequencyMs  int
	FlushBytes        int
	ChannelBufferSize int
}

type RedisConfig struct {
	Addresses []string
	Username  string
	Password  string
	Database  int
	PoolSize  int
}

type PostgresConfig struct {
	DSN               string
	MaxConns          int32
	MinConns          int32
	MigrationsPath    string
	MaxConnLifetime   time.Duration
	MaxConnIdleTime   time.Duration
	HealthCheckPeriod time.Duration
}

type WorkerConfig struct {
	Concurrency     int
	QueueSize       int
	ShutdownTimeout time.Duration
	Queues          []string
}

type SchedulerConfig struct {
	Enabled           bool
	TickInterval      time.Duration
	MaxConcurrentRuns int
}

type ReconcilerConfig struct {
	Enabled      bool
	Interval     time.Duration
	IdleInterval time.Duration
	BatchSize    int
	RunningLease time.Duration
	// BacklogInterval throttles the jobs_backlog gauge sample. Negative disables
	// it.
	BacklogInterval time.Duration
}

// RetentionConfig is how long finished jobs are kept. A zero age means keep
// forever, which is the only way to say "delete nothing" and has to stay
// expressible: the DEAD age defaults to it, because a dead-lettered job is the
// one somebody wants to look at.
//
// Sweeping is the reconciler's job, so an instance with the reconciler disabled
// prunes nothing.
type RetentionConfig struct {
	Completed time.Duration
	Dead      time.Duration
	Interval  time.Duration
	BatchSize int
}

type MetricsConfig struct {
	Namespace     string
	Subsystem     string
	ListenAddress string
}

type LoggerConfig struct {
	Level       string
	Format      string
	ServiceName string
	Environment string
	Pretty      bool
	AddCaller   bool
}

type WebhookConfig struct {
	Timeout              time.Duration
	MaxRedirects         int
	AllowPrivateNetworks bool
}

// Transport names the wake-up path for a freshly enqueued job. Neither option
// carries authority: Postgres is the source of truth and the reconciler is the
// guaranteed delivery path, so a transport that drops a message costs dispatch
// latency and nothing else.
const (
	TransportPostgres = "postgres"
	TransportKafka    = "kafka"
)

// Lock names the strategy that stops two instances executing one job at the
// same time. It guards duplicated effort, not correctness: the lease token in
// every state-changing WHERE clause is what makes execution safe.
const (
	LockNone     = "none"
	LockAdvisory = "advisory"
	LockRedlock  = "redlock"
)

type Config struct {
	// Transport is one of TransportPostgres or TransportKafka. Kafka's client is
	// only constructed when it is selected, which is what makes the broker
	// optional rather than mandatory at boot.
	Transport string
	// Lock is one of LockNone, LockAdvisory, or LockRedlock. Redis clients are
	// only constructed for LockRedlock.
	Lock       string
	HTTP       HTTPConfig
	Kafka      KafkaConfig
	Redis      RedisConfig
	Postgres   PostgresConfig
	Worker     WorkerConfig
	Scheduler  SchedulerConfig
	Reconciler ReconcilerConfig
	Retention  RetentionConfig
	Metrics    MetricsConfig
	Logger     LoggerConfig
	Webhook    WebhookConfig
}
