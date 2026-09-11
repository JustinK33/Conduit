package models

import "time"

// JobState is the durable lifecycle stage of a queued task.
type JobState string

const (
	JobStatePending   JobState = "PENDING"
	JobStateRunning   JobState = "RUNNING"
	JobStateCompleted JobState = "COMPLETED"
	JobStateFailed    JobState = "FAILED"
	JobStateDead      JobState = "DEAD"
)

// Task describes the payload and execution metadata for a job.
type Task struct {
	ID             string        `json:"id"`
	Name           string        `json:"name"`
	Payload        []byte        `json:"payload,omitempty"`
	RetryCount     int           `json:"retry_count"`
	MaxRetries     int           `json:"max_retries"`
	Timeout        time.Duration `json:"timeout"`
	CronExpression string        `json:"cron_expression,omitempty"`
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

type Config struct {
	HTTP       HTTPConfig
	Kafka      KafkaConfig
	Redis      RedisConfig
	Postgres   PostgresConfig
	Worker     WorkerConfig
	Scheduler  SchedulerConfig
	Reconciler ReconcilerConfig
	Metrics    MetricsConfig
	Logger     LoggerConfig
	Webhook    WebhookConfig
}
