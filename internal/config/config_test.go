package config

import (
	"testing"
	"time"

	"github.com/JustinK33/Conduit/pkg/models"
)

type mockEnvironment struct {
	values map[string]string
}

func (m mockEnvironment) LookupEnv(key string) (string, bool) {
	value, ok := m.values[key]
	return value, ok
}

func TestDefault(t *testing.T) {
	t.Run("returns a config without panicking", func(t *testing.T) {
		cfg := Default()
		// Default() must return a usable zero-value config.
		// Once you implement it, assert the specific defaults here.
		_ = cfg
	})
}

func TestLoad(t *testing.T) {
	t.Run("returns config from process environment", func(t *testing.T) {
		// Load reads real environment variables; set them in the test process if needed.
		_, err := Load()
		// An unset environment may return an error or a zero config - both are valid outcomes.
		// Once you implement Validate, assert that a fully set env returns err == nil.
		_ = err
	})
}

func TestLoadFromEnvironment(t *testing.T) {
	tests := []struct {
		name    string
		env     mockEnvironment
		wantErr bool
	}{
		{
			name: "reads HTTP address from environment",
			env: mockEnvironment{values: map[string]string{
				"CONDUIT_HTTP_ADDRESS": ":8080",
			}},
			wantErr: false,
		},
		{
			name:    "empty environment returns default or error",
			env:     mockEnvironment{values: map[string]string{}},
			wantErr: false, // adjust once you decide whether empty env is an error
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := LoadFromEnvironment(tc.env)
			if tc.wantErr && err == nil {
				t.Error("expected an error but got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			// Assert the specific field once you pick your env var names.
			_ = cfg
		})
	}
}

func TestLoadFromEnvironmentReadsWebhookConfig(t *testing.T) {
	cfg, err := LoadFromEnvironment(mockEnvironment{values: map[string]string{
		"CONDUIT_WEBHOOK_TIMEOUT":                "3s",
		"CONDUIT_WEBHOOK_MAX_REDIRECTS":          "2",
		"CONDUIT_WEBHOOK_ALLOW_PRIVATE_NETWORKS": "true",
	}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Webhook.Timeout != 3*time.Second {
		t.Fatalf("webhook timeout = %s, want 3s", cfg.Webhook.Timeout)
	}
	if cfg.Webhook.MaxRedirects != 2 {
		t.Fatalf("webhook max redirects = %d, want 2", cfg.Webhook.MaxRedirects)
	}
	if !cfg.Webhook.AllowPrivateNetworks {
		t.Fatal("webhook allow private networks = false, want true")
	}
}

func TestValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     models.Config
		wantErr bool
	}{
		{
			name:    "empty config is invalid",
			cfg:     models.Config{},
			wantErr: true,
		},
		{
			name: "config with required fields is valid",
			cfg: models.Config{
				HTTP:     models.HTTPConfig{Address: ":8080", ReadTimeout: 5e9, WriteTimeout: 5e9},
				Kafka:    models.KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "jobs", ConsumerGroup: "workers"},
				Postgres: models.PostgresConfig{DSN: "postgres://localhost/conduit"},
				Worker:   models.WorkerConfig{Concurrency: 4, QueueSize: 32},
				Webhook:  models.WebhookConfig{Timeout: time.Second},
			},
			wantErr: false,
		},
		{
			name: "zero worker concurrency is invalid",
			cfg: models.Config{
				HTTP:     models.HTTPConfig{Address: ":8080", ReadTimeout: 5e9, WriteTimeout: 5e9},
				Kafka:    models.KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "jobs", ConsumerGroup: "workers"},
				Postgres: models.PostgresConfig{DSN: "postgres://localhost/conduit"},
				Worker:   models.WorkerConfig{Concurrency: 0, QueueSize: 32},
			},
			wantErr: true,
		},
		{
			name: "empty API_KEYS is valid",
			cfg: models.Config{
				HTTP:     models.HTTPConfig{Address: ":8080", ReadTimeout: 5e9, WriteTimeout: 5e9, APIKeys: []string{}},
				Kafka:    models.KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "jobs", ConsumerGroup: "workers"},
				Postgres: models.PostgresConfig{DSN: "postgres://localhost/conduit"},
				Worker:   models.WorkerConfig{Concurrency: 4, QueueSize: 32},
				Webhook:  models.WebhookConfig{Timeout: time.Second},
			},
			wantErr: false,
		},
		{
			name: "API_KEYS with valid length is valid",
			cfg: models.Config{
				HTTP:     models.HTTPConfig{Address: ":8080", ReadTimeout: 5e9, WriteTimeout: 5e9, APIKeys: []string{"abcdefghijklmnop"}},
				Kafka:    models.KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "jobs", ConsumerGroup: "workers"},
				Postgres: models.PostgresConfig{DSN: "postgres://localhost/conduit"},
				Worker:   models.WorkerConfig{Concurrency: 4, QueueSize: 32},
				Webhook:  models.WebhookConfig{Timeout: time.Second},
			},
			wantErr: false,
		},
		{
			name: "API_KEYS shorter than 16 chars is invalid",
			cfg: models.Config{
				HTTP:     models.HTTPConfig{Address: ":8080", ReadTimeout: 5e9, WriteTimeout: 5e9, APIKeys: []string{"short"}},
				Kafka:    models.KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "jobs", ConsumerGroup: "workers"},
				Postgres: models.PostgresConfig{DSN: "postgres://localhost/conduit"},
				Worker:   models.WorkerConfig{Concurrency: 4, QueueSize: 32},
				Webhook:  models.WebhookConfig{Timeout: time.Second},
			},
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Validate(tc.cfg)
			if tc.wantErr && err == nil {
				t.Error("expected error but got nil")
			}
			if !tc.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

func TestLoadFromEnvironmentParsesAPIKeys(t *testing.T) {
	cfg, err := LoadFromEnvironment(mockEnvironment{values: map[string]string{
		"CONDUIT_API_KEYS": "key-one-0123456789,key-two-abcdefghij,key-three-xyz123456",
	}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.HTTP.APIKeys) != 3 {
		t.Fatalf("API_KEYS length = %d, want 3", len(cfg.HTTP.APIKeys))
	}
	if cfg.HTTP.APIKeys[0] != "key-one-0123456789" {
		t.Errorf("API_KEYS[0] = %q, want %q", cfg.HTTP.APIKeys[0], "key-one-0123456789")
	}
	if cfg.HTTP.APIKeys[1] != "key-two-abcdefghij" {
		t.Errorf("API_KEYS[1] = %q, want %q", cfg.HTTP.APIKeys[1], "key-two-abcdefghij")
	}
	if cfg.HTTP.APIKeys[2] != "key-three-xyz123456" {
		t.Errorf("API_KEYS[2] = %q, want %q", cfg.HTTP.APIKeys[2], "key-three-xyz123456")
	}
}

func TestLoadFromEnvironmentTrimsWhitespaceInCommaSeparatedLists(t *testing.T) {
	cfg, err := LoadFromEnvironment(mockEnvironment{values: map[string]string{
		"CONDUIT_API_KEYS":        "key-one-0123456789, key-two-abcdefghij , key-three-xyz123456",
		"CONDUIT_WORKER_QUEUES":   "queue-a , queue-b,  queue-c  ",
		"CONDUIT_KAFKA_BROKERS":   "localhost:9092 , kafka:9092",
		"CONDUIT_REDIS_ADDRESSES": "redis:6379 , redis-2:6379",
	}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.HTTP.APIKeys) != 3 {
		t.Fatalf("API_KEYS length = %d, want 3", len(cfg.HTTP.APIKeys))
	}
	if cfg.HTTP.APIKeys[0] != "key-one-0123456789" || cfg.HTTP.APIKeys[1] != "key-two-abcdefghij" || cfg.HTTP.APIKeys[2] != "key-three-xyz123456" {
		t.Errorf("API_KEYS not trimmed: %v", cfg.HTTP.APIKeys)
	}
	if len(cfg.Worker.Queues) != 3 {
		t.Fatalf("WORKER_QUEUES length = %d, want 3", len(cfg.Worker.Queues))
	}
	if cfg.Worker.Queues[0] != "queue-a" || cfg.Worker.Queues[1] != "queue-b" || cfg.Worker.Queues[2] != "queue-c" {
		t.Errorf("WORKER_QUEUES not trimmed: %v", cfg.Worker.Queues)
	}
	if len(cfg.Kafka.Brokers) != 2 {
		t.Fatalf("KAFKA_BROKERS length = %d, want 2", len(cfg.Kafka.Brokers))
	}
	if cfg.Kafka.Brokers[0] != "localhost:9092" || cfg.Kafka.Brokers[1] != "kafka:9092" {
		t.Errorf("KAFKA_BROKERS not trimmed: %v", cfg.Kafka.Brokers)
	}
	if len(cfg.Redis.Addresses) != 2 {
		t.Fatalf("REDIS_ADDRESSES length = %d, want 2", len(cfg.Redis.Addresses))
	}
	if cfg.Redis.Addresses[0] != "redis:6379" || cfg.Redis.Addresses[1] != "redis-2:6379" {
		t.Errorf("REDIS_ADDRESSES not trimmed: %v", cfg.Redis.Addresses)
	}
}

// Every variable is namespaced, and an unprefixed name must be ignored rather
// than half-honoured. API_KEYS is the case that matters: something else in a
// shared environment owning that name must not be able to set who may claim
// jobs here.
func TestLoadFromEnvironmentIgnoresUnprefixedNames(t *testing.T) {
	cfg, err := LoadFromEnvironment(mockEnvironment{values: map[string]string{
		"API_KEYS":     "someone-elses-key-0123456789",
		"HTTP_ADDRESS": ":9999",
	}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.HTTP.APIKeys) != 0 {
		t.Errorf("unprefixed API_KEYS was read: %v", cfg.HTTP.APIKeys)
	}
	if cfg.HTTP.Address != Default().HTTP.Address {
		t.Errorf("unprefixed HTTP_ADDRESS was read: %q", cfg.HTTP.Address)
	}
}

func TestLoadFromEnvironmentUnsetMeansEmpty(t *testing.T) {
	cfg, err := LoadFromEnvironment(mockEnvironment{values: map[string]string{}})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(cfg.HTTP.APIKeys) != 0 {
		t.Errorf("unset API_KEYS should be empty, got %v", cfg.HTTP.APIKeys)
	}
	if len(cfg.Worker.Queues) != 0 {
		t.Errorf("unset WORKER_QUEUES should be empty, got %v", cfg.Worker.Queues)
	}
}
