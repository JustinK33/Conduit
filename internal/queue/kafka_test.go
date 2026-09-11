package queue

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	"github.com/JustinK33/Conduit/pkg/models"
)

type mockMessageHandler struct{}

func (mockMessageHandler) Handle(context.Context, *sarama.ConsumerMessage) error { return nil }

func TestNewKafkaClient(t *testing.T) {
	t.Skip("requires a running Kafka broker: start the stack with `make docker-up`")

	tests := []struct {
		name string
		cfg  models.KafkaConfig
	}{
		{name: "builds kafka wrapper", cfg: models.KafkaConfig{Brokers: []string{"localhost:9092"}, Topic: "jobs", ConsumerGroup: "conduit"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client, err := NewKafkaClient(tc.cfg)
			if err != nil {
				t.Fatalf("NewKafkaClient error: %v", err)
			}
			if client == nil {
				t.Fatal("NewKafkaClient returned nil client")
			}
			_ = client.Close()
		})
	}
}

func TestKafkaClientPublish(t *testing.T) {
	t.Skip("requires a running Kafka broker: start the stack with `make docker-up`")
}

func TestKafkaClientConsume(t *testing.T) {
	t.Skip("requires a running Kafka broker: start the stack with `make docker-up`")
}

func TestKafkaClientClose(t *testing.T) {
	t.Skip("requires a running Kafka broker: start the stack with `make docker-up`")
}
