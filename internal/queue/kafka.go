package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/IBM/sarama"
	"github.com/example/conduit/pkg/models"
)

type MessageHandler interface {
	Handle(context.Context, *sarama.ConsumerMessage) error
}

type Producer interface {
	Publish(context.Context, string, models.Job) error
	Close() error
}

type Consumer interface {
	Consume(context.Context, []string, MessageHandler) error
	Close() error
}

type KafkaClient struct {
	Producer        sarama.SyncProducer
	ConsumerGroup   sarama.ConsumerGroup
	Topic           string
	ConsumerGroupID string
}

func NewKafkaClient(cfg models.KafkaConfig) (*KafkaClient, error) {
	sc := sarama.NewConfig()
	sc.ClientID = cfg.ClientID
	sc.Version = sarama.V3_3_0_0
	sc.ChannelBufferSize = cfg.ChannelBufferSize

	sc.Producer.RequiredAcks = sarama.RequiredAcks(cfg.RequiredAcks)
	sc.Producer.Return.Successes = true
	sc.Producer.Compression = sarama.CompressionCodec(cfg.CompressionCodec)
	sc.Producer.Flush.Frequency = time.Duration(cfg.FlushFrequencyMs) * time.Millisecond
	sc.Producer.Flush.Bytes = cfg.FlushBytes

	sc.Consumer.Return.Errors = true
	// OffsetOldest, not OffsetNewest. Newest means "skip everything published
	// before this group finished joining", which silently drops every job
	// enqueued during startup or a rebalance: the group commits nothing, so the
	// jobs sit PENDING until the reconciler notices. Replaying an already-run
	// job is harmless because the Postgres claim decides who executes it.
	sc.Consumer.Offsets.Initial = sarama.OffsetOldest

	producer, err := sarama.NewSyncProducer(cfg.Brokers, sc)
	if err != nil {
		return nil, fmt.Errorf("queue: create producer: %w", err)
	}

	cg, err := sarama.NewConsumerGroup(cfg.Brokers, cfg.ConsumerGroup, sc)
	if err != nil {
		producer.Close()
		return nil, fmt.Errorf("queue: create consumer group: %w", err)
	}

	return &KafkaClient{
		Producer:        producer,
		ConsumerGroup:   cg,
		Topic:           cfg.Topic,
		ConsumerGroupID: cfg.ConsumerGroup,
	}, nil
}

func (k *KafkaClient) Publish(_ context.Context, topic string, job models.Job) error {
	payload, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("queue: marshal job: %w", err)
	}

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(job.ID),
		Value: sarama.ByteEncoder(payload),
		Headers: []sarama.RecordHeader{
			{Key: []byte("job_id"), Value: []byte(job.ID)},
			{Key: []byte("job_name"), Value: []byte(job.Task.Name)},
		},
	}

	_, _, err = k.Producer.SendMessage(msg)
	return err
}

// cgHandler bridges sarama's ConsumerGroupHandler to our MessageHandler contract.
type cgHandler struct {
	handler MessageHandler
}

func (h *cgHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *cgHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (h *cgHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			if err := h.handler.Handle(session.Context(), msg); err == nil {
				session.MarkMessage(msg, "")
			}
		case <-session.Context().Done():
			return nil
		}
	}
}

func (k *KafkaClient) Consume(ctx context.Context, topics []string, handler MessageHandler) error {
	h := &cgHandler{handler: handler}
	for {
		if err := k.ConsumerGroup.Consume(ctx, topics, h); err != nil {
			return err
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
	}
}

func (k *KafkaClient) Close() error {
	var errs []error
	if err := k.Producer.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := k.ConsumerGroup.Close(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}
