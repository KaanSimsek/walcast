// Package kafka is the Apache Kafka backend (built on franz-go).
package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"walcast/internal/sink"
)

func init() { sink.Register("kafka", New) }

type Sink struct{ client *kgo.Client }

func New(ctx context.Context, sc sink.Config) (sink.Sink, error) {
	var cfg Config
	if err := sc.Decode(&cfg); err != nil {
		return nil, err
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: brokers is required")
	}
	opts, err := cfg.options()
	if err != nil {
		return nil, err
	}
	client, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: %w", err)
	}
	if err := client.Ping(ctx); err != nil {
		client.Close()
		return nil, fmt.Errorf("kafka: cannot reach brokers %v: %w", cfg.Brokers, err)
	}
	return &Sink{client: client}, nil
}

func (s *Sink) Publish(ctx context.Context, m *sink.Message) error {
	rec := &kgo.Record{
		Topic:   m.Topic,
		Key:     m.Key,
		Value:   m.Value,
		Headers: []kgo.RecordHeader{{Key: "op", Value: []byte(m.Op)}},
	}
	if err := s.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("kafka produce to %s: %w", m.Topic, err)
	}
	return nil
}

func (s *Sink) Close() error {
	s.client.Close()
	return nil
}
