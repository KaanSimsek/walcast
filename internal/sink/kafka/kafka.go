// Package kafka is the Apache Kafka backend (built on franz-go).
package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"walcast/internal/sink"
)

func init() { sink.Register("kafka", New) }

type Sink struct {
	client   *kgo.Client
	dlqTopic string
}

func New(ctx context.Context, sc sink.Config) (sink.Sink, error) {
	var cfg Config
	if err := sc.Decode(&cfg); err != nil {
		return nil, err
	}
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("kafka: brokers is required")
	}
	if cfg.DLQ != nil && cfg.DLQ.Topic == "" {
		return nil, fmt.Errorf("kafka: dlq.topic is required when dlq is configured")
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
	s := &Sink{client: client}
	if cfg.DLQ != nil {
		s.dlqTopic = cfg.DLQ.Topic
	}
	return s, nil
}

func record(m *sink.Message) *kgo.Record {
	rec := &kgo.Record{
		Topic:   m.Topic,
		Key:     m.Key,
		Value:   m.Value,
		Headers: []kgo.RecordHeader{{Key: "op", Value: []byte(m.Op)}},
	}
	if m.ID != "" {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: "id", Value: []byte(m.ID)})
	}
	if m.LSN != "" {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: "lsn", Value: []byte(m.LSN)})
	}
	return rec
}

func (s *Sink) Publish(ctx context.Context, m *sink.Message) error {
	rec := record(m)
	if err := s.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		err = fmt.Errorf("kafka produce to %s: %w", m.Topic, err)
		if permanent, reason := classifyProduceError(err); permanent {
			return sink.Permanent(err, reason)
		}
		return err
	}
	return nil
}

func classifyProduceError(err error) (permanent bool, reason string) {
	if kerr.IsRetriable(err) {
		return false, ""
	}
	var ke *kerr.Error
	if !errors.As(err, &ke) {
		return false, ""
	}
	switch ke.Code {
	case kerr.MessageTooLarge.Code:
		return true, "too_large"
	case kerr.InvalidTopicException.Code:
		return true, "invalid_topic"
	case kerr.RecordListTooLarge.Code:
		return true, "record_list_too_large"
	case kerr.TopicAuthorizationFailed.Code:
		return true, "topic_auth"
	case kerr.ClusterAuthorizationFailed.Code:
		return true, "cluster_auth"
	case kerr.UnsupportedSaslMechanism.Code:
		return true, "sasl_mechanism"
	case kerr.UnsupportedVersion.Code:
		return true, "unsupported_version"
	case kerr.SaslAuthenticationFailed.Code:
		return true, "sasl_auth"
	case kerr.InvalidRecord.Code:
		return true, "invalid_record"
	}
	return false, ""
}

func deadLetterRecord(topic string, m *sink.Message, f sink.Failure) *kgo.Record {
	rec := &kgo.Record{Topic: topic, Key: m.Key, Value: m.Value}
	for k, v := range sink.DLQHeaders(m, f) {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: k, Value: []byte(v)})
	}
	return rec
}

func (s *Sink) DeadLetter(ctx context.Context, m *sink.Message, f sink.Failure) error {
	if s.dlqTopic == "" {
		return sink.ErrDeadLetterNotConfigured
	}
	if err := s.client.ProduceSync(ctx, deadLetterRecord(s.dlqTopic, m, f)).FirstErr(); err != nil {
		return fmt.Errorf("kafka dead letter to %s: %w", s.dlqTopic, err)
	}
	return nil
}

func (s *Sink) Close() error {
	s.client.Close()
	return nil
}
