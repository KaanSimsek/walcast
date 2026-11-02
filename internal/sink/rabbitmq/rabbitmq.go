// Package rabbitmq is the RabbitMQ backend.
//
// Events are published to an exchange with routing key "<topic>.<op>", e.g.
// walcast.public.users.insert, so queues can bind by table and/or operation.
// Every publish waits for a broker confirm; a message with no matching queue is
// returned and treated as an error instead of being silently dropped.
package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"walcast/internal/sink"
)

func init() { sink.Register("rabbitmq", New) }

type Sink struct {
	cfg  Config
	log  *slog.Logger
	conn *amqp.Connection
	ch   *amqp.Channel

	confirms chan amqp.Confirmation
	returns  chan amqp.Return

	mu sync.Mutex
}

func New(ctx context.Context, sc sink.Config) (sink.Sink, error) {
	var cfg Config
	if err := sc.Decode(&cfg); err != nil {
		return nil, err
	}
	if err := cfg.withDefaults(sc.Prefix); err != nil {
		return nil, err
	}

	log := sc.Log
	if log == nil {
		log = slog.Default()
	}
	s := &Sink{cfg: cfg, log: log}
	if err := s.connect(ctx); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Sink) connect(ctx context.Context) error {
	conn, err := amqp.DialConfig(s.cfg.URL, amqp.Config{
		Properties: amqp.Table{"connection_name": "walcast"},
		Dial: func(network, addr string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 10 * time.Second}).DialContext(ctx, network, addr)
		},
	})
	if err != nil {
		return fmt.Errorf("rabbitmq: connect: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("rabbitmq: open channel: %w", err)
	}
	if boolOr(s.cfg.Exchange.Declare, true) {
		if err := declareExchange(ch, s.cfg.Exchange.Name, s.cfg.Exchange.Type, boolOr(s.cfg.Exchange.Durable, true)); err != nil {
			ch.Close()
			conn.Close()
			return fmt.Errorf("rabbitmq: declare exchange %q: %w", s.cfg.Exchange.Name, err)
		}
	}
	if s.cfg.DLQ != nil && boolOr(s.cfg.DLQ.Declare, true) {
		if err := declareExchange(ch, s.cfg.DLQ.Exchange, s.cfg.DLQ.ExchangeType, boolOr(s.cfg.DLQ.Durable, true)); err != nil {
			ch.Close()
			conn.Close()
			return fmt.Errorf("rabbitmq: declare DLQ exchange %q: %w", s.cfg.DLQ.Exchange, err)
		}
	}
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("rabbitmq: enable publisher confirms: %w", err)
	}

	s.conn = conn
	s.ch = ch
	s.confirms = ch.NotifyPublish(make(chan amqp.Confirmation, 1))
	s.returns = ch.NotifyReturn(make(chan amqp.Return, 1))
	return nil
}

func (s *Sink) Publish(ctx context.Context, m *sink.Message) error {
	err := s.publish(ctx, s.cfg.Exchange.Name, routingKey(m), publishing(m))
	if err == nil || sink.IsPermanent(err) {
		return err
	}
	if permanent, reason := classifyPublishError(err); permanent {
		return sink.Permanent(err, reason)
	}
	return err
}

func (s *Sink) publish(ctx context.Context, exchange, key string, pub amqp.Publishing) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.ch == nil || s.ch.IsClosed() || s.conn.IsClosed() {
		s.closeLocked()
		if err := s.connect(ctx); err != nil {
			return fmt.Errorf("rabbitmq: reconnect: %w", err)
		}
		s.log.Info("rabbitmq reconnected")
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.PublishTimeout)
	defer cancel()

	// mandatory makes an unroutable message come back instead of disappearing.
	if err := s.ch.PublishWithContext(ctx, exchange, key, true, false, pub); err != nil {
		s.closeLocked()
		return fmt.Errorf("rabbitmq publish to %s: %w", key, err)
	}

	select {
	case r := <-s.returns:
		// RabbitMQ sends the return before the ack, so drain the confirmation
		// to keep the next publish from reading a stale one.
		select {
		case c := <-s.confirms:
			if !c.Ack {
				s.closeLocked()
				return fmt.Errorf("rabbitmq: broker nacked message to %s", key)
			}
		case <-ctx.Done():
			s.closeLocked()
			return fmt.Errorf("rabbitmq publish to %s: %w", key, ctx.Err())
		}
		return sink.Permanent(fmt.Errorf("rabbitmq: message to %s is unroutable (%d %s), bind a queue to the exchange", key, r.ReplyCode, r.ReplyText), "unroutable")
	case c := <-s.confirms:
		if !c.Ack {
			s.closeLocked()
			return fmt.Errorf("rabbitmq: broker nacked message to %s", key)
		}
		return nil
	case <-ctx.Done():
		s.closeLocked()
		return fmt.Errorf("rabbitmq publish to %s: %w", key, ctx.Err())
	}
}

func classifyPublishError(err error) (permanent bool, reason string) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false, ""
	}
	if errors.Is(err, amqp.ErrClosed) {
		return false, ""
	}
	var ae *amqp.Error
	if !errors.As(err, &ae) {
		return false, ""
	}
	switch ae.Code {
	case amqp.AccessRefused:
		return true, "auth"
	case amqp.NotFound:
		return true, "not_found"
	case amqp.ResourceLocked:
		return true, "resource_locked"
	case amqp.PreconditionFailed:
		return true, "precondition_failed"
	case amqp.NotAllowed:
		return true, "not_allowed"
	case amqp.NotImplemented:
		return true, "not_implemented"
	}
	return false, ""
}

func declareExchange(ch *amqp.Channel, name, typ string, durable bool) error {
	return ch.ExchangeDeclare(name, typ, durable, false, false, false, nil)
}

func routingKey(m *sink.Message) string {
	return m.Topic + "." + m.Op
}

func publishing(m *sink.Message) amqp.Publishing {
	headers := amqp.Table{"op": m.Op, "key": string(m.Key)}
	if m.ID != "" {
		headers["id"] = m.ID
	}
	if m.LSN != "" {
		headers["lsn"] = m.LSN
	}
	return amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    m.ID,
		Type:         m.Op,
		Timestamp:    time.Now(),
		Headers:      headers,
		Body:         m.Value,
	}
}

func dlqPublishing(m *sink.Message, f sink.Failure) amqp.Publishing {
	pub := publishing(m)
	pub.Headers = amqp.Table{}
	for k, v := range sink.DLQHeaders(m, f) {
		pub.Headers[k] = v
	}
	return pub
}

func (s *Sink) DeadLetter(ctx context.Context, m *sink.Message, f sink.Failure) error {
	if s.cfg.DLQ == nil {
		return sink.ErrDeadLetterNotConfigured
	}
	if err := s.publish(ctx, s.cfg.DLQ.Exchange, s.cfg.DLQ.RoutingKey, dlqPublishing(m, f)); err != nil {
		return fmt.Errorf("rabbitmq dead letter to %s: %w", s.cfg.DLQ.Exchange, err)
	}
	return nil
}

func (s *Sink) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.closeLocked()
	return err
}

func (s *Sink) closeLocked() error {
	var err error
	if s.ch != nil {
		err = s.ch.Close()
	}
	if s.conn != nil {
		if e := s.conn.Close(); err == nil {
			err = e
		}
	}
	s.conn, s.ch, s.confirms, s.returns = nil, nil, nil, nil
	return err
}
