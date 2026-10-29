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
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"walcast/internal/sink"
)

func init() { sink.Register("rabbitmq", New) }

type Config struct {
	URL      string `yaml:"url"`
	Exchange struct {
		// Name defaults to routing.prefix.
		Name string `yaml:"name"`
		// Type is direct|fanout|topic|headers (default topic). A topic exchange
		// lets consumers bind e.g. "<prefix>.public.users.*".
		Type string `yaml:"type"`
		// Durable defaults to true.
		Durable *bool `yaml:"durable"`
		// Declare creates the exchange if it is missing (default true). Declaring
		// an existing exchange with different settings is an error; disable this
		// when the exchange is managed elsewhere.
		Declare *bool `yaml:"declare"`
	} `yaml:"exchange"`
	// PublishTimeout bounds one publish+confirm round trip (default 10s).
	PublishTimeout time.Duration `yaml:"publish_timeout"`
}

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

func (c *Config) withDefaults(prefix string) error {
	if c.URL == "" {
		return errors.New("rabbitmq: url is required")
	}
	if c.Exchange.Name == "" {
		c.Exchange.Name = prefix
	}
	switch c.Exchange.Type = strings.ToLower(c.Exchange.Type); c.Exchange.Type {
	case "":
		c.Exchange.Type = "topic"
	case "direct", "fanout", "topic", "headers":
	default:
		return fmt.Errorf("rabbitmq: unknown exchange.type %q", c.Exchange.Type)
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 10 * time.Second
	}
	return nil
}

func boolOr(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
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
		err := ch.ExchangeDeclare(
			s.cfg.Exchange.Name,
			s.cfg.Exchange.Type,
			boolOr(s.cfg.Exchange.Durable, true),
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			ch.Close()
			conn.Close()
			return fmt.Errorf("rabbitmq: declare exchange %q: %w", s.cfg.Exchange.Name, err)
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

	key := routingKey(m)
	// mandatory makes an unroutable message come back instead of disappearing.
	if err := s.ch.PublishWithContext(ctx, s.cfg.Exchange.Name, key, true, false, publishing(m)); err != nil {
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
		return fmt.Errorf("rabbitmq: message to %s is unroutable (%d %s), bind a queue to the exchange", key, r.ReplyCode, r.ReplyText)
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

func routingKey(m *sink.Message) string {
	return m.Topic + "." + m.Op
}

func publishing(m *sink.Message) amqp.Publishing {
	return amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    m.ID,
		Type:         m.Op,
		Timestamp:    time.Now(),
		Headers:      amqp.Table{"op": m.Op, "key": string(m.Key)},
		Body:         m.Value,
	}
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
