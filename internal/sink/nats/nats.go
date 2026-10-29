// Package nats is the NATS JetStream backend.
//
// Subjects are "<topic>.<op>", e.g. walcast.public.users.insert, so consumers
// can filter by table and/or operation with wildcards.
package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"walcast/internal/redact"
	"walcast/internal/sink"
)

func init() { sink.Register("nats", New) }

type Sink struct {
	nc      *natsgo.Conn
	js      jetstream.JetStream
	timeout time.Duration
}

func New(ctx context.Context, sc sink.Config) (sink.Sink, error) {
	var cfg Config
	if err := sc.Decode(&cfg); err != nil {
		return nil, err
	}
	if err := cfg.withDefaults(); err != nil {
		return nil, err
	}

	opts := []natsgo.Option{
		natsgo.Name("walcast"),
		natsgo.MaxReconnects(-1), // keep trying forever once connected
		natsgo.DisconnectErrHandler(func(_ *natsgo.Conn, err error) {
			sc.Log.Warn("nats disconnected", "error", err)
		}),
		natsgo.ReconnectHandler(func(c *natsgo.Conn) {
			sc.Log.Info("nats reconnected", "url", redact.URL(c.ConnectedUrl()))
		}),
	}
	switch {
	case cfg.CredsFile != "":
		opts = append(opts, natsgo.UserCredentials(cfg.CredsFile))
	case cfg.Token != "":
		opts = append(opts, natsgo.Token(cfg.Token))
	case cfg.User != "":
		opts = append(opts, natsgo.UserInfo(cfg.User, cfg.Password))
	}

	nc, err := natsgo.Connect(cfg.URL, opts...)
	if err != nil {
		return nil, fmt.Errorf("nats: connect %s: %w", redact.URL(cfg.URL), err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, fmt.Errorf("nats: jetstream: %w", err)
	}
	if err := ensureStream(ctx, js, cfg, sc.Prefix); err != nil {
		nc.Close()
		return nil, err
	}
	return &Sink{nc: nc, js: js, timeout: cfg.PublishTimeout}, nil
}

func ensureStream(ctx context.Context, js jetstream.JetStream, cfg Config, prefix string) error {
	name := cfg.Stream.Name
	_, err := js.Stream(ctx, name)
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("nats: look up stream %q: %w", name, err)
	}
	if !cfg.Stream.Create {
		return fmt.Errorf("nats: stream %q does not exist (set sinks.nats.stream.create: true, or create it for subjects %q)", name, prefix+".>")
	}
	if _, err := js.CreateStream(ctx, cfg.streamConfig(prefix)); err != nil {
		return fmt.Errorf("nats: create stream %q: %w", name, err)
	}
	return nil
}

func subject(m *sink.Message) string {
	return m.Topic + "." + m.Op
}

func message(m *sink.Message) *natsgo.Msg {
	msg := &natsgo.Msg{Subject: subject(m), Data: m.Value, Header: natsgo.Header{}}
	msg.Header.Set("op", m.Op)
	if m.ID != "" {
		msg.Header.Set("id", m.ID)
	}
	if m.LSN != "" {
		msg.Header.Set("lsn", m.LSN)
	}
	return msg
}

func (s *Sink) Publish(ctx context.Context, m *sink.Message) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var opts []jetstream.PublishOpt
	if m.ID != "" {
		opts = append(opts, jetstream.WithMsgID(m.ID)) // JetStream drops redelivered duplicates in its dedupe window
	}
	msg := message(m)
	if _, err := s.js.PublishMsg(ctx, msg, opts...); err != nil {
		return fmt.Errorf("nats publish to %s: %w", msg.Subject, err)
	}
	return nil
}

func (s *Sink) Close() error {
	// Drain flushes anything buffered before closing.
	return s.nc.Drain()
}
