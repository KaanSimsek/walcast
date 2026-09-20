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

	"walcast/internal/sink"
)

func init() { sink.Register("nats", New) }

type Config struct {
	URL       string `yaml:"url"`
	CredsFile string `yaml:"creds_file"`
	User      string `yaml:"user"`
	Password  string `yaml:"password"`
	Token     string `yaml:"token"`
	// PublishTimeout bounds one publish+ack round trip (default 10s).
	PublishTimeout time.Duration `yaml:"publish_timeout"`
	Stream         struct {
		Name string `yaml:"name"`
		// Create makes the stream (subjects "<prefix>.>") if it is missing. An
		// existing stream is never modified. Without it the stream must already
		// exist and cover the subjects, otherwise publishes fail.
		Create   bool   `yaml:"create"`
		Storage  string `yaml:"storage"` // file (default) | memory
		Replicas int    `yaml:"replicas"`
	} `yaml:"stream"`
}

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
	if cfg.URL == "" {
		cfg.URL = natsgo.DefaultURL
	}
	if cfg.Stream.Name == "" {
		return nil, errors.New("nats: stream.name is required")
	}
	if cfg.PublishTimeout <= 0 {
		cfg.PublishTimeout = 10 * time.Second
	}

	opts := []natsgo.Option{
		natsgo.Name("walcast"),
		natsgo.MaxReconnects(-1), // keep trying forever once connected
		natsgo.DisconnectErrHandler(func(_ *natsgo.Conn, err error) {
			sc.Log.Warn("nats disconnected", "error", err)
		}),
		natsgo.ReconnectHandler(func(c *natsgo.Conn) {
			sc.Log.Info("nats reconnected", "url", c.ConnectedUrl())
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
		return nil, fmt.Errorf("nats: connect %s: %w", cfg.URL, err)
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

	storage := jetstream.FileStorage
	switch cfg.Stream.Storage {
	case "", "file":
	case "memory":
		storage = jetstream.MemoryStorage
	default:
		return fmt.Errorf("nats: unknown stream.storage %q", cfg.Stream.Storage)
	}
	if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     name,
		Subjects: []string{prefix + ".>"},
		Storage:  storage,
		Replicas: max(cfg.Stream.Replicas, 1),
	}); err != nil {
		return fmt.Errorf("nats: create stream %q: %w", name, err)
	}
	return nil
}

func (s *Sink) Publish(ctx context.Context, m *sink.Message) error {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	var opts []jetstream.PublishOpt
	if m.ID != "" {
		opts = append(opts, jetstream.WithMsgID(m.ID)) // JetStream drops redelivered duplicates in its dedupe window
	}
	subject := m.Topic + "." + m.Op
	if _, err := s.js.Publish(ctx, subject, m.Value, opts...); err != nil {
		return fmt.Errorf("nats publish to %s: %w", subject, err)
	}
	return nil
}

func (s *Sink) Close() error {
	// Drain flushes anything buffered before closing.
	return s.nc.Drain()
}
