package nats

import (
	"errors"
	"fmt"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

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

func (c *Config) withDefaults() error {
	if c.URL == "" {
		c.URL = natsgo.DefaultURL
	}
	if c.Stream.Name == "" {
		return errors.New("nats: stream.name is required")
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 10 * time.Second
	}
	switch c.Stream.Storage {
	case "", "file":
		c.Stream.Storage = "file"
	case "memory":
	default:
		return fmt.Errorf("nats: unknown stream.storage %q", c.Stream.Storage)
	}
	if c.Stream.Replicas < 1 {
		c.Stream.Replicas = 1
	}
	return nil
}

func (c Config) storage() jetstream.StorageType {
	if c.Stream.Storage == "memory" {
		return jetstream.MemoryStorage
	}
	return jetstream.FileStorage
}

func (c Config) streamConfig(prefix string) jetstream.StreamConfig {
	return jetstream.StreamConfig{
		Name:     c.Stream.Name,
		Subjects: []string{prefix + ".>"},
		Storage:  c.storage(),
		Replicas: c.Stream.Replicas,
	}
}
