// Package sink defines the contract every message broker backend implements,
// plus a registry so backends can be selected by name from config.
//
// To add a backend (redis, ...): create a sub-package, implement Sink,
// call Register from its init(), and blank-import it in cmd/walcast.
package sink

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sort"
)

// Message is one encoded event, routed and ready to hand to a broker.
type Message struct {
	// Topic is the logical destination: "<prefix>.<schema>.<table>".
	// Kafka uses it as the topic name; NATS and RabbitMQ append the op to form
	// the subject/routing key.
	Topic string
	Op    string
	// Key drives partitioning/ordering: all changes to one row share a key.
	Key []byte
	// Value is the JSON-encoded event.
	Value []byte
	// ID is a stable per-change identifier for broker-side dedup; may be empty.
	ID string
}

// Sink publishes messages to one broker.
type Sink interface {
	// Publish must not return nil until the broker has durably accepted the
	// message: the WAL position is acknowledged right after, so anything
	// weaker loses data on a crash. Messages arrive one at a time, in WAL order.
	Publish(ctx context.Context, m *Message) error
	Close() error
}

// Config is what a Factory receives.
type Config struct {
	// Decode unmarshals this sink's section of the YAML config into v.
	Decode func(v any) error
	Log    *slog.Logger
	// Prefix is routing.prefix, for sinks that must pre-declare subjects.
	Prefix string
}

type Factory func(ctx context.Context, cfg Config) (Sink, error)

var factories = map[string]Factory{}

// Register is meant to be called from a backend package's init().
func Register(name string, f Factory) {
	if _, dup := factories[name]; dup {
		panic("sink: duplicate registration of " + name)
	}
	factories[name] = f
}

func New(ctx context.Context, name string, cfg Config) (Sink, error) {
	f, ok := factories[name]
	if !ok {
		return nil, fmt.Errorf("unknown sink %q (available: %v)", name, Names())
	}
	return f(ctx, cfg)
}

func Names() []string {
	names := make([]string, 0, len(factories))
	for n := range factories {
		names = append(names, n)
	}
	sort.Strings(names)
	return slices.Clip(names)
}
