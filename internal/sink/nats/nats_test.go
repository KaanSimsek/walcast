package nats

import (
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"walcast/internal/sink"
	"walcast/internal/sink/sinktest"
)

func TestConfigDefaults(t *testing.T) {
	var cfg Config
	if err := sinktest.Decode(t, "stream: {name: WALCAST}")(&cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.withDefaults(); err != nil {
		t.Fatal(err)
	}
	if cfg.URL != natsgo.DefaultURL {
		t.Errorf("url = %q, want %q", cfg.URL, natsgo.DefaultURL)
	}
	if cfg.PublishTimeout != 10*time.Second {
		t.Errorf("publish timeout = %v, want 10s", cfg.PublishTimeout)
	}
	if cfg.Stream.Storage != "file" || cfg.Stream.Replicas != 1 {
		t.Errorf("stream defaults = %q/%d, want file/1", cfg.Stream.Storage, cfg.Stream.Replicas)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name, cfg, wantErr string
	}{
		{"missing stream name", "url: nats://localhost:4222", "stream.name is required"},
		{"bad storage", "stream: {name: WALCAST, storage: disk}", "unknown stream.storage"},
		{"dlq without subject", "stream: {name: WALCAST}\ndlq: {}", "dlq.subject is required"},
	}
	for _, c := range cases {
		var cfg Config
		if err := sinktest.Decode(t, c.cfg)(&cfg); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		err := cfg.withDefaults()
		if err == nil || !strings.Contains(err.Error(), c.wantErr) {
			t.Errorf("%s: err = %v, want containing %q", c.name, err, c.wantErr)
		}
	}
}

func TestStreamConfig(t *testing.T) {
	var cfg Config
	if err := sinktest.Decode(t, "stream: {name: WALCAST, storage: memory, replicas: 3}")(&cfg); err != nil {
		t.Fatal(err)
	}
	if err := cfg.withDefaults(); err != nil {
		t.Fatal(err)
	}
	sc := cfg.streamConfig("walcast")
	if sc.Name != "WALCAST" || len(sc.Subjects) != 1 || sc.Subjects[0] != "walcast.>" {
		t.Errorf("stream config = %+v", sc)
	}
	if sc.Storage != jetstream.MemoryStorage || sc.Replicas != 3 {
		t.Errorf("storage/replicas = %v/%d", sc.Storage, sc.Replicas)
	}
}

func TestSubject(t *testing.T) {
	m := &sink.Message{Topic: "walcast.public.users", Op: "insert"}
	if got := subject(m); got != "walcast.public.users.insert" {
		t.Errorf("subject = %q", got)
	}
}

func TestMessageHeaders(t *testing.T) {
	m := &sink.Message{
		Topic: "walcast.public.users", Op: "update",
		ID: "0/1/public.users/update", LSN: "0/1", Value: []byte(`{}`),
	}
	msg := message(m)
	if msg.Subject != "walcast.public.users.update" {
		t.Errorf("subject = %q", msg.Subject)
	}
	if msg.Header.Get("op") != "update" || msg.Header.Get("id") != m.ID || msg.Header.Get("lsn") != "0/1" {
		t.Errorf("headers = %v", msg.Header)
	}
}

func TestClassifyPublishError(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		permanent bool
		reason    string
	}{
		{"max payload", natsgo.ErrMaxPayload, true, "max_payload"},
		{"auth", natsgo.ErrAuthorization, true, "auth"},
		{"bad subject", natsgo.ErrBadSubject, true, "invalid_subject"},
		{"stream missing", jetstream.ErrStreamNotFound, true, "stream_not_found"},
		{"api forbidden", &jetstream.APIError{Code: 403, Description: "forbidden"}, true, "auth"},
		{"no responders", natsgo.ErrNoResponders, false, ""},
		{"unknown", errors.New("boom"), false, ""},
	}
	for _, c := range cases {
		permanent, reason := classifyPublishError(fmt.Errorf("publish: %w", c.err))
		if permanent != c.permanent || reason != c.reason {
			t.Errorf("%s: got %v/%q, want %v/%q", c.name, permanent, reason, c.permanent, c.reason)
		}
	}
}

func TestDeadLetterMessage(t *testing.T) {
	m := &sink.Message{Topic: "walcast.public.users", Op: "update", Key: []byte("7"), Value: []byte(`{}`), ID: "id-1", LSN: "0/1"}
	msg := deadLetterMessage("walcast.dlq", m, sink.Failure{Reason: "stream_not_found", Attempts: 4})
	if msg.Subject != "walcast.dlq" || string(msg.Data) != "{}" {
		t.Errorf("message = %+v", msg)
	}
	if msg.Header.Get("walcast-dlq-source") != m.Topic || msg.Header.Get("walcast-dlq-reason") != "stream_not_found" || msg.Header.Get("walcast-dlq-attempts") != "4" {
		t.Errorf("headers = %v", msg.Header)
	}
}
