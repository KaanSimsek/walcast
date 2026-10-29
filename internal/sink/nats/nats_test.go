package nats

import (
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
