package rabbitmq

import (
	"strings"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"walcast/internal/sink"
)

func TestConfigDefaults(t *testing.T) {
	cfg := Config{URL: "amqp://guest:guest@localhost:5672/"}
	if err := cfg.withDefaults("walcast"); err != nil {
		t.Fatal(err)
	}
	if cfg.Exchange.Name != "walcast" {
		t.Errorf("exchange name = %q, want walcast", cfg.Exchange.Name)
	}
	if cfg.Exchange.Type != "topic" {
		t.Errorf("exchange type = %q, want topic", cfg.Exchange.Type)
	}
	if !boolOr(cfg.Exchange.Durable, true) {
		t.Error("exchange should default to durable")
	}
	if !boolOr(cfg.Exchange.Declare, true) {
		t.Error("exchange should default to declared")
	}
	if cfg.PublishTimeout != 10*time.Second {
		t.Errorf("publish timeout = %v, want 10s", cfg.PublishTimeout)
	}
}

func TestConfigValidation(t *testing.T) {
	cases := []struct {
		name, url, typ, wantErr string
	}{
		{"ok", "amqp://localhost", "fanout", ""},
		{"missing url", "", "topic", "url is required"},
		{"bad exchange type", "amqp://localhost", "random", "unknown exchange.type"},
	}
	for _, c := range cases {
		cfg := Config{URL: c.url}
		cfg.Exchange.Type = c.typ
		err := cfg.withDefaults("walcast")
		switch {
		case c.wantErr == "" && err != nil:
			t.Errorf("%s: unexpected error %v", c.name, err)
		case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
			t.Errorf("%s: err = %v, want containing %q", c.name, err, c.wantErr)
		}
	}
}

func TestPublishing(t *testing.T) {
	m := &sink.Message{
		Topic: "walcast.public.users",
		Op:    "update",
		Key:   []byte("42"),
		Value: []byte(`{"op":"update"}`),
		ID:    "0/1/public.users/update",
	}
	if got := routingKey(m); got != "walcast.public.users.update" {
		t.Errorf("routing key = %q", got)
	}
	pub := publishing(m)
	if pub.MessageId != m.ID {
		t.Errorf("message id = %q", pub.MessageId)
	}
	if pub.Type != "update" {
		t.Errorf("type = %q", pub.Type)
	}
	if pub.DeliveryMode != amqp.Persistent {
		t.Errorf("delivery mode = %d, want persistent", pub.DeliveryMode)
	}
	if pub.ContentType != "application/json" {
		t.Errorf("content type = %q", pub.ContentType)
	}
	if pub.Headers["op"] != "update" || pub.Headers["key"] != "42" {
		t.Errorf("headers = %v", pub.Headers)
	}
	if string(pub.Body) != string(m.Value) {
		t.Errorf("body = %q", pub.Body)
	}
}

func TestExplicitExchangeSettings(t *testing.T) {
	durable, declare := false, false
	cfg := Config{URL: "amqp://localhost"}
	cfg.Exchange.Name = "events"
	cfg.Exchange.Durable = &durable
	cfg.Exchange.Declare = &declare

	if err := cfg.withDefaults("walcast"); err != nil {
		t.Fatal(err)
	}
	if boolOr(cfg.Exchange.Durable, true) {
		t.Error("durable should be false")
	}
	if boolOr(cfg.Exchange.Declare, true) {
		t.Error("declare should be false")
	}
}
