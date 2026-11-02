package rabbitmq

import (
	"context"
	"errors"
	"fmt"
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
		LSN:   "0/1",
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
	if pub.Headers["op"] != "update" || pub.Headers["key"] != "42" || pub.Headers["id"] != m.ID || pub.Headers["lsn"] != "0/1" {
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

func TestDLQDefaultsAndValidation(t *testing.T) {
	cfg := Config{URL: "amqp://localhost"}
	cfg.DLQ = &DLQConfig{Exchange: "walcast.dlq"}
	if err := cfg.withDefaults("walcast"); err != nil {
		t.Fatal(err)
	}
	if cfg.DLQ.RoutingKey != "failed" || cfg.DLQ.ExchangeType != "topic" {
		t.Errorf("DLQ defaults = %+v", cfg.DLQ)
	}
	if !boolOr(cfg.DLQ.Durable, true) || !boolOr(cfg.DLQ.Declare, true) {
		t.Error("DLQ should default to durable and declared")
	}

	bad := Config{URL: "amqp://localhost", DLQ: &DLQConfig{}}
	if err := bad.withDefaults("walcast"); err == nil || !strings.Contains(err.Error(), "dlq.exchange is required") {
		t.Errorf("err = %v, want dlq.exchange error", err)
	}

	badType := Config{URL: "amqp://localhost", DLQ: &DLQConfig{Exchange: "x", ExchangeType: "random"}}
	if err := badType.withDefaults("walcast"); err == nil || !strings.Contains(err.Error(), "dlq.exchange_type") {
		t.Errorf("err = %v, want dlq.exchange_type error", err)
	}
}

func TestClassifyPublishError(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		permanent bool
		reason    string
	}{
		{"access refused", &amqp.Error{Code: amqp.AccessRefused}, true, "auth"},
		{"not found", &amqp.Error{Code: amqp.NotFound}, true, "not_found"},
		{"precondition", &amqp.Error{Code: amqp.PreconditionFailed}, true, "precondition_failed"},
		{"channel closed", amqp.ErrClosed, false, ""},
		{"timeout", context.DeadlineExceeded, false, ""},
		{"unknown", errors.New("boom"), false, ""},
	}
	for _, c := range cases {
		permanent, reason := classifyPublishError(fmt.Errorf("publish: %w", c.err))
		if permanent != c.permanent || reason != c.reason {
			t.Errorf("%s: got %v/%q, want %v/%q", c.name, permanent, reason, c.permanent, c.reason)
		}
	}
}

func TestDLQPublishing(t *testing.T) {
	m := &sink.Message{Topic: "walcast.public.users", Op: "insert", Key: []byte("1"), Value: []byte(`{}`), ID: "id-1", LSN: "0/1"}
	pub := dlqPublishing(m, sink.Failure{Reason: "unroutable", Attempts: 3, Permanent: true})
	if pub.Headers["walcast-dlq-source"] != m.Topic || pub.Headers["walcast-dlq-reason"] != "unroutable" || pub.Headers["walcast-dlq-attempts"] != "3" {
		t.Errorf("headers = %v", pub.Headers)
	}
	if pub.DeliveryMode != amqp.Persistent || string(pub.Body) != string(m.Value) {
		t.Errorf("publishing = %+v", pub)
	}
}
