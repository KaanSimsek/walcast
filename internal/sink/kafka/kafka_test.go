package kafka

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"

	"walcast/internal/sink"
	"walcast/internal/sink/sinktest"
)

func TestNewRejectsEmptyDLQ(t *testing.T) {
	_, err := New(context.Background(), sink.Config{Decode: sinktest.Decode(t, "brokers: [b:9092]\ndlq: {}")})
	if err == nil || !strings.Contains(err.Error(), "dlq.topic is required") {
		t.Fatalf("err = %v, want dlq.topic error", err)
	}
}

func TestRecordHeaders(t *testing.T) {
	m := &sink.Message{
		Topic: "walcast.public.users", Op: "insert", Key: []byte("1"),
		Value: []byte(`{}`), ID: "0/1/public.users/insert", LSN: "0/1",
	}
	rec := record(m)
	if rec.Topic != m.Topic || string(rec.Key) != "1" || string(rec.Value) != "{}" {
		t.Errorf("record = %+v", rec)
	}
	got := map[string]string{}
	for _, h := range rec.Headers {
		got[h.Key] = string(h.Value)
	}
	if got["op"] != "insert" || got["id"] != m.ID || got["lsn"] != "0/1" {
		t.Errorf("headers = %v", got)
	}
}

func TestOptionsValidation(t *testing.T) {
	cases := []struct {
		name, cfg, wantErr string
	}{
		{"ok", "brokers: [b:9092]\ncompression: zstd\nsasl: {mechanism: scram-sha-512, username: u, password: p}", ""},
		{"bad compression", "brokers: [b:9092]\ncompression: brotli", "unknown compression"},
		{"bad sasl", "brokers: [b:9092]\nsasl: {mechanism: kerberos}", "unknown sasl.mechanism"},
		{"no brokers", "compression: lz4", "brokers is required"},
	}
	for _, c := range cases {
		var cfg Config
		if err := sinktest.Decode(t, c.cfg)(&cfg); err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		var err error
		if len(cfg.Brokers) == 0 {
			_, err = New(context.Background(), sink.Config{Decode: sinktest.Decode(t, c.cfg)})
		} else {
			_, err = cfg.options()
		}
		switch {
		case c.wantErr == "" && err != nil:
			t.Errorf("%s: unexpected error %v", c.name, err)
		case c.wantErr != "" && (err == nil || !strings.Contains(err.Error(), c.wantErr)):
			t.Errorf("%s: err = %v, want containing %q", c.name, err, c.wantErr)
		}
	}
}

func TestClassifyProduceError(t *testing.T) {
	cases := []struct {
		name      string
		err       error
		permanent bool
		reason    string
	}{
		{"too large", kerr.MessageTooLarge, true, "too_large"},
		{"topic auth", kerr.TopicAuthorizationFailed, true, "topic_auth"},
		{"sasl auth", kerr.SaslAuthenticationFailed, true, "sasl_auth"},
		{"request timeout", kerr.RequestTimedOut, false, ""},
		{"unknown", fmt.Errorf("boom"), false, ""},
	}
	for _, c := range cases {
		permanent, reason := classifyProduceError(fmt.Errorf("produce: %w", c.err))
		if permanent != c.permanent || reason != c.reason {
			t.Errorf("%s: got %v/%q, want %v/%q", c.name, permanent, reason, c.permanent, c.reason)
		}
	}
}

func TestDeadLetterRecord(t *testing.T) {
	m := &sink.Message{Topic: "walcast.public.users", Op: "insert", Key: []byte("1"), Value: []byte(`{}`), ID: "id-1", LSN: "0/1"}
	rec := deadLetterRecord("walcast.dlq", m, sink.Failure{Reason: "too_large", Attempts: 2, Permanent: true})
	if rec.Topic != "walcast.dlq" || string(rec.Key) != "1" || string(rec.Value) != "{}" {
		t.Errorf("record = %+v", rec)
	}
	got := map[string]string{}
	for _, h := range rec.Headers {
		got[h.Key] = string(h.Value)
	}
	if got["walcast-dlq-source"] != m.Topic || got["walcast-dlq-reason"] != "too_large" || got["walcast-dlq-attempts"] != "2" {
		t.Errorf("headers = %v", got)
	}
}
