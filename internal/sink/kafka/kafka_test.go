package kafka

import (
	"context"
	"strings"
	"testing"

	"walcast/internal/sink"
	"walcast/internal/sink/sinktest"
)

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
