package config

import (
	"strings"
	"testing"
	"time"
)

const minimal = `
postgres:
  host: h
  username: u
  password: ${PW}
  database: d
  publication: {name: pub, createIfNotExists: true, operations: [INSERT], tables: [{name: t}]}
  slot: {name: s, createIfNotExists: true}
sinks:
  kafka: {enabled: true, brokers: [b:9092]}
  nats:  {enabled: false}
`

func TestParseDefaultsAndEnabledSinks(t *testing.T) {
	t.Setenv("PW", "p$ss")
	cfg, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Postgres.Password != "p$ss" {
		t.Errorf("password = %q", cfg.Postgres.Password)
	}
	if cfg.Routing.Prefix != "walcast" || cfg.Retry.InitialBackoff != 200*time.Millisecond || cfg.Retry.MaxBackoff != 30*time.Second {
		t.Errorf("defaults not applied: %+v %+v", cfg.Routing, cfg.Retry)
	}
	names, _ := cfg.EnabledSinks()
	if len(names) != 1 || names[0] != "kafka" {
		t.Errorf("enabled sinks = %v, want [kafka]", names)
	}
}

func TestParseRejectsNoEnabledSink(t *testing.T) {
	t.Setenv("PW", "x")
	_, err := Parse([]byte(strings.Replace(minimal, "enabled: true", "enabled: false", 1)))
	if err == nil || !strings.Contains(err.Error(), "no sink enabled") {
		t.Fatalf("err = %v", err)
	}
}

func TestParseLogFormat(t *testing.T) {
	t.Setenv("PW", "x")
	cfg, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Observability.LogFormat != "json" {
		t.Errorf("default log format = %q, want json", cfg.Observability.LogFormat)
	}

	withText := strings.Replace(minimal, "sinks:", "observability: {log_format: text}\nsinks:", 1)
	cfg, err = Parse([]byte(withText))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Observability.LogFormat != "text" {
		t.Errorf("log format = %q, want text", cfg.Observability.LogFormat)
	}

	bad := strings.Replace(minimal, "sinks:", "observability: {log_format: yaml}\nsinks:", 1)
	if _, err := Parse([]byte(bad)); err == nil || !strings.Contains(err.Error(), "log_format") {
		t.Fatalf("err = %v, want log_format error", err)
	}
}

func TestParseHealthPort(t *testing.T) {
	t.Setenv("PW", "x")
	cfg, err := Parse([]byte(minimal))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Observability.HealthPortValue(); got != DefaultHealthPort {
		t.Errorf("default health port = %d, want %d", got, DefaultHealthPort)
	}

	cfg, err = Parse([]byte(strings.Replace(minimal, "sinks:", "observability: {health_port: 8082}\nsinks:", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Observability.HealthPortValue(); got != 8082 {
		t.Errorf("health port = %d, want 8082", got)
	}

	cfg, err = Parse([]byte(strings.Replace(minimal, "sinks:", "observability: {health_port: 0}\nsinks:", 1)))
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Observability.HealthPortValue(); got != 0 {
		t.Errorf("disabled health port = %d, want 0", got)
	}

	bad := strings.Replace(minimal, "sinks:", "observability: {health_port: 70000}\nsinks:", 1)
	if _, err := Parse([]byte(bad)); err == nil || !strings.Contains(err.Error(), "health_port") {
		t.Fatalf("err = %v, want health_port error", err)
	}

	conflict := strings.Replace(minimal, "sinks:", "observability: {health_port: 8080}\nsinks:", 1)
	if _, err := Parse([]byte(conflict)); err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("err = %v, want port conflict error", err)
	}

	withMetric := strings.Replace(minimal, "  host: h", "  host: h\n  metric: {port: 8081}", 1)
	if _, err := Parse([]byte(withMetric)); err == nil || !strings.Contains(err.Error(), "must differ") {
		t.Fatalf("default health port should conflict with metric port: err = %v", err)
	}
}

func TestExpandEnv(t *testing.T) {
	t.Setenv("SET", "v")
	got, err := expandEnv("${SET} ${UNSET_X:-def} ${UNSET_Y:-} $plain")
	if err != nil || got != "v def  $plain" {
		t.Fatalf("got %q, %v", got, err)
	}
	if _, err := expandEnv("${DEFINITELY_UNSET_Z}"); err == nil {
		t.Fatal("expected error for unset variable without default")
	}
}
