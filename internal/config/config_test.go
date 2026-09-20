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
