// Package config loads walcast's YAML configuration.
package config

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strings"
	"time"

	cdcconfig "github.com/Trendyol/go-pq-cdc/config"
	"gopkg.in/yaml.v3"
)

type Config struct {
	// Postgres is go-pq-cdc's config verbatim (connection, publication, slot,
	// snapshot, heartbeat, metrics), so every upstream option works unchanged.
	Postgres      cdcconfig.Config `yaml:"postgres"`
	Routing       Routing          `yaml:"routing"`
	Retry         Retry            `yaml:"retry"`
	Observability Observability    `yaml:"observability"`
	// Sinks maps a sink name (kafka, nats, ...) to its raw section. Each
	// backend decodes its own section, so core config never changes when a
	// backend is added.
	Sinks map[string]yaml.Node `yaml:"sinks"`
}

type Routing struct {
	// Prefix starts every topic/subject: "<prefix>.<schema>.<table>".
	Prefix string `yaml:"prefix"`
	// KeyColumns maps "schema.table" to the columns forming the message key.
	// Tables not listed are keyed by "schema.table", which preserves ordering
	// but sends the whole table to a single Kafka partition.
	KeyColumns map[string][]string `yaml:"key_columns"`
}

// Retry controls backoff when a sink rejects a message. Publishing is retried
// forever (the WAL cannot advance past an unpublished change).
type Retry struct {
	InitialBackoff time.Duration `yaml:"initial_backoff"`
	MaxBackoff     time.Duration `yaml:"max_backoff"`
}

type Observability struct {
	// LogFormat is json (default) or text and applies to walcast and go-pq-cdc.
	LogFormat string `yaml:"log_format"`
}

// EnabledSinks returns the names of sinks with `enabled: true`, sorted by name
// so startup order is deterministic.
func (c *Config) EnabledSinks() ([]string, error) {
	var names []string
	for name, node := range c.Sinks {
		var head struct {
			Enabled bool `yaml:"enabled"`
		}
		if err := node.Decode(&head); err != nil {
			return nil, fmt.Errorf("sinks.%s: %w", name, err)
		}
		if head.Enabled {
			names = append(names, name)
		}
	}
	slices.Sort(names)
	return names, nil
}

func Load(path string) (*Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return Parse(raw)
}

func Parse(raw []byte) (*Config, error) {
	expanded, err := expandEnv(string(raw))
	if err != nil {
		return nil, err
	}

	var cfg Config
	dec := yaml.NewDecoder(strings.NewReader(expanded))
	dec.KnownFields(false) // postgres.* is owned by go-pq-cdc; don't reject its future fields
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	if cfg.Routing.Prefix == "" {
		cfg.Routing.Prefix = "walcast"
	}
	if cfg.Retry.InitialBackoff <= 0 {
		cfg.Retry.InitialBackoff = 200 * time.Millisecond
	}
	if cfg.Retry.MaxBackoff <= 0 {
		cfg.Retry.MaxBackoff = 30 * time.Second
	}
	if cfg.Retry.MaxBackoff < cfg.Retry.InitialBackoff {
		return nil, errors.New("retry.max_backoff must be >= retry.initial_backoff")
	}
	switch cfg.Observability.LogFormat = strings.ToLower(cfg.Observability.LogFormat); cfg.Observability.LogFormat {
	case "":
		cfg.Observability.LogFormat = "json"
	case "json", "text":
	default:
		return nil, fmt.Errorf("observability.log_format must be json or text, got %q", cfg.Observability.LogFormat)
	}

	names, err := cfg.EnabledSinks()
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, errors.New("no sink enabled: set `enabled: true` on at least one entry under `sinks`")
	}
	return &cfg, nil
}

var envRef = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)(:-([^}]*))?\}`)

// expandEnv substitutes ${VAR} and ${VAR:-default}. A bare $ is left alone so
// passwords containing $ survive. An unset variable with no default is an error.
func expandEnv(s string) (string, error) {
	var missing []string
	out := envRef.ReplaceAllStringFunc(s, func(m string) string {
		sub := envRef.FindStringSubmatch(m)
		if v, ok := os.LookupEnv(sub[1]); ok {
			return v
		}
		if sub[2] != "" { // ":-default" present (default may be empty after the dash)
			return sub[3]
		}
		missing = append(missing, sub[1])
		return m
	})
	if len(missing) > 0 {
		return "", fmt.Errorf("config references unset environment variables: %v", missing)
	}
	return out, nil
}
