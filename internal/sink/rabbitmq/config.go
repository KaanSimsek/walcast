package rabbitmq

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

type Config struct {
	URL      string `yaml:"url"`
	Exchange struct {
		// Name defaults to routing.prefix.
		Name string `yaml:"name"`
		// Type is direct|fanout|topic|headers (default topic). A topic exchange
		// lets consumers bind e.g. "<prefix>.public.users.*".
		Type string `yaml:"type"`
		// Durable defaults to true.
		Durable *bool `yaml:"durable"`
		// Declare creates the exchange if it is missing (default true). Declaring
		// an existing exchange with different settings is an error; disable this
		// when the exchange is managed elsewhere.
		Declare *bool `yaml:"declare"`
	} `yaml:"exchange"`
	// PublishTimeout bounds one publish+confirm round trip (default 10s).
	PublishTimeout time.Duration `yaml:"publish_timeout"`
}

func (c *Config) withDefaults(prefix string) error {
	if c.URL == "" {
		return errors.New("rabbitmq: url is required")
	}
	if c.Exchange.Name == "" {
		c.Exchange.Name = prefix
	}
	switch c.Exchange.Type = strings.ToLower(c.Exchange.Type); c.Exchange.Type {
	case "":
		c.Exchange.Type = "topic"
	case "direct", "fanout", "topic", "headers":
	default:
		return fmt.Errorf("rabbitmq: unknown exchange.type %q", c.Exchange.Type)
	}
	if c.PublishTimeout <= 0 {
		c.PublishTimeout = 10 * time.Second
	}
	return nil
}

func boolOr(p *bool, def bool) bool {
	if p == nil {
		return def
	}
	return *p
}
