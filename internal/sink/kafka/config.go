package kafka

import (
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Config struct {
	Brokers []string `yaml:"brokers"`
	// AutoCreateTopics lets the producer request topic creation; the broker
	// must also allow it (auto.create.topics.enable). Default false.
	AutoCreateTopics bool   `yaml:"auto_create_topics"`
	Compression      string `yaml:"compression"` // none|gzip|snappy|lz4|zstd (default lz4)
	ClientID         string `yaml:"client_id"`
	TLS              bool   `yaml:"tls"`
	SASL             struct {
		Mechanism string `yaml:"mechanism"` // plain|scram-sha-256|scram-sha-512
		Username  string `yaml:"username"`
		Password  string `yaml:"password"`
	} `yaml:"sasl"`
}

func (c Config) options() ([]kgo.Opt, error) {
	clientID := c.ClientID
	if clientID == "" {
		clientID = "walcast"
	}
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.Brokers...),
		kgo.ClientID(clientID),
		// The WAL is acked right after Publish returns, so require the full ISR.
		// Idempotent production (franz-go default) prevents retry duplicates.
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}

	var codec kgo.CompressionCodec
	switch strings.ToLower(c.Compression) {
	case "", "lz4":
		codec = kgo.Lz4Compression()
	case "none":
		codec = kgo.NoCompression()
	case "gzip":
		codec = kgo.GzipCompression()
	case "snappy":
		codec = kgo.SnappyCompression()
	case "zstd":
		codec = kgo.ZstdCompression()
	default:
		return nil, fmt.Errorf("kafka: unknown compression %q", c.Compression)
	}
	opts = append(opts, kgo.ProducerBatchCompression(codec))

	if c.AutoCreateTopics {
		opts = append(opts, kgo.AllowAutoTopicCreation())
	}
	if c.TLS {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{MinVersion: tls.VersionTLS12}))
	}
	switch strings.ToLower(c.SASL.Mechanism) {
	case "":
	case "plain":
		opts = append(opts, kgo.SASL(plain.Auth{User: c.SASL.Username, Pass: c.SASL.Password}.AsMechanism()))
	case "scram-sha-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.SASL.Username, Pass: c.SASL.Password}.AsSha256Mechanism()))
	case "scram-sha-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: c.SASL.Username, Pass: c.SASL.Password}.AsSha512Mechanism()))
	default:
		return nil, fmt.Errorf("kafka: unknown sasl.mechanism %q", c.SASL.Mechanism)
	}
	return opts, nil
}
