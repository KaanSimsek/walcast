package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	cdc "github.com/Trendyol/go-pq-cdc"

	"walcast/internal/config"
	"walcast/internal/pipeline"
	"walcast/internal/sink"

	_ "walcast/internal/sink/kafka"
	_ "walcast/internal/sink/nats"
	_ "walcast/internal/sink/rabbitmq"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to the YAML config file")
	flag.Parse()

	if err := run(*configPath); err != nil {
		slog.Error("walcast stopped", "error", err)
		os.Exit(1)
	}
}

func run(configPath string) error {
	cfg, err := config.Load(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.Postgres.Logger.LogLevel}))

	sigCtx, stopSignals := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopSignals()
	ctx, fail := context.WithCancelCause(sigCtx)
	defer fail(nil)

	pl := pipeline.New(ctx, fail, pipeline.NewRouter(cfg.Routing), cfg.Retry, log)
	defer pl.Close()

	names, err := cfg.EnabledSinks()
	if err != nil {
		return err
	}
	for _, name := range names {
		node := cfg.Sinks[name]
		s, err := sink.New(ctx, name, sink.Config{Decode: node.Decode, Log: log.With("sink", name), Prefix: cfg.Routing.Prefix})
		if err != nil {
			return fmt.Errorf("sink %s: %w", name, err)
		}
		pl.AddSink(name, s)
		log.Info("sink enabled", "sink", name)
	}

	connector, err := cdc.NewConnector(ctx, cfg.Postgres, pl.Handle)
	if err != nil {
		return fmt.Errorf("create connector: %w", err)
	}

	// Start blocks until the connector is closed (it doesn't watch ctx itself),
	// and it also returns on its own if setup fails, so wait on both.
	started := make(chan struct{})
	go func() {
		defer close(started)
		connector.Start(ctx)
	}()

	select {
	case <-ctx.Done():
	case <-started:
		if ctx.Err() == nil {
			connector.Close()
			return errors.New("connector exited unexpectedly (see the go-pq-cdc log above)")
		}
	}

	log.Info("shutting down")
	connector.Close() // unblocks Start
	select {
	case <-started:
	case <-time.After(35 * time.Second):
		log.Warn("connector did not stop in time")
	}

	// A signal is a clean stop; anything else (fail(err)) is an error exit.
	// The signal context's own cause is compared rather than context.Canceled
	// because signal.NotifyContext records a signal-specific cause.
	cause := context.Cause(ctx)
	if sig := context.Cause(sigCtx); sig != nil && errors.Is(cause, sig) {
		return nil
	}
	return cause
}
