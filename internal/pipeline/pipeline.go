// Package pipeline glues go-pq-cdc to the sinks: it is the replication listener.
//
// Delivery is at-least-once. The WAL position is acknowledged only after every
// enabled sink has accepted the change, so a crash replays from the last ack
// and consumers may see duplicates (use Event.ID to dedupe).
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/replication"

	"walcast/internal/config"
	"walcast/internal/event"
	"walcast/internal/metrics"
	"walcast/internal/sink"
)

type Pipeline struct {
	ctx     context.Context
	fail    context.CancelCauseFunc
	router  *Router
	sinks   []namedSink
	retry   config.Retry
	log     *slog.Logger
	metrics *metrics.Metrics
}

type namedSink struct {
	name string
	sink.Sink
}

// New builds a Pipeline. ctx is the process-lifetime context: cancelling it
// aborts any in-progress retry wait. fail is invoked with the cause when a
// change can never be published (e.g. it cannot be encoded); the change is
// left unacknowledged and the caller is expected to shut down.
func New(ctx context.Context, fail context.CancelCauseFunc, router *Router, retry config.Retry, log *slog.Logger, m *metrics.Metrics) *Pipeline {
	if m == nil {
		m = metrics.New()
	}
	return &Pipeline{ctx: ctx, fail: fail, router: router, retry: retry, log: log, metrics: m}
}

func (p *Pipeline) AddSink(name string, s sink.Sink) {
	p.sinks = append(p.sinks, namedSink{name, s})
	p.metrics.SinkUp(name)
}

// Handle is the go-pq-cdc listener. go-pq-cdc calls it sequentially in WAL
// order and has no error return, so it blocks until the change is published
// (which also applies backpressure to the replication stream) and returns
// without acking only when shutting down.
func (p *Pipeline) Handle(lctx *replication.ListenerContext) {
	events := event.FromReplication(lctx.Message)
	for _, e := range events {
		msg, err := p.router.Route(e)
		if err != nil {
			p.metrics.PipelineError(metrics.StageEncode)
			p.fail(err)
			return
		}
		if !p.publish(msg) {
			return
		}
	}
	if err := lctx.Ack(); err != nil {
		p.metrics.Ack(metrics.ResultError)
		p.metrics.PipelineError(metrics.StageAck)
		p.log.Error("ack failed", "error", err)
		return
	}
	p.metrics.Ack(metrics.ResultOK)
	for _, e := range events {
		if !e.Timestamp.IsZero() {
			p.metrics.EventAcked(string(e.Op), time.Since(e.Timestamp))
		}
	}
}

// pendingSink tracks retries for one sink while a message is delivered.
type pendingSink struct {
	namedSink
	attempts int
}

// publish delivers m to every sink, retrying transient failures with jittered
// exponential backoff. A sink that already accepted m is not sent it again.
//
// Permanent failures go straight to the sink's dead letter destination. After
// retry.MaxAttempts transient failures the same happens. If the sink has no DLQ,
// the pipeline fails instead of acknowledging a message that was not delivered.
// Returns false only if the pipeline context ended first.
func (p *Pipeline) publish(m *sink.Message) bool {
	pending := make([]pendingSink, len(p.sinks))
	for i, s := range p.sinks {
		pending[i] = pendingSink{namedSink: s}
	}
	backoff := p.retry.InitialBackoff
	for {
		next := make([]pendingSink, 0, len(pending))
		for _, ps := range pending {
			start := time.Now()
			err := ps.Publish(p.ctx, m)
			took := time.Since(start)
			p.metrics.Publish(ps.name, m.Op, err, took)
			if err == nil {
				continue
			}
			if p.ctx.Err() != nil {
				return false
			}

			ps.attempts++
			permanent := sink.IsPermanent(err)
			reason := sink.Reason(err)
			if reason == "" {
				if permanent {
					reason = "permanent"
				} else {
					reason = "transient"
				}
			}
			exhausted := p.retry.MaxAttempts > 0 && ps.attempts >= p.retry.MaxAttempts
			if permanent || exhausted {
				if exhausted && !permanent {
					p.metrics.RetriesExhausted(ps.name)
				}
				failure := "retries exhausted"
				if permanent {
					failure = "permanent publish failure"
				}
				p.log.Error(failure,
					"sink", ps.name, "topic", m.Topic, "op", m.Op, "event_id", m.ID, "lsn", m.LSN,
					"attempts", ps.attempts, "reason", reason, "error", err)
				if p.ctx.Err() != nil {
					return false
				}
				if err := p.deadLetter(ps.namedSink, m, err, ps.attempts, permanent, reason); err != nil {
					if p.ctx.Err() != nil {
						return false
					}
					p.fail(err)
					return false
				}
				continue
			}

			p.metrics.Retry(ps.name)
			p.log.Warn("publish failed, will retry",
				"sink", ps.name, "topic", m.Topic, "op", m.Op, "event_id", m.ID, "lsn", m.LSN,
				"attempt", ps.attempts, "duration", took, "backoff", backoff, "reason", reason, "error", err)
			next = append(next, ps)
		}
		if len(next) == 0 {
			return true
		}
		pending = next

		// Full jitter in [backoff/2, backoff] so parallel instances don't sync up.
		wait := backoff/2 + rand.N(backoff/2+1)
		select {
		case <-p.ctx.Done():
			return false
		case <-time.After(wait):
		}
		backoff = min(backoff*2, p.retry.MaxBackoff)
	}
}

func (p *Pipeline) deadLetter(s namedSink, m *sink.Message, cause error, attempts int, permanent bool, reason string) error {
	dl, ok := s.Sink.(sink.DeadLetterer)
	if !ok {
		return fmt.Errorf("sink %s: no dead letter queue configured: %w", s.name, cause)
	}
	f := sink.Failure{Err: cause, Reason: reason, Attempts: attempts, Permanent: permanent}
	if err := dl.DeadLetter(p.ctx, m, f); err != nil {
		if errors.Is(err, sink.ErrDeadLetterNotConfigured) {
			return fmt.Errorf("sink %s: no dead letter queue configured: %w", s.name, cause)
		}
		p.metrics.DeadLetterError(s.name)
		return sink.DeadLetterError(s.name, err)
	}
	p.metrics.DeadLetter(s.name, reason)
	p.log.Warn("event moved to dead letter queue",
		"sink", s.name, "topic", m.Topic, "op", m.Op, "event_id", m.ID, "lsn", m.LSN,
		"attempts", attempts, "permanent", permanent, "reason", reason)
	return nil
}

func (p *Pipeline) Close() {
	for _, s := range p.sinks {
		if err := s.Close(); err != nil {
			p.log.Warn("closing sink", "sink", s.name, "error", err)
		}
	}
}
