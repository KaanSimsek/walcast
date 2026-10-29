// Package pipeline glues go-pq-cdc to the sinks: it is the replication listener.
//
// Delivery is at-least-once. The WAL position is acknowledged only after every
// enabled sink has accepted the change, so a crash replays from the last ack
// and consumers may see duplicates (use Event.ID to dedupe).
package pipeline

import (
	"context"
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

// publish delivers m to every sink, retrying with jittered exponential backoff.
// A sink that already accepted m is not sent it again when another one fails.
// Returns false only if the pipeline context ended first.
func (p *Pipeline) publish(m *sink.Message) bool {
	pending := p.sinks
	backoff := p.retry.InitialBackoff
	for attempt := 1; ; attempt++ {
		var failed []namedSink
		for _, s := range pending {
			start := time.Now()
			err := s.Publish(p.ctx, m)
			took := time.Since(start)
			p.metrics.Publish(s.name, m.Op, err, took)
			if err != nil {
				if p.ctx.Err() != nil {
					return false
				}
				p.metrics.Retry(s.name)
				p.log.Warn("publish failed, will retry",
					"sink", s.name, "topic", m.Topic, "op", m.Op, "event_id", m.ID, "lsn", m.LSN,
					"attempt", attempt, "duration", took, "backoff", backoff, "error", err)
				failed = append(failed, s)
			}
		}
		if len(failed) == 0 {
			return true
		}
		pending = failed

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

func (p *Pipeline) Close() {
	for _, s := range p.sinks {
		if err := s.Close(); err != nil {
			p.log.Warn("closing sink", "sink", s.name, "error", err)
		}
	}
}
