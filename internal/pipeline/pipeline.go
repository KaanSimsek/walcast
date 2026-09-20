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
	"walcast/internal/sink"
)

type Pipeline struct {
	ctx    context.Context
	fail   context.CancelCauseFunc
	router *Router
	sinks  []namedSink
	retry  config.Retry
	log    *slog.Logger
}

type namedSink struct {
	name string
	sink.Sink
}

// New builds a Pipeline. ctx is the process-lifetime context: cancelling it
// aborts any in-progress retry wait. fail is invoked with the cause when a
// change can never be published (e.g. it cannot be encoded); the change is
// left unacknowledged and the caller is expected to shut down.
func New(ctx context.Context, fail context.CancelCauseFunc, router *Router, retry config.Retry, log *slog.Logger) *Pipeline {
	return &Pipeline{ctx: ctx, fail: fail, router: router, retry: retry, log: log}
}

func (p *Pipeline) AddSink(name string, s sink.Sink) {
	p.sinks = append(p.sinks, namedSink{name, s})
}

// Handle is the go-pq-cdc listener. go-pq-cdc calls it sequentially in WAL
// order and has no error return, so it blocks until the change is published
// (which also applies backpressure to the replication stream) and returns
// without acking only when shutting down.
func (p *Pipeline) Handle(lctx *replication.ListenerContext) {
	for _, e := range event.FromReplication(lctx.Message) {
		msg, err := p.router.Route(e)
		if err != nil {
			p.fail(err)
			return
		}
		if !p.publish(msg) {
			return
		}
	}
	if err := lctx.Ack(); err != nil {
		p.log.Error("ack failed", "error", err)
	}
}

// publish delivers m to every sink, retrying with jittered exponential backoff.
// A sink that already accepted m is not sent it again when another one fails.
// Returns false only if the pipeline context ended first.
func (p *Pipeline) publish(m *sink.Message) bool {
	pending := p.sinks
	backoff := p.retry.InitialBackoff
	for {
		var failed []namedSink
		for _, s := range pending {
			if err := s.Publish(p.ctx, m); err != nil {
				if p.ctx.Err() != nil {
					return false
				}
				p.log.Warn("publish failed, will retry", "sink", s.name, "topic", m.Topic, "op", m.Op, "backoff", backoff, "error", err)
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
