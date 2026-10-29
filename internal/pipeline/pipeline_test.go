package pipeline

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Trendyol/go-pq-cdc/pq/message/format"
	"github.com/Trendyol/go-pq-cdc/pq/replication"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"walcast/internal/config"
	"walcast/internal/metrics"
	"walcast/internal/sink"
)

type fakeSink struct {
	mu       sync.Mutex
	failures int // fail this many calls first
	calls    int
	got      []*sink.Message
}

func (f *fakeSink) Publish(_ context.Context, m *sink.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.failures > 0 {
		f.failures--
		return errors.New("boom")
	}
	f.got = append(f.got, m)
	return nil
}
func (f *fakeSink) Close() error { return nil }

func newPipeline(t *testing.T, sinks map[string]*fakeSink) (*Pipeline, context.CancelCauseFunc, *metrics.Metrics) {
	t.Helper()
	ctx, cancel := context.WithCancelCause(context.Background())
	t.Cleanup(func() { cancel(nil) })
	m := metrics.New()
	p := New(ctx, cancel, NewRouter(config.Routing{Prefix: "wc", KeyColumns: map[string][]string{"public.users": {"id"}}}),
		config.Retry{InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond},
		slog.New(slog.NewTextHandler(io.Discard, nil)), m)
	for name, s := range sinks {
		p.AddSink(name, s)
	}
	return p, cancel, m
}

// metricValue gathers m and returns the value of one series. For histograms it
// returns the sample count.
func metricValue(t *testing.T, m *metrics.Metrics, name string, labels map[string]string) float64 {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(m.Collectors()...)
	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, metric := range f.GetMetric() {
			if seriesMatches(metric, labels) {
				switch {
				case metric.GetCounter() != nil:
					return metric.GetCounter().GetValue()
				case metric.GetGauge() != nil:
					return metric.GetGauge().GetValue()
				case metric.GetHistogram() != nil:
					return float64(metric.GetHistogram().GetSampleCount())
				}
			}
		}
	}
	return 0 // uninitialized vectors have no series yet
}

func seriesMatches(metric interface {
	GetLabel() []*dto.LabelPair
}, labels map[string]string) bool {
	for k, want := range labels {
		found := false
		for _, l := range metric.GetLabel() {
			if l.GetName() == k && l.GetValue() == want {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func insertMsg() *format.Insert {
	return &format.Insert{TableNamespace: "public", TableName: "users", Decoded: map[string]any{"id": 7, "name": "a"}}
}

func listenerCtx(msg any, acked *int) *replication.ListenerContext {
	return &replication.ListenerContext{Message: msg, Ack: func() error { *acked++; return nil }}
}

func TestHandleAcksOnlyAfterEverySinkAccepted(t *testing.T) {
	flaky := &fakeSink{failures: 3}
	steady := &fakeSink{}
	p, _, m := newPipeline(t, map[string]*fakeSink{"flaky": flaky, "steady": steady})

	var acked int
	p.Handle(listenerCtx(insertMsg(), &acked))

	if acked != 1 {
		t.Fatalf("acked = %d, want 1", acked)
	}
	if len(flaky.got) != 1 || flaky.calls != 4 {
		t.Errorf("flaky: delivered %d after %d calls, want 1 after 4", len(flaky.got), flaky.calls)
	}
	// The healthy sink must not be re-sent the message while the other one retries.
	if len(steady.got) != 1 || steady.calls != 1 {
		t.Errorf("steady: delivered %d after %d calls, want exactly 1", len(steady.got), steady.calls)
	}
	gotMsg := steady.got[0]
	if gotMsg.Topic != "wc.public.users" || string(gotMsg.Key) != "7" || gotMsg.Op != "insert" {
		t.Errorf("unexpected routing: topic=%q key=%q op=%q", gotMsg.Topic, gotMsg.Key, gotMsg.Op)
	}

	if got := metricValue(t, m, "walcast_publish_total", map[string]string{"sink": "steady", "op": "insert", "result": metrics.ResultOK}); got != 1 {
		t.Errorf("steady publishes = %v, want 1", got)
	}
	if got := metricValue(t, m, "walcast_publish_total", map[string]string{"sink": "flaky", "op": "insert", "result": metrics.ResultError}); got != 3 {
		t.Errorf("flaky failures = %v, want 3", got)
	}
	if got := metricValue(t, m, "walcast_publish_retries_total", map[string]string{"sink": "flaky"}); got != 3 {
		t.Errorf("flaky retries = %v, want 3", got)
	}
	if got := metricValue(t, m, "walcast_sink_up", map[string]string{"sink": "flaky"}); got != 1 {
		t.Errorf("flaky up = %v after eventual success, want 1", got)
	}
	if got := metricValue(t, m, "walcast_ack_total", map[string]string{"result": metrics.ResultOK}); got != 1 {
		t.Errorf("acks = %v, want 1", got)
	}
}

func TestHandleDoesNotAckWhenShuttingDown(t *testing.T) {
	always := &fakeSink{failures: 1 << 30}
	p, cancel, m := newPipeline(t, map[string]*fakeSink{"down": always})

	done := make(chan struct{})
	var acked int
	go func() { p.Handle(listenerCtx(insertMsg(), &acked)); close(done) }()

	time.Sleep(20 * time.Millisecond) // let it fail and back off a few times
	cancel(context.Canceled)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Handle did not return after context cancellation")
	}
	if acked != 0 {
		t.Fatalf("acked = %d after shutdown with undelivered change, want 0", acked)
	}
	if got := metricValue(t, m, "walcast_sink_up", map[string]string{"sink": "down"}); got != 0 {
		t.Errorf("down sink up = %v, want 0", got)
	}
	if got := metricValue(t, m, "walcast_ack_total", map[string]string{"result": metrics.ResultOK}); got != 0 {
		t.Errorf("acks = %v, want 0", got)
	}
}

func TestHandleAcksMessagesWithoutEvents(t *testing.T) {
	s := &fakeSink{}
	p, _, m := newPipeline(t, map[string]*fakeSink{"s": s})

	var acked int
	p.Handle(listenerCtx(&format.Snapshot{EventType: format.SnapshotEventTypeBegin}, &acked))

	if acked != 1 || s.calls != 0 {
		t.Fatalf("acked=%d publishes=%d, want 1 and 0", acked, s.calls)
	}
	if got := metricValue(t, m, "walcast_ack_total", map[string]string{"result": metrics.ResultOK}); got != 1 {
		t.Errorf("acks = %v, want 1", got)
	}
}

func TestEncodeFailureIsFatalAndUnacked(t *testing.T) {
	s := &fakeSink{}
	p, _, m := newPipeline(t, map[string]*fakeSink{"s": s})
	bad := &format.Insert{TableNamespace: "public", TableName: "t", Decoded: map[string]any{"c": make(chan int)}}

	var acked int
	p.Handle(listenerCtx(bad, &acked))

	if acked != 0 || s.calls != 0 {
		t.Fatalf("acked=%d publishes=%d, want 0 and 0", acked, s.calls)
	}
	if p.ctx.Err() == nil {
		t.Fatal("pipeline context should be cancelled after an unencodable change")
	}
	if got := metricValue(t, m, "walcast_pipeline_errors_total", map[string]string{"stage": metrics.StageEncode}); got != 1 {
		t.Errorf("encode errors = %v, want 1", got)
	}
}
