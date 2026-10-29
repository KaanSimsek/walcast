package metrics

import (
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMetricsRecordPublishResults(t *testing.T) {
	m := New()
	m.SinkUp("kafka")
	m.Publish("kafka", "insert", nil, 12*time.Millisecond)
	m.Publish("kafka", "update", errors.New("boom"), 3*time.Millisecond)
	m.Retry("kafka")

	if got := testutil.ToFloat64(m.publishTotal.WithLabelValues("kafka", "insert", ResultOK)); got != 1 {
		t.Errorf("successful publishes = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.publishTotal.WithLabelValues("kafka", "update", ResultError)); got != 1 {
		t.Errorf("failed publishes = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.publishRetries.WithLabelValues("kafka")); got != 1 {
		t.Errorf("retries = %v, want 1", got)
	}
	// The last publish failed, so the sink is down even though it started up.
	if got := testutil.ToFloat64(m.sinkUp.WithLabelValues("kafka")); got != 0 {
		t.Errorf("sink up = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.lastSuccess.WithLabelValues("kafka")); got == 0 {
		t.Error("last success timestamp was not updated")
	}
	if got := testutil.CollectAndCount(m.publishDuration); got != 2 {
		t.Errorf("publish duration series = %d, want 2", got)
	}
}

func TestMetricsRecordAckAndLag(t *testing.T) {
	m := New()
	m.Ack(ResultOK)
	m.Ack(ResultError)
	m.EventAcked("insert", 2*time.Second)
	m.PipelineError(StageEncode)
	m.SetReady(true)

	if got := testutil.ToFloat64(m.ackTotal.WithLabelValues(ResultOK)); got != 1 {
		t.Errorf("successful acks = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.ackTotal.WithLabelValues(ResultError)); got != 1 {
		t.Errorf("failed acks = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.pipelineErrors.WithLabelValues(StageEncode)); got != 1 {
		t.Errorf("pipeline errors = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.ready); got != 1 {
		t.Errorf("ready = %v, want 1", got)
	}
	if got := testutil.CollectAndCount(m.eventLag); got != 1 {
		t.Errorf("event lag series = %d, want 1", got)
	}

	m.SetReady(false)
	if got := testutil.ToFloat64(m.ready); got != 0 {
		t.Errorf("ready = %v after shutdown, want 0", got)
	}
}

func TestCollectorsRegister(t *testing.T) {
	m := New()
	m.Publish("kafka", "insert", nil, time.Millisecond)
	m.Retry("kafka")
	m.EventAcked("insert", time.Second)
	m.Ack(ResultOK)
	m.PipelineError(StageEncode)
	m.SetReady(true)

	reg := prometheus.NewRegistry()
	reg.MustRegister(m.Collectors()...)

	families, err := reg.Gather()
	if err != nil {
		t.Fatal(err)
	}
	names := make([]string, 0, len(families))
	for _, f := range families {
		names = append(names, f.GetName())
	}
	for _, want := range []string{
		"walcast_publish_total",
		"walcast_publish_duration_seconds",
		"walcast_ack_total",
		"walcast_sink_up",
		"walcast_ready",
		"walcast_build_info",
	} {
		if !slices.Contains(names, want) {
			t.Errorf("metric %q not registered (got %v)", want, names)
		}
	}
}
