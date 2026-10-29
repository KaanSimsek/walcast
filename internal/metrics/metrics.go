// Package metrics defines the Prometheus metrics walcast emits about delivery.
//
// Consumed-WAL metrics (insert/update/delete counts, slot lag, snapshot
// progress) come from go-pq-cdc. These metrics cover the other half: what
// happens between an event arriving and the WAL position being acknowledged.
package metrics

import (
	"runtime"
	"runtime/debug"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const (
	ResultOK    = "ok"
	ResultError = "error"

	StageEncode = "encode"
	StageAck    = "ack"
)

var (
	publishBuckets = []float64{.001, .005, .01, .025, .05, .1, .25, .5, 1, 2.5, 5, 10, 30}
	lagBuckets     = []float64{.01, .05, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600}
)

type Metrics struct {
	publishTotal    *prometheus.CounterVec
	publishDuration *prometheus.HistogramVec
	publishRetries  *prometheus.CounterVec
	eventLag        *prometheus.HistogramVec
	ackTotal        *prometheus.CounterVec
	sinkUp          *prometheus.GaugeVec
	lastSuccess     *prometheus.GaugeVec
	pipelineErrors  *prometheus.CounterVec
	ready           prometheus.Gauge
	buildInfo       prometheus.Gauge
	process         prometheus.Collector
}

func New() *Metrics {
	m := &Metrics{
		publishTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "walcast",
			Subsystem: "publish",
			Name:      "total",
			Help:      "Publish attempts by sink, operation and result.",
		}, []string{"sink", "op", "result"}),
		publishDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "walcast",
			Subsystem: "publish",
			Name:      "duration_seconds",
			Help:      "Time spent publishing one event to a sink.",
			Buckets:   publishBuckets,
		}, []string{"sink", "result"}),
		publishRetries: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "walcast",
			Subsystem: "publish",
			Name:      "retries_total",
			Help:      "Publish retries caused by a sink failure.",
		}, []string{"sink"}),
		eventLag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: "walcast",
			Subsystem: "event",
			Name:      "lag_seconds",
			Help:      "Time between the source event timestamp and the WAL acknowledgement.",
			Buckets:   lagBuckets,
		}, []string{"op"}),
		ackTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "walcast",
			Subsystem: "ack",
			Name:      "total",
			Help:      "WAL acknowledgement attempts by result.",
		}, []string{"result"}),
		sinkUp: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "walcast",
			Subsystem: "sink",
			Name:      "up",
			Help:      "Whether the last publish to a sink succeeded (1) or failed (0).",
		}, []string{"sink"}),
		lastSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "walcast",
			Subsystem: "sink",
			Name:      "last_success_timestamp_seconds",
			Help:      "Unix timestamp of the last successful publish to a sink.",
		}, []string{"sink"}),
		pipelineErrors: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "walcast",
			Subsystem: "pipeline",
			Name:      "errors_total",
			Help:      "Pipeline errors by stage.",
		}, []string{"stage"}),
		ready: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "walcast",
			Name:      "ready",
			Help:      "Whether the connector has reached its ready state (1) or not (0).",
		}),
		buildInfo: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace:   "walcast",
			Name:        "build_info",
			Help:        "Build information as labels; the value is always 1.",
			ConstLabels: buildLabels(),
		}),
		process: collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	}
	m.buildInfo.Set(1)
	return m
}

func buildLabels() prometheus.Labels {
	version, commit := "dev", "unknown"
	if info, ok := debug.ReadBuildInfo(); ok {
		if info.Main.Version != "" && info.Main.Version != "(devel)" {
			version = info.Main.Version
		}
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" && s.Value != "" {
				commit = s.Value
				if len(commit) > 12 {
					commit = commit[:12]
				}
			}
		}
	}
	return prometheus.Labels{
		"version":    version,
		"commit":     commit,
		"go_version": runtime.Version(),
	}
}

// Collectors returns every collector that should be registered with the
// connector's Prometheus registry.
func (m *Metrics) Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		m.publishTotal,
		m.publishDuration,
		m.publishRetries,
		m.eventLag,
		m.ackTotal,
		m.sinkUp,
		m.lastSuccess,
		m.pipelineErrors,
		m.ready,
		m.buildInfo,
		m.process,
	}
}

// Publish records the result and duration of one sink publish attempt.
func (m *Metrics) Publish(sink, op string, err error, d time.Duration) {
	result := ResultOK
	if err != nil {
		result = ResultError
	}
	m.publishTotal.WithLabelValues(sink, op, result).Inc()
	m.publishDuration.WithLabelValues(sink, result).Observe(d.Seconds())
	if err != nil {
		m.sinkUp.WithLabelValues(sink).Set(0)
		return
	}
	m.sinkUp.WithLabelValues(sink).Set(1)
	m.lastSuccess.WithLabelValues(sink).SetToCurrentTime()
}

func (m *Metrics) Retry(sink string) {
	m.publishRetries.WithLabelValues(sink).Inc()
}

func (m *Metrics) SinkUp(sink string) {
	m.sinkUp.WithLabelValues(sink).Set(1)
}

func (m *Metrics) EventAcked(op string, lag time.Duration) {
	m.eventLag.WithLabelValues(op).Observe(lag.Seconds())
}

func (m *Metrics) Ack(result string) {
	m.ackTotal.WithLabelValues(result).Inc()
}

func (m *Metrics) PipelineError(stage string) {
	m.pipelineErrors.WithLabelValues(stage).Inc()
}

func (m *Metrics) SetReady(ready bool) {
	if ready {
		m.ready.Set(1)
		return
	}
	m.ready.Set(0)
}
