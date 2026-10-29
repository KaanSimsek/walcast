# Observability

walcast exposes its metrics through the go-pq-cdc metrics server, so there is a
single scrape target:

```
http://<host>:<postgres.metric.port>/metrics
```

The port comes from `postgres.metric.port` (default `8080`). go-pq-cdc's own
metrics are served there too.

## Metrics

Consumed-WAL metrics come from go-pq-cdc: insert/update/delete counts, CDC and
process latency, replication slot lag and snapshot progress. The `walcast_*`
metrics below cover delivery: what happens between an event arriving and its WAL
position being acknowledged.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `walcast_publish_total` | counter | `sink`, `op`, `result` | Publish attempts. `result` is `ok` or `error`. |
| `walcast_publish_duration_seconds` | histogram | `sink`, `result` | Time spent on one publish attempt. |
| `walcast_publish_retries_total` | counter | `sink` | Retries triggered by a sink failure. |
| `walcast_event_lag_seconds` | histogram | `op` | Source event timestamp to successful WAL ack. |
| `walcast_ack_total` | counter | `result` | WAL acknowledgement attempts. |
| `walcast_sink_up` | gauge | `sink` | `1` after a successful publish, `0` after a failure. |
| `walcast_sink_last_success_timestamp_seconds` | gauge | `sink` | Unix time of the last successful publish. |
| `walcast_pipeline_errors_total` | counter | `stage` | Pipeline errors (`encode` or `ack`). |
| `walcast_ready` | gauge | - | `1` once the connector is ready, `0` during startup/shutdown. |
| `walcast_build_info` | gauge | `version`, `commit`, `go_version` | Build metadata; value is always `1`. |

`walcast_sink_up` only reflects the last publish attempt. A sink can be `1`
while idle; use `walcast_sink_last_success_timestamp_seconds` together with
traffic to detect staleness.

Labels are deliberately bounded: `op` has five values, `sink` one per enabled
backend, and `result` is `ok`/`error`. Schema and table are not metric labels;
use logs for per-table debugging.

## Useful queries

```promql
# publish errors by sink
sum by (sink) (rate(walcast_publish_total{result="error"}[5m]))

# p99 publish latency
histogram_quantile(0.99,
  sum by (le, sink) (rate(walcast_publish_duration_seconds_bucket[5m])))

# time since the last successful publish, per sink
time() - walcast_sink_last_success_timestamp_seconds

# WAL retained by the slot, from go-pq-cdc
go_pq_cdc_replication_slot_slot_lag

# connector readiness
walcast_ready
```

Example alert rules:

```yaml
groups:
  - name: walcast
    rules:
      - alert: WalcastNotReady
        expr: walcast_ready == 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: walcast connector is not ready

      - alert: WalcastPublishErrors
        expr: sum(rate(walcast_publish_total{result="error"}[5m])) > 0
        for: 10m
        labels:
          severity: critical
        annotations:
          summary: walcast is failing to publish events

      - alert: WalcastSinkStale
        expr: time() - walcast_sink_last_success_timestamp_seconds > 300
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: a walcast sink has not accepted an event recently

      - alert: WalcastPipelineErrors
        expr: increase(walcast_pipeline_errors_total[5m]) > 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: walcast hit an encode or ack error
```

`WalcastSinkStale` is only meaningful while events are flowing; pair it with the
go-pq-cdc insert/update/delete counters if the workload is bursty.

## Health endpoints

The go-pq-cdc metrics server exposes:

- `GET /status` returns `OK` (liveness).
- `GET /slot` returns replication slot information.
- `GET /metrics` is the Prometheus endpoint.

For HTTP readiness probes, set `observability.health_port`. This starts a second,
small server because go-pq-cdc owns the metrics listener:

```yaml
observability:
  health_port: 8082
```

It exposes:

- `GET /healthz`: always `200` while the process is running.
- `GET /readyz`: `200` when the connector is ready, `503` before that and during
  shutdown.

Readiness follows `walcast_ready`. It intentionally does not flap when a sink is
failing, because restarting the process would not fix a broker outage; use
`walcast_sink_up` and the publish metrics for that.

Example Kubernetes probes:

```yaml
livenessProbe:
  httpGet: {path: /healthz, port: 8082}
readinessProbe:
  httpGet: {path: /readyz, port: 8082}
```

## Logs

Logs are JSON by default and go to stderr:

```yaml
observability:
  log_format: json        # json | text
```

walcast and go-pq-cdc share the same handler, so both use the same format. Every
record has a `component` field (`walcast` or `go-pq-cdc`). Pipeline records add
`sink`, `topic`, `op`, `event_id`, `lsn`, `attempt` and `backoff` where relevant.
Row values and message bodies are never logged. URLs are logged with credentials
redacted.

## Correlation

Where the broker supports headers, each message carries:

- `op`: `insert`, `update`, `delete`, `truncate` or `snapshot`
- `id`: stable per-change identifier (also used as the broker-native message ID
  for NATS and RabbitMQ)
- `lsn`: source WAL position, when available

Kafka also uses the routed row key as the record key. The `id` is the field to
deduplicate on after a redelivery.
