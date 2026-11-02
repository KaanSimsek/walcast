# walcast

Streams PostgreSQL changes (logical replication, via [go-pq-cdc](https://github.com/Trendyol/go-pq-cdc)) to the message brokers you enable in config. Kafka, NATS JetStream and RabbitMQ are built in; adding another broker is one package.

```
Postgres WAL ─▶ go-pq-cdc ─▶ pipeline ─┬─▶ kafka
                (slot+publication)      ├─▶ nats jetstream
                                        ├─▶ rabbitmq
                                        └─▶ (redis, ... later)
```

## Run

```sh
cp config.example.yaml config.yaml   # edit; enable the sinks you want
go run ./cmd/walcast -config config.yaml
```

Postgres needs `wal_level=logical` and a role with `REPLICATION` (plus rights to create the publication/slot, or create them yourself).

## Docker

Build a release image (nothing is pushed automatically):

```sh
docker build \
  --build-arg VERSION=0.1.0 \
  --build-arg COMMIT=$(git rev-parse --short HEAD) \
  -t kaansimsek/walcast:0.1.0 .

docker run --rm \
  -p 8080:8080 -p 8081:8081 \
  -v "$PWD/config.yaml:/etc/walcast/config.yaml:ro" \
  kaansimsek/walcast:0.1.0
```

Metrics are on `8080` and health probes on `8081` by default.

## Kubernetes

A Helm chart lives in [`charts/walcast`](charts/walcast):

```sh
helm install walcast ./charts/walcast -f my-values.yaml
```

The defaults still need Postgres, publication tables and at least one sink; see the [chart README](charts/walcast/README.md). Keep `replicaCount` at `1` because a logical replication slot has a single active consumer.

## Event format

Every sink receives the same JSON envelope:

```json
{"id":"0/175A220/public.users/update","op":"update","schema":"public","table":"users",
 "lsn":"0/175A220","ts":"2026-09-20T12:13:28Z","before":{...},"after":{...}}
```

`op` is `insert | update | delete | truncate | snapshot`. `before` on updates is the full old row only with `REPLICA IDENTITY FULL`.

| | destination | key / dedup |
|---|---|---|
| Kafka | topic `<prefix>.<schema>.<table>` | record key = `routing.key_columns` values, else `schema.table` |
| NATS | subject `<prefix>.<schema>.<table>.<op>` | `Nats-Msg-Id` = event `id` (JetStream dedupes redelivery) |
| RabbitMQ | exchange, routing key `<prefix>.<schema>.<table>.<op>` | `MessageId` = event `id`; `Type`/`op` header = operation |

RabbitMQ publishes are persistent and wait for publisher confirms. An unroutable message (no queue bound to the exchange) is returned and treated as a failure, so the WAL stalls instead of losing the change. The exchange is declared on startup by default; use `exchange.declare: false` if it is managed elsewhere.

Messages also carry `op`, `id` and `lsn` headers where the broker supports them, for correlation and deduplication.

## Observability

Metrics are served by go-pq-cdc at `postgres.metric.port/metrics` (default `8080`). walcast registers its collectors there, so there is a single Prometheus target; `/status` is liveness and `/slot` shows replication slot state. The health server exposes `/healthz` and `/readyz` on a separate port, `observability.health_port` (default `8081`; set `0` to disable).

The `walcast_*` metrics cover publish attempts, latency, retries, sink health, event lag, acknowledgements and readiness. See [docs/observability.md](docs/observability.md) for the metric catalog, PromQL examples and alert rules.

Logs are JSON by default and share a handler with go-pq-cdc. Set `observability.log_format: text` if you prefer readable logs; credentials in URLs are redacted.

## Delivery guarantees

At-least-once, in WAL order. The WAL position is acknowledged only after **every enabled sink** has durably accepted the change (or accepted it into a configured DLQ). After a crash, recent changes may be redelivered; dedupe on `id`.

Transient failures are retried with backoff up to `retry.max_attempts` (default `0` = forever). Permanent errors are not retried. When retries are exhausted, or the error is permanent:

- If the sink has a DLQ configured, the event is published there and the WAL can advance.
- Without a DLQ, walcast fails fast: the WAL position is not acknowledged and the process exits. Set `max_attempts: 0` if you prefer transient errors to keep retrying forever; permanent errors still stop without a DLQ.

A DLQ publish failure also stops the pipeline; walcast never acknowledges an event that was neither delivered nor stored. See [docs/error-handling.md](docs/error-handling.md) for per-broker DLQ configuration and caveats.

With several sinks enabled, a slow or failing one delays all of them until it is delivered or moved to its DLQ.

## Adding a broker

1. `internal/sink/<name>/` with `config.go` (config struct, defaults and validation) and `<name>.go` implementing `sink.Sink`, with `func init() { sink.Register("<name>", New) }`. Decode its own config section via `sink.Config.Decode`. Classify permanent errors with `sink.Permanent` and optionally implement `sink.DeadLetterer`.
2. Blank-import it in `cmd/walcast/main.go`.
3. Add a `sinks.<name>` section with `enabled: true`.

Nothing in core config or the pipeline changes.
