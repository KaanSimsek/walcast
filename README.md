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

Metrics are served by go-pq-cdc at `postgres.metric.port/metrics` (default `8080`). walcast registers its collectors there, so there is a single Prometheus target; `/status` is liveness and `/slot` shows replication slot state. Set `observability.health_port` to expose `/healthz` and `/readyz` on a separate port for HTTP probes.

The `walcast_*` metrics cover publish attempts, latency, retries, sink health, event lag, acknowledgements and readiness. See [docs/observability.md](docs/observability.md) for the metric catalog, PromQL examples and alert rules.

Logs are JSON by default and share a handler with go-pq-cdc. Set `observability.log_format: text` if you prefer readable logs; credentials in URLs are redacted.

## Delivery guarantees

At-least-once, in WAL order. The WAL position is acknowledged only after **every enabled sink** has durably accepted the change. If a sink is down, walcast retries with backoff and stops advancing the slot (Postgres retains WAL meanwhile, so alert on slot lag). After a crash, recent changes may be redelivered; dedupe on `id`.

With several sinks enabled, a slow or failing one delays all of them.

## Adding a broker

1. `internal/sink/<name>/` with `config.go` (config struct, defaults and validation) and `<name>.go` implementing `sink.Sink`, with `func init() { sink.Register("<name>", New) }`. Decode its own config section via `sink.Config.Decode`.
2. Blank-import it in `cmd/walcast/main.go`.
3. Add a `sinks.<name>` section with `enabled: true`.

Nothing in core config or the pipeline changes.
