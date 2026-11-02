# Error handling and dead letters

walcast separates delivery errors into two kinds:

- **Transient**: retrying may succeed. Examples: a broker restart, a network
  timeout, a temporary leader election.
- **Permanent**: retrying the same message cannot succeed. Examples: missing
  authorization, an invalid topic or subject, a routing failure, or a message
  that exceeds a broker limit.

Unknown errors are treated as transient. Sinks mark permanent errors explicitly
with `sink.Permanent(err, reason)`.

## Delivery flow

For every sink, per event:

1. A successful publish is done.
2. A permanent error skips retries.
3. A transient error is retried with the configured backoff until
   `retry.max_attempts` is reached. `0` means retry forever.
4. On a permanent error or exhausted retries:
   - with a DLQ configured, the event is published there;
   - without a DLQ, the pipeline stops without acknowledging the WAL position.
5. If the DLQ publish fails, the pipeline stops without acknowledging.

A message is acknowledged only after every sink either published it normally or
accepted it into its DLQ. walcast never silently drops an undelivered event.

`retry.max_attempts` is per sink and event, and does not count permanent errors.
A sink that succeeded is never re-sent while another sink is retried.

## Fail fast

Stopping without an ack is deliberate. The WAL slot keeps the event, and the
next start retries it. This is safer than acknowledging an event that was not
delivered.

- Permanent failure and no DLQ: stop immediately.
- Retries exhausted and no DLQ: stop.
- DLQ publish failure: stop.
- If you want the old “retry forever” behavior for transient errors, leave
  `max_attempts: 0` (the default). Permanent errors still fail fast when no DLQ
  is configured.

## Dead letter messages

The original JSON body is preserved. DLQ publishers add these headers where the
broker supports them:

| Header | Meaning |
|---|---|
| `op` | Original operation. |
| `key` | Routed row key. |
| `id` | Stable event ID, when available. |
| `lsn` | Source WAL position, when available. |
| `walcast-dlq-source` | Original topic/subject/routing key. |
| `walcast-dlq-reason` | Classification reason, e.g. `too_large`, `unroutable`. |
| `walcast-dlq-attempts` | Number of delivery attempts. |
| `walcast-dlq-permanent` | `true` when the error was permanent. |

Replay a DLQ message by publishing its body to the original destination. Keep
the original `id` for deduplication.

## Kafka

```yaml
sinks:
  kafka:
    dlq:
      topic: walcast.dlq
```

Permanent examples: `MESSAGE_TOO_LARGE`, `INVALID_TOPIC`,
`RECORD_LIST_TOO_LARGE`, `TOPIC_AUTHORIZATION_FAILED`,
`CLUSTER_AUTHORIZATION_FAILED`, `SASL_AUTHENTICATION_FAILED`,
`UNSUPPORTED_VERSION`, `INVALID_RECORD`.

The DLQ topic must exist unless `auto_create_topics` is enabled. It uses the
same key and waits for all in-sync replicas. Broker-wide problems such as
authorization or a message larger than the broker limit will usually fail on the
DLQ topic too; fix the broker/topic configuration in those cases. A DLQ topic
with a larger `max.message.bytes` can be used for oversized events.

## NATS JetStream

```yaml
sinks:
  nats:
    dlq:
      subject: walcast.dlq
```

Permanent examples: maximum payload, authorization/permission violations,
invalid subjects, missing streams, and JetStream API responses with HTTP status
400, 401, 403, 404 or 409.

The DLQ subject must be covered by the configured stream. With the default
stream subjects (`<prefix>.>`) and subject (`<prefix>.dlq`) this is automatic.
DLQ messages use `dlq/<id>` as the JetStream message ID so a DLQ retry does not
collide with the original event in the dedupe window. Maximum payload is a
server-wide limit; an oversized event will usually fail on the DLQ too.

## RabbitMQ

```yaml
sinks:
  rabbitmq:
    dlq:
      exchange: walcast.dlq
      routing_key: failed
      exchange_type: topic   # default
      declare: true          # default
      durable: true          # default
```

Permanent examples: unroutable messages and AMQP errors `403 ACCESS_REFUSED`,
`404 NOT_FOUND`, `405 RESOURCE_LOCKED`, `406 PRECONDITION_FAILED`,
`530 NOT_ALLOWED`, `540 NOT_IMPLEMENTED`.

This is an application-level DLQ, not RabbitMQ's queue dead-lettering (walcast
publishes to exchanges and does not consume queues). DLQ publishes are
persistent, mandatory and wait for confirms. Bind a queue to the DLQ exchange
and routing key, otherwise the DLQ publish is unroutable and the pipeline stops.
A broker-wide authorization failure will usually affect the DLQ exchange too.

## Observability

See [observability.md](observability.md) for the DLQ metrics and alert examples.
Useful signals:

- `walcast_dead_letter_total{sink,reason}` increasing: delivery is degraded but
  the WAL is still moving through the DLQ.
- `walcast_retries_exhausted_total{sink}` increasing: transient failures are
  lasting longer than the retry budget.
- `walcast_dead_letter_errors_total{sink}` increasing: DLQ publishing is broken;
  the pipeline stops rather than losing the event.
