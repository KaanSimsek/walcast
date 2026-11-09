# walcast Helm chart

Deploys walcast to Kubernetes.

## Install

```sh
helm install walcast ./charts/walcast -f my-values.yaml
```

The default values are a starting point, not a working installation. Set
`config.postgres`, `config.postgres.publication.tables` and at least one entry
under `config.sinks`; otherwise the pod exits with a configuration error.

`config.postgres.password` defaults to `${PG_PASSWORD}`. Provide it through
`env` or `envFrom`:

```yaml
env:
  - name: PG_PASSWORD
    valueFrom:
      secretKeyRef:
        name: walcast-postgres
        key: password
```

Use `existingConfigMap` instead of `config` if the configuration is managed
outside the chart.

## Ports

- `metrics.port` (default `8080`): Prometheus `/metrics`.
- `health.port` (default `8081`): `/healthz` and `/readyz`.

These must match `config.postgres.metric.port` and
`config.observability.health_port`.

## Heartbeat

The chart enables a dedicated `walcast_heartbeat` table with a 5s interval so
an idle CDC database does not retain WAL while other databases on the same
instance are busy. The table is included in `config.postgres.publication.tables`.

The CDC role needs `CREATE` on the schema for the first run, then `INSERT` and
`UPDATE` on the heartbeat table. If the publication is managed outside walcast,
include the heartbeat table there as well. Set
`config.postgres.heartbeat.table.name` to `""` to disable it.

## Scaling

PostgreSQL logical replication slots allow a single active consumer. Keep
`replicaCount: 1` and the default `updateStrategy: Recreate`.

## Prometheus Operator

Set `metrics.serviceMonitor.enabled: true` to create a `ServiceMonitor`. The
Prometheus Operator CRDs must already be installed.

## Test

```sh
helm test walcast
```

The test calls `/healthz` through the Service.

## Image

The image defaults to `kaansimsek/walcast` with the tag set to `Chart.appVersion`.
Override `image.repository` and `image.tag` to use another registry or release.
