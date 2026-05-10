# Observability Architecture

## Goal

Tee-router should run with minimal observability infrastructure colocated with
the router services. The colocated stack should only collect, batch, and forward
telemetry. Durable storage and dashboards should live in a separate Railway
observability stack.

## Endgame Topology

```text
tee-router runtime stack
  router-api
  router-worker
  temporal
  temporal-worker
  sauron
  alloy

Railway observability stack
  VictoriaMetrics
  Loki
  Tempo
  Grafana
```

Alloy is the only observability process that should run next to tee-router in a
production-like deployment. It should:

- scrape router Prometheus metrics endpoints
- scrape Tempo's local metrics endpoint in development so Grafana can show trace
  ingestion health
- receive OTLP logs, traces, and any future OTLP metrics
- batch and retry outbound telemetry
- remote-write metrics to VictoriaMetrics
- push logs to Loki
- export traces to Tempo

The router services keep exposing Prometheus-format metrics:

- `router-api:9100`
- `router-worker:9101`
- `sauron:9102`
- `temporal:9090`

`temporal:9090` is Temporal Server's Prometheus endpoint. The
`temporal-worker` process is currently observed through process logs and the
Temporal Server metrics for task queue/workflow health; adding first-class
temporal-worker application metrics requires code instrumentation and is
outside the deployment-only cutover.

The router services may also emit OTLP logs/traces when
`OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT`, or
`OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` is configured.

## Metrics Backend Choice

We chose VictoriaMetrics for the Railway-side metrics backend.

### Prometheus

Prometheus is the simplest local metrics server and remains useful for quick
developer experiments.

Pros:

- canonical PromQL ecosystem
- excellent Grafana support
- simple scrape model

Cons:

- primarily a scraper plus local TSDB
- awkward as the remote ingestion backend for a separate tee-router runtime
- weaker single-node long-retention and HA story than purpose-built remote
  metrics stores

Use it for local experiments, not as the preferred Railway backend.

### VictoriaMetrics

VictoriaMetrics is the preferred backend for tee-router.

Pros:

- single small service
- Prometheus-compatible query API, so Grafana can use a normal Prometheus
  datasource
- accepts Prometheus remote write from Alloy
- efficient storage and retention for a single-node Railway service
- low operational overhead

Cons:

- single-node VictoriaMetrics is not horizontally scalable or strongly HA
- if metrics become high-volume or business-critical, we may need a cluster or a
  later Mimir migration

This is the best fit for our current desired architecture.

### Grafana Mimir

Mimir is the heavier Grafana-native metrics backend for large-scale,
multi-tenant, long-term metrics.

Pros:

- horizontally scalable
- HA-oriented
- Prometheus-compatible remote write and PromQL
- good long-term storage story

Cons:

- more components and operational surface
- overkill for the current tee-router stack
- a worse fit for a small Railway observability deployment

Use Mimir only if VictoriaMetrics stops being enough.

## Local Test Deployment

Local development mirrors the endgame topology in one compose overlay:

```sh
docker compose \
  -f etc/compose.local-full.yml \
  -f etc/compose.local-observability.yml \
  up -d
```

Local ports:

- Grafana: `http://localhost:3002`
- Temporal UI: `http://localhost:8080`
- VictoriaMetrics: `http://localhost:8428`
- Loki: `http://localhost:3100`
- Tempo: `http://localhost:3200`
- Alloy UI: `http://localhost:12345`
- Alloy OTLP gRPC: `localhost:4317`
- Alloy OTLP HTTP: `localhost:4318`

The main local Grafana dashboard is:

```text
http://localhost:3002/d/tee-router-local-overview/tee-router-local-overview
```

The dashboard includes router metrics, Tempo trace ingestion/activity, and recent
router logs. Individual trace inspection remains available through the Tempo
datasource in Grafana Explore.

## Why Keep Prometheus-Format Metrics?

The existing Rust metrics use the `metrics` crate and
`metrics-exporter-prometheus`. Replacing all of that with native OTLP metrics
would create churn without improving the architecture.

The clean practical model is:

- Rust `metrics` crate for counters, gauges, and timings
- Alloy scrapes Prometheus-format `/metrics`
- Alloy remote-writes metrics to VictoriaMetrics
- Rust OTLP is used for logs and traces
- Grafana reads metrics from VictoriaMetrics, logs from Loki, and traces from
  Tempo

This keeps instrumentation simple while still giving us a unified Grafana
experience.
