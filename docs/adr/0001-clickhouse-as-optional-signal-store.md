# ADR 0001: ClickHouse as an optional event sink and signal store

- **Status:** Accepted
- **Date:** 2026-11-19
- **Context:** The three cataloged signals were computed entirely in-process
  (`internal/signals`) from the in-memory/DynamoDB client-state store. That
  is cheap and correct, but it caps analysis at "current state": there is no
  history to query, no way to reproduce a signal's value from an hour ago,
  and no columnar store to benchmark realistic event volumes against.

## Context

FleetPulse's event flow is append-only and cohort-labeled: heartbeats,
captures, and errors arrive, are validated, processed by the worker pool,
and reflected into client state. Analytics over that flow is a natural fit
for ClickHouse (wide rows, heavy group-by over time windows, TTL-friendly
retention), but ClickHouse must not become a mandatory dependency for
running the demo, and the existing memory/SQS paths must not change
behavior.

## Decision

- Add ClickHouse as a **third optional backend**, selected by
  `FLEETPULSE_SINK_BACKEND=memory|clickhouse` (default `memory`), not by
  changing `FLEETPULSE_QUEUE_BACKEND` or `FLEETPULSE_STATE_BACKEND`.
- Implement it in `internal/clickhouse` behind the existing
  `sink.Sink` (writes) and `state.Store` (reads) interfaces. The worker
  drains processed events into the sink; the signals computer's
  `All`/`Get` reads become SQL aggregations over the `events` table
  (backed by the `cohort_last_seen` / `session_capture_progress`
  materialized views). `Upsert` is a no-op: ClickHouse is an append-only
  event log, the sink write is the only write path.
- Schema in `migrations/clickhouse/`: `events`, `heartbeats`,
  `mapping_results` (all TTL'd), plus the two materialized views. Applied
  idempotently by `make ch-migrate`.
- The three signal queries live in `internal/clickhouse/queries.go` so
  `cmd/bench`, the Grafana ClickHouse dashboard, and the code cannot drift.

## Consequences

- Default behavior is byte-for-byte unchanged (`memory` backend, no
  ClickHouse connection attempted).
- Freshness/completeness SQL mirrors the Go formulas, including the
  session-set semantics that make queue redelivery harmless; the formulas
  now have two implementations that can cross-check each other.
- Mapping availability keeps the in-process tracker feeding the Prometheus
  gauge; `mapping_results` records the same outcomes for SQL/Grafana, but
  the Go computer does not read the table back. Re-deriving that gauge from
  ClickHouse would put a query on the hot tick path for a signal that is a
  downstream probe, not fleet data.
- `go.mod` gains `clickhouse-go/v2`; because `go mod tidy` has not been run
  (no `go.sum` yet), its transitive dependencies are unresolved until the
  first tidy -- recorded in `docs/verify.md` step 1.
- No local execution was performed when authoring any of this; the SQL is
  written to be valid ClickHouse but is unverified until run.
