# ClickHouse signal-query benchmark

> **Status: not yet measured.** Every table below is a template. Nothing in
> this file is a real result; `cmd/bench` exists, but this benchmark has not
> been run. Fill in the tables only from your own runs, and keep the machine
> specs alongside the numbers so they are comparable later.

## How to run

```bash
# 1. Bring up ClickHouse and apply the schema
make ch-migrate

# 2. Optional smoke: time the queries against an empty/migrated database
go run ./cmd/bench -query-only

# 3. Full run: generate + batch-insert N synthetic events, then time the
#    three signal queries (default 7 timed runs each, median/p95 reported)
go run ./cmd/bench -events 1000000 -batch 5000 -concurrency 4
```

Useful flags (see `cmd/bench/main.go`): `-events`, `-batch`, `-concurrency`,
`-runs`, `-addr` / `-database` / `-user` / `-password`
(or the `FLEETPULSE_CLICKHOUSE_*` environment variables), `-query-only`.

## Machine specs

| Field | Value |
|---|---|
| CPU model | not yet measured |
| Cores / threads | not yet measured |
| RAM | not yet measured |
| Disk | not yet measured |
| OS | not yet measured |
| ClickHouse version / image | `clickhouse/clickhouse-server:24.8.4.13` (compose-pinned) |
| clickhouse-go/v2 | v2.30.0 |

## Workload

| Field | Value |
|---|---|
| Total events | not yet measured (default 1,000,000) |
| Cohorts / clients | 4 cohorts, ~300 clients (simulator defaults) |
| Event mix | not yet measured |
| Insert batch size / concurrency | 5000 / 4 (defaults) |
| Wall-clock insert time | not yet measured |
| Sustained insert rate (rows/s) | not yet measured |

## Results: signal query latency

| Query | Rows scanned | Median | p95 | Notes |
|---|---|---|---|---|
| heartbeat freshness (`QueryFreshnessByCohort`) | not yet measured | not yet measured | not yet measured | trailing 10-minute scan of `events` |
| capture completeness (`QueryCompletenessByCohort`) | not yet measured | not yet measured | not yet measured | over `session_capture_progress` MV |
| mapping availability (`QueryMappingAvailability`) | not yet measured | not yet measured | not yet measured | trailing 5-minute scan of `mapping_results` |

Repeat each row for 100k / 1M / 10M events if you want a scaling curve;
add rows rather than editing existing ones so earlier runs stay comparable.
