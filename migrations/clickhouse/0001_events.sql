-- FleetPulse ClickHouse schema, part 1: raw event tables.
--
-- Applied by `scripts/ch-migrate.sh` (and `make ch-migrate`) with
-- clickhouse-client --multiquery. Every statement is idempotent so re-running
-- the migration is safe.
--
-- Retention is enforced by TTL, not by a cron job:
--   events            90 days (dashboard/benchmark history)
--   heartbeats        30 days (the freshness signal only needs minutes)
--   mapping_results   90 days
--
-- All timestamps are the client-asserted event time (ingest.Event.Timestamp)
-- in UTC. DateTime64(3) keeps millisecond precision so sub-second event
-- ordering survives.

CREATE DATABASE IF NOT EXISTS fleetpulse;

-- One row per ingested event (heartbeat, capture, or error).
-- ORDER BY (cohort, client_id, ts) makes "one client's timeline" and "one
-- cohort's timeline" cheap range scans; the signals queries in
-- internal/clickhouse/queries.go aggregate over exactly these prefixes.
CREATE TABLE IF NOT EXISTS fleetpulse.events
(
    ts                   DateTime64(3, 'UTC'),
    client_id            String,
    type                 LowCardinality(String),
    version              LowCardinality(String),
    os                   LowCardinality(String),
    region               LowCardinality(String),
    -- cohort is denormalized on purpose: every dashboard panel and signal
    -- query filters/groups by this single string, so the write cost of one
    -- extra String buys much cheaper reads than concat() at query time.
    cohort               String DEFAULT concat(version, '/', os, '/', region),
    session_id           String,
    sequence_number      UInt32,
    expected_event_count UInt32,
    error_code           String,
    message              String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (cohort, client_id, ts)
TTL toDateTime(ts) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Compact heartbeat-only table. Events already contain heartbeats; this table
-- exists so freshness-oriented scans read far fewer columns/bytes than the
-- full events table (a real fleet sends heartbeats far more often than
-- captures or errors). The worker writes both tables in the same batch.
CREATE TABLE IF NOT EXISTS fleetpulse.heartbeats
(
    ts        DateTime64(3, 'UTC'),
    client_id String,
    cohort    String DEFAULT concat(version, '/', os, '/', region),
    version   LowCardinality(String),
    os        LowCardinality(String),
    region    LowCardinality(String)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (cohort, client_id, ts)
TTL toDateTime(ts) + INTERVAL 30 DAY
SETTINGS index_granularity = 8192;

-- One row per call the worker's enrichment step makes to the downstream
-- Workflow Mapping stub. This is NOT fleet data: it is written by the worker
-- after each capture enrichment call, and is the SQL source for the
-- mapping-availability signal (see docs/signals.md).
CREATE TABLE IF NOT EXISTS fleetpulse.mapping_results
(
    ts      DateTime64(3, 'UTC'),
    success UInt8,
    reason  String
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(ts)
ORDER BY (ts)
TTL toDateTime(ts) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;
