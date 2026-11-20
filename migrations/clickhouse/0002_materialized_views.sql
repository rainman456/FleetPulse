-- FleetPulse ClickHouse schema, part 2: materialized views that keep the
-- per-client and per-session aggregates the signals need incrementally
-- up-to-date as events are inserted.
--
-- Pattern: an AggregatingMergeTree target table holds AggregateFunction
-- *states*; a MATERIALIZED VIEW transforms each inserted block into states
-- and writes them to the target. Readers call argMaxMerge/maxMerge/
-- uniqExactMerge, which is why the query in internal/clickhouse/queries.go
-- looks different from the raw-events equivalent.

-- Last-seen-per-client rollup. column order matches
-- (cohort, client_id) so "every client in this cohort" is a range scan.
CREATE TABLE IF NOT EXISTS fleetpulse.cohort_last_seen
(
    cohort            String,
    client_id         String,
    version           AggregateFunction(argMax, String, DateTime64(3, 'UTC')),
    os                AggregateFunction(argMax, String, DateTime64(3, 'UTC')),
    region            AggregateFunction(argMax, String, DateTime64(3, 'UTC')),
    last_event_at     AggregateFunction(max, DateTime64(3, 'UTC')),
    last_heartbeat_at AggregateFunction(maxIf, DateTime64(3, 'UTC'), UInt8)
)
ENGINE = AggregatingMergeTree
ORDER BY (cohort, client_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS fleetpulse.cohort_last_seen_mv
TO fleetpulse.cohort_last_seen
AS
SELECT
    cohort,
    client_id,
    -- toString() pins the state argument type to String even though the
    -- source column is LowCardinality(String).
    argMaxState(toString(version), ts) AS version,
    argMaxState(toString(os), ts)      AS os,
    argMaxState(toString(region), ts)  AS region,
    maxState(ts)                       AS last_event_at,
    maxIfState(ts, type = 'heartbeat') AS last_heartbeat_at
FROM fleetpulse.events
GROUP BY cohort, client_id;

-- Per-session capture progress: how many DISTINCT sequence numbers have
-- arrived for a session, and what the client declared it would send.
-- uniqExactState is a set state, so queue redelivery of the same sequence
-- number cannot inflate completeness (docs/signals.md section 2).
CREATE TABLE IF NOT EXISTS fleetpulse.session_capture_progress
(
    cohort          String,
    session_id      String,
    sequences       AggregateFunction(uniqExact, UInt32),
    expected_events AggregateFunction(max, UInt32),
    first_seen      AggregateFunction(min, DateTime64(3, 'UTC')),
    last_seen       AggregateFunction(max, DateTime64(3, 'UTC'))
)
ENGINE = AggregatingMergeTree
ORDER BY (cohort, session_id);

CREATE MATERIALIZED VIEW IF NOT EXISTS fleetpulse.session_capture_progress_mv
TO fleetpulse.session_capture_progress
AS
SELECT
    cohort,
    session_id,
    uniqExactState(sequence_number) AS sequences,
    maxState(expected_event_count)  AS expected_events,
    minState(ts)                    AS first_seen,
    maxState(ts)                    AS last_seen
FROM fleetpulse.events
WHERE type = 'capture'
GROUP BY cohort, session_id;

-- Example: the two MV-backed signal queries, ready to paste into
-- clickhouse-client. (Not a view; comments only.)
--
-- Per-cohort completeness (mean over sessions of received/expected):
--   SELECT cohort, avg(least(received / nullIf(expected, 0), 1.0))
--   FROM (
--     SELECT cohort, session_id,
--            uniqExactMerge(sequences) AS received,
--            maxMerge(expected_events) AS expected
--     FROM fleetpulse.session_capture_progress
--     GROUP BY cohort, session_id
--   )
--   GROUP BY cohort;
--
-- Per-cohort last-seen (argMax over merged states):
--   SELECT cohort,
--          argMaxMerge(version) AS version,
--          maxIfMerge(last_heartbeat_at) AS last_heartbeat_at
--   FROM fleetpulse.cohort_last_seen
--   GROUP BY cohort;
