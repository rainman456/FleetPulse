package clickhouse

// The three catalog signals (docs/signals.md) as ClickHouse SQL. The table
// names are unqualified because every connection in this module sets
// Auth.Database to the configured database (default "fleetpulse").
//
// These constants are shared by cmd/bench and by the SQL copied into
// docs/runbook.md; keep them in sync with the Grafana dashboard
// (deploy/grafana/provisioning/dashboards/fleetpulse-clickhouse.json).
const (
	// QueryFreshnessByCohort mirrors internal/signals.computeHeartbeatFreshness:
	// clients with a heartbeat in the trailing 60 seconds over clients heard
	// from in the trailing 10 minutes (a bounded stand-in for "all known
	// clients", which ClickHouse cannot enumerate for free).
	QueryFreshnessByCohort = `
SELECT
    cohort,
    uniqExactIf(client_id, type = 'heartbeat' AND ts >= now() - INTERVAL 60 SECOND)
        / uniqExact(client_id) AS freshness
FROM events
WHERE ts >= now() - INTERVAL 10 MINUTE
GROUP BY cohort
ORDER BY cohort`

	// QueryCompletenessByCohort mirrors internal/signals.computeCaptureCompleteness:
	// the mean, over sessions in a cohort, of
	// distinct sequence numbers received / expected_event_count.
	// The session_capture_progress materialized view holds the per-session
	// aggregate states (uniqExactState / maxState); least(...,1.0) is the same
	// defensive cap the Go implementation applies.
	QueryCompletenessByCohort = `
SELECT
    cohort,
    avg(least(received / nullIf(expected, 0), 1.0)) AS capture_completeness
FROM
(
    SELECT
        cohort,
        session_id,
        uniqExactMerge(sequences) AS received,
        maxMerge(expected_events) AS expected
    FROM session_capture_progress
    GROUP BY cohort, session_id
)
GROUP BY cohort
ORDER BY cohort`

	// QueryMappingAvailability is a trailing-window ratio over the rows the
	// worker writes to mapping_results. It is not derived from fleet events;
	// see docs/signals.md ("ClickHouse implementation").
	QueryMappingAvailability = `
SELECT if(count() = 0, 1.0, countIf(success = 1) / count()) AS mapping_availability
FROM mapping_results
WHERE ts >= now() - INTERVAL 5 MINUTE`
)
