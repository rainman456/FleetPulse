package clickhouse

import (
	"context"
	"fmt"
	"time"

	"github.com/mstone/fleetpulse/internal/ingest"
	"github.com/mstone/fleetpulse/internal/state"
)

const queryAllClients = `
SELECT
    client_id,
    argMax(version, ts)           AS version,
    argMax(os, ts)                AS os,
    argMax(region, ts)            AS region,
    maxIf(ts, type = 'heartbeat') AS last_heartbeat_at,
    argMax(session_id, ts)        AS last_session_id,
    max(ts)                       AS last_event_at,
    min(ts)                       AS registered_at
FROM events
GROUP BY client_id`

const queryClientByID = `
SELECT
    client_id,
    argMax(version, ts)           AS version,
    argMax(os, ts)                AS os,
    argMax(region, ts)            AS region,
    maxIf(ts, type = 'heartbeat') AS last_heartbeat_at,
    argMax(session_id, ts)        AS last_session_id,
    max(ts)                       AS last_event_at,
    min(ts)                       AS registered_at
FROM events
WHERE client_id = ?
GROUP BY client_id`

// All implements state.Store.All. It reads the same per-client record the
// in-memory and DynamoDB stores maintain, reconstructed from the event log:
// the newest cohort labels and session id (argMax), the newest heartbeat
// timestamp (maxIf ... type = 'heartbeat'), and first/last event times.
//
// A client that has never heartbeated gets the DateTime64 zero value
// (1970-01-01), which the unchanged freshness logic in internal/signals
// correctly treats as stale.
func (s *Store) All(ctx context.Context) ([]state.ClientInfo, error) {
	rows, err := s.conn.Query(ctx, queryAllClients)
	if err != nil {
		return nil, fmt.Errorf("query all clients: %w", err)
	}
	defer rows.Close()

	out := make([]state.ClientInfo, 0, 1024)
	for rows.Next() {
		info, err := scanClientRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, info)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate client rows: %w", err)
	}
	return out, nil
}

// Get implements state.Store.Get.
func (s *Store) Get(ctx context.Context, clientID string) (state.ClientInfo, bool, error) {
	rows, err := s.conn.Query(ctx, queryClientByID, clientID)
	if err != nil {
		return state.ClientInfo{}, false, fmt.Errorf("query client %q: %w", clientID, err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return state.ClientInfo{}, false, fmt.Errorf("iterate client row: %w", err)
		}
		return state.ClientInfo{}, false, nil
	}
	info, err := scanClientRow(rows)
	if err != nil {
		return state.ClientInfo{}, false, err
	}
	return info, true, nil
}

// Upsert is a deliberate no-op. ClickHouse holds an append-only event log, so
// there is nothing to update in place: the worker's sink Write is the only
// write path, and All/Get derive the current per-client view from those rows.
// Implemented so *Store satisfies state.Store unchanged.
func (s *Store) Upsert(context.Context, string, ingest.Cohort, time.Time, bool, string) error {
	return nil
}

// MappingAvailability returns the trailing 5-minute success ratio from
// mapping_results. With no rows in the window it returns 1.0, matching the
// in-process tracker's "no data is not an outage" rule (docs/signals.md).
func (s *Store) MappingAvailability(ctx context.Context) (float64, error) {
	rows, err := s.conn.Query(ctx, QueryMappingAvailability)
	if err != nil {
		return 0, fmt.Errorf("query mapping availability: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return 0, fmt.Errorf("iterate mapping availability: %w", err)
		}
		return 1.0, nil
	}
	var ratio float64
	if err := rows.Scan(&ratio); err != nil {
		return 0, fmt.Errorf("scan mapping availability: %w", err)
	}
	return ratio, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanClientRow(row rowScanner) (state.ClientInfo, error) {
	var (
		clientID      string
		version       string
		osName        string
		region        string
		lastHeartbeat time.Time
		lastSessionID string
		lastEvent     time.Time
		registered    time.Time
	)
	if err := row.Scan(&clientID, &version, &osName, &region, &lastHeartbeat, &lastSessionID, &lastEvent, &registered); err != nil {
		return state.ClientInfo{}, fmt.Errorf("scan client row: %w", err)
	}
	return state.ClientInfo{
		ClientID:        clientID,
		Cohort:          ingest.Cohort{Version: version, OS: osName, Region: region},
		LastHeartbeatAt: lastHeartbeat,
		LastEventAt:     lastEvent,
		LastSessionID:   lastSessionID,
		RegisteredAt:    registered,
	}, nil
}
