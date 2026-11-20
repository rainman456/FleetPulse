// Package sink defines the optional event-sink boundary used by the worker
// pool. The default pipeline (in-memory queue + in-memory state) needs no
// sink at all, so Config.Sink defaults to NoopSink and existing behavior is
// unchanged.
//
// The only implemented non-noop sink is internal/clickhouse.Store, which
// writes fully-processed events into ClickHouse so the same events back both
// SQL dashboards and (optionally) the heartbeat-freshness state store.
package sink

import (
	"context"

	"github.com/mstone/fleetpulse/internal/ingest"
)

// Sink receives events that the worker pool has fully processed (state
// update and enrichment both succeeded) and the outcome of each capture's
// mapping call. Implementations must be safe for concurrent use: every
// worker goroutine calls Write.
type Sink interface {
	// Write persists one successfully processed event.
	Write(ctx context.Context, evt ingest.Event) error

	// RecordMapping persists the outcome of one Workflow Mapping call made
	// while enriching a capture event. reason is empty on success and the
	// error text on failure.
	RecordMapping(ctx context.Context, success bool, reason string) error
}

// NoopSink is the default Sink. It keeps the existing memory-only pipeline
// byte-for-byte identical to its pre-sink behavior.
type NoopSink struct{}

func (NoopSink) Write(context.Context, ingest.Event) error { return nil }

func (NoopSink) RecordMapping(context.Context, bool, string) error { return nil }

// Ensure NoopSink implements Sink.
var _ Sink = NoopSink{}
