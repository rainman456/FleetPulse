// Package clickhouse implements an optional ClickHouse-backed event sink and
// client-state store.
//
// Two roles, one type:
//
//   - sink.Sink: every event the worker pool processes successfully is
//     written to the events table (heartbeats additionally go to the compact
//     heartbeats table), and every capture's mapping outcome is written to
//     mapping_results.
//   - state.Store: All/Get are implemented as SQL aggregations over the
//     events table so the existing signals computer can keep its
//     heartbeat-freshness logic unchanged while reading from ClickHouse.
//     Upsert is intentionally a no-op: ClickHouse is an append-only event
//     log, the worker's sink write is the only write path.
//
// Writes are buffered (channel + ticker, batch size 500, flush every 1s) and
// inserted with clickhouse-go's native protocol. A failed batch is retried
// with exponential backoff; a batch that still fails is logged and dropped
// rather than blocking the pipeline forever.
package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"sync"
	"time"

	chgo "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/mstone/fleetpulse/internal/ingest"
	"github.com/mstone/fleetpulse/internal/sink"
	"github.com/mstone/fleetpulse/internal/state"
)

const (
	// DefaultAddr is clickhouse-go's native protocol port (HTTP is 8123).
	DefaultAddr = "localhost:9000"
	// DefaultDatabase matches migrations/clickhouse/*.sql.
	DefaultDatabase = "fleetpulse"
	// DefaultUsername is ClickHouse's built-in user for local development.
	DefaultUsername = "default"
	// DefaultBatchSize and DefaultFlushInterval bound write latency and
	// memory; 500 rows / 1s keeps a 300-client demo dashboard fresh.
	DefaultBatchSize     = 500
	DefaultFlushInterval = time.Second
	DefaultQueueSize     = 65536
)

var errClosed = errors.New("clickhouse store is closed")

var databaseNameRE = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// Config controls the ClickHouse connection and write buffering.
type Config struct {
	Addr          string
	Database      string
	Username      string
	Password      string
	BatchSize     int
	FlushInterval time.Duration
	QueueSize     int
}

// ConfigFromEnv reads FLEETPULSE_CLICKHOUSE_* environment variables. The
// password is read from the environment only; it is never logged.
func ConfigFromEnv() Config {
	return Config{
		Addr:          envString("FLEETPULSE_CLICKHOUSE_ADDR", DefaultAddr),
		Database:      envString("FLEETPULSE_CLICKHOUSE_DB", DefaultDatabase),
		Username:      envString("FLEETPULSE_CLICKHOUSE_USER", DefaultUsername),
		Password:      os.Getenv("FLEETPULSE_CLICKHOUSE_PASSWORD"),
		BatchSize:     DefaultBatchSize,
		FlushInterval: DefaultFlushInterval,
		QueueSize:     DefaultQueueSize,
	}
}

type writeKind int

const (
	writeEvent writeKind = iota
	writeMapping
)

type writeOp struct {
	kind    writeKind
	evt     ingest.Event
	success bool
	reason  string
	at      time.Time
}

// Store is the ClickHouse-backed sink + state store. It is safe for
// concurrent use.
type Store struct {
	conn clickhouseConn

	cfg    Config
	logger *slog.Logger

	ops      chan writeOp
	flushReq chan chan error
	done     chan struct{}

	wg        sync.WaitGroup
	closeOnce sync.Once
	closeErr  error
}

// clickhouseConn is the subset of clickhouse-go's Conn we use; keeping it an
// interface keeps the batching logic separately reviewable.
type clickhouseConn interface {
	PrepareBatch(ctx context.Context, query string) (driver.Batch, error)
	Query(ctx context.Context, query string, args ...any) (driver.Rows, error)
	Ping(ctx context.Context) error
	Close() error
}

var (
	_ sink.Sink   = (*Store)(nil)
	_ state.Store = (*Store)(nil)
)

// New opens a native-protocol connection, verifies it with Ping, and starts
// the background batching goroutine. The migrations under
// migrations/clickhouse/ must have been applied first (make ch-migrate).
func New(ctx context.Context, cfg Config, logger *slog.Logger) (*Store, error) {
	if logger == nil {
		logger = slog.Default()
	}
	if cfg.Addr == "" {
		cfg.Addr = DefaultAddr
	}
	if cfg.Database == "" {
		cfg.Database = DefaultDatabase
	}
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = DefaultBatchSize
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = DefaultFlushInterval
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = DefaultQueueSize
	}
	if !databaseNameRE.MatchString(cfg.Database) {
		return nil, fmt.Errorf("invalid ClickHouse database name %q", cfg.Database)
	}

	conn, err := chgo.Open(&chgo.Options{
		Addr:         []string{cfg.Addr},
		Auth:         chgo.Auth{Database: cfg.Database, Username: cfg.Username, Password: cfg.Password},
		DialTimeout:  5 * time.Second,
		MaxOpenConns: 10,
		MaxIdleConns: 5,
		Compression:  &chgo.Compression{Method: chgo.CompressionLZ4},
	})
	if err != nil {
		return nil, fmt.Errorf("open clickhouse: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("ping clickhouse at %s: %w", cfg.Addr, err)
	}

	s := &Store{
		conn:     conn,
		cfg:      cfg,
		logger:   logger,
		ops:      make(chan writeOp, cfg.QueueSize),
		flushReq: make(chan chan error),
		done:     make(chan struct{}),
	}
	s.wg.Add(1)
	go s.loop()

	logger.Info("clickhouse opened", "addr", cfg.Addr, "database", cfg.Database, "batch_size", cfg.BatchSize, "flush_interval", cfg.FlushInterval)
	return s, nil
}

// Write buffers one successfully processed event. It returns an error only if
// the store is closed or ctx is canceled, so the worker can retry the event.
func (s *Store) Write(ctx context.Context, evt ingest.Event) error {
	select {
	case <-s.done:
		return errClosed
	default:
	}

	select {
	case s.ops <- writeOp{kind: writeEvent, evt: evt, at: time.Now().UTC()}:
		return nil
	case <-s.done:
		return errClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// RecordMapping buffers one Workflow Mapping outcome.
func (s *Store) RecordMapping(ctx context.Context, success bool, reason string) error {
	select {
	case <-s.done:
		return errClosed
	default:
	}

	select {
	case s.ops <- writeOp{kind: writeMapping, success: success, reason: reason, at: time.Now().UTC()}:
		return nil
	case <-s.done:
		return errClosed
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Flush blocks until everything queued so far has been inserted.
func (s *Store) Flush(ctx context.Context) error {
	req := make(chan error, 1)
	select {
	case s.flushReq <- req:
	case <-s.done:
		return errClosed
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-req:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Close drains buffered writes, flushes them, and closes the connection. It is
// idempotent.
func (s *Store) Close() error {
	s.closeOnce.Do(func() {
		close(s.done)
		s.wg.Wait()
		s.closeErr = s.conn.Close()
	})
	return s.closeErr
}

func (s *Store) loop() {
	defer s.wg.Done()

	ticker := time.NewTicker(s.cfg.FlushInterval)
	defer ticker.Stop()

	pending := make([]writeOp, 0, s.cfg.BatchSize)

	flush := func(ctx context.Context) {
		if len(pending) == 0 {
			return
		}
		if err := s.flushOps(ctx, pending); err != nil {
			s.logger.Error("clickhouse flush failed; batch dropped", "rows", len(pending), "error", err)
		}
		pending = pending[:0]
	}

	drain := func() {
		for {
			select {
			case op := <-s.ops:
				pending = append(pending, op)
			default:
				return
			}
		}
	}

	for {
		select {
		case op := <-s.ops:
			pending = append(pending, op)
			if len(pending) >= s.cfg.BatchSize {
				flush(context.Background())
			}
		case <-ticker.C:
			flush(context.Background())
		case req := <-s.flushReq:
			drain()
			flush(context.Background())
			req <- nil
		case <-s.done:
			drain()
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			flush(ctx)
			cancel()
			return
		}
	}
}

func (s *Store) flushOps(ctx context.Context, ops []writeOp) error {
	var events []ingest.Event
	var mappings []writeOp
	for _, op := range ops {
		switch op.kind {
		case writeEvent:
			events = append(events, op.evt)
		case writeMapping:
			mappings = append(mappings, op)
		}
	}

	if len(events) > 0 {
		if err := s.retry(ctx, "events", func() error { return s.insertEvents(ctx, events) }); err != nil {
			return err
		}
	}
	if len(mappings) > 0 {
		if err := s.retry(ctx, "mapping_results", func() error { return s.insertMappings(ctx, mappings) }); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) retry(ctx context.Context, label string, fn func() error) error {
	backoff := 250 * time.Millisecond
	var lastErr error
	for attempt := 1; attempt <= 5; attempt++ {
		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
	return fmt.Errorf("%s insert failed after 5 attempts: %w", label, lastErr)
}

// insertEvents writes the events batch, then the heartbeats subset into the
// compact heartbeats table. The two are separate statements so a duplicate
// insert of the compact table can never duplicate rows in events.
func (s *Store) insertEvents(ctx context.Context, evts []ingest.Event) error {
	eventBatch, err := s.conn.PrepareBatch(ctx, "INSERT INTO events")
	if err != nil {
		return fmt.Errorf("prepare events batch: %w", err)
	}
	for _, evt := range evts {
		if err := eventBatch.Append(
			evt.Timestamp.UTC(),
			evt.ClientID,
			string(evt.Type),
			evt.Cohort.Version,
			evt.Cohort.OS,
			evt.Cohort.Region,
			cohortKey(evt.Cohort),
			evt.SessionID,
			uint32(evt.SequenceNumber),
			uint32(evt.ExpectedEventCount),
			errorCode(evt),
			evt.ErrorMessage,
		); err != nil {
			_ = eventBatch.Abort()
			return fmt.Errorf("append events row: %w", err)
		}
	}
	if err := eventBatch.Send(); err != nil {
		return fmt.Errorf("send events batch: %w", err)
	}

	heartbeats := make([]ingest.Event, 0, len(evts))
	for _, evt := range evts {
		if evt.Type == ingest.EventHeartbeat {
			heartbeats = append(heartbeats, evt)
		}
	}
	if len(heartbeats) == 0 {
		return nil
	}

	hbBatch, err := s.conn.PrepareBatch(ctx, "INSERT INTO heartbeats")
	if err != nil {
		return fmt.Errorf("prepare heartbeats batch: %w", err)
	}
	for _, evt := range heartbeats {
		if err := hbBatch.Append(
			evt.Timestamp.UTC(),
			evt.ClientID,
			cohortKey(evt.Cohort),
			evt.Cohort.Version,
			evt.Cohort.OS,
			evt.Cohort.Region,
		); err != nil {
			_ = hbBatch.Abort()
			return fmt.Errorf("append heartbeats row: %w", err)
		}
	}
	if err := hbBatch.Send(); err != nil {
		return fmt.Errorf("send heartbeats batch: %w", err)
	}
	return nil
}

func (s *Store) insertMappings(ctx context.Context, ops []writeOp) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO mapping_results")
	if err != nil {
		return fmt.Errorf("prepare mapping_results batch: %w", err)
	}
	for _, op := range ops {
		var success uint8
		if op.success {
			success = 1
		}
		if err := batch.Append(op.at, success, op.reason); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append mapping_results row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send mapping_results batch: %w", err)
	}
	return nil
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func cohortKey(c ingest.Cohort) string {
	return c.Version + "/" + c.OS + "/" + c.Region
}

func errorCode(evt ingest.Event) string {
	if evt.Type == ingest.EventError {
		return "client_error"
	}
	return ""
}
