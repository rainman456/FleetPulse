// Command bench is a ClickHouse signal-query benchmark, not a load test of
// the ingestion HTTP endpoint. It synthesizes fleet events (default 1M across
// ~300 clients and 4 cohorts, with an 8% capture-fragment loss rate), inserts
// them in batches, then times the three catalog signal queries
// (internal/clickhouse/queries.go) and prints a Markdown table on stdout.
//
// It writes no files and needs a running ClickHouse with the migrations
// applied:
//
//	make ch-migrate
//	go run ./cmd/bench -events 1000000 -batch 5000 -concurrency 4
//
// The numbers belong in docs/benchmarks/clickhouse.md, together with the
// machine specs; the template there says "not yet measured" until someone
// actually runs this.
package main

import (
	"context"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	chgo "github.com/ClickHouse/clickhouse-go/v2"

	fleetch "github.com/mstone/fleetpulse/internal/clickhouse"
)

// queryResult is one row of the printed results table.
type queryResult struct {
	name   string
	median time.Duration
	p95    time.Duration
}

func main() {
	var (
		totalEvents = flag.Int("events", 1_000_000, "number of synthetic events to generate (ignored with -query-only)")
		batchSize   = flag.Int("batch", 5000, "rows per INSERT batch")
		addr        = flag.String("addr", envString("FLEETPULSE_CLICKHOUSE_ADDR", fleetch.DefaultAddr), "ClickHouse native address")
		database    = flag.String("database", envString("FLEETPULSE_CLICKHOUSE_DB", fleetch.DefaultDatabase), "ClickHouse database")
		username    = flag.String("user", envString("FLEETPULSE_CLICKHOUSE_USER", fleetch.DefaultUsername), "ClickHouse user")
		password    = flag.String("password", os.Getenv("FLEETPULSE_CLICKHOUSE_PASSWORD"), "ClickHouse password (prefer the env var)")
		concurrency = flag.Int("concurrency", 4, "concurrent inserter goroutines")
		queryOnly   = flag.Bool("query-only", false, "skip inserts and only time the signal queries")
		runs        = flag.Int("runs", 7, "number of timed runs per signal query")
	)

	flag.Parse()

	if *batchSize <= 0 || *concurrency <= 0 || *runs <= 0 {
		fmt.Fprintln(os.Stderr, "-batch, -concurrency and -runs must be > 0")
		os.Exit(2)
	}

	if err := run(*totalEvents, *batchSize, *addr, *database, *username, *password, *concurrency, *runs, *queryOnly); err != nil {
		fmt.Fprintln(os.Stderr, "bench failed:", err)
		os.Exit(1)
	}
}

func run(totalEvents, batchSize int, addr, database, username, password string, concurrency, runs int, queryOnly bool) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	conn, err := chgo.Open(&chgo.Options{
		Addr:         []string{addr},
		Auth:         chgo.Auth{Database: database, Username: username, Password: password},
		DialTimeout:  10 * time.Second,
		MaxOpenConns: concurrency + 2,
		MaxIdleConns: concurrency + 2,
	})
	if err != nil {
		return fmt.Errorf("open clickhouse: %w", err)
	}
	defer conn.Close()

	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	err = conn.Ping(pingCtx)
	pingCancel()
	if err != nil {
		return fmt.Errorf("ping clickhouse at %s: %w", addr, err)
	}

	var inserted, lost int64
	var insertDuration time.Duration

	if !queryOnly {
		start := time.Now()
		if err := generateAndInsert(ctx, conn, totalEvents, batchSize, concurrency, &inserted, &lost); err != nil {
			return err
		}
		insertDuration = time.Since(start)

		if err := insertMappingResults(ctx, conn, totalEvents/10, batchSize, 0.97); err != nil {
			return fmt.Errorf("insert mapping results: %w", err)
		}
	}

	signals := []struct {
		name string
		fn   func(context.Context) error
	}{
		{"heartbeat freshness by cohort", func(ctx context.Context) error {
			return scanCohortRatio(ctx, conn, fleetch.QueryFreshnessByCohort)
		}},
		{"capture completeness by cohort", func(ctx context.Context) error {
			return scanCohortRatio(ctx, conn, fleetch.QueryCompletenessByCohort)
		}},
		{"mapping availability", func(ctx context.Context) error {
			return scanSingleRatio(ctx, conn, fleetch.QueryMappingAvailability)
		}},
	}

	results := make([]queryResult, 0, len(signals))
	for _, sig := range signals {
		samples := make([]time.Duration, 0, runs)
		for i := 0; i < runs; i++ {
			queryCtx, queryCancel := context.WithTimeout(ctx, 60*time.Second)
			start := time.Now()
			err := sig.fn(queryCtx)
			queryCancel()
			if err != nil {
				return fmt.Errorf("query %q: %w", sig.name, err)
			}
			samples = append(samples, time.Since(start))
		}
		results = append(results, queryResult{
			name:   sig.name,
			median: percentile(samples, 0.50),
			p95:    percentile(samples, 0.95),
		})
	}

	printReport(totalEvents, batchSize, concurrency, addr, database, queryOnly, runs, inserted, lost, insertDuration, results)
	return nil
}

// eventRow is the column order of fleetpulse.events.
type eventRow struct {
	ts        time.Time
	clientID  string
	typ       string
	version   string
	os        string
	region    string
	cohort    string
	sessionID string
	seq       uint32
	expected  uint32
	errorCode string
	message   string
}

var benchVersions = []string{"2.4.0", "2.5.0", "2.5.1", "2.6.0-beta"}

func generateAndInsert(ctx context.Context, conn chgo.Conn, totalEvents, batchSize, concurrency int, inserted, lost *int64) error {
	clientIDs := make([]string, 300)
	clientVersion := make([]string, 300)
	for i := range clientIDs {
		clientIDs[i] = fmt.Sprintf("bench-client-%05d", i)
		clientVersion[i] = benchVersions[i%len(benchVersions)]
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	batches := make(chan []eventRow, concurrency*2)
	errCh := make(chan error, concurrency+1)

	var wg sync.WaitGroup
	for w := 0; w < concurrency; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for rows := range batches {
				if err := insertEvents(ctx, conn, rows); err != nil {
					select {
					case errCh <- err:
					default:
					}
					cancel()
					return
				}
				atomic.AddInt64(inserted, int64(len(rows)))
			}
		}()
	}

	// Producer: deterministic (seed 42) so repeated runs insert comparable
	// workloads. Timestamps spread over the last hour so time-window queries
	// have a realistic amount of data in each bucket.
	produceErr := func() error {
		rng := rand.New(rand.NewSource(42))
		base := time.Now().UTC()
		captureSeq := make(map[string]uint32, len(clientIDs))

		for produced := 0; produced < totalEvents; {
			size := batchSize
			if remaining := totalEvents - produced; remaining < size {
				size = remaining
			}
			rows := make([]eventRow, 0, size)

			for len(rows) < size {
				idx := rng.Intn(len(clientIDs))
				clientID := clientIDs[idx]
				version := clientVersion[idx]
				cohort := version + "/windows/us-east"
				ts := base.Add(-time.Duration(rng.Intn(3600)) * time.Second)

				row := eventRow{
					ts:       ts,
					clientID: clientID,
					version:  version,
					os:       "windows",
					region:   "us-east",
					cohort:   cohort,
				}

				switch roll := rng.Float64(); {
				case roll < 0.70:
					row.typ = "heartbeat"
				case roll < 0.95:
					row.typ = "capture"
					seq := captureSeq[clientID]
					captureSeq[clientID] = seq + 1
					row.sessionID = fmt.Sprintf("%s-s-%d", clientID, seq/20)
					row.seq = seq % 20
					row.expected = 20
					// 8% of capture fragments never arrive, mirroring the
					// simulator's default CaptureLossRate.
					if rng.Float64() < 0.08 {
						atomic.AddInt64(lost, 1)
						produced++
						continue
					}
				default:
					row.typ = "error"
					row.errorCode = "client_error"
					row.message = "simulated benchmark error"
				}

				rows = append(rows, row)
				produced++
			}

			select {
			case batches <- rows:
			case <-ctx.Done():
				return nil
			}
		}
		return nil
	}()

	close(batches)
	wg.Wait()

	if produceErr != nil {
		return produceErr
	}
	select {
	case err := <-errCh:
		return err
	default:
		return nil
	}
}

func insertEvents(ctx context.Context, conn chgo.Conn, rows []eventRow) error {
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO events")
	if err != nil {
		return fmt.Errorf("prepare events batch: %w", err)
	}
	for _, r := range rows {
		if err := batch.Append(
			r.ts, r.clientID, r.typ, r.version, r.os, r.region, r.cohort,
			r.sessionID, r.seq, r.expected, r.errorCode, r.message,
		); err != nil {
			_ = batch.Abort()
			return fmt.Errorf("append event row: %w", err)
		}
	}
	if err := batch.Send(); err != nil {
		return fmt.Errorf("send events batch: %w", err)
	}
	return nil
}

func insertMappingResults(ctx context.Context, conn chgo.Conn, total, batchSize int, successRate float64) error {
	if total <= 0 {
		return nil
	}
	rng := rand.New(rand.NewSource(7))
	base := time.Now().UTC()

	for produced := 0; produced < total; {
		size := batchSize
		if remaining := total - produced; remaining < size {
			size = remaining
		}
		batch, err := conn.PrepareBatch(ctx, "INSERT INTO mapping_results")
		if err != nil {
			return fmt.Errorf("prepare mapping_results batch: %w", err)
		}
		for i := 0; i < size; i++ {
			var success uint8
			reason := ""
			if rng.Float64() < successRate {
				success = 1
			} else {
				reason = "simulated mapping failure"
			}
			ts := base.Add(-time.Duration(rng.Intn(3600)) * time.Second)
			if err := batch.Append(ts, success, reason); err != nil {
				_ = batch.Abort()
				return fmt.Errorf("append mapping row: %w", err)
			}
		}
		if err := batch.Send(); err != nil {
			return fmt.Errorf("send mapping_results batch: %w", err)
		}
		produced += size
	}
	return nil
}

func scanCohortRatio(ctx context.Context, conn chgo.Conn, query string) error {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var cohort string
		var ratio float64
		if err := rows.Scan(&cohort, &ratio); err != nil {
			return err
		}
	}
	return rows.Err()
}

func scanSingleRatio(ctx context.Context, conn chgo.Conn, query string) error {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return rows.Err()
	}
	var ratio float64
	if err := rows.Scan(&ratio); err != nil {
		return err
	}
	return rows.Err()
}

func percentile(samples []time.Duration, p float64) time.Duration {
	if len(samples) == 0 {
		return 0
	}
	sorted := append([]time.Duration(nil), samples...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	idx := int(math.Ceil(p*float64(len(sorted)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func printReport(totalEvents, batchSize, concurrency int, addr, database string, queryOnly bool, runs int, inserted, lost int64, insertDuration time.Duration, results []queryResult) {
	fmt.Println("# FleetPulse ClickHouse signal-query benchmark")
	fmt.Println()
	fmt.Printf("- Go: %s\n", runtime.Version())
	fmt.Printf("- GOOS/GOARCH: %s/%s\n", runtime.GOOS, runtime.GOARCH)
	fmt.Printf("- NumCPU: %d\n", runtime.NumCPU())
	fmt.Printf("- ClickHouse: %s (database %s)\n", addr, database)
	if queryOnly {
		fmt.Println("- Mode: query-only (no inserts)")
	} else {
		fmt.Printf("- Events requested: %d, inserted: %d, capture fragments dropped (8%% loss): %d\n", totalEvents, inserted, lost)
		fmt.Printf("- Batch size: %d, insert concurrency: %d\n", batchSize, concurrency)
		fmt.Printf("- Insert wall time: %.2fs", insertDuration.Seconds())
		if insertDuration > 0 {
			fmt.Printf(" (%.0f events/s)", float64(inserted)/insertDuration.Seconds())
		}
		fmt.Println()
	}
	fmt.Printf("- Timed runs per query: %d (median and p95)\n\n", runs)

	fmt.Println("| signal | median (ms) | p95 (ms) |")
	fmt.Println("|---|---:|---:|")
	for _, r := range results {
		fmt.Printf("| %s | %.1f | %.1f |\n", r.name, durationMS(r.median), durationMS(r.p95))
	}
	fmt.Println()
	fmt.Println("Copy this table into docs/benchmarks/clickhouse.md with the machine specs.")
}

func durationMS(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000.0
}

func envString(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
