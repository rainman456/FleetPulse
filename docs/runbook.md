# Runbook: silent-cohort-failure

This is FleetPulse's one fully-demoed failure scenario (per the project's own
scope discipline: one scenario demoed end-to-end beats three shallow ones).
It walks through diagnosing "a version of the client has gone dark" from
what the dashboard shows down to the simulated root cause, the way an
on-call engineer would actually work the problem.

## Scenario

The simulator is started with a kill-cohort configured, e.g.:

```bash
go run ./cmd/simulator \
  --clients=300 \
  --ingest-url=http://localhost:8080/v1/events \
  --kill-cohort=2.6.0-beta \
  --kill-cohort-after=2m
```

Two minutes in, every simulated client whose version is `2.6.0-beta` (an
~8% slice of the fleet by default -- see `internal/simulator/cohort.go`'s
`versionWeights`) stops sending any traffic at all: no heartbeats, no
capture events, nothing. This models a real failure mode of installed
software that can't self-report its own death -- a broken update, a crashed
process, a networking regression -- where the absence of signal *is* the
signal.

## Step 1: the alert / what the dashboard shows

`fleetpulse_heartbeat_freshness_ratio{version="2.6.0-beta", ...}` drops from
its steady-state value (should be at or near 1.0 in a healthy fleet) toward
0 over the course of the 60-second freshness window
(`docs/signals.md` section 1), while every other version's freshness ratio
stays flat and healthy. On the Grafana "FleetPulse Overview" dashboard
(`deploy/grafana/provisioning/dashboards/fleetpulse-overview.json`), this is
the top-left panel: one line among several drops to zero while the rest
hold steady -- this is deliberately the first panel on the dashboard because
it's the fastest signal to notice a cohort going dark.

Per the target thresholds in `docs/signals.md`, this crosses the **page**
condition specifically because it's a single cohort dropping to near-zero
freshness while representing more than 1% of fleet traffic, not because the
fleet-wide aggregate moved much (an aggregate-only view would likely still
show >90% fleet-wide freshness, which is exactly why the signal is labeled
per-cohort in the first place).

## Step 2: confirm it's not a false alarm

Before treating this as a real incident:

1. Check `fleetpulse_ingest_events_rejected_total` -- if rejections spiked
   for that cohort's traffic right before the drop, this might be a
   validation regression (e.g. a client build started sending malformed
   timestamps) rather than a client actually going silent. In the simulated
   scenario this will be flat, because the clients aren't sending anything
   at all, malformed or otherwise -- that distinction itself is diagnostic.
2. Check `fleetpulse_queue_backlog` and `fleetpulse_queue_in_flight` -- if
   the queue is backed up, freshness could be lagging because processed
   heartbeats haven't updated client state yet, not because heartbeats
   stopped arriving. In this scenario backlog stays normal, because there's
   genuinely nothing from that cohort arriving to back up.
3. Confirm capture completeness for the same cohort also degrades (see
   `fleetpulse_capture_completeness_ratio{version="2.6.0-beta",...}`) --
   this should also fall toward zero (no data at all, not just missing
   heartbeats), since a client that's gone completely dark stops sending
   captures too. If freshness dropped but completeness stayed healthy, that
   would point toward a heartbeat-specific bug rather than a fully silent
   client, which is a different (narrower) incident.

## Step 3: trace to a cause

In a real fleet, the next step would be pulling logs/crash reports for a
sample of client IDs in the affected cohort (FleetPulse's simulated clients
don't produce those, since they're goroutines, not real installs -- this is
the honest boundary of what a simulated fleet can demonstrate). What
FleetPulse *can* show at this layer:

- `internal/state.Store.Get(clientID)` for a specific client ID in the
  affected cohort shows its `LastHeartbeatAt` frozen at the moment the kill-
  cohort timer fired -- confirming the client didn't degrade gradually, it
  stopped instantaneously, which is itself a diagnostic clue (gradual decay
  points toward a resource leak or slow degradation; an instantaneous stop
  points toward a crash, a killed process, or a network path that was cut
  off all at once).
- Cross-referencing the affected `version` against a release calendar (in a
  real org, this is "what shipped to the `2.6.0-beta` channel recently") is
  the step that turns "this cohort is silent" into "this cohort is silent
  *because of build X*" -- FleetPulse's contribution stops at pinpointing
  the *which* and *when*, which is the expensive part to build well and the
  part this project focuses on.

## Step 4: mitigate and confirm recovery

For the simulated scenario, "mitigation" is calling
`SilencedCohorts.Unsilence("2.6.0-beta")` (not currently wired to an admin
HTTP endpoint in this scaffold -- see roadmap for what's not built yet) or
simply restarting the simulator without `--kill-cohort` set. In a real
incident, mitigation is whatever fixes the actual client-side cause (a
hotfix release, a server-side compatibility shim, a rollback of the bad
build).

Confirmation of recovery is the same panel, in reverse: watch
`fleetpulse_heartbeat_freshness_ratio{version="2.6.0-beta",...}` climb back
toward 1.0 as clients resume heartbeating, and note that the panel uses a
trailing 60s window (not an all-time average), so recovery is visible within
about one freshness window of the fix actually taking effect -- not
gradually diluted into a long-running average the way an all-time ratio
would be. The same fast-recovery design choice is used for mapping
availability in `docs/signals.md` section 3, for the same reason.

## Postmortem note

The single most important design decision this runbook depends on is that
**freshness and completeness are labeled by cohort, not just reported in
aggregate.** An aggregate-only dashboard would have shown this exact
incident as "fleet-wide heartbeat freshness dipped from 100% to 92%" -- a
number many teams wouldn't page on at all -- instead of "one specific,
identifiable version's fleet went completely dark," which is both more
alarming and dramatically easier to act on. Any future signal added to this
catalog should default to per-cohort labeling unless there's a specific
reason not to.

---

## Tracing the dark cohort in ClickHouse

The steps above use the Prometheus dashboard and the in-process state. If
`FLEETPULSE_SINK_BACKEND=clickhouse` is on, the same questions can be asked
directly of the event log -- which is how you would interrogate history
that Prometheus has already aged out of its retention window. All queries
below are copy-pasteable into `docker compose exec clickhouse clickhouse-client`
and match the constants in `internal/clickhouse/queries.go`.

**Step A -- is the cohort really dark, or just quiet?** Freshness per
cohort, now and five minutes ago (the version must appear at all; an empty
result for `2.6.0-beta` in the recent window while other cohorts show 1.0
is itself the answer):

```sql
SELECT cohort,
       uniqExactIf(client_id, type = 'heartbeat' AND ts >= now() - INTERVAL 60 SECOND)
         / uniqExact(client_id) AS freshness
FROM events
WHERE ts >= now() - INTERVAL 10 MINUTE
GROUP BY cohort
ORDER BY cohort;
```

**Step B -- did ingestion reject the traffic, or did it never arrive?** The
same cross-check as runbook step 2, but against the event log: compare the
per-type event rate for the cohort before and after the kill time. A cliff
in heartbeat volume with no matching spike in rejects (which never reach
ClickHouse anyway -- validation happens before the sink) means the clients
stopped sending, not that the pipeline dropped them:

```sql
SELECT toStartOfMinute(ts) AS minute, type, count() AS events
FROM events
WHERE version = '2.6.0-beta'
  AND ts >= now() - INTERVAL 15 MINUTE
GROUP BY minute, type
ORDER BY minute;
```

**Step C -- per-client last activity.** Which clients in the cohort went
silent at exactly the same instant (a kill switch) versus trailing off
gradually (a crash loop)? This uses the `cohort_last_seen` materialized
view:

```sql
SELECT client_id, maxIfMerge(last_heartbeat_at) AS last_heartbeat_at
FROM cohort_last_seen
WHERE cohort = '2.6.0-beta/Linux/x64'
GROUP BY client_id
ORDER BY last_heartbeat_at DESC
LIMIT 20;
```

(Adjust the cohort string to the version/os/region slice you killed --
`cohort` is `version/os/region`, as in `internal/simulator/cohort.go`.)

**Step D -- completeness at the moment the cohort died.** Sessions that
were in flight when the clients went silent will show
`received < expected` forever; that residual incompleteness is the data the
fleet never got to send:

```sql
SELECT session_id,
       uniqExactMerge(sequences)  AS received,
       maxMerge(expected_events)  AS expected
FROM session_capture_progress
WHERE cohort LIKE '2.6.0-beta/%'
GROUP BY session_id
HAVING received < expected
ORDER BY expected - received DESC;
```

**Step E -- downstream sanity.** If mapping availability (from
`mapping_results`) is unchanged, the Workflow Mapping dependency was healthy
at the time, which rules out a downstream outage as the cause and keeps the
focus on the silent cohort itself:

```sql
SELECT if(count() = 0, 1.0, countIf(success = 1) / count()) AS mapping_availability
FROM mapping_results
WHERE ts >= now() - INTERVAL 5 MINUTE;
```

The diagnostic chain is the same as the Prometheus-based steps 1-4 above:
freshness first (is it dark?), then ingestion counters (did we drop it?),
then per-client timestamps (instantaneous or gradual?), then mitigation.
ClickHouse just makes every step replayable over a longer window.
