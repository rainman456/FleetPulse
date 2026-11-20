# FleetPulse

Observability and ingestion muscle for a fleet of desktop-app /
browser-extension installs you don't control the internals of -- clients
"phone home" with heartbeats, capture events, and errors; FleetPulse ingests,
validates, queues, processes, and turns that into three concretely-defined
reliability signals with real thresholds, demoed against one fully-worked
failure scenario.

> **Scaffold status:** this repository was generated as a portfolio scaffold.
> Every file is real, substantive starter code, but **nothing in this repo
> has been built, run, or tested.** No `go build`, `go run`, `go mod tidy`,
> or `docker compose up` has been executed. Treat every command below as
> "the command that would be run next," not as something already verified to
> work. `go.sum` does not exist yet because `go mod tidy` hasn't run --
> `go.mod`'s dependency versions are hand-authored best guesses, not a
> resolved graph.
>
> This also covers everything added on top of the original scaffold: the
> optional ClickHouse backend, the Terraform AWS module, the benchmark tool,
> and the CI workflow. All are written, none executed. `docs/verify.md` is
> the ordered checklist for changing that, and every result table in
> `docs/benchmarks/` stays "not yet measured" until you personally run it.

## Problem statement

A fleet of installed clients you don't control (can't force-update, can't
SSH into, can't instrument beyond what you shipped) is phoning home. The
questions that matter operationally are simple to state and hard to answer
well:

- Is a given slice of the fleet still alive?
- Is the data it's sending actually getting through completely?
- Is the service downstream of ingestion, that the data has to flow through
  to be useful, actually up?

FleetPulse answers all three with a small number of precisely-defined
signals (see `docs/signals.md`) computed from real ingested traffic, rather
than either skipping the definition work or building an unbounded
"observability platform." The fleet itself is simulated (`cmd/simulator`) --
see **Scope** below for why that's the right tradeoff for this project.

## Scope (read this before wondering "where's the real client?")

This project deliberately does **not** build a real desktop app or browser
extension. The fleet is simulated by `cmd/simulator` spinning up N concurrent
goroutines, each a fixed-cohort fake client emitting realistic traffic over
HTTP. That's a genuine, useful abstraction boundary: everything downstream of
the ingestion endpoint (validation, queueing, worker processing, signal
computation, dashboards) can't tell the difference between a simulated
client and a real one, so building the real client would add engineering
effort without adding anything the observability stack needs to prove.

Similarly, this project fully demos **one** failure scenario end-to-end (a
cohort silently going dark -- see `docs/runbook.md`) rather than
shallowly touching several. That's a deliberate scope choice: one scenario
walked all the way from "what the dashboard shows" to "how you'd trace it to
a cause" is a stronger signal than three scenarios that only get as far as
"and then an alert would fire."

## Architecture

See `docs/architecture.md` for the full request/data-flow diagram (ASCII and
Mermaid) and component table. Short version:

```
simulator --> ingestion (validate/normalize) --> queue --> worker pool (enrich, retry)
                                                              |            |
                                                       client state    signals computer --> /metrics --> Prometheus --> Grafana
                                                       (state store)         ^
                                                                             |
                                                                 downstream stub (Workflow Mapping)
```

The queue and client-state store each have two implementations behind the
same interface: an in-process one (buffered channel / in-memory map) for
zero-dependency local development, and a LocalStack/real-AWS one (SQS /
DynamoDB, in `internal/aws`) for the one real AWS-shaped deployment used to
record a demo video. See `internal/queue`'s and `internal/state`'s package
docs for the tradeoffs.

There is a third, optional backend for the *event* side: ClickHouse
(`internal/clickhouse`), selected by `FLEETPULSE_SINK_BACKEND=clickhouse`.
The worker writes every processed event into ClickHouse (batched inserts),
and the signals computer's state reads (`All`/`Get`) become SQL aggregations
over that event log, so freshness and completeness are computed from the
same rows the Grafana ClickHouse dashboard queries. The default stays
`memory`; nothing about the existing flow changes unless the flag is set.

## Repository layout

```
cmd/
  server/       ingestion HTTP server + worker pool + /metrics (wires everything together)
  simulator/    fake fleet client generator
  downstream/   "Workflow Mapping" dependency stub, with on-demand failure injection
  bench/        ClickHouse ingest + signal-query benchmark (docs/benchmarks/clickhouse.md)
internal/
  ingest/       wire schema (RawEvent), validation/normalization, HTTP handler
  queue/        Queue interface + in-memory (buffered channel + visibility timeout) implementation
  worker/       worker pool: enrichment, retry/dead-letter, ack/nack against the queue
  signals/      computes + exposes the three cataloged signals as Prometheus metrics
  state/        per-client last-seen store, Store interface + in-memory implementation
  aws/          LocalStack/real-AWS-targeting SQS queue + DynamoDB store implementations
  clickhouse/   optional ClickHouse event sink + state.Store reader (SQL signal queries)
  sink/         tiny Sink interface the worker drains into (NoopSink default)
  simulator/    fleet simulator implementation (cohorts, per-client goroutine loop, kill-cohort timer)
  downstreamclient/  HTTP client the worker pool uses to call cmd/downstream
migrations/
  clickhouse/   idempotent SQL schema: events/heartbeats/mapping_results + materialized views
docs/
  signals.md        the signals catalog -- read this first
  architecture.md   data-flow diagram + LocalStack bootstrap commands
  roadmap.md        what to instrument next + a real protocol-versioning tradeoff writeup
  runbook.md        the silent-cohort-failure demo script, end to end
  aws-setup.md      Terraform for the SQS/DynamoDB pair, real AWS and LocalStack modes
  benchmarks/       benchmark templates ("not yet measured" until you run them)
  verify.md         the ordered checklist for proving any of this works
deploy/
  prometheus/       scrape config
  grafana/          datasource + dashboard provisioning (Prometheus + ClickHouse dashboards)
infra/
  terraform/aws/    SQS queue + DLQ + DynamoDB table module (see docs/aws-setup.md)
Makefile            up/down, ch-migrate, bench, tf-init/plan/apply/destroy (AWS_MODE=aws|localstack)
docker-compose.yml  server, simulator, downstream, clickhouse (+migrate), localstack, prometheus, grafana
.github/workflows/  CI: go build/vet/test, docker builds, terraform fmt/validate/tflint
```

## Setup (commands that WOULD be run -- none have been executed yet)

```bash
# 1. Resolve and download dependencies (go.mod was hand-authored; this has
#    not been run, so version pins may need adjusting once it is).
go mod tidy

# 2. Sanity-check everything compiles.
go build ./...

# 3a. Run the ingestion server + worker pool locally (in-memory queue/state,
#     no external dependencies required).
go run ./cmd/server

# 3b. In another terminal, run the downstream Workflow Mapping stub.
go run ./cmd/downstream

# 3c. In another terminal, run the fake fleet against the local server.
go run ./cmd/simulator --clients=300 --ingest-url=http://localhost:8080/v1/events

# 4. Check the signals.
curl http://localhost:8080/metrics | grep fleetpulse_

# 5. Or bring up the whole stack (server, simulator, downstream, LocalStack,
#    Prometheus, Grafana) via Docker Compose -- also not run/verified yet.
docker compose up --build
```

Grafana would be reachable at `http://localhost:3000` (default provisioned
dashboard: "FleetPulse Overview") and Prometheus at
`http://localhost:9090`.

### Running against LocalStack / AWS instead of in-memory

```bash
export FLEETPULSE_QUEUE_BACKEND=sqs
export FLEETPULSE_STATE_BACKEND=dynamodb
export FLEETPULSE_SQS_QUEUE_URL=http://localhost:4566/000000000000/fleetpulse-events
export FLEETPULSE_DYNAMODB_TABLE=fleetpulse-client-state
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

go run ./cmd/server
```

This requires the LocalStack queue/table to already exist -- see
`docs/architecture.md`'s bootstrap commands, or provision them with the
Terraform module in **LocalStack mode** (next section). Not run as part of
this scaffold.

On the app side, `internal/aws` routes every SDK call by one rule: if
`AWS_ENDPOINT_URL` is set, SQS and DynamoDB go there (LocalStack); if it is
unset, the standard AWS credential chain is used (environment, shared
profile, SSO, instance/task role) and dummy credentials are never injected.
`make tf-apply-local` creates the LocalStack resources via Terraform instead
of the older `awslocal` bootstrap.

## AWS with Terraform (real AWS or LocalStack)

`infra/terraform/aws` provisions exactly what `internal/aws` expects: the
`fleetpulse-events` SQS queue plus a dead-letter queue (redrive after 5
receives), and the `fleetpulse-client-state` DynamoDB table
(PAY_PER_REQUEST). Everything is tagged `project=fleetpulse` so it is easy
to find and delete.

```bash
# LocalStack mode (no real AWS account touched)
make tf-init AWS_MODE=localstack
make tf-plan  AWS_MODE=localstack
make tf-apply-local                 # apply against dockerized LocalStack

# Real AWS mode (uses your normal credential chain; nothing is committed)
make tf-init
make tf-plan
make tf-apply

# Teardown for either mode
make tf-destroy AWS_MODE=localstack   # or plain `make tf-destroy`
```

Both modes, the exported outputs, the teardown checklist, and a reminder to
set an AWS Budgets alert before the first real-AWS apply are in
`docs/aws-setup.md`. Terraform state and any real tfvars are gitignored.

## ClickHouse backend (optional)

```bash
make ch-migrate      # or: docker compose run --rm clickhouse-migrate
FLEETPULSE_SINK_BACKEND=clickhouse make run-server
```

- `FLEETPULSE_SINK_BACKEND=memory|clickhouse` (default `memory`); connection
  via `FLEETPULSE_CLICKHOUSE_ADDR` (native protocol, default
  `localhost:9000`), `FLEETPULSE_CLICKHOUSE_DB` (default `fleetpulse`),
  `FLEETPULSE_CLICKHOUSE_USER` / `FLEETPULSE_CLICKHOUSE_PASSWORD`.
- Writes are batched (500 rows / 1s flush) and retried with backoff; a batch
  that still fails is logged and dropped rather than blocking the pipeline.
- The three signal queries live in `internal/clickhouse/queries.go` and are
  mirrored by the provisioned Grafana dashboard "FleetPulse (ClickHouse)"
  (uid `fleetpulse-clickhouse`). `docs/signals.md` documents which parts are
  SQL-backed and the one deliberate caveat (mapping availability).
- `docs/runbook.md` gained a "Tracing the dark cohort in ClickHouse"
  section with the exact SQL to run during the demo scenario.

## Benchmarks

`cmd/bench` generates synthetic fleet traffic, batch-inserts it into
ClickHouse, and times the signal queries:

```bash
go run ./cmd/bench -events 1000000 -batch 5000 -concurrency 4
go run ./cmd/bench -query-only          # just time the queries
```

Results go to `docs/benchmarks/clickhouse.md` -- which is a template marked
**not yet measured** until someone runs it on real hardware and fills in
the numbers and machine specs.

## The silent-cohort-failure demo

This is the project's single fully-worked demo scenario:

```bash
go run ./cmd/simulator \
  --clients=300 \
  --ingest-url=http://localhost:8080/v1/events \
  --kill-cohort=2.6.0-beta \
  --kill-cohort-after=2m
```

Two minutes in, every simulated client on version `2.6.0-beta` goes
completely silent -- no heartbeats, no captures, nothing -- exactly as a real
cohort would if a bad update broke its networking. `docs/runbook.md` walks
through what the dashboard shows, how to confirm it's a real incident rather
than a false alarm, and how you'd trace it down to a cause, step by step.

## Signals

Three signals, each with a precise formula and a stated target threshold:
**heartbeat freshness**, **capture completeness**, and **mapping
availability**. Full definitions, rationale, and thresholds are in
`docs/signals.md` -- per the project's own planning notes, that document is
meant to do more work proving seniority than any single file of code here.

## What's intentionally not built

- A real desktop app or browser extension (see **Scope** above).
- More than one fully-demoed failure scenario.
- The Temporal/Kinesis/SageMaker/ECS-scale version of this idea that a
  broader draft of this project considered -- deliberately out of scope for
  a portfolio-sized scaffold; see `docs/roadmap.md` for what a next
  iteration would prioritize instead.
- An admin HTTP endpoint to unsilence a cohort at runtime (referenced as a
  gap in `docs/runbook.md`).
