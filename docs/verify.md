# Verify

The one page to run before claiming any of this works. Every command below
is written to be copy-pasted in order; each block states what "good" looks
like. None of these commands were executed while the repository was authored
-- treat the expected output as a contract to check, not a recording.

Prerequisites: Go 1.24+, Docker with Compose v2, Terraform 1.6+ (CI pins
1.9.8), and (Terraform/AWS only) TFLint 0.53.0 if you want to reproduce CI
exactly.

## 1. Go module and build

```bash
go mod tidy          # writes go.sum; commit it (it is not gitignored)
git diff --exit-code go.mod go.sum   # pins should not move unexpectedly
go build ./...
go vet ./...
go test ./...
```

Pass: tidy makes no further changes on a second run, build/vet/test exit 0.
`go.sum` must exist in git; CI's `go mod verify` fails without it.

## 2. Compose file is structurally valid

```bash
docker compose config -q
```

Pass: exit 0, no output. This validates YAML and interpolation only -- it
does not start anything.

## 3. Full local stack, memory backends

```bash
docker compose up --build -d
docker compose ps
curl -sf localhost:8080/healthz
curl -s localhost:8080/metrics | grep '^fleetpulse_'
```

Pass: `server`, `simulator`, `downstream`, `prometheus`, `grafana`,
`localstack`, `clickhouse` all `running` (or `exited 0` for the one-shot
`clickhouse-migrate`); healthz returns 200; all three `fleetpulse_*` metric
families are present and moving.

Grafana at http://localhost:3000 has two dashboards: "FleetPulse Overview"
(Prometheus) and "FleetPulse (ClickHouse)".

## 4. ClickHouse backend enabled

```bash
docker compose run --rm clickhouse-migrate      # idempotent (IF NOT EXISTS)
FLEETPULSE_SINK_BACKEND=clickhouse \
FLEETPULSE_CLICKHOUSE_ADDR=localhost:9000 \
FLEETPULSE_CLICKHOUSE_DB=fleetpulse \
go run ./cmd/server
```

Then, after a minute of simulator traffic:

```bash
docker compose exec clickhouse clickhouse-client --query \
  "SELECT cohort, count() FROM fleetpulse.events GROUP BY cohort"
docker compose exec clickhouse clickhouse-client --query \
  "SELECT count() FROM fleetpulse.session_capture_progress"
docker compose exec clickhouse clickhouse-client --query \
  "SELECT count(), countIf(success = 1) FROM fleetpulse.mapping_results"
```

Pass: four cohorts with non-trivial counts, materialized-view rows > 0, and
mapping rows > 0 with a success ratio near 1.0 (the worker records every
enrichment attempt, so this proves the sink path is wired, not just writes).

## 5. Signal-query benchmark

```bash
go run ./cmd/bench -query-only            # smoke: queries run, times print
go run ./cmd/bench -events 1000000 -batch 5000 -concurrency 4
```

Pass: query-only exits 0 against a migrated database; the full run prints
insert rate and a three-row median/p95 table. Record the numbers and machine
specs in `docs/benchmarks/clickhouse.md`; leave them as "not yet measured"
until you personally ran it.

## 6. Terraform (no cloud calls)

```bash
terraform -chdir=infra/terraform/aws fmt -check -recursive
terraform -chdir=infra/terraform/aws init -backend=false
terraform -chdir=infra/terraform/aws validate
terraform -chdir=infra/terraform/aws validate -var-file=localstack.tfvars.example   # plan-time only
tflint --chdir=infra/terraform/aws --init
tflint --chdir=infra/terraform/aws --format compact
```

Pass: fmt silent, validate "Success", tflint no Issues found. No credentials
are needed for any of these.

## 7. Terraform against LocalStack

```bash
docker compose up -d localstack
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
terraform -chdir=infra/terraform/aws apply -var-file=localstack.tfvars.example
terraform -chdir=infra/terraform/aws output -json
awk --version >/dev/null 2>&1   # awslocal/aqs may not be installed; use the app instead
```

Pass: outputs include `queue_url`, `dlq_url`, `table_name`; the server started
with those values (`FLEETPULSE_QUEUE_BACKEND=sqs`,
`FLEETPULSE_STATE_BACKEND=dynamodb`, `AWS_ENDPOINT_URL` set) ingests traffic.
Then destroy and prove it:

```bash
terraform -chdir=infra/terraform/aws destroy -var-file=localstack.tfvars.example
aws --endpoint-url=$AWS_ENDPOINT_URL sqs list-queues     # must not list fleetpulse-*
aws --endpoint-url=$AWS_ENDPOINT_URL dynamodb list-tables # must not list fleetpulse-*
```

## 8. Real AWS (costs money -- see docs/aws-setup.md)

Only after step 7, and only with a budget alert configured. The pass
criterion is the same output/teardown sequence against real AWS, followed by
`aws sqs list-queues` and `aws dynamodb list-tables` showing the resources
gone. Never paste account IDs or keys into notes; the teardown proof is the
list output, redacted if needed.

## 9. CI equivalence

```bash
go mod verify && go build ./... && go vet ./... && go test ./...
docker build -f Dockerfile.server . && docker build -f Dockerfile.simulator . && docker build -f Dockerfile.downstream .
terraform -chdir=infra/terraform/aws fmt -check -recursive
terraform -chdir=infra/terraform/aws init -backend=false && terraform -chdir=infra/terraform/aws validate
tflint --chdir=infra/terraform/aws --init && tflint --chdir=infra/terraform/aws --format compact
```

Pass: identical to `.github/workflows/ci.yml`, all exit 0.

## Evidence checklist

- [ ] `go.sum` committed; `go mod tidy` re-run is a no-op
- [ ] `docker compose ps` showing every service up
- [ ] `/metrics` output with the three `fleetpulse_*` families
- [ ] ClickHouse row counts from step 4
- [ ] Benchmark table committed with machine specs
- [ ] Terraform outputs + post-destroy `list-*` output
- [ ] Screenshots of both dashboards, including the dark cohort
- [ ] Billing/budget alert screenshot (real AWS only)
