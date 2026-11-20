# AWS setup: SQS + DynamoDB for FleetPulse

The Terraform module in `infra/terraform/aws` provisions exactly what
`internal/aws` consumes at runtime:

| Resource | Name | Purpose |
|---|---|---|
| SQS queue | `${name_prefix}-events` | event queue (`FLEETPULSE_QUEUE_BACKEND=sqs`) |
| SQS DLQ | `${name_prefix}-events-dlq` | redrive target after 5 receives, 14-day retention |
| DynamoDB table | `${name_prefix}-client-state` | per-client state (`FLEETPULSE_STATE_BACKEND=dynamodb`), PAY_PER_REQUEST |

Everything carries `project=fleetpulse` via the provider's `default_tags`,
so `aws resourcegroupstaggingapi get-resources --tag-filters
Key=project,Values=fleetpulse` lists it all for cleanup.

Both modes share one module; `use_localstack` switches the provider wiring.
Nothing here has been executed against real AWS or LocalStack as part of
this scaffold -- `docs/verify.md` sections 6-7 are the checklist.

## Prerequisites

- Terraform >= 1.6 (CI pins 1.9.8), TFLint 0.53.0 if you want to reproduce
  CI exactly.
- Real AWS mode: a working AWS credential chain (`aws sso login`, a profile
  via `AWS_PROFILE`, or environment variables). **Set an AWS Budgets alert
  first** (console -> Billing -> Budgets, or `aws budgets create-budget`).
  The resources here are cheap (PAY_PER_REQUEST table, two empty queues),
  but the habit matters before any AWS work.

## Mode 1: real AWS

```bash
cd infra/terraform/aws
cp aws.tfvars.example aws.tfvars      # edit region/name_prefix/tags if needed
terraform init
terraform plan  -var-file=aws.tfvars
terraform apply -var-file=aws.tfvars
```

Credentials are never set in tfvars: the provider uses the standard
credential chain, exactly like the app. Wire the outputs into the server:

```bash
export FLEETPULSE_QUEUE_BACKEND=sqs
export FLEETPULSE_STATE_BACKEND=dynamodb
export FLEETPULSE_SQS_QUEUE_URL="$(terraform output -raw queue_url)"
export FLEETPULSE_DYNAMODB_TABLE="$(terraform output -raw table_name)"
export AWS_REGION="$(terraform output -raw region)"
# AWS_ENDPOINT_URL intentionally unset -> real AWS endpoints + credential chain
go run ./cmd/server
```

## Mode 2: LocalStack

Start LocalStack from compose (`docker compose up -d localstack`), then:

```bash
cd infra/terraform/aws
cp localstack.tfvars.example localstack.tfvars
terraform init
terraform apply -var-file=localstack.tfvars
```

`use_localstack = true` points the provider's `sqs`/`dynamodb` endpoints at
`localstack_endpoint` (default `http://localhost:4566`), uses dummy
`test`/`test` credentials (overridable via the sensitive `access_key` /
`secret_key` variables), and sets
`skip_credentials_validation` / `skip_metadata_api_check` /
`skip_requesting_account_id`. Real-AWS mode leaves all of that at secure
defaults.

The app talks to LocalStack through `AWS_ENDPOINT_URL`:

```bash
export FLEETPULSE_QUEUE_BACKEND=sqs
export FLEETPULSE_STATE_BACKEND=dynamodb
export FLEETPULSE_SQS_QUEUE_URL="$(terraform output -raw queue_url)"
export FLEETPULSE_DYNAMODB_TABLE="$(terraform output -raw table_name)"
export AWS_ENDPOINT_URL=http://localhost:4566
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
go run ./cmd/server
```

Or end to end with make:

```bash
make tf-apply-local
```

## Teardown (both modes)

```bash
terraform destroy -var-file=aws.tfvars        # or localstack.tfvars
```

Then prove it is empty:

```bash
aws sqs list-queues                                 # no fleetpulse-* queues
aws dynamodb list-tables --output json              # no fleetpulse-client-state
# LocalStack instead:
aws --endpoint-url=http://localhost:4566 sqs list-queues
aws --endpoint-url=http://localhost:4566 dynamodb list-tables
```

If the DLQ holds dead letters, destroy still succeeds (SQS deletion is
immediate); check the account-wide tag search above as the final sweep.

## CI

`.github/workflows/ci.yml` runs `terraform fmt -check -recursive`,
`terraform init -backend=false && terraform validate` (both modes), and
TFLint with the AWS ruleset -- none of which need cloud credentials. State
is local-only in this repo; before any shared/real usage, move `init` to an
S3/DynamoDB backend and do not commit state.
