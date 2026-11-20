# ADR 0002: Dual-mode Terraform provider (real AWS vs. LocalStack)

- **Status:** Accepted
- **Date:** 2026-11-19
- **Context:** `internal/aws` targets two runtimes: LocalStack for local
  development and real AWS otherwise, switched by the `AWS_ENDPOINT_URL`
  environment variable. The infrastructure for both runtimes (SQS queue +
  DLQ, DynamoDB table) previously existed only as hand-run `awslocal`
  commands in `docs/architecture.md`.

## Context

The module must be usable in CI with no credentials (fmt/validate/tflint
only), locally against LocalStack with dummy credentials, and against a
real AWS account with the normal credential chain -- without maintaining
two module copies or committing any credentials.

## Decision

- One module, `infra/terraform/aws`, with `use_localstack` (bool, default
  `false`) and `localstack_endpoint` (default `http://localhost:4566`)
  variables.
- The provider block branches on `local.is_localstack`: the `sqs` and
  `dynamodb` endpoints and the
  `skip_credentials_validation` / `skip_metadata_api_check` /
  `skip_requesting_account_id` flags are set only in LocalStack mode;
  dummy `test`/`test` credentials are injected only in LocalStack mode
  (overridable via the sensitive `access_key`/`secret_key` variables).
  Real-AWS mode passes `null` for credentials so the standard credential
  chain is used.
- Resources are tagged `project=fleetpulse` via `default_tags` (merged with
  the `tags` variable) for find-and-delete hygiene; `.tfvars` files are
  gitignored while `*.tfvars.example` for both modes are committed.
- App-side symmetry: `internal/aws` injects dummy credentials only when
  `AWS_ENDPOINT_URL` is set; unset means real AWS with the standard chain
  (this was tightened as part of this work).

## Consequences

- `make tf-apply AWS_MODE=localstack` / `AWS_MODE=aws` is the single
  entrypoint; CI needs no credentials and no var files.
- State is local-only; any shared or real use must move to an S3 backend
  with locking before anyone else touches the module.
- No execution against real AWS or LocalStack happened while authoring the
  module; `docs/verify.md` sections 6-7 (and `docs/aws-setup.md`) are the
  proof checklist, including the AWS Budgets reminder.
