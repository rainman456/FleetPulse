locals {
  is_localstack = var.use_localstack

  # project=fleetpulse is applied by the provider's default_tags so anything
  # created here is easy to find (and delete) in the console or with
  # `aws resourcegroupstaggingapi get-resources --tag-filters Key=project,Values=fleetpulse`.
  tags = merge({ project = "fleetpulse" }, var.tags)

  # Dummy static credentials ONLY in LocalStack mode. In real-AWS mode both
  # are null, which leaves the provider on the standard credential chain
  # (environment, profile, SSO, instance/task role).
  localstack_access_key = local.is_localstack ? (var.access_key != "" ? var.access_key : "test") : null
  localstack_secret_key = local.is_localstack ? (var.secret_key != "" ? var.secret_key : "test") : null
}

provider "aws" {
  region = var.region

  access_key = local.localstack_access_key
  secret_key = local.localstack_secret_key

  # Only relax identity/credential checks when we are pointed at LocalStack;
  # against real AWS these stay at their secure defaults.
  skip_credentials_validation = local.is_localstack
  skip_metadata_api_check     = local.is_localstack
  skip_requesting_account_id  = local.is_localstack
  s3_force_path_style         = local.is_localstack

  # The service-specific endpoint overrides are what internal/aws's
  # AWS_ENDPOINT_URL does at runtime; Terraform needs its own copy.
  endpoints {
    sqs      = local.is_localstack ? var.localstack_endpoint : null
    dynamodb = local.is_localstack ? var.localstack_endpoint : null
  }

  default_tags {
    tags = local.tags
  }
}

# Dead-letter queue first so the main queue can reference its ARN.
resource "aws_sqs_queue" "dlq" {
  name                      = "${var.name_prefix}-events-dlq"
  message_retention_seconds = 1209600 # 14 days, SQS maximum
}

# Main event queue. The worker pool's retry semantics rely on visibility
# timeout + redrive: Nack (or a dead worker) makes the message visible again,
# and after maxReceiveCount=5 deliveries SQS moves it to the DLQ.
resource "aws_sqs_queue" "events" {
  name                       = "${var.name_prefix}-events"
  visibility_timeout_seconds = 30
  message_retention_seconds  = 345600 # 4 days

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq.arn
    maxReceiveCount     = 5
  })
}

# Per-client last-seen table expected by internal/aws.DynamoDBStore. The
# partition key matches the Go record's client_id; cohort attributes are
# ordinary items (no GSI yet -- see internal/aws/dynamodb_store.go).
resource "aws_dynamodb_table" "client_state" {
  name         = "${var.name_prefix}-client-state"
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "client_id"

  attribute {
    name = "client_id"
    type = "S"
  }
}
