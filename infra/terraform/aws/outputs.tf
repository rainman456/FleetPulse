output "queue_url" {
  description = "SQS queue URL for FLEETPULSE_SQS_QUEUE_URL."
  value       = aws_sqs_queue.events.url
}

output "dlq_url" {
  description = "Dead-letter queue URL (operational visibility, not read by the app)."
  value       = aws_sqs_queue.dlq.url
}

output "table_name" {
  description = "DynamoDB table name for FLEETPULSE_DYNAMODB_TABLE."
  value       = aws_dynamodb_table.client_state.name
}

output "region" {
  description = "Region the resources were created in (for AWS_REGION)."
  value       = var.region
}
