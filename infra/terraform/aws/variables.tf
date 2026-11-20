variable "region" {
  description = "AWS region for the queue and table."
  type        = string
  default     = "us-east-1"
}

variable "name_prefix" {
  description = "Prefix for resource names, e.g. fleetpulse -> fleetpulse-events."
  type        = string
  default     = "fleetpulse"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,32}$", var.name_prefix))
    error_message = "name_prefix must be 2-33 lowercase alphanumeric or hyphen characters."
  }
}

variable "tags" {
  description = "Extra tags merged onto every resource (project=fleetpulse is always applied)."
  type        = map(string)
  default     = {}
}

variable "use_localstack" {
  description = "Route SQS/DynamoDB at localstack_endpoint with dummy credentials instead of real AWS."
  type        = bool
  default     = false
}

variable "localstack_endpoint" {
  description = "LocalStack gateway used when use_localstack is true."
  type        = string
  default     = "http://localhost:4566"
}

variable "access_key" {
  description = "Optional LocalStack access key. Defaults to the conventional dummy value 'test' in LocalStack mode; ignored for real AWS (credential chain is used). Never commit real credentials."
  type        = string
  default     = ""
  sensitive   = true
}

variable "secret_key" {
  description = "Optional LocalStack secret key. Defaults to the conventional dummy value 'test' in LocalStack mode; ignored for real AWS. Never commit real credentials."
  type        = string
  default     = ""
  sensitive   = true
}
