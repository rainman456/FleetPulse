# TFLint configuration used by CI (.github/workflows/ci.yml). Pinned so a
# ruleset release cannot silently change the checks.
config {
  module = true
}

plugin "aws" {
  enabled = true
  version = "0.34.0"
  source  = "github.com/terraform-linters/tflint-ruleset-aws"
}
