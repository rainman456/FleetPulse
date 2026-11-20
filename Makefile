# FleetPulse developer entrypoints.
#
# Recipes below are TAB-indented (required by make). Nothing here has been
# executed as part of the scaffold; docs/verify.md lists the exact commands
# to run on a machine with Go, Docker Compose and Terraform installed.
#
# AWS_MODE=aws|localstack selects the Terraform variable file:
#   aws         -> aws.tfvars.example        (real credentials from the chain)
#   localstack  -> localstack.tfvars.example (dummy creds, local endpoints)
# Override TF_VAR_FILE=path/to/your.tfvars after copying an example.

SHELL := /bin/sh

AWS_MODE ?= aws
TF_DIR := infra/terraform/aws
TF_VAR_FILE ?= $(if $(filter localstack,$(AWS_MODE)),localstack.tfvars.example,aws.tfvars.example)
COMPOSE ?= docker compose

.PHONY: up down build test run-server run-simulator ch-migrate bench \
	tf-init tf-plan tf-apply tf-destroy tf-apply-local verify

up:
	$(COMPOSE) up --build -d

down:
	$(COMPOSE) down

build:
	go build ./...

test:
	go test ./...

run-server:
	go run ./cmd/server

run-simulator:
	go run ./cmd/simulator --clients=300 --ingest-url=http://localhost:8080/v1/events

ch-migrate:
	$(COMPOSE) run --rm clickhouse-migrate

bench:
	go run ./cmd/bench

tf-init:
	terraform -chdir=$(TF_DIR) init

tf-plan:
	terraform -chdir=$(TF_DIR) plan -var-file=$(TF_VAR_FILE)

tf-apply:
	terraform -chdir=$(TF_DIR) apply -var-file=$(TF_VAR_FILE)

tf-destroy:
	terraform -chdir=$(TF_DIR) destroy -var-file=$(TF_VAR_FILE)

tf-apply-local:
	$(MAKE) tf-apply AWS_MODE=localstack

verify:
	go vet ./...
	$(COMPOSE) config -q
	terraform -chdir=$(TF_DIR) fmt -check -recursive
