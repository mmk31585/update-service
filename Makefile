SHELL := /bin/bash
.DEFAULT_GOAL := help

APP_NAME := update
MODULE := update
CMD_DIR := cmd/service
MIGRATIONS_DIR := internal/infrastructure/mariadb/migrations
SWAGGER_DIR := docs

GO := go
GOTEST := $(GO) test
GOFMT := gofmt
SWAG := swag
GOOSE := goose

DOCKER_COMPOSE := docker compose

ifneq ($(wildcard .env),)
  include .env
  export $(shell sed 's/=.*//' .env)
endif

.PHONY: help build run run-dev test test-cover lint fmt clean deps docker-build docker-up docker-down docker-logs migrate-up migrate-down migrate-status swagger-gen swagger-serve tidy docker-es docker-es-down

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build the application binary
	@echo "Building $(APP_NAME)..."
	@$(GO) build -ldflags="-s -w" -o bin/$(APP_NAME) ./$(CMD_DIR)

run: ## Run the application locally
	@echo "Running $(APP_NAME)..."
	@$(GO) run ./$(CMD_DIR)

run-dev: ## Run with hot-reload (requires air)
	@echo "Starting dev server with air..."
	@air

test: ## Run all tests
	@echo "Running tests..."
	@$(GOTEST) -v ./...

test-cover: ## Run tests with coverage report
	@echo "Running tests with coverage..."
	@$(GOTEST) -coverprofile=coverage.out ./...
	@$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

lint: ## Run golangci-lint
	@echo "Running linter..."
	@golangci-lint run ./...

fmt: ## Format Go source files
	@echo "Formatting..."
	@$(GOFMT) -w -s .

fmt-check: ## Check formatting without modifying files
	@echo "Checking formatting..."
	@test -z $$($(GOFMT) -d -s . | tee /dev/stderr)

vet: ## Run go vet
	@echo "Running go vet..."
	@$(GO) vet ./...

tidy: ## Tidy go modules
	@echo "Tidying modules..."
	@$(GO) mod tidy

deps: ## Download dependencies
	@echo "Downloading dependencies..."
	@$(GO) mod download

clean: ## Remove build artifacts
	@echo "Cleaning..."
	@rm -rf bin/
	@rm -f coverage.out coverage.html
	@echo "Clean complete."

swagger-gen: ## Generate Swagger documentation
	@echo "Generating swagger docs..."
	@$(SWAG) init -g ./$(CMD_DIR)/main.go -o ./$(SWAGGER_DIR) --parseDependency --parseInternal

swagger-serve: ## Serve Swagger UI locally
	@echo "Starting Swagger UI..."
	@swagger serve --no-open --port 8080 ./$(SWAGGER_DIR)/swagger.json

migrate-up: ## Run database migrations up
	@echo "Running migrations up..."
	@$(GOOSE) -dir $(MIGRATIONS_DIR) mysql "$(DB_USER):$(DB_PASSWORD)@tcp($(DB_HOST):$(DB_PORT))/$(DB_NAME)?parseTime=true" up

migrate-down: ## Run database migrations down (all)
	@echo "Running migrations down..."
	@$(GOOSE) -dir $(MIGRATIONS_DIR) mysql "$(DB_USER):$(DB_PASSWORD)@tcp($(DB_HOST):$(DB_PORT))/$(DB_NAME)?parseTime=true" down

migrate-status: ## Check migration status
	@echo "Checking migration status..."
	@$(GOOSE) -dir $(MIGRATIONS_DIR) mysql "$(DB_USER):$(DB_PASSWORD)@tcp($(DB_HOST):$(DB_PORT))/$(DB_NAME)?parseTime=true" status

migrate-create: ## Create a new migration file (usage: make migrate-create name=migration_name)
	@if [ -z "$(name)" ]; then echo "Usage: make migrate-create name=<migration_name>"; exit 1; fi
	@echo "Creating migration: $(name)"
	@$(GOOSE) -dir $(MIGRATIONS_DIR) create $(name) sql

docker-build: ## Build Docker image
	@echo "Building Docker image..."
	@$(DOCKER_COMPOSE) build

docker-up: ## Start all services with docker-compose
	@echo "Starting services..."
	@$(DOCKER_COMPOSE) up -d
	@echo "Services started:"
	@echo "  - API (entry):       http://localhost:8081"
	@echo "  - API (worker):      http://localhost:8082"
	@echo "  - API (worker):      http://localhost:8083"
	@echo "  - MariaDB:           localhost:3306"
	@echo "  - NATS:              localhost:4222"
	@echo "  - NATS Monitor:      http://localhost:8222"
	@echo "  - Elasticsearch:     http://localhost:9200"

docker-es: ## Start only elasticsearch
	@echo "Starting elasticsearch..."
	@$(DOCKER_COMPOSE) up -d elasticsearch
	@echo "Elasticsearch running at http://localhost:9200"

docker-es-down: ## Stop elasticsearch
	@echo "Stopping elasticsearch..."
	@$(DOCKER_COMPOSE) stop elasticsearch

docker-down: ## Stop all services
	@echo "Stopping services..."
	@$(DOCKER_COMPOSE) down

docker-logs: ## Tail logs from all services
	@$(DOCKER_COMPOSE) logs -f

docker-restart: ## Restart the app service (rebuilds)
	@echo "Rebuilding and restarting..."
	@$(DOCKER_COMPOSE) up -d --build

dev: fmt vet test build ## Run full dev pipeline (fmt, vet, test, build)
