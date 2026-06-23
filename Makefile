DATABASE_URL ?= postgres://dropless:dropless@127.0.0.1:5432/dropless
export DATABASE_URL

.PHONY: help build check fmt lint test test-all migrate kill9 bench docker-up docker-down

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

build: ## Build the workspace (no DB needed)
	cargo build --workspace

check: ## Type-check the workspace (no DB needed)
	cargo check --workspace

fmt: ## Format
	cargo fmt --all

lint: ## Clippy with warnings denied
	cargo clippy --workspace --all-targets -- -D warnings

test: ## Unit tests (no DB)
	cargo test --workspace

test-all: ## All tests including #[ignore]d integration proofs (needs DATABASE_URL)
	cargo test --workspace -- --include-ignored --nocapture

migrate: ## Apply migrations (needs DATABASE_URL)
	cargo run --bin dropless -- migrate

kill9: ## Run the real kill -9 proof (needs DATABASE_URL)
	bash scripts/kill9_test.sh

bench: ## Run the 0-loss throughput benchmark and write bench/REPORT.md
	bash bench/bench.sh

docker-up: ## docker compose up (app + postgres)
	docker compose up --build

docker-down: ## Tear down and remove volumes
	docker compose down -v
