# Contributing to Dropless

Thanks for your interest! Bug reports, feature ideas, and PRs are all welcome.

## Development setup

You need Rust (stable) and Docker (for Postgres).

```sh
# everything below needs NO database:
cargo build --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace            # unit tests, no DB

# integration / proofs need a Postgres:
docker compose up -d db
export DATABASE_URL=postgres://dropless:dropless@127.0.0.1:5432/dropless
make migrate
make test-all                     # includes the no-loss crash proof
make kill9                        # the real kill -9 proof
```

`make help` lists every target.

## The one hard rule: no compile-time sqlx macros

Dropless uses the **runtime-checked** sqlx API (`sqlx::query`,
`sqlx::query_as::<_, T>`, `sqlx::query_scalar`) — never `query!` / `query_as!`.
This is deliberate: `cargo build` and `cargo test` must succeed **without** a
database and without `DATABASE_URL`. The DB-less build is a CI gate; a PR that
reintroduces a compile-time macro will fail CI.

## Conventions

- Keep it `cargo fmt`-clean and `clippy -D warnings`-clean (CI enforces both).
- Match the surrounding style; document public items (`#![warn(missing_docs)]`
  is on in `dropless-core`).
- Add a test for behavior changes. The reliability claims (no-loss, lease
  ownership, dedup) are covered by integration tests — keep them green.
- Conventional-ish commit subjects (`feat:`, `fix:`, `perf:`, `docs:`) are
  appreciated but not required.

## Pull requests

1. Fork and branch from `main`.
2. Make the change with tests; run the checks above.
3. Open a PR describing **what** and **why**, and how you verified it.

## Project layout

```
crates/dropless-core   # the engine: store, outbox, inbox, dispatcher, signing, …
crates/dropless        # the binary: HTTP API (axum), CLI, dashboard
migrations             # sqlx migrations (embedded at build time)
clients/typescript     # zero-dep TS SDK + Svix-compatible verifier
openapi.yaml           # the REST API spec
scripts, bench         # the kill -9 proof and the throughput benchmark
```
