**What & why**
A short description of the change and the motivation.

**How verified**
Tests added / commands run (e.g. `cargo test`, `make test-all`, `make kill9`).

**Checklist**
- [ ] `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` pass
- [ ] No compile-time sqlx macros (`query!` / `query_as!`) — the DB-less build stays green
- [ ] Tests added/updated for behavior changes
