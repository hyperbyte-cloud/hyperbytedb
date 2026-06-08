# Contributing

How to contribute to HyperbyteDB.

---

## Before you open a PR

1. Branch from `main`.
2. Follow [Coding standards](coding-standards.md).
3. Run the same checks as CI:

   ```bash
   cargo fmt --all --check
   cargo clippy --all-targets -- -D warnings
   cargo test --lib
   cargo test --test '*'
   ```

4. Document new config keys in [Configuration](../user-guide/configuration.md) and `config.toml.example`.
5. Open a PR against `main`. CI must pass.

---

## Code review

PRs are reviewed against the [code review rubric](../engineering/code-review-rubric.md). Highlights:

- **Correctness** — data path integrity, cluster safety, InfluxDB v1 compatibility, query translation.
- **Tests** — each suite has a defined scope; do not duplicate coverage without reason.
- **Docs** — config and operator changes need matching documentation updates.

---

## Commit conventions

- Use clear, descriptive commit messages.
- Reference issues when applicable.
- Keep commits focused on one logical change.

---

## Dependencies

- Use `Arc<dyn Trait>` for dependency injection at service boundaries.
- Keep domain types free of I/O dependencies.
- Pin major versions of GitHub Actions.
- Commit `Cargo.lock`.

---

## Getting help

| Question | Start here |
|----------|------------|
| System design | [Architecture](architecture.md) |
| Where is this code? | [Core modules](internals/core-modules.md) |
| How do I add a feature? | [Extension points](internals/extension-points.md) |
| How do I run tests? | [Testing](testing.md) |

---

## Further reading

| Document | Topic |
|----------|-------|
| [Architecture](architecture.md) | System design |
| [Coding standards](coding-standards.md) | Code conventions |
| [Testing](testing.md) | Test strategy |
| [Building & CI](building-and-ci.md) | CI and releases |
| [Code review rubric](../engineering/code-review-rubric.md) | Review checklist |
