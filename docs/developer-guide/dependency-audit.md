# Dependency Audit Process

## Scope

Dependencies are audited across three supply chains:

| Layer | Artifact | Mechanism |
|-------|----------|-----------|
| Rust crates | `Cargo.lock` | `cargo audit` |
| Docker base images | `FROM debian:bookworm-slim` in Dockerfile | Docker Scout / `trivy` |
| libchdb | Shared library linked at build time | Tracked via chdb-rust crate version |
| GitHub Actions | Workflow YAML pinned versions | `actions/checkout@v4` style pinning |

## Rust Dependency Audits

### Cadence

| Trigger | Action |
|---------|--------|
| Every PR | `cargo clippy` + CI (gates advisories via `RUSTFLAGS="-D warnings"` for clippy, but not `cargo audit` yet) |
| Weekly (automated) | `cargo audit` via scheduled workflow |
| Release tag | `cargo audit` must pass before tagging |
| CVE announcement | Immediate triage |

### Tooling

```bash
# Manual run
cargo audit

# Check for outdated crates (informational, not blocking)
cargo outdated --exit-code 1
```

### Triage SLA

| Severity | Response | Remediation |
|----------|----------|-------------|
| Critical (CVSS >= 9) | < 24h | Upgrade or patch within 48h |
| High (CVSS 7-8.9) | < 72h | Upgrade within 1 week |
| Medium/Low | Next release | Upgrade within release cycle |

### Exception process

If a vulnerable dependency cannot be upgraded (breaking change, no patch available):

1. File an issue documenting the vulnerability, impact, and mitigation.
2. Use `cargo audit`'s `advisory` allowlist in `.cargo/audit.toml` with a reference to the issue.
3. Re-evaluate every release cycle.

## Docker Image Audits

- Base image updates are applied via automated Dependabot PRs to the Dockerfile.
- Before each release, scan the runtime image:

```bash
trivy image ghcr.io/hyperbyte-cloud/hyperbytedb:latest
```

- Critical/high findings in the runtime image are blocking for release.

## libchdb Pinning

- `chdb-rust` is a path dependency pinned to a specific branch/commit.
- libchdb is downloaded from `https://lib.chdb.io` during Docker build and CI.
- The CI install script (`scripts/install-dev-deps.sh`) pins the libchdb version.
- On upgrade, test both ingestion and query paths for regressions.

## GitHub Actions Pinning

- All actions are pinned to major version tags (e.g., `@v4`).
- Dependabot is configured to open PRs for action version updates.
- Review CHANGELOG / release notes before merging action updates.

## Record Keeping

- Run `cargo audit --json > audit-report.json` before each release.
- Attach the audit report to the release notes.
- Track exceptions in GitHub Issues with label `security`.
