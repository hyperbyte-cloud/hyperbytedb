# Security Policy

## Supported Versions

Security fixes are provided for the latest minor release line. Older releases may
receive backports at maintainer discretion.

| Version | Supported |
| ------- | --------- |
| 0.8.x   | Yes       |
| < 0.8   | No        |

## Reporting a Vulnerability

**Do not open a public GitHub issue for security vulnerabilities.**

Report security issues privately using one of these channels:

1. **[GitHub private vulnerability reporting](https://github.com/hyperbyte-cloud/hyperbytedb/security/advisories/new)** (preferred)
2. Email the maintainers at [security@hyperbyte.cloud](mailto:security@hyperbyte.cloud)

Include:

- Affected component (server, CLI, proxy, Kubernetes operator)
- HyperbyteDB version or Docker image tag
- Steps to reproduce and expected vs actual behavior
- Impact assessment (confidentiality, integrity, availability)

## Response SLA

| Severity | Target initial response | Target fix |
| -------- | ----------------------- | ---------- |
| Critical | 2 business days         | 14 days    |
| High     | 5 business days         | 30 days    |
| Medium   | 10 business days        | 90 days    |
| Low      | Best effort             | Next release |

Severity is assessed by maintainers based on exploitability and data-at-risk.

## Scope

**In scope**

- HyperbyteDB server (`hyperbytedb` binary)
- HyperbyteDB CLI (`hyperbytedb-cli`)
- HyperbyteDB proxy (`hyperbytedb-proxy`)
- Kubernetes operator and Helm chart in this repository

**Out of scope**

- Upstream [chDB](https://github.com/chdb-io/chdb) vulnerabilities (report to chDB maintainers; we will track dependency updates)
- Third-party dependencies not bundled with HyperbyteDB releases
- Deployments with authentication disabled on untrusted networks
- Issues requiring physical access to the host

## Disclosure Policy

We follow coordinated disclosure:

1. Reporter submits a private report.
2. Maintainers acknowledge and assign severity.
3. A fix is developed and tested.
4. A GitHub Security Advisory and patched release are published.
5. Public disclosure occurs after users can upgrade (typically within 7 days of the patch release).

Reporters may be credited in the advisory unless they request anonymity.

## Security Best Practices

- Enable `[auth]` in production and restrict network access to admin routes.
- Use TLS (`server.tls_enabled`) when traffic crosses untrusted networks.
- Keep Docker images and release tarballs updated to the latest patch release.
- Review [Authentication](docs/user-guide/authentication.md) and
  [Administration](docs/user-guide/administration.md) before exposing cluster endpoints.
