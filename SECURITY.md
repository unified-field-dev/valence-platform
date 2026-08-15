# Security Policy

## Supported versions

Security fixes are accepted against the latest `main` branch and tagged releases (`0.1.x`) of this repository's `valence-platform` crate. Report `chronon-coordinator-macros` issues against [unified-field-dev/chronon-coordinator-macros](https://github.com/unified-field-dev/chronon-coordinator-macros).

## Reporting a vulnerability

Please **do not** open a public GitHub issue for security-sensitive reports.

Prefer one of the following:

1. **GitHub Security Advisories** — use [Report a vulnerability](https://github.com/unified-field-dev/valence-platform/security/advisories/new) on this repository when available.
2. Contact the maintainers privately via the repository owner listed at https://github.com/unified-field-dev/valence-platform.

Include:

- a description of the issue and its impact
- steps to reproduce or a proof of concept when possible
- affected crate names and versions

We will acknowledge receipt as soon as practical and coordinate a fix and disclosure timeline with you.

## Debug endpoints (development only)

Deletion diagnostics under `/__debug/valence/*` are **disabled by default**. To enable locally:

1. Set `VALENCE_DEBUG_DELETIONS=1`.
2. Set non-empty `VALENCE_DEBUG_ADMIN_TOKEN` to a shared secret.
3. Send header `x-valence-debug-token` with the same value on every debug request.

If debug is enabled without a configured token, or the header does not match, routes return **401**.
When debug is disabled, routes return **404**. Never enable these endpoints in production without
network-level access controls.

## Scope

In scope: vulnerabilities in this repository's published crates and documentation that could cause unsafe production defaults, plus CI/supply-chain issues in this repository.

Out of scope: vulnerabilities solely in third-party dependencies unless this project mishandles them in a security-relevant way.
