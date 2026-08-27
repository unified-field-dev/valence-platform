# valence-platform

[![CI](https://github.com/unified-field-dev/valence-platform/actions/workflows/ci.yml/badge.svg)](https://github.com/unified-field-dev/valence-platform/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

[GitHub](https://github.com/unified-field-dev/valence-platform) · `cargo doc -p valence-platform --open`

Orchestrator for Valence on Unified Field hosts.

Hosts use it when schema work has to run across many rows: scan a table and apply
per-row logic, cascade a delete under the requester's privacy, or expire Deferred
rows under a budget. Chronon starts those jobs on a schedule or on demand. Boson
executes the workers. Photon can sit on the same path when a host wires it in.
Hosts install Boson's own queue backend (typically
[`boson-backend-sqlite`](https://github.com/unified-field-dev/boson/tree/main/boson-backend-sqlite))
— this crate does not persist Boson jobs through Valence.

```toml
[dependencies]
valence-platform = { git = "https://github.com/unified-field-dev/valence-platform", branch = "main" }
# Chronon×coordinator script macros (standalone; not a workspace member here)
chronon-coordinator-macros = { git = "https://github.com/unified-field-dev/chronon-coordinator-macros", branch = "main" }
```

```rust
use std::sync::Arc;
use valence_platform::iter::dispatch::register_iter_dispatch;
use valence_platform::iter::run_service::{IterRunOptions, IterService};
// Host boot: register_iter_dispatch(+ deletion/ttl registers), then IterService::start.
```

List an iter type in your schema `iters: [...]` and implement `should_run` /
`execute` on that type (**uf-valence** codegen registers the `IterDescriptor`);
this crate runs the orchestrator and row workers. See the seeded examples under
`valence-platform/examples/`.

## About

- Host boot wiring for deletion, Deferred TTL, and iter Chronon dispatch
- Iter runs — `IterService::start` → Chronon → Boson (same on every backend)
- Deletion cascade under the requester; Delete side effects after CascadeDelete
- TTL sweep — budgeted Deferred expiry discover+enqueue (`valence-ttl-sweep`)
- Hosts wire Boson queue via `boson-backend-sqlite` (or another Boson `QueueBackend`); not Valence tables
- Chronon scripts use [`chronon-coordinator-macros`](https://github.com/unified-field-dev/chronon-coordinator-macros) (`#[chronon_coordinator_macros::script]`)

See [`valence-platform/README.md`](valence-platform/README.md) for module detail.

## Examples

Canonical teaching path and run commands: [valence-platform/examples/README.md](valence-platform/examples/README.md).

## Verify

```bash
export CARGO_BUILD_JOBS=1
cargo test --workspace --all-features
```

How to re-run checks: [`docs/VERIFICATION.md`](docs/VERIFICATION.md).

## License

MIT. See [LICENSE](LICENSE), [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
