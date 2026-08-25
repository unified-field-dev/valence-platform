# valence-platform

Orchestrator for Valence on Unified Field hosts.

Hosts use it when schema work has to run across many rows: scan a table and apply
per-row logic, cascade a delete, or expire Deferred rows under a budget. You
define that logic in Valence (`iters: [...]`, deletion rules, TTL). Chronon
starts jobs on a schedule or on demand; Boson runs the workers. Photon can join
the path when a host wires it in. Hosts install Boson queue persistence with
`boson-backend-sqlite` (or another Boson `QueueBackend`).

Deletion runs restore the deleting user's Valence from
`valence_deletion_run.requested_by` before cascade privacy and physical delete.
Chronon may boot the job as System; orchestrator and step work switch to the
requester. CascadeDelete authorizes Delete only (Read is optional) and runs
registered schema `side_effects` Delete hooks after a successful physical delete.

```toml
valence-platform = { git = "https://github.com/unified-field-dev/valence-platform", package = "valence-platform" }
```

## Use this crate when you need to

- Wire deletion / Deferred TTL / iter Chronon at host boot
  (`register_deletion_dispatch`, `register_ttl_service`, `register_iter_dispatch`,
  cron resync helpers)
- Start an iter with `IterService::start` (same path on every backend)
- Cascade delete under the requester actor; budgeted Deferred TTL sweep
- Depend on generated system models (`ValenceIterRun`, …)

List an iter type in your schema `iters: [...]` and implement `should_run` /
`execute` on that type using **uf-valence** (codegen submits `IterDescriptor`);
this crate does not define app iterators.

**Source of truth for teaching:** `cargo doc -p valence-platform` (crate root
Features → Getting started → Concern → API Guide column → module guides). Runnable
seeded demos: [examples/README.md](examples/README.md).

## Modules

| Module | Purpose |
|--------|---------|
| `iter` | Start and drive table iters |
| `deletion` | Wire cascade delete and related host helpers |
| `ttl` | Wire Deferred TTL sweep |

Leaf helpers (workers, paging, task names, debug) are covered on each module's rustdoc page
after the host-facing examples.

## Host path

`register_iter_dispatch` then `IterService::start` — Chronon `run_now`, Boson row
workers. The same pattern applies for deletion and TTL registers at boot. Seeded
examples under [examples/README.md](examples/README.md) drive an inline harness when
Chronon is not wired.

## Feature flags

| Feature | Effect |
|---------|--------|
| (default) | Platform system schemas use SQLite |
| `db-hybrid` | Platform system schemas use the hybrid engine |

## Examples

Canonical teaching path and run commands: [examples/README.md](examples/README.md).
