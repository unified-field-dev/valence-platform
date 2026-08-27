# valence-platform verification

Valence orchestration (iter, deletion, TTL) for Unified Field hosts. Re-run after
code or doc changes. Chronon `#[script]` macros live in
[`chronon-coordinator-macros`](https://github.com/unified-field-dev/chronon-coordinator-macros)
(`git` dep; see root README for the pin).

Hosts install Boson's queue with [`boson-backend-sqlite`](https://github.com/unified-field-dev/boson/tree/main/boson-backend-sqlite)
(or another Boson `QueueBackend`). This workspace does not ship a Valence-backed
Boson queue.

## Environment

```bash
export CARGO_BUILD_JOBS=1
export CARGO_TARGET_DIR=target-valence-platform
```

## Unit + integration (CI)

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace --all-features
```

Optional narrower runs:

```bash
cargo test -p valence-platform --all-features
```

Useful filters:

```bash
cargo test -p valence-platform --all-features --test iter_scan_complete
cargo test -p valence-platform --all-features --test hybrid_m2m_delete
cargo test -p valence-platform --all-features --test ttl_sweep_hybrid
cargo test -p valence-platform --all-features --test ttl_native_skip
```

## Notes

- Unregistered Chronon sad paths use
  `force_deletion_chronon_unregistered_for_tests` /
  `force_iter_chronon_unregistered_for_tests` so OnceLock stickiness cannot
  silently skip assertions.
- Hybrid tests require `--features db-hybrid` (included in `--all-features`).
