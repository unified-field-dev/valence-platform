# valence-platform examples

Seeded Valence Iter demo on in-memory SQLite. Seeds rows, runs the **test**
harness via `IterService::run_for_tests` (inline orchestrator; no Chronon/Boson),
and asserts outcomes.

## Flow

Open [`iter_orchestrator_sqlite.rs`](iter_orchestrator_sqlite.rs) for the full
authoring path in one file:

1. Declare `DemoNote` with `iters: [MarkProcessedIter]` and implement
   `should_run` / `execute` on that type.
2. Seed three rows (`seq` 1, 2, 3; `marker = pending`).
3. Call `IterService::run_for_tests` (creates a pending `ValenceIterRun` and drives
   the inline orchestrator).
4. Even `seq` → `execute` sets `marker = done`; odd → skipped.
5. Expect `processed=1`, `skipped=2`, even row marker `done`.

SQLite boot wiring lives in [`support/sqlite_boot.rs`](support/sqlite_boot.rs).

Host path: `register_iter_dispatch` at boot, then `IterService::start` (Chronon
`run_now` → Boson). Author hooks the same way in your schema crate: list the type
in `iters: [...]` and implement the two methods.

## `iter_orchestrator_sqlite`

```bash
CARGO_BUILD_JOBS=1 cargo run -p valence-platform --example iter_orchestrator_sqlite
```

Success: stdout shows `processed=1 skipped=2` and `even row n2 marker=done`.

Look next: host boot with `register_iter_dispatch` + `boson-backend-sqlite` and
`IterService::start`; Chronon job `valence-iter-orchestrator`.
