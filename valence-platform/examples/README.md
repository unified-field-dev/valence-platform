# valence-platform examples

Three seeded SQLite demos. Each asserts an observable outcome. Host Chronon/Boson
paths are documented on the rustdoc Guide pages; these binaries use inline harnesses
so they run without a coordinator.

## `iter_orchestrator_sqlite`

1. Declare `DemoNote` with `iters: [MarkProcessedIter]` and implement
   `should_run` / `execute`.
2. Seed three rows (`seq` 1, 2, 3; `marker = pending`).
3. Call `IterService::run_for_tests` (test harness: create run + inline orchestrator).
4. Even `seq` → `execute` sets `marker = done`; odd → skipped.
5. Expect `processed=1`, `skipped=2`, even row marker `done`.

Host path: `register_iter_dispatch` then `IterService::start` (Chronon
`run_now` → Boson `valence_iter_row_worker`).

```bash
CARGO_BUILD_JOBS=1 cargo run -p valence-platform --example iter_orchestrator_sqlite
```

## `deletion_cascade_sqlite`

1. Register `ex_del_parent` CascadeDelete → `ex_del_child`.
2. Seed parent `p1` and child `c1`.
3. `DeletionService::create_run` + `run_valence_deletion_orchestrator_inline_steps`.
4. Expect run `status=completed` and both rows gone.

Host path: `register_deletion_dispatch` so `Model::delete` drives
`valence-deletion-orchestrator`.

```bash
CARGO_BUILD_JOBS=1 cargo run -p valence-platform --example deletion_cascade_sqlite
```

## `ttl_sweep_sqlite`

1. Register Deferred TTL table `ex_ttl_probe`.
2. Seed `e1` with `__valence_expire_at` in the past.
3. `run_valence_ttl_sweep_inline` (discover + queue + inline deletion drain).
4. Expect `queued_deletes >= 1` and row gone.

Host path: `register_ttl_service`; Chronon job `valence-ttl-sweep`.

```bash
CARGO_BUILD_JOBS=1 cargo run -p valence-platform --example ttl_sweep_sqlite
```
