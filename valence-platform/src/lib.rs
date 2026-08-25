//! Orchestrator for Valence on Unified Field hosts.
//!
//! Use this crate when schema work spans many rows: scan a table and apply per-row logic, cascade a
//! delete under the requester's privacy, or expire Deferred rows under a budget. Define that logic
//! in Valence (`iters: [...]`, deletion rules, TTL). Chronon starts jobs on a schedule or on demand;
//! Boson runs the workers.
//!
//! ## Features
//!
//! - **Table iters** — register Chronon at boot, then [`iter::run_service::IterService::start`] to
//!   page a table and run per-row `should_run` / `execute` ([`iter`] guide)
//! - **Cascade deletion** — wire [`deletion::dispatch::register_deletion_dispatch`] so
//!   `Model::delete` runs a DAG under the requester actor ([`deletion`] guide)
//! - **Deferred TTL** — register a budgeted Chronon sweep that queues expired rows into deletion
//!   ([`ttl`] guide)
//! - **Deletion debug** — opt-in Axum routes behind env/header gates
//!   ([debug Guide](crate::deletion::debug#guide-wire-the-debug-router))
//! - **Boson task names** — iter
//!   [Guide](crate::iter::boson_setup#guide-boson-task-names)
//!   ([`VALENCE_ITER_ROW_WORKER_TASK`](crate::iter::boson_setup::VALENCE_ITER_ROW_WORKER_TASK));
//!   deletion [Guide](crate::deletion::boson_setup#guide-boson-task-names)
//!   ([`VALENCE_DELETION_STEP_WORKER_TASK`](crate::deletion::boson_setup::VALENCE_DELETION_STEP_WORKER_TASK))
//!
//! ## Getting started
//!
//! Register all three dispatch paths once at host boot (Chronon backend from your coordinator):
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use chronon_coordinator::ChrononCoordinatorBackend;
//! use valence_platform::deletion::dispatch::register_deletion_dispatch;
//! use valence_platform::iter::dispatch::register_iter_dispatch;
//! use valence_platform::ttl::sweep::register_ttl_service;
//!
//! fn wire_valence_platform(chronon: Arc<dyn ChrononCoordinatorBackend>) {
//!     register_iter_dispatch(Arc::clone(&chronon));
//!     register_deletion_dispatch(Arc::clone(&chronon));
//!     register_ttl_service(chronon);
//! }
//! ```
//!
//! After Chronon default jobs exist, call the matching `resync_*_cron_if_present` helpers so
//! sweep jobs pick up host cron overrides. Then start iters with
//! [`iter::run_service::IterService::start`].
//!
//! ## Concern → API
//!
//! | Concern | Guide | API reference |
//! |---------|-------|---------------|
//! | Wire deletion at host boot | [Register at boot](crate::deletion#register-at-boot) | [`deletion::dispatch::register_deletion_dispatch`], [`deletion::sweep::resync_valence_deletion_sweep_job_cron_if_present`] |
//! | Drive a cascade delete | [Cascade a delete](crate::deletion#cascade-a-delete) | [`deletion::orchestrator::run_valence_deletion_orchestrator_inline_steps`], [`deletion::run_service::DeletionService`] |
//! | Re-queue stale deletion runs | [Sweep queued runs](crate::deletion#sweep-queued-runs) | [`deletion::sweep::reenqueue_swept_queued_runs`], [`deletion::sweep::resync_valence_deletion_sweep_job_cron_if_present`] |
//! | Wire Deferred TTL at host boot | [Register at boot](crate::ttl#register-at-boot) | [`ttl::sweep::register_ttl_service`], [`ttl::sweep::resync_valence_ttl_sweep_job_cron_if_present`] |
//! | Run a TTL sweep tick | [Sweep expired rows](crate::ttl#sweep-expired-rows) | [`ttl::sweep::sweep_expired_ttl_rows`], [`ttl::sweep::run_valence_ttl_sweep_inline`] |
//! | Wire iter Chronon at host boot | [Register at boot](crate::iter#register-at-boot) | [`iter::dispatch::register_iter_dispatch`] |
//! | Start an iter | [Start an iter](crate::iter#start-an-iter) | [`iter::run_service::IterService::start`] |
//! | Deletion debug router | [Wire the debug router](crate::deletion::debug#guide-wire-the-debug-router) | [`deletion::debug::deletion_debug_router`] |
//! | Boson iter task name | [Boson task names](crate::iter::boson_setup#guide-boson-task-names) | [`iter::boson_setup::VALENCE_ITER_ROW_WORKER_TASK`] |
//! | Boson deletion task name | [Boson task names](crate::deletion::boson_setup#guide-boson-task-names) | [`deletion::boson_setup::VALENCE_DELETION_STEP_WORKER_TASK`] |
//!
//! Chronon scripts, Boson workers, and paging live on the [`iter`], [`deletion`], and [`ttl`]
//! module pages after you call the host APIs.
//!
//! ## Examples
//!
//! | Level | Where | What |
//! |-------|-------|------|
//! | Highlight | Getting started above | Boot trilogy: iter + deletion + TTL registration |
//! | Mid | [`iter`], [`deletion`], [`ttl`] module guides | Per-path registration, primary calls, outcomes |
//! | Detailed | `examples/iter_orchestrator_sqlite` | Seeded inline iter (no Chronon/Boson); host path is [`IterService::start`](iter::run_service::IterService::start) |
//! | Detailed | `examples/deletion_cascade_sqlite` | Parent/child cascade under requester actor (inline orchestrator) |
//! | Detailed | `examples/ttl_sweep_sqlite` | Expired Deferred row → budgeted sweep → physical delete |
//!
//! ```bash
//! cargo run -p valence-platform --example iter_orchestrator_sqlite
//! cargo run -p valence-platform --example deletion_cascade_sqlite
//! cargo run -p valence-platform --example ttl_sweep_sqlite
//! ```
//!
//! ## Feature flags
//!
//! | Feature | Effect |
//! |---------|--------|
//! | (default) | Platform system schemas use SQLite via [`DEFAULT_PLATFORM_STORAGE`] |
//! | `db-hybrid` | [`DEFAULT_PLATFORM_STORAGE`] switches to the hybrid engine (`valence/hybrid`) |
//!
//! Hybrid hosts also exercise `tests/ttl_sweep_hybrid.rs` and `tests/hybrid_m2m_delete.rs`.
//! Doc builds with default features and `--all-features` / `db-hybrid` share the same Concern → API
//! and Guide links; only [`DEFAULT_PLATFORM_STORAGE`] and hybrid-gated tests differ.
//!
//! ## Further reading
//!
//! Crate [`README.md`](../README.md). System run/batch models ([`ValenceIterRun`], …) are
//! re-exported at the crate root.

/// Default storage evaluator for platform system schemas (SQLite unless `db-hybrid`).
#[cfg(feature = "db-hybrid")]
pub const DEFAULT_PLATFORM_STORAGE: valence::DatabaseFromEngine =
    valence::Database::from_engine("default", valence::HYBRID_ENGINE_ID);

/// Default storage evaluator for platform system schemas (SQLite unless `db-hybrid`).
#[cfg(not(feature = "db-hybrid"))]
pub const DEFAULT_PLATFORM_STORAGE: valence::DatabaseFromEngine =
    valence::Database::from_engine("default", valence::SQLITE_ENGINE_ID);

mod valence_iter_run_schema {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/schemas/valence_iter_run_schema.rs"
    ));
}
mod valence_iter_batch_schema {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/schemas/valence_iter_batch_schema.rs"
    ));
}
mod valence_iter_row_error_schema {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/schemas/valence_iter_row_error_schema.rs"
    ));
}
mod valence_deletion_step_schema {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/schemas/valence_deletion_step_schema.rs"
    ));
}
mod valence_deletion_error_schema {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/schemas/valence_deletion_error_schema.rs"
    ));
}

mod generated {
    #![allow(
        dead_code,
        unused_imports,
        missing_docs,
        clippy::all,
        clippy::pedantic,
        clippy::nursery,
        clippy::restriction
    )]

    use valence::privacy_policies::common::{AUTHENTICATED, SYSTEM_ONLY};

    include!(concat!(env!("OUT_DIR"), "/generated_models.rs"));
}

pub use generated::{
    ValenceDeletionError, ValenceDeletionStep, ValenceDeletionStepAction,
    ValenceDeletionStepStatus, ValenceIterBatch, ValenceIterBatchStatus, ValenceIterRowError,
    ValenceIterRowErrorErrorKind, ValenceIterRun, ValenceIterRunStatus,
};

pub mod deletion;
pub mod iter;
pub mod ttl;
