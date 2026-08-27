//! Orchestrator for Valence on Unified Field hosts.
//!
//! Use this crate when schema work spans many rows: scan a table and apply per-row logic, cascade a
//! delete under the requester's privacy, or expire Deferred rows under a budget. Define that logic
//! in Valence (`iters: [...]`, deletion rules, TTL). Chronon starts jobs on a schedule or on demand;
//! Boson runs the workers.
//!
//! ## Features
//!
//! - **Table iters** — Pages a Valence table and runs per-row `should_run` / `execute` through
//!   Chronon and Boson after you register dispatch at host boot.
//!   [Get started](crate::iter#register-at-boot).
//!   [Start an iter](crate::iter#start-an-iter).
//!   API reference: [`iter::dispatch::register_iter_dispatch`],
//!   [`iter::run_service::IterService::start`].
//! - **Cascade deletion** — Runs a privacy-aware deletion DAG under the requester actor when
//!   `Model::delete` fires, with an optional Chronon sweep for stale queued runs.
//!   [Get started](crate::deletion#register-at-boot).
//!   [Cascade a delete](crate::deletion#cascade-a-delete).
//!   [Sweep queued runs](crate::deletion#sweep-queued-runs).
//!   API reference: [`deletion::dispatch::register_deletion_dispatch`],
//!   [`deletion::orchestrator::run_valence_deletion_orchestrator_inline_steps`],
//!   [`deletion::sweep::reenqueue_swept_queued_runs`],
//!   [`deletion::sweep::resync_valence_deletion_sweep_job_cron_if_present`].
//! - **Deferred TTL** — Discovers expired Deferred rows under a budget and queues them into the
//!   deletion path on a Chronon tick (or an inline host call).
//!   [Get started](crate::ttl#register-at-boot).
//!   [Sweep expired rows](crate::ttl#sweep-expired-rows).
//!   API reference: [`ttl::sweep::register_ttl_service`],
//!   [`ttl::sweep::run_valence_ttl_sweep_inline`],
//!   [`ttl::sweep::resync_valence_ttl_sweep_job_cron_if_present`].
//! - **Deletion debug** — Exposes opt-in Axum routes that list deletion runs and traces behind
//!   env and header gates for local debugging.
//!   [Get started](crate::deletion::debug#guide-wire-the-debug-router).
//!   API reference: [`deletion::debug::deletion_debug_router`].
//! - **Boson task names** — Holds the string constants hosts and workers must share so Boson
//!   dispatches iter row work and deletion steps to the right task bodies.
//!   [Get started](crate::iter::boson_setup#guide-boson-task-names).
//!   [Boson deletion task names](crate::deletion::boson_setup#guide-boson-task-names).
//!   API reference: [`iter::boson_setup::VALENCE_ITER_ROW_WORKER_TASK`],
//!   [`deletion::boson_setup::VALENCE_DELETION_STEP_WORKER_TASK`].
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
//! [`iter::run_service::IterService::start`]. Chronon scripts, Boson workers, and paging live on
//! the [`iter`], [`deletion`], and [`ttl`] module pages after you call the host APIs.
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
//! Doc builds with default features and `--all-features` / `db-hybrid` share the same Features
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
