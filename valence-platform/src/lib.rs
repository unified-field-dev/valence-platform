//! Orchestrator for Valence on Unified Field hosts.
//!
//! Use this crate when schema work spans many rows: scan a table and apply per-row logic, cascade a
//! delete under the requester's privacy, or expire Deferred rows under a budget. Define that logic
//! in Valence (`iters: [...]`, deletion rules, TTL). Chronon starts jobs on a schedule or on demand;
//! Boson runs the workers.
//!
//! ## Features
//!
//! | Feature | Effect |
//! |---------|--------|
//! | (default) | Platform system schemas use SQLite via [`DEFAULT_PLATFORM_STORAGE`] |
//! | `db-hybrid` | [`DEFAULT_PLATFORM_STORAGE`] switches to the hybrid engine (`valence/hybrid`) |
//!
//! Hybrid hosts also exercise `tests/ttl_sweep_hybrid.rs` and `tests/hybrid_m2m_delete.rs`.
//!
//! ## Concern → API
//!
//! | Concern | API |
//! |---------|-----|
//! | Wire deletion at host boot | [`deletion::dispatch::register_deletion_dispatch`], [`deletion::sweep::resync_valence_deletion_sweep_job_cron_if_present`] |
//! | Wire Deferred TTL at host boot | [`ttl::sweep::register_ttl_service`], [`ttl::sweep::resync_valence_ttl_sweep_job_cron_if_present`] |
//! | Wire iter Chronon at host boot | [`iter::dispatch::register_iter_dispatch`] |
//! | Start an iter | [`iter::run_service::IterService::start`] |
//!
//! Chronon scripts, Boson workers, and paging live on the [`iter`], [`deletion`], and [`ttl`]
//! module pages after you call the host APIs.
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
//! ## Examples
//!
//! | Level | Where | What |
//! |-------|-------|------|
//! | Highlight | Getting started above | Boot trilogy: iter + deletion + TTL registration |
//! | Mid | [`iter`], [`deletion`], [`ttl`] module pages | Per-path registration + resync |
//! | Detailed | `examples/iter_orchestrator_sqlite` | Seeded inline iter (no Chronon/Boson) |
//!
//! ```bash
//! cargo run -p valence-platform --example iter_orchestrator_sqlite
//! ```
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
