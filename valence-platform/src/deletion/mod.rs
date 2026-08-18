//! Cascade deletion for Valence: host wiring, orchestrator, and related helpers.
//!
//! At boot, call [`dispatch::register_deletion_dispatch`] so `Model::delete` can `run_now` the
//! Chronon deletion orchestrator. Optionally
//! [`sweep::resync_valence_deletion_sweep_job_cron_if_present`] after Chronon jobs exist.
//! Tests and embedded hosts can drive a queued run with
//! [`orchestrator::run_valence_deletion_orchestrator_inline_steps`].
//!
//! # Examples
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use chronon_coordinator::ChrononCoordinatorBackend;
//! use valence_platform::deletion::dispatch::register_deletion_dispatch;
//!
//! fn wire_deletion(chronon: Arc<dyn ChrononCoordinatorBackend>) {
//!     register_deletion_dispatch(chronon);
//!     // Optional: resync_valence_deletion_sweep_job_cron_if_present after default jobs exist.
//!     // Model::delete then dispatches Chronon job valence-deletion-orchestrator.
//! }
//! ```
//!
//! # After registration
//!
//! Once dispatch is wired, Chronon drives a deletion DAG and Boson runs each step. Related pieces:
//!
//! - [`orchestrator`] — DAG waves; restores the requester actor before privacy and physical delete
//! - [`step_worker`] — [`step_worker::run_valence_deletion_step_worker`] (Boson task body)
//! - [`boson_setup`] — [`boson_setup::VALENCE_DELETION_STEP_WORKER_TASK`]
//! - [`run_service`] — platform [`run_service::DeletionService`] for run CRUD / polling;
//!   `Model::delete` creates runs through [`valence::deletion::DeletionService`]
//! - [`sweep`] — [`sweep::reenqueue_swept_queued_runs`] on a Chronon tick, plus cron resync
//! - [`debug`] — [`debug::deletion_debug_router`] when `VALENCE_DEBUG_DELETIONS=1`

pub mod boson_setup;
pub mod debug;
pub mod dispatch;
pub mod orchestrator;
pub mod run_service;
pub mod step_worker;
pub mod sweep;
