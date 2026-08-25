//! Cascade deletion for Valence: host wiring, orchestrator, and related helpers.
//!
//! At boot, call [`dispatch::register_deletion_dispatch`] so `Model::delete` can `run_now` the
//! Chronon deletion orchestrator. Optionally
//! [`sweep::resync_valence_deletion_sweep_job_cron_if_present`] after Chronon jobs exist.
//! Tests and embedded hosts can drive a queued run with
//! [`orchestrator::run_valence_deletion_orchestrator_inline_steps`].
//!
//! # Register at boot
//!
//! **Prerequisites:** Chronon coordinator backend; default jobs include manual
//! `valence-deletion-orchestrator` and cron `valence-deletion-sweep-queued` (`*/10 * * * * *`).
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use chronon_coordinator::ChrononCoordinatorBackend;
//! use valence_platform::deletion::dispatch::register_deletion_dispatch;
//! use valence_platform::deletion::sweep::resync_valence_deletion_sweep_job_cron_if_present;
//!
//! async fn wire_deletion(
//!     chronon: Arc<dyn ChrononCoordinatorBackend>,
//!     valence: &valence::Valence,
//! ) -> anyhow::Result<()> {
//!     register_deletion_dispatch(Arc::clone(&chronon));
//!     resync_valence_deletion_sweep_job_cron_if_present(chronon.as_ref(), valence).await?;
//!     Ok(())
//! }
//! ```
//!
//! **Outcome:** Valence's deletion dispatcher `run_now`s `valence-deletion-orchestrator` with
//! `{"run_id": …}` on `Model::delete`; [`dispatch::is_deletion_chronon_registered`] is `true`.
//!
//! **Failure:** `Model::delete` / sweep `run_now` before registration → Internal ("Chronon backend
//! not registered"). Duplicate register logs a warning and keeps the first backend.
//!
//! **Next:** [Cascade a delete](#cascade-a-delete).
//!
//! # Cascade a delete
//!
//! **Prerequisites:** deletion dispatch registered (host) **or** an inline harness for tests;
//! schema `on_delete` / CascadeDelete connections; requester actor stored on the run.
//!
//! Host path after registration: `Model::delete` creates a `valence_deletion_run` and Chronon
//! drives job `valence-deletion-orchestrator`. The orchestrator rebuilds Valence as the requester
//! via [`run_service::DeletionService::requester_valence_from_run`] before privacy and physical
//! delete, then Boson runs [`boson_setup::VALENCE_DELETION_STEP_WORKER_TASK`]
//! (`valence_deletion_step_worker`) per step.
//!
//! Embedded / tests without Chronon (full call sequence + observable outcome):
//!
//! ```rust,no_run
//! use serde_json::json;
//! use valence::Actor;
//! use valence_platform::deletion::orchestrator::run_valence_deletion_orchestrator_inline_steps;
//! use valence_platform::deletion::run_service::DeletionService;
//!
//! # async fn demo(boot: valence::Valence) -> anyhow::Result<()> {
//! let user = Actor::User {
//!     user_id: "deleter".into(),
//! };
//! let run_id = DeletionService::create_run(
//!     "ex_del_parent",
//!     "p1",
//!     serde_json::to_value(&user)?,
//!     &boot,
//! )
//! .await?;
//! run_valence_deletion_orchestrator_inline_steps(boot.clone(), run_id.clone()).await?;
//! let run = DeletionService::get_run_json(&run_id, &boot)
//!     .await?
//!     .expect("deletion run row");
//! assert_eq!(
//!     run.get("status").and_then(|s| s.as_str()),
//!     Some("completed")
//! );
//! # let _ = json!({});
//! # Ok(())
//! # }
//! ```
//!
//! **Outcome:** run `status` is `completed` (or `failed` on Restrict / privacy denial); root and
//! cascade targets are gone when CascadeDelete succeeds. Runnable detail:
//! `cargo run -p valence-platform --example deletion_cascade_sqlite`.
//!
//! **Failure:** Restrict with remaining children → run `failed`, children kept; missing
//! `requested_by` → Internal (no System fallback for privacy). See [`run_service`] and
//! [`crate::ValenceDeletionError`] rows for per-step errors.
//!
//! **Next:** [Sweep queued runs](#sweep-queued-runs); debug with [`debug`].
//!
//! # Sweep queued runs
//!
//! **Prerequisites:** [`dispatch::register_deletion_dispatch`]; Chronon job
//! `valence-deletion-sweep-queued`.
//!
//! ```rust,ignore
//! use valence_platform::deletion::sweep::{
//!     reenqueue_swept_queued_runs, resync_valence_deletion_sweep_job_cron_if_present,
//!     DEFAULT_STALE_SECS,
//! };
//!
//! resync_valence_deletion_sweep_job_cron_if_present(chronon.as_ref(), &valence).await?;
//! let n = reenqueue_swept_queued_runs(&valence, DEFAULT_STALE_SECS, 32).await?;
//! // Each success is another run_now of valence-deletion-orchestrator for a stale queued run.
//! assert!(n <= 32);
//! ```
//!
//! **Outcome:** up to `cap` stale `queued` runs get another `run_now`. If Chronon was never
//! registered, returns `0` (no error).
//!
//! **Failure / next:** per-run `run_now` failures are logged and skipped; claim races use
//! [`run_service::DeletionService::try_claim_queued_to_scanning`].
//!
//! # Examples
//!
//! Boot registration (same as [Register at boot](#register-at-boot)):
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
//! ```bash
//! cargo run -p valence-platform --example deletion_cascade_sqlite
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
