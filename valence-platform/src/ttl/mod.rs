//! Expire Deferred rows past their deadline and queue them into deletion under a budget.
//!
//! At host boot, call [`sweep::register_ttl_service`], then optionally
//! [`sweep::resync_valence_ttl_sweep_job_cron_if_present`]. Tests and embedded hosts can run one
//! discover-and-enqueue tick with [`sweep::run_valence_ttl_sweep_inline`].
//!
//! # Register at boot
//!
//! TTL registration wires the budgeted Deferred expiry sweep into Chronon so expired rows can
//! enqueue into the deletion DAG. Call this once at host boot after deletion dispatch is available.
//!
//! **Prerequisites:** Chronon coordinator backend; default cron job `valence-ttl-sweep`
//! (`*/30 * * * * *`); deletion dispatch already wired so queued deletes can complete.
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use chronon_coordinator::ChrononCoordinatorBackend;
//! use valence_platform::ttl::sweep::{
//!     register_ttl_service, resync_valence_ttl_sweep_job_cron_if_present,
//! };
//!
//! async fn wire_ttl(
//!     chronon: Arc<dyn ChrononCoordinatorBackend>,
//!     valence: &valence::Valence,
//! ) -> anyhow::Result<()> {
//!     register_ttl_service(Arc::clone(&chronon));
//!     resync_valence_ttl_sweep_job_cron_if_present(chronon.as_ref(), valence).await?;
//!     Ok(())
//! }
//! ```
//!
//! **Outcome:** [`sweep::is_ttl_service_registered`] is `true`; Chronon ticks call
//! [`sweep::sweep_expired_ttl_rows`].
//!
//! **Failure:** duplicate register logs a warning and keeps the first registration. Unregistered
//! Chronon ticks no-op with an empty [`sweep::TtlSweepReport`].
//!
//! **Next:** [Sweep expired rows](#sweep-expired-rows).
//!
//! # Sweep expired rows
//!
//! A TTL sweep finds Deferred rows past `__valence_expire_at` under a shared budget and queues
//! deletes for the cascade path. Chronon runs this when the job `valence-ttl-sweep` ticks; tests
//! and embedded hosts call the inline helper for one discover-and-enqueue pass.
//!
//! **Prerequisites:** [`sweep::register_ttl_service`] (or inline helper which marks registered);
//! Deferred TTL schemas with `__valence_expire_at`; deletion path available for physical delete.
//!
//! Host path: Chronon job `valence-ttl-sweep` → [`sweep::sweep_expired_ttl_rows`] with
//! [`sweep::DEFAULT_TTL_SWEEP_CAP`] (fair per-table share via [`sweep::fair_table_limit`]).
//! Native TTL backends are skipped ([`sweep::ttl_capability_included_in_deferred_sweep`]).
//!
//! Embedded / tests (discover + queue + inline deletion drain):
//!
//! ```rust,no_run
//! use valence_platform::ttl::sweep::{run_valence_ttl_sweep_inline, DEFAULT_TTL_SWEEP_CAP};
//!
//! # async fn demo(valence: valence::Valence) -> anyhow::Result<()> {
//! let report = run_valence_ttl_sweep_inline(valence, DEFAULT_TTL_SWEEP_CAP).await?;
//! // After seeding an expired Deferred row, expect at least one queued delete:
//! assert!(report.queued_deletes >= 1);
//! // Each run_id was drained with run_valence_deletion_orchestrator_inline_steps.
//! assert_eq!(report.run_ids.len(), report.queued_deletes as usize);
//! # Ok(())
//! # }
//! ```
//!
//! **Outcome:** [`sweep::TtlSweepReport`] lists `queued_deletes`, `run_ids`, `skipped_native`, and
//! `budget_exhausted`. Physical delete follows the deletion DAG. Runnable detail:
//! `cargo run -p valence-platform --example ttl_sweep_sqlite`.
//!
//! **Failure:** list/queue errors log `error_class` and continue the batch when possible; privacy
//! / validation failures skip that row without consuming budget on `Ok(None)`.
//!
//! **Next:** lower the cap (e.g. `2`) to see fairness across tables; enable `db-hybrid` for hybrid
//! Deferred TTL tables.
//!
//! # Examples
//!
//! ```rust,no_run
//! use std::sync::Arc;
//! use chronon_coordinator::ChrononCoordinatorBackend;
//! use valence_platform::ttl::sweep::register_ttl_service;
//!
//! fn wire_ttl(chronon: Arc<dyn ChrononCoordinatorBackend>) {
//!     register_ttl_service(chronon);
//!     // Optional: resync_valence_ttl_sweep_job_cron_if_present after default jobs exist.
//! }
//! ```
//!
//! ```bash
//! cargo run -p valence-platform --example ttl_sweep_sqlite
//! ```
//!
//! # Chronon tick
//!
//! After registration, the `valence-ttl-sweep` job calls [`sweep::sweep_expired_ttl_rows`] each
//! tick: discover expired Deferred rows under a budget, queue deletes into the deletion DAG, and
//! return. Physical delete follows the deletion path already wired at boot.

pub mod sweep;
