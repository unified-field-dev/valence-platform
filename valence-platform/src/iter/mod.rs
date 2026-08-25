//! Page through a Valence table and apply per-row logic from the schema.
//!
//! A run walks row ids in order, calls `should_run`, then `execute` on matching rows. Register
//! Chronon at host boot with [`dispatch::register_iter_dispatch`], then
//! [`run_service::IterService::start`] to create the run and `run_now` the orchestrator (Boson
//! does the row work).
//!
//! # Register at boot
//!
//! Iter Chronon wiring installs the coordinator backend so later
//! [`run_service::IterService::start`] calls can `run_now` the orchestrator. Call this once at
//! host boot before any iter run is created.
//!
//! **Prerequisites:** a Chronon coordinator backend and default job
//! `valence-iter-orchestrator` (manual) ensured by Chronon script registration.
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use valence_platform::iter::dispatch::register_iter_dispatch;
//!
//! register_iter_dispatch(Arc::clone(&chronon_backend));
//! ```
//!
//! **Outcome:** [`dispatch::is_iter_chronon_registered`] is `true`; later
//! [`run_service::IterService::start`] can `run_now` the orchestrator.
//!
//! **Failure:** calling `start` before registration returns
//! `valence::Error::Internal` ("Chronon backend not registered").
//!
//! **Next:** [Start an iter](#start-an-iter).
//!
//! # Start an iter
//!
//! Starting an iter creates a platform run row and asks Chronon to drive
//! `valence-iter-orchestrator`. Use this when a host or operator needs a table scan with per-row
//! `should_run` / `execute` without hand-rolling Chronon payloads.
//!
//! **Prerequisites:** [`dispatch::register_iter_dispatch`] at boot; schema lists the iter type in
//! `iters: [...]` with `should_run` / `execute` implemented (uf-valence codegen).
//!
//! ```rust,ignore
//! use valence_platform::iter::run_service::{IterRunOptions, IterService};
//!
//! let run_id = IterService::start(
//!     &valence,
//!     "MarkProcessedIter",
//!     "demo_note",
//!     IterRunOptions::default(),
//! )
//! .await?;
//! // Chronon accepted run_now for job valence-iter-orchestrator with {"run_id": run_id}.
//! assert!(!run_id.is_empty());
//! ```
//!
//! **Outcome:** a `pending` [`crate::ValenceIterRun`] row exists and Chronon has accepted
//! `run_now` for `valence-iter-orchestrator`. Row work finishes asynchronously via Boson task
//! [`boson_setup::VALENCE_ITER_ROW_WORKER_TASK`] (`valence_iter_row_worker`).
//!
//! **Failure / next:** empty `iter_name` / `target_table` → validation error; missing Chronon job →
//! Internal. For a single-row scan, pass
//! [`IterRunOptions::target_row_id`](run_service::IterRunOptions::target_row_id). Without Chronon,
//! the seeded example uses the test harness (see [Examples](#examples)).
//!
//! # Examples
//!
//! Host path (register + start):
//!
//! ```rust,ignore
//! use std::sync::Arc;
//! use valence_platform::iter::dispatch::register_iter_dispatch;
//! use valence_platform::iter::run_service::{IterRunOptions, IterService};
//!
//! register_iter_dispatch(Arc::clone(&chronon_backend));
//!
//! let run_id = IterService::start(
//!     &valence,
//!     "MarkProcessedIter",
//!     "demo_note",
//!     IterRunOptions::default(),
//! )
//! .await?;
//! ```
//!
//! Detailed runnable (inline harness, no Chronon/Boson):
//!
//! ```bash
//! cargo run -p valence-platform --example iter_orchestrator_sqlite
//! ```
//!
//! Expect stdout `processed=1 skipped=2` and even-row `marker=done`.
//!
//! # How a run proceeds
//!
//! After Chronon accepts the job, the script pages the target table and dispatches each id to
//! Boson. These modules implement that path:
//!
//! - [`dispatch`] — [`dispatch::register_iter_dispatch`], Chronon `run_now` helpers
//! - [`orchestrator`] — [`orchestrator::run_valence_iter_orchestrator`] and the
//!   `valence_iter_orchestrator` Chronon script (job `valence-iter-orchestrator`)
//! - [`row_worker`] — [`row_worker::run_valence_iter_row_worker`] (Boson task body)
//! - [`paging`] — [`paging::count_table_rows`], [`paging::page_row_ids`] (ascending id pages)
//! - [`boson_setup`] — [`boson_setup::VALENCE_ITER_ROW_WORKER_TASK`]; task configs come from
//!   [`boson_coordinator::ensure_default_task_configs_embedded`] at host Boson startup
//!
//! # Realistic variant
//!
//! Restrict the scan to one row with
//! `IterRunOptions::default().target_row_id("n2")` on [`run_service::IterService::start`].
//! Change platform storage with Cargo feature `db-hybrid` ([`crate::DEFAULT_PLATFORM_STORAGE`]);
//! the start call shape stays the same.

pub mod boson_setup;
pub mod dispatch;
pub mod orchestrator;
pub mod paging;
pub mod row_worker;
pub mod run_service;
