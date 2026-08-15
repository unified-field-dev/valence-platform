//! Start and drive Valence table iters.
//!
//! At host boot, call [`dispatch::register_iter_dispatch`]. Start a run with
//! [`run_service::IterService::start`] (create + Chronon `run_now`).
//!
//! # Examples
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
//! # How a run proceeds
//!
//! After Chronon accepts the job, the script pages the target table and dispatches each id to
//! Boson. These modules implement that path:
//!
//! - [`dispatch`] — [`dispatch::register_iter_dispatch`], Chronon `run_now` helpers
//! - [`orchestrator`] — [`orchestrator::run_valence_iter_orchestrator`] and the
//!   `valence_iter_orchestrator` Chronon script
//! - [`row_worker`] — [`row_worker::run_valence_iter_row_worker`] (Boson task body)
//! - [`paging`] — [`paging::count_table_rows`], [`paging::page_row_ids`]
//! - [`boson_setup`] — [`boson_setup::VALENCE_ITER_ROW_WORKER_TASK`]; task configs come from
//!   [`boson_coordinator::ensure_default_task_configs_embedded`] at host Boson startup

pub mod boson_setup;
pub mod dispatch;
pub mod orchestrator;
pub mod paging;
pub mod row_worker;
pub mod run_service;
