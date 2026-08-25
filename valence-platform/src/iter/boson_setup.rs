//! Constants for Valence platform Boson worker task names.
//!
//! Task configuration rows are ensured via [`boson_coordinator::ensure_default_task_configs_embedded`]
//! at host Boson startup, not via hard-coded upserts in this module.
//!
//! # Guide: Boson task names
//!
//! This module holds the iter row-worker task name Boson must enqueue. Keeping the constant aligned
//! with the embedded task config avoids silent dispatch misses when the worker starts.
//!
//! **Prerequisites:** host Boson startup that embeds default task configs via
//! [`boson_coordinator::ensure_default_task_configs_embedded`].
//!
//! ```rust
//! use valence_platform::iter::boson_setup::VALENCE_ITER_ROW_WORKER_TASK;
//! assert_eq!(VALENCE_ITER_ROW_WORKER_TASK, "valence_iter_row_worker");
//! ```
//!
//! **Outcome:** enqueue / worker registration uses the string `valence_iter_row_worker`.
//!
//! **Failure:** a mismatched or renamed string means Boson never dispatches to
//! [`super::row_worker`]; keep the constant and the embedded task config in lockstep.
//!
//! **Next:** [`super::row_worker`] body; host path [`super::run_service::IterService::start`].

/// Task name for the iter row worker (`#[boson::task]` in [`super::row_worker`]).
pub const VALENCE_ITER_ROW_WORKER_TASK: &str = "valence_iter_row_worker";
