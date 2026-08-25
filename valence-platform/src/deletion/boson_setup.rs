//! Constants for Valence platform deletion Boson worker task names.
//!
//! Task configuration rows are ensured via [`boson_coordinator::ensure_default_task_configs_embedded`].
//!
//! # Guide: Boson task names
//!
//! This module holds the deletion step-worker task name Boson must enqueue. Keeping the constant
//! aligned with the embedded task config avoids silent dispatch misses when the worker starts.
//!
//! **Prerequisites:** host Boson startup that embeds default task configs via
//! [`boson_coordinator::ensure_default_task_configs_embedded`].
//!
//! ```rust
//! use valence_platform::deletion::boson_setup::VALENCE_DELETION_STEP_WORKER_TASK;
//! assert_eq!(VALENCE_DELETION_STEP_WORKER_TASK, "valence_deletion_step_worker");
//! ```
//!
//! **Outcome:** step enqueue uses the string `valence_deletion_step_worker`.
//!
//! **Failure:** a mismatched or renamed string means Boson never dispatches to
//! [`super::step_worker`]; keep the constant and the embedded task config in lockstep.
//!
//! **Next:** [`super::step_worker`] body; cascade guide on [`crate::deletion`].

/// Task name for `#[boson::task]` in [`super::step_worker`].
pub const VALENCE_DELETION_STEP_WORKER_TASK: &str = "valence_deletion_step_worker";
