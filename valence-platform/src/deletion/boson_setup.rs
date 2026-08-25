//! Constants for Valence platform deletion Boson worker task names.
//!
//! Task configuration rows are ensured via [`boson_coordinator::ensure_default_task_configs_embedded`].
//!
//! # Guide: Boson task names
//!
//! Use these string constants when registering or matching the deletion step worker; keep the
//! same literal the embedded Boson task config expects.
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
