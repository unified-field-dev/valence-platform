//! Constants for Valence platform Boson worker task names.
//!
//! Task configuration rows are ensured via [`boson_coordinator::ensure_default_task_configs_embedded`]
//! at host Boson startup, not via hard-coded upserts in this module.

/// Task name for the iter row worker (`#[boson::task]` in [`super::row_worker`]).
pub const VALENCE_ITER_ROW_WORKER_TASK: &str = "valence_iter_row_worker";
