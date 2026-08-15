//! Constants for Valence platform deletion Boson worker task names.
//!
//! Task configuration rows are ensured via [`boson_coordinator::ensure_default_task_configs_embedded`].

/// Task name for `#[boson::task]` in [`super::step_worker`].
pub const VALENCE_DELETION_STEP_WORKER_TASK: &str = "valence_deletion_step_worker";
