//! Periodic sweep: re-`run_now` the manual `valence-deletion-orchestrator` for
//! `valence_deletion_run` rows stuck in `queued` (stale `requested_at`).
//!
//! The orchestrator body uses
//! [`super::run_service::DeletionService::try_claim_queued_to_scanning`] so concurrent
//! `run_now` + sweep cannot double-insert steps.
//!
//! # Guide: sweep queued runs
//!
//! **Prerequisites:** [`super::dispatch::register_deletion_dispatch`]; Chronon job
//! `valence-deletion-sweep-queued` (default cron `*/10 * * * * *`).
//!
//! ```rust,ignore
//! use valence_platform::deletion::sweep::{
//!     reenqueue_swept_queued_runs, resync_valence_deletion_sweep_job_cron_if_present,
//!     DEFAULT_STALE_SECS,
//! };
//!
//! resync_valence_deletion_sweep_job_cron_if_present(chronon.as_ref(), &valence).await?;
//! let n = reenqueue_swept_queued_runs(&valence, DEFAULT_STALE_SECS, 32).await?;
//! assert!(n <= 32);
//! ```
//!
//! **Outcome:** each successful call is another `run_now` of `valence-deletion-orchestrator`.
//! Unregistered Chronon → `Ok(0)`.
//!
//! **Failure / next:** per-run failures are logged and skipped; see the parent
//! [`crate::deletion#sweep-queued-runs`] guide.

use super::run_service::DeletionService;
use chrono::Utc;
use valence::Valence;

use super::dispatch::{
    is_deletion_chronon_registered, run_deletion_orchestrator_now_for_registered_backend,
};

/// Age threshold (seconds) for treating a `queued` `valence_deletion_run` as stale for sweep re-`run_now`.
pub const DEFAULT_STALE_SECS: u64 = 10;
const DEFAULT_SWEEP_CAP: u32 = 32;

/// Re-trigger `run_now` for up to `cap` runs in `queued` with `requested_at` older than
/// `now() - older_than_secs`. Returns the number of successful `run_now` calls. If Chronon
/// was not registered via [`super::dispatch::register_deletion_dispatch`], returns `0`.
///
/// # Errors
///
/// Propagates Valence list/query failures. Individual `run_now` failures are logged and skipped.
pub async fn reenqueue_swept_queued_runs(
    v: &Valence,
    older_than_secs: u64,
    cap: u32,
) -> valence::Result<usize> {
    if !is_deletion_chronon_registered() {
        return Ok(0);
    }
    let before = Utc::now() - chrono::Duration::seconds(older_than_secs as i64);
    let rows = DeletionService::list_queued_runs_requested_before(before, cap, v).await?;
    let mut n = 0;
    for row in rows {
        let Some(s) = row.get("id").and_then(|x| x.as_str()) else {
            continue;
        };
        let bare = s
            .strip_prefix("valence_deletion_run:")
            .map(str::to_string)
            .unwrap_or_else(|| s.to_string());
        match run_deletion_orchestrator_now_for_registered_backend(&bare).await {
            Ok(()) => n += 1,
            Err(e) => {
                log::warn!(
                    target: "valence_deletion",
                    "sweep: run_now failed for valence_deletion_run {}: {}",
                    bare,
                    e
                );
            }
        }
    }
    Ok(n)
}

#[chronon_coordinator_macros::script(
    name = "valence_deletion_sweep_queued",
    default_job(job = "valence-deletion-sweep-queued", cron = "*/10 * * * * *")
)]
/// Chronon script entry: forwards to [`reenqueue_swept_queued_runs`].
pub async fn valence_deletion_sweep_queued_chronon(
    ctx: Box<dyn chronon_core::ScriptContext>,
) -> anyhow::Result<()> {
    let valence = chronon_valence_identity::valence_from_context(&*ctx)?;
    if !is_deletion_chronon_registered() {
        log::debug!(target: "valence_deletion", "sweep: Chronon backend not set, skip");
        return Ok(());
    }
    let n = reenqueue_swept_queued_runs(&valence, DEFAULT_STALE_SECS, DEFAULT_SWEEP_CAP)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    if n > 0 {
        log::info!(target: "valence_deletion", "sweep: re-queued {n} stale valence_deletion_run(s)");
    }
    Ok(())
}

const DELETION_SWEEP_JOB_NAME: &str = "valence-deletion-sweep-queued";
const DELETION_SWEEP_CRON: &str = "*/10 * * * * *";

/// If the persisted `valence-deletion-sweep-queued` job row still reflects an older cron (embedded
/// `default_job` ensure only creates the row on first insert), patch schedule + `next_run_at`
/// to match this crate.
///
/// Call after [`super::dispatch::register_deletion_dispatch`] once Chronon default jobs exist.
///
/// # Examples
///
/// ```rust,ignore
/// use valence_platform::deletion::sweep::resync_valence_deletion_sweep_job_cron_if_present;
///
/// resync_valence_deletion_sweep_job_cron_if_present(chronon_backend.as_ref(), &valence).await?;
/// ```
pub async fn resync_valence_deletion_sweep_job_cron_if_present(
    backend: &dyn chronon_coordinator::ChrononCoordinatorBackend,
    valence: &Valence,
) -> anyhow::Result<()> {
    use chronon_coordinator::{
        default_job_schedule_equivalent, merge_default_job_schedule_fields, JobBuilder,
    };

    let Some(existing) = backend.get_job_by_name(DELETION_SWEEP_JOB_NAME).await else {
        return Ok(());
    };
    let desired = JobBuilder::new(&ValenceDeletionSweepQueuedChrononScript::handle())
        .with_valence(valence.clone())
        .name(DELETION_SWEEP_JOB_NAME)
        .cron(DELETION_SWEEP_CRON)?
        .build()
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    if !default_job_schedule_equivalent(&existing, &desired) {
        let merged = merge_default_job_schedule_fields(existing, &desired);
        let job_id = merged.job_id.clone();
        backend
            .update_job_config_with_valence(valence, &job_id, merged)
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
        log::info!(
            target: "valence_deletion",
            "Resynced {} cron expression (schedule drift vs crate default)",
            DELETION_SWEEP_JOB_NAME
        );
    }
    Ok(())
}
