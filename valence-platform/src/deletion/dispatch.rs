//! Register [`valence::deletion::dispatch`] to start the Chronon deletion orchestrator, and a shared
//! [`std::sync::OnceLock`] so [`super::sweep::reenqueue_swept_queued_runs`] can call
//! `run_now` in-process.
//!
//! Host boot: [`register_deletion_dispatch`]. See that item's `# Examples`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;

use chronon_coordinator::ChrononCoordinatorBackend;

static DELETION_CHRONON: OnceLock<Arc<dyn ChrononCoordinatorBackend>> = OnceLock::new();
static FORCE_UNREGISTERED_FOR_TESTS: AtomicBool = AtomicBool::new(false);

const CHRONON_DELETION_ORCHESTRATOR: &str = "valence-deletion-orchestrator";

/// `true` after a successful [`register_deletion_dispatch`] (unless a test forces unregistered).
pub fn is_deletion_chronon_registered() -> bool {
    if FORCE_UNREGISTERED_FOR_TESTS.load(Ordering::SeqCst) {
        return false;
    }
    DELETION_CHRONON.get().is_some()
}

/// Test seam: treat Chronon as unregistered so sad paths always assert (OnceLock is sticky).
#[doc(hidden)]
pub fn force_deletion_chronon_unregistered_for_tests(force: bool) {
    FORCE_UNREGISTERED_FOR_TESTS.store(force, Ordering::SeqCst);
}

/// Same as the delete dispatch path: `run_now` the manual `valence-deletion-orchestrator` with
/// `{"run_id": run_id}` (bare uuid), using the Chronon instance from [`register_deletion_dispatch`].
pub async fn run_deletion_orchestrator_now_for_registered_backend(
    run_id: &str,
) -> valence::Result<()> {
    if FORCE_UNREGISTERED_FOR_TESTS.load(Ordering::SeqCst) {
        return Err(valence::Error::Internal(
            "deletion: Chronon backend not registered; call register_deletion_dispatch at bootstrap"
                .into(),
        ));
    }
    let b = DELETION_CHRONON.get().ok_or_else(|| {
        valence::Error::Internal(
            "deletion: Chronon backend not registered; call register_deletion_dispatch at bootstrap"
                .into(),
        )
    })?;
    let job = b
        .get_job_by_name(CHRONON_DELETION_ORCHESTRATOR)
        .await
        .ok_or_else(|| {
            valence::Error::Internal("Chronon job valence-deletion-orchestrator not found".into())
        })?;
    b.run_now_with_params(&job.job_id, Some(serde_json::json!({ "run_id": run_id })))
        .await
        .map_err(|e| valence::Error::Internal(e.to_string()))?;
    Ok(())
}

/// Wire `Model::delete` → Chronon `valence-deletion-orchestrator` (call once from host bootstrap).
///
/// After this returns, deletes that go through Valence's deletion dispatcher `run_now` the
/// manual Chronon job with `{"run_id": …}`. Pair with
/// [`super::sweep::resync_valence_deletion_sweep_job_cron_if_present`] once Chronon jobs exist.
///
/// # Examples
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use valence_platform::deletion::dispatch::register_deletion_dispatch;
/// use valence_platform::deletion::sweep::resync_valence_deletion_sweep_job_cron_if_present;
///
/// register_deletion_dispatch(Arc::clone(&chronon_backend));
/// resync_valence_deletion_sweep_job_cron_if_present(chronon_backend.as_ref(), &valence).await?;
/// ```
pub fn register_deletion_dispatch(backend: Arc<dyn ChrononCoordinatorBackend>) {
    if DELETION_CHRONON.set(Arc::clone(&backend)).is_err() {
        log::warn!(target: "valence_deletion", "register_deletion_dispatch: Chronon already set, ignoring");
        return;
    }
    valence::deletion::register_deletion_dispatcher(Box::new(move |req| {
        let run_id = req.run_id;
        Box::pin(async move { run_deletion_orchestrator_now_for_registered_backend(&run_id).await })
    }));
}
