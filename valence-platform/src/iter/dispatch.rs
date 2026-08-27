//! Register Chronon so [`super::run_service::IterService::start`] can `run_now` the iter orchestrator.
//!
//! Host boot: [`register_iter_dispatch`]. See that item's `# Examples`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;

use chronon_coordinator::ChrononCoordinatorBackend;

static ITER_CHRONON: OnceLock<Arc<dyn ChrononCoordinatorBackend>> = OnceLock::new();
static FORCE_UNREGISTERED_FOR_TESTS: AtomicBool = AtomicBool::new(false);

const CHRONON_ITER_ORCHESTRATOR: &str = "valence-iter-orchestrator";

/// `true` after a successful [`register_iter_dispatch`] (unless a test forces unregistered).
#[must_use]
pub fn is_iter_chronon_registered() -> bool {
    if FORCE_UNREGISTERED_FOR_TESTS.load(Ordering::SeqCst) {
        return false;
    }
    ITER_CHRONON.get().is_some()
}

/// Test seam: treat Chronon as unregistered so sad paths always assert (OnceLock is sticky).
#[doc(hidden)]
pub fn force_iter_chronon_unregistered_for_tests(force: bool) {
    FORCE_UNREGISTERED_FOR_TESTS.store(force, Ordering::SeqCst);
}

/// `run_now` the manual `valence-iter-orchestrator` with `{"run_id": run_id}` (bare uuid), using
/// the Chronon instance from [`register_iter_dispatch`].
///
/// # Errors
///
/// Chronon not registered, job missing, or `run_now` failure.
pub async fn run_iter_orchestrator_now_for_registered_backend(run_id: &str) -> valence::Result<()> {
    if FORCE_UNREGISTERED_FOR_TESTS.load(Ordering::SeqCst) {
        return Err(valence::Error::Internal(
            "iter: Chronon backend not registered; call register_iter_dispatch at bootstrap".into(),
        ));
    }
    let b = ITER_CHRONON.get().ok_or_else(|| {
        valence::Error::Internal(
            "iter: Chronon backend not registered; call register_iter_dispatch at bootstrap".into(),
        )
    })?;
    let job = b
        .get_job_by_name(CHRONON_ITER_ORCHESTRATOR)
        .await
        .ok_or_else(|| {
            valence::Error::Internal("Chronon job valence-iter-orchestrator not found".into())
        })?;
    b.run_now_with_params(&job.job_id, Some(serde_json::json!({ "run_id": run_id })))
        .await
        .map_err(|e| valence::Error::Internal(e.to_string()))?;
    Ok(())
}

/// Store Chronon for [`super::run_service::IterService::start`] (call once from host bootstrap).
///
/// After this returns, [`super::run_service::IterService::start`] creates a pending run and
/// `run_now`s Chronon job `valence-iter-orchestrator`.
///
/// # Examples
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use valence_platform::iter::dispatch::register_iter_dispatch;
/// use valence_platform::iter::run_service::{IterRunOptions, IterService};
///
/// register_iter_dispatch(Arc::clone(&chronon_backend));
///
/// let run_id = IterService::start(
///     &valence,
///     "MarkProcessedIter",
///     "demo_note",
///     IterRunOptions::default(),
/// )
/// .await?;
/// ```
pub fn register_iter_dispatch(backend: Arc<dyn ChrononCoordinatorBackend>) {
    if ITER_CHRONON.set(backend).is_err() {
        log::warn!(
            target: "valence_iter",
            "register_iter_dispatch: Chronon already set, ignoring"
        );
    }
}
