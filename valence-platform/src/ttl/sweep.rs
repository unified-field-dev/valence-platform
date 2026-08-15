//! Budgeted Chronon TTL sweep for Deferred backends.
//!
//! Each tick discovers expired rows via an indexed `__valence_expire_at` range query
//! (`LIMIT` ≤ remaining cap), queues deletes through [`valence::queue_delete_entity`],
//! then returns. Physical delete runs on the existing deletion DAG / Boson path.
//!
//! # Owns
//!
//! - Discover + enqueue expired Deferred schema-TTL rows
//! - Chronon job `valence-ttl-sweep` and cron resync
//! - Host registration gate ([`register_ttl_service`])
//!
//! # Does not own
//!
//! Native Redis/Mongo expiry, IndraDB TTL, sliding TTL, long-horizon retention enrollment.

use std::sync::{Arc, OnceLock};

use chrono::Utc;
use chronon_coordinator::ChrononCoordinatorBackend;
use valence::{
    list_ttl_table_names, queue_delete_entity_returning_run_id,
    register_noop_deletion_dispatcher_for_tests, BackendTtlCapability, QueryCore, SchemaRegistry,
    SortDirection, StringPredicate, Valence, EXPIRE_AT_FIELD,
};

use crate::deletion::orchestrator::run_valence_deletion_orchestrator_inline_steps;

/// Global queued-delete budget per Chronon tick.
pub const DEFAULT_TTL_SWEEP_CAP: u32 = 32;

const TTL_SWEEP_JOB_NAME: &str = "valence-ttl-sweep";
const TTL_SWEEP_CRON: &str = "*/30 * * * * *";

static TTL_SERVICE_REGISTERED: OnceLock<()> = OnceLock::new();
static TTL_CHRONON: OnceLock<Arc<dyn ChrononCoordinatorBackend>> = OnceLock::new();

/// Outcome of one budgeted sweep tick.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TtlSweepReport {
    /// Tables considered (Deferred TTL schemas only).
    pub tables_considered: u32,
    /// Tables skipped because the backend is native TTL.
    pub skipped_native: u32,
    /// Deletes successfully queued into the deletion DAG.
    pub queued_deletes: u32,
    /// Deletion run ids created this tick (for inline drain).
    pub run_ids: Vec<String>,
    /// `true` when the global cap was fully consumed.
    pub budget_exhausted: bool,
}

/// `true` after a successful [`register_ttl_service`].
#[must_use]
pub fn is_ttl_service_registered() -> bool {
    TTL_SERVICE_REGISTERED.get().is_some()
}

/// Wire the platform TTL sweeper (call once from host bootstrap).
///
/// Sets the registration gate used by the Chronon script and stores the Chronon
/// backend for cron resync helpers. Pair with
/// [`resync_valence_ttl_sweep_job_cron_if_present`] once Chronon jobs exist.
///
/// # Examples
///
/// ```rust,ignore
/// use std::sync::Arc;
/// use valence_platform::ttl::sweep::{
///     register_ttl_service, resync_valence_ttl_sweep_job_cron_if_present,
/// };
///
/// register_ttl_service(Arc::clone(&chronon_backend));
/// resync_valence_ttl_sweep_job_cron_if_present(chronon_backend.as_ref(), &valence).await?;
/// ```
pub fn register_ttl_service(backend: Arc<dyn ChrononCoordinatorBackend>) {
    if TTL_SERVICE_REGISTERED.set(()).is_err() {
        log::warn!(
            target: "valence_ttl",
            "register_ttl_service: already registered, ignoring"
        );
        return;
    }
    let _ = TTL_CHRONON.set(backend);
}

/// Whether a backend TTL capability is included in the Deferred sweeper discover set.
///
/// [`BackendTtlCapability::SupportedNative`] and [`BackendTtlCapability::Unsupported`] are
/// skipped (native engines own expiry; Unsupported has no stamp contract).
#[must_use]
pub const fn ttl_capability_included_in_deferred_sweep(cap: BackendTtlCapability) -> bool {
    matches!(cap, BackendTtlCapability::Deferred)
}

/// List Deferred TTL tables (skip SupportedNative / Unsupported).
fn deferred_ttl_tables(v: &Valence) -> valence::Result<Vec<String>> {
    let registry = SchemaRegistry::global();
    let mut out = Vec::new();
    for table in list_ttl_table_names(registry) {
        let backend = v.backend_for_table(&table)?;
        if ttl_capability_included_in_deferred_sweep(backend.ttl_capability()) {
            out.push(table);
        }
    }
    Ok(out)
}

fn count_native_ttl_tables(v: &Valence) -> valence::Result<u32> {
    let registry = SchemaRegistry::global();
    let mut n = 0u32;
    for table in list_ttl_table_names(registry) {
        let backend = v.backend_for_table(&table)?;
        if matches!(
            backend.ttl_capability(),
            BackendTtlCapability::SupportedNative
        ) {
            n = n.saturating_add(1);
        }
    }
    Ok(n)
}

/// Indexed `expire_at < now` query with `LIMIT` (RFC3339 string order).
async fn list_expired_ids(table: &str, limit: u32, v: &Valence) -> valence::Result<Vec<String>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let now = Utc::now().to_rfc3339();
    let rows: Vec<serde_json::Value> = QueryCore::new(table.to_string())
        .select(vec!["id".to_string()])
        .where_string(EXPIRE_AT_FIELD.to_string(), StringPredicate::LessThan(now))
        .order_by(EXPIRE_AT_FIELD.to_string(), SortDirection::Asc)
        .limit(limit)
        .execute(v)
        .await
        .map_err(|e| valence::Error::database(e.to_string()))?;

    let mut ids = Vec::with_capacity(rows.len());
    for row in rows {
        if let Some(bare) = bare_record_id(row.get("id")) {
            ids.push(bare);
        }
    }
    Ok(ids)
}

fn bare_record_id(raw: Option<&serde_json::Value>) -> Option<String> {
    let raw = raw?;
    match raw {
        serde_json::Value::String(s) => Some(
            s.rsplit_once(':')
                .map(|(_, id)| id.to_string())
                .unwrap_or_else(|| s.clone()),
        ),
        serde_json::Value::Object(map) => map
            .get("id")
            .and_then(|x| x.as_str())
            .map(str::to_string)
            .filter(|s| !s.is_empty()),
        _ => None,
    }
}

/// Fair per-table share of remaining budget (ceil division across remaining tables).
#[must_use]
pub fn fair_table_limit(remaining: u32, tables_left: usize) -> u32 {
    if remaining == 0 || tables_left == 0 {
        return 0;
    }
    let left = u32::try_from(tables_left).unwrap_or(u32::MAX).max(1);
    remaining.div_ceil(left)
}

/// One budgeted sweep: discover expired Deferred rows and queue deletes (no DAG wait).
///
/// When the TTL service is not registered, returns an empty report (Chronon no-op path).
/// Callers that invoke the domain helper directly from tests should
/// [`register_ttl_service`] or use [`run_valence_ttl_sweep_inline`].
///
/// # Errors
///
/// Propagates backend / privacy / queue failures after logging per-row warnings where
/// the batch can continue.
pub async fn sweep_expired_ttl_rows(v: &Valence, cap: u32) -> valence::Result<TtlSweepReport> {
    if !is_ttl_service_registered() {
        log::debug!(
            target: "valence_ttl",
            "sweep: TTL service not registered, skip"
        );
        return Ok(TtlSweepReport::default());
    }
    sweep_expired_ttl_rows_inner(v, cap).await
}

async fn sweep_expired_ttl_rows_inner(v: &Valence, cap: u32) -> valence::Result<TtlSweepReport> {
    let skipped_native = count_native_ttl_tables(v)?;
    let tables = deferred_ttl_tables(v)?;
    let tables_considered = u32::try_from(tables.len()).unwrap_or(u32::MAX);
    let mut remaining = cap;
    let mut queued_deletes = 0u32;
    let mut run_ids = Vec::new();

    for (i, table) in tables.iter().enumerate() {
        if remaining == 0 {
            break;
        }
        let tables_left = tables.len() - i;
        let limit = fair_table_limit(remaining, tables_left);
        if limit == 0 {
            break;
        }

        let ids = match list_expired_ids(table, limit, v).await {
            Ok(ids) => ids,
            Err(e) => {
                log::warn!(
                    target: "valence_ttl",
                    "sweep: list expired failed for table {table}: {e}"
                );
                continue;
            }
        };

        for id in ids {
            if remaining == 0 {
                break;
            }
            match queue_delete_entity_returning_run_id(table, &id, v).await {
                Ok(Some(run_id)) => {
                    run_ids.push(run_id);
                    queued_deletes = queued_deletes.saturating_add(1);
                    remaining = remaining.saturating_sub(1);
                }
                Ok(None) => {
                    // Already pending or missing — do not consume budget.
                }
                Err(e) => {
                    log::warn!(
                        target: "valence_ttl",
                        "sweep: queue_delete failed table={table} error_class={}",
                        error_class(&e)
                    );
                }
            }
        }
    }

    Ok(TtlSweepReport {
        tables_considered,
        skipped_native,
        queued_deletes,
        run_ids,
        budget_exhausted: cap > 0 && remaining == 0,
    })
}

fn error_class(e: &valence::Error) -> &'static str {
    match e {
        valence::Error::Validation(_) => "validation",
        valence::Error::NotFound(_) => "not_found",
        valence::Error::Database { .. } => "database",
        valence::Error::Internal(_) => "internal",
        valence::Error::Privacy(_) => "privacy",
        valence::Error::PendingDeletion(_) => "pending_deletion",
        valence::Error::Serialization { .. } => "serialization",
        valence::Error::Identity(_) => "identity",
    }
}

/// Tests / embedded: queue expired deletes then run the deletion orchestrator inline.
///
/// Ensures a no-op deletion dispatcher is installed when none is registered so
/// [`valence::queue_delete_entity`] can complete, then drains each created run with
/// [`crate::deletion::orchestrator::run_valence_deletion_orchestrator_inline_steps`].
///
/// # Errors
///
/// Propagates sweep or inline orchestrator failures.
///
/// # Examples
///
/// ```rust,ignore
/// use valence_platform::ttl::sweep::{run_valence_ttl_sweep_inline, DEFAULT_TTL_SWEEP_CAP};
///
/// let report = run_valence_ttl_sweep_inline(valence, DEFAULT_TTL_SWEEP_CAP).await?;
/// ```
pub async fn run_valence_ttl_sweep_inline(
    valence: Valence,
    cap: u32,
) -> anyhow::Result<TtlSweepReport> {
    if !is_ttl_service_registered() {
        // Domain helper gates on registration; mark registered without Chronon for tests.
        let _ = TTL_SERVICE_REGISTERED.set(());
    }
    if !valence::is_deletion_dispatcher_registered() {
        register_noop_deletion_dispatcher_for_tests();
    }

    let report = sweep_expired_ttl_rows_inner(&valence, cap)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    for run_id in &report.run_ids {
        run_valence_deletion_orchestrator_inline_steps(valence.clone(), run_id.clone())
            .await
            .map_err(|e| anyhow::anyhow!("{e}"))?;
    }
    Ok(report)
}

#[chronon_coordinator_macros::script(
    name = "valence_ttl_sweep",
    default_job(job = "valence-ttl-sweep", cron = "*/30 * * * * *")
)]
/// Chronon script entry: one budgeted discover+enqueue tick.
pub async fn valence_ttl_sweep_chronon(
    ctx: Box<dyn chronon_core::ScriptContext>,
) -> anyhow::Result<()> {
    let valence = chronon_valence_identity::valence_from_context(&*ctx)?;
    if !is_ttl_service_registered() {
        log::debug!(target: "valence_ttl", "sweep: TTL service not registered, skip");
        return Ok(());
    }
    let report = sweep_expired_ttl_rows(&valence, DEFAULT_TTL_SWEEP_CAP)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    if report.queued_deletes > 0 {
        log::info!(
            target: "valence_ttl",
            "sweep: queued {} delete(s) across {} Deferred TTL table(s)",
            report.queued_deletes,
            report.tables_considered
        );
    }
    Ok(())
}

/// If the persisted `valence-ttl-sweep` job row still reflects an older cron, patch it.
///
/// Call after [`register_ttl_service`] once Chronon default jobs exist.
///
/// # Examples
///
/// ```rust,ignore
/// use valence_platform::ttl::sweep::resync_valence_ttl_sweep_job_cron_if_present;
///
/// resync_valence_ttl_sweep_job_cron_if_present(chronon_backend.as_ref(), &valence).await?;
/// ```
pub async fn resync_valence_ttl_sweep_job_cron_if_present(
    backend: &dyn ChrononCoordinatorBackend,
    valence: &Valence,
) -> anyhow::Result<()> {
    use chronon_coordinator::{
        default_job_schedule_equivalent, merge_default_job_schedule_fields, JobBuilder,
    };

    let Some(existing) = backend.get_job_by_name(TTL_SWEEP_JOB_NAME).await else {
        return Ok(());
    };
    let desired = JobBuilder::new(&ValenceTtlSweepChrononScript::handle())
        .with_valence(valence.clone())
        .name(TTL_SWEEP_JOB_NAME)
        .cron(TTL_SWEEP_CRON)?
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
            target: "valence_ttl",
            "Resynced {} cron expression (schedule drift vs crate default)",
            TTL_SWEEP_JOB_NAME
        );
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fair_table_limit_ceil_share() {
        assert_eq!(fair_table_limit(32, 3), 11);
        assert_eq!(fair_table_limit(32, 1), 32);
        assert_eq!(fair_table_limit(0, 5), 0);
        assert_eq!(fair_table_limit(5, 0), 0);
    }
}
