//! Chronon orchestrator for **Valence Iter** runs.
//!
//! Loads a [`ValenceIterRun`], counts rows, then repeatedly calls [`super::paging::page_row_ids`] with
//! the **bare** id of the last row as the cursor for the next page.
//! For each page it creates a [`ValenceIterBatch`] and dispatches each row via
//! [`ValenceIterOrchestratorRowDispatch`] (Boson queue vs inline worker).
//!
//! After the scan completes, merges run status to `processing` and polls until
//! `processed_rows + skipped_rows + failed_rows >= total_rows`, then sets `completed` or `failed`.
//!
//! The Chronon script [`valence_iter_orchestrator`] calls [`run_valence_iter_orchestrator`] (Boson
//! enqueue). Harnesses without Chronon/Boson can drive the row worker on the same task.
//!
//! The `#[chronon_coordinator_macros::script]` entry [`valence_iter_orchestrator`] is a `ScriptHandle` factory, not a `Future`.

// `#[chronon_coordinator_macros::script]` expands `run_id` into a `*Params` struct field without a doc
// hook; every hand-written item in this module already carries its own doc comment.
#![allow(missing_docs)]

use super::boson_setup::VALENCE_ITER_ROW_WORKER_TASK;
use super::paging;
use super::row_worker::run_valence_iter_row_worker;
use crate::{ValenceIterBatch, ValenceIterBatchStatus, ValenceIterRun, ValenceIterRunStatus};
use anyhow::{anyhow, Context};
use boson_core::BosonError;
use chrono::Utc;
use tokio::time::{sleep, Duration};
use valence::Model;
use valence::Valence;

const DEFAULT_BATCH_SIZE: usize = 1000;
const RATE_LIMIT_BACKOFF_START_MS: u64 = 1000;
const RATE_LIMIT_BACKOFF_MAX_MS: u64 = 30_000;
const POLL_INTERVAL_MS: u64 = 2000;

/// How each table row is handed off to the iter row worker.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValenceIterOrchestratorRowDispatch {
    /// Enqueue [`super::boson_setup::VALENCE_ITER_ROW_WORKER_TASK`] (production).
    BosonQueue,
    /// Call [`run_valence_iter_row_worker`] immediately (test harness).
    InlineWorker,
}

async fn enqueue_row_job(
    valence: &Valence,
    run_id: &str,
    batch_id: &str,
    row_id: &str,
    iter_name: &str,
    target_table: &str,
) -> anyhow::Result<()> {
    let b = boson_runtime::default().ok_or_else(|| anyhow!("boson not configured"))?;
    let actor_json = serde_json::to_value(valence.actor())?;
    let params = serde_json::json!({
        "run_id": run_id,
        "batch_id": batch_id,
        "row_id": row_id,
        "iter_name": iter_name,
        "table_name": target_table,
    });
    let idem = Some(format!("{run_id}:{row_id}"));
    let mut backoff = RATE_LIMIT_BACKOFF_START_MS;
    loop {
        match b
            .enqueue(
                VALENCE_ITER_ROW_WORKER_TASK,
                actor_json.clone(),
                params.clone(),
                idem.clone(),
            )
            .await
        {
            Ok(_) => return Ok(()),
            Err(BosonError::RateLimited(_)) => {
                sleep(Duration::from_millis(backoff)).await;
                backoff = (backoff * 2).min(RATE_LIMIT_BACKOFF_MAX_MS);
            }
            Err(e) => return Err(anyhow!("{}", e)),
        }
    }
}

async fn run_valence_iter_orchestrator_impl(
    valence: Valence,
    run_id: String,
    row_dispatch: ValenceIterOrchestratorRowDispatch,
) -> anyhow::Result<()> {
    let run = ValenceIterRun::get(&run_id, &valence)
        .await
        .map_err(|e| anyhow!("{}", e))?
        .context("iter run not found")?;

    if *run.status() == ValenceIterRunStatus::Cancelled {
        return Ok(());
    }

    let target_table = run.target_table().clone();
    let iter_name = run.iter_name().clone();

    ValenceIterRun::merge(
        &run_id,
        serde_json::json!({
            "status": "scanning",
            "started_at": Utc::now().timestamp(),
        }),
        &valence,
    )
    .await
    .map_err(|e| anyhow!("{}", e))?;

    let total = paging::count_table_rows(&valence, &target_table).await?;
    ValenceIterRun::merge(
        &run_id,
        serde_json::json!({ "total_rows": total }),
        &valence,
    )
    .await
    .map_err(|e| anyhow!("{}", e))?;

    let mut after_cursor: Option<String> = None;
    let mut batch_index: i64 = 0;

    loop {
        let run_row = ValenceIterRun::get(&run_id, &valence)
            .await
            .map_err(|e| anyhow!("{}", e))?
            .context("iter run disappeared")?;
        if *run_row.status() == ValenceIterRunStatus::Cancelled {
            return Ok(());
        }

        let page = paging::page_row_ids(
            &valence,
            &target_table,
            after_cursor.as_deref(),
            DEFAULT_BATCH_SIZE,
        )
        .await?;

        if page.is_empty() {
            break;
        }

        let batch_id = uuid::Uuid::new_v4().to_string();
        let row_count = page.len() as i64;
        let last_bare_id = page.last().cloned();

        let batch = ValenceIterBatch::new(
            run_id.clone(),
            batch_index,
            ValenceIterBatchStatus::Enqueuing,
            row_count,
            0,
            0,
            0,
            0,
            last_bare_id.clone(),
            Utc::now(),
            None,
        )
        .map_err(|e| anyhow!("{}", e))?;
        ValenceIterBatch::upsert(&batch_id, batch, &valence)
            .await
            .map_err(|e| anyhow!("{}", e))?;

        let mut enqueued: i64 = 0;
        for rid in &page {
            match row_dispatch {
                ValenceIterOrchestratorRowDispatch::BosonQueue => {
                    enqueue_row_job(&valence, &run_id, &batch_id, rid, &iter_name, &target_table)
                        .await?;
                }
                ValenceIterOrchestratorRowDispatch::InlineWorker => {
                    run_valence_iter_row_worker(
                        valence.clone(),
                        run_id.clone(),
                        batch_id.clone(),
                        rid.clone(),
                        iter_name.clone(),
                        target_table.clone(),
                    )
                    .await?;
                }
            }
            enqueued += 1;
            ValenceIterBatch::merge(
                &batch_id,
                serde_json::json!({ "enqueued_count": enqueued }),
                &valence,
            )
            .await
            .map_err(|e| anyhow!("{}", e))?;
        }

        ValenceIterBatch::merge(
            &batch_id,
            serde_json::json!({ "status": "processing" }),
            &valence,
        )
        .await
        .map_err(|e| anyhow!("{}", e))?;

        let scanned_so_far = *ValenceIterRun::get(&run_id, &valence)
            .await
            .map_err(|e| anyhow!("{}", e))?
            .context("run missing")?
            .scanned_rows();
        ValenceIterRun::merge(
            &run_id,
            serde_json::json!({ "scanned_rows": scanned_so_far + row_count }),
            &valence,
        )
        .await
        .map_err(|e| anyhow!("{}", e))?;

        after_cursor = last_bare_id;
        batch_index += 1;
    }

    ValenceIterRun::merge(
        &run_id,
        serde_json::json!({ "status": "processing" }),
        &valence,
    )
    .await
    .map_err(|e| anyhow!("{}", e))?;

    loop {
        let r = ValenceIterRun::get(&run_id, &valence)
            .await
            .map_err(|e| anyhow!("{}", e))?
            .context("run missing")?;

        if *r.status() == ValenceIterRunStatus::Cancelled {
            return Ok(());
        }

        let done = *r.processed_rows() + *r.skipped_rows() + *r.failed_rows();
        if done >= *r.total_rows() {
            let terminal = if *r.failed_rows() > 0 {
                "failed"
            } else {
                "completed"
            };
            ValenceIterRun::merge(
                &run_id,
                serde_json::json!({
                    "status": terminal,
                    "completed_at": Utc::now().timestamp(),
                }),
                &valence,
            )
            .await
            .map_err(|e| anyhow!("{}", e))?;
            return Ok(());
        }

        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
}

/// Run the full scan → enqueue → poll loop using the **Boson** queue for each row.
///
/// Same behavior as the [`valence_iter_orchestrator`] Chronon script body.
/// Hosts start runs via [`super::run_service::IterService::start`] (Chronon `run_now`); this
/// function is what that job invokes.
///
/// # Examples
///
/// ```rust,ignore
/// use valence_platform::iter::dispatch::register_iter_dispatch;
/// use valence_platform::iter::run_service::{IterRunOptions, IterService};
/// use std::sync::Arc;
///
/// register_iter_dispatch(Arc::clone(&chronon_backend));
/// let run_id = IterService::start(
///     &valence,
///     "MarkProcessedIter",
///     "demo_note",
///     IterRunOptions::default(),
/// )
/// .await?;
/// ```
pub async fn run_valence_iter_orchestrator(valence: Valence, run_id: String) -> anyhow::Result<()> {
    run_valence_iter_orchestrator_impl(
        valence,
        run_id,
        ValenceIterOrchestratorRowDispatch::BosonQueue,
    )
    .await
}

/// Like [`run_valence_iter_orchestrator`], but calls [`super::row_worker::run_valence_iter_row_worker`]
/// **inline** for each row (no Boson enqueue).
///
/// For tests and examples without Chronon/Boson. Hosts should use
/// [`super::run_service::IterService::start`].
#[doc(hidden)]
pub async fn run_valence_iter_orchestrator_for_tests(
    valence: Valence,
    run_id: String,
) -> anyhow::Result<()> {
    run_valence_iter_orchestrator_impl(
        valence,
        run_id,
        ValenceIterOrchestratorRowDispatch::InlineWorker,
    )
    .await
}

#[chronon_coordinator_macros::script(
    name = "valence_iter_orchestrator",
    default_job(job = "valence-iter-orchestrator", manual)
)]
/// Chronon script entry: forwards to [`run_valence_iter_orchestrator`].
pub async fn valence_iter_orchestrator(
    ctx: Box<dyn chronon_core::ScriptContext>,
    run_id: String,
) -> anyhow::Result<()> {
    let valence = chronon_valence_identity::valence_from_context(&*ctx)?;
    run_valence_iter_orchestrator(valence, run_id).await
}
