//! Chronon orchestrator for **Valence deletion** runs (DAG → step rows → Boson workers).

// `#[chronon_coordinator_macros::script]` expands `run_id` into a `*Params` struct field without a doc
// hook; every hand-written item in this module already carries its own doc comment.
#![allow(missing_docs)]

use super::boson_setup::VALENCE_DELETION_STEP_WORKER_TASK;
use super::run_service::DeletionService;
use super::step_worker::run_valence_deletion_step_worker;
use crate::{ValenceDeletionStep, ValenceDeletionStepAction, ValenceDeletionStepStatus};
use anyhow::{anyhow, Context};
use boson_core::BosonError;
use chrono::Utc;
use serde_json::Value;
use tokio::time::{sleep, Duration};
use valence::deletion::dag::DeletionDag;
use valence::Model;
use valence::Valence;
use valence::{IntPredicate, QueryCore, StringPredicate};

const RATE_LIMIT_BACKOFF_START_MS: u64 = 1000;
const RATE_LIMIT_BACKOFF_MAX_MS: u64 = 30_000;
const POLL_INTERVAL_MS: u64 = 200;

/// How each [`ValenceDeletionStep`] is executed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValenceDeletionOrchestratorStepDispatch {
    /// Enqueue [`VALENCE_DELETION_STEP_WORKER_TASK`] (production).
    BosonQueue,
    /// Call [`run_valence_deletion_step_worker`] inline (embedded / tests).
    InlineWorker,
}

async fn enqueue_step_job(valence: &Valence, run_id: &str, step_id: &str) -> anyhow::Result<()> {
    let b = boson_runtime::default().ok_or_else(|| anyhow!("boson not configured"))?;
    let actor_json = serde_json::to_value(valence.actor())?;
    let params = serde_json::json!({
        "run_id": run_id,
        "step_id": step_id,
    });
    let idem = Some(format!("{run_id}:{step_id}"));
    let mut backoff = RATE_LIMIT_BACKOFF_START_MS;
    loop {
        match b
            .enqueue(
                VALENCE_DELETION_STEP_WORKER_TASK,
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

/// Whether any step of `run_id` at `depth` is still queued or in progress.
///
/// Built with [`QueryCore`] so Valence compiles it for whichever engine stores the steps.
async fn wave_has_inflight(valence: &Valence, run_id: &str, depth: u32) -> anyhow::Result<bool> {
    for status in [
        ValenceDeletionStepStatus::Queued,
        ValenceDeletionStepStatus::InProgress,
    ] {
        let rows: Vec<Value> = QueryCore::new("valence_deletion_step".to_string())
            .where_string("run_id".to_string(), StringPredicate::Equals(run_id.to_string()))
            .where_int("depth".to_string(), IntPredicate::Equals(i64::from(depth)))
            .where_string(
                "status".to_string(),
                StringPredicate::Equals(status.as_str().to_string()),
            )
            .limit(1)
            .execute(valence, valence::use_!(r"In **Valence platform iter and deletion**, we **check for unfinished deletion steps in one wave** so the orchestrator starts the next wave only after the current one is done. This is an internal system check, not shown to operators or end users."))
            .await
            .map_err(|e| anyhow!("{}", e))?;
        if !rows.is_empty() {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn wait_wave(valence: &Valence, run_id: &str, depth: u32) -> anyhow::Result<()> {
    loop {
        let st = DeletionService::get_run_json(run_id, valence)
            .await?
            .and_then(|j| {
                j.get("status")
                    .and_then(|s| s.as_str().map(|x| x.to_string()))
            });
        if st.as_deref() == Some("cancelled") {
            return Ok(());
        }
        if !wave_has_inflight(valence, run_id, depth).await? {
            break;
        }
        sleep(Duration::from_millis(POLL_INTERVAL_MS)).await;
    }
    Ok(())
}

/// Mark every still-queued step of a cancelled run `skipped`.
async fn mark_remaining_queued_skipped(valence: &Valence, run_id: &str) -> anyhow::Result<()> {
    // Each pass skips up to one page of steps, and skipped steps leave the filter, so the
    // next pass sees the rest. A run has at most one step per DAG node.
    loop {
        let mut query = ValenceDeletionStep::query(
            valence,
            valence::use_!(
                r"In **Valence platform iter and deletion**, we **list the queued steps of a cancelled deletion run** so each can be marked skipped instead of running. This is an internal system step, not shown to operators or end users."
            ),
        );
        query.inner = query
            .inner
            .where_string(
                "run_id".to_string(),
                StringPredicate::Equals(run_id.to_string()),
            )
            .where_string(
                "status".to_string(),
                StringPredicate::Equals(ValenceDeletionStepStatus::Queued.as_str().to_string()),
            );
        let queued = query.await.map_err(|e| anyhow!("{}", e))?;
        if queued.is_empty() {
            return Ok(());
        }
        for step in queued {
            step.get_mutable(
                valence,
                valence::use_!(r"In **Valence platform iter and deletion**, we **load the deletion step as a mutable builder** so it can be marked skipped after its run was cancelled. The same actors who can run **Valence platform iter and deletion** use the updated values."),
            )
            .set_status(ValenceDeletionStepStatus::Skipped)
            .and_then(|b| b.set_completed_at(Utc::now()))
            .map_err(|e| anyhow!("{}", e))?
            .commit()
            .await
            .map_err(|e| anyhow!("{}", e))?;
        }
    }
}

/// Run [`orchestrate`]; if it fails partway, mark the run `failed` with the error, so a
/// run whose orchestrator died never sits in `scanning` or `processing` for good.
async fn run_valence_deletion_orchestrator_impl(
    boot_valence: Valence,
    run_id: String,
    dispatch: ValenceDeletionOrchestratorStepDispatch,
) -> anyhow::Result<()> {
    let result = orchestrate(boot_valence.clone(), run_id.clone(), dispatch).await;
    if let Err(e) = &result {
        mark_run_failed(&boot_valence, &run_id, e).await;
    }
    result
}

/// Merge `failed` onto a run this orchestrator had started (`scanning` or `processing`).
/// A run still `queued` was never claimed here, and a terminal run is left as it is.
async fn mark_run_failed(boot_valence: &Valence, run_id: &str, error: &anyhow::Error) {
    let sys = boot_valence.with_actor(valence::Actor::System {
        operation: "valence_deletion_orchestrator".into(),
    });
    let status = match DeletionService::get_run_json(run_id, &sys).await {
        Ok(Some(run)) => run
            .get("status")
            .and_then(|s| s.as_str())
            .map(str::to_string),
        Ok(None) => return,
        Err(e) => {
            log::warn!("deletion run {run_id}: status unreadable after orchestrator error: {e}");
            return;
        }
    };
    if !matches!(status.as_deref(), Some("scanning" | "processing")) {
        return;
    }
    let patch = serde_json::json!({
        "status": "failed",
        "error_message": format!("orchestrator failed: {error}"),
        "completed_at": Utc::now(),
    });
    if let Err(e) = DeletionService::merge_run(run_id, patch, &sys).await {
        log::warn!("deletion run {run_id}: could not mark failed: {e}");
    }
}

async fn orchestrate(
    boot_valence: Valence,
    run_id: String,
    dispatch: ValenceDeletionOrchestratorStepDispatch,
) -> anyhow::Result<()> {
    if !DeletionService::try_claim_queued_to_scanning(&run_id, &boot_valence)
        .await
        .map_err(|e| anyhow!("{}", e))?
    {
        // Another `run_now` won the race, or the run is no longer `queued` (cancelled / already started).
        return Ok(());
    }

    let valence = match DeletionService::requester_valence_from_run(&run_id, &boot_valence).await {
        Ok(v) => v,
        Err(e) => {
            let _ = DeletionService::merge_run(
                &run_id,
                serde_json::json!({
                    "status": "failed",
                    "error_message": format!("requester actor restore failed: {e}"),
                    "completed_at": Utc::now(),
                }),
                &boot_valence,
            )
            .await;
            return Err(anyhow!("requester actor restore failed: {e}"));
        }
    };
    // Step/run row policies are SYSTEM_ONLY — keep persistence on boot/system.
    let sys = boot_valence.with_actor(valence::Actor::System {
        operation: "valence_deletion_orchestrator".into(),
    });

    let run_j = DeletionService::get_run_json(&run_id, &sys)
        .await?
        .context("deletion run not found")?;
    let root_table = run_j
        .get("root_table")
        .and_then(|v| v.as_str())
        .context("root_table missing")?
        .to_string();
    let root_record_id = run_j
        .get("root_record_id")
        .and_then(|v| v.as_str())
        .context("root_record_id missing")?
        .to_string();

    let dag = DeletionDag::compute(&root_table, &root_record_id, &valence)
        .await
        .map_err(|e| anyhow!("{}", e))?;
    if !dag.restrict_violations.is_empty() {
        DeletionService::merge_run(
            &run_id,
            serde_json::json!({
                "status": "failed",
                "error_message": format!("{violations:?}", violations = dag.restrict_violations),
                "completed_at": Utc::now(),
            }),
            &sys,
        )
        .await
        .map_err(|e| anyhow!("{}", e))?;
        return Ok(());
    }

    let total_steps = dag.nodes.len() as i64;
    DeletionService::merge_run(
        &run_id,
        serde_json::json!({
            "status": "processing",
            "total_steps": total_steps,
        }),
        &sys,
    )
    .await
    .map_err(|e| anyhow!("{}", e))?;

    if total_steps == 0 {
        DeletionService::merge_run(
            &run_id,
            serde_json::json!({
                "status": "completed",
                "completed_at": Utc::now(),
            }),
            &sys,
        )
        .await
        .map_err(|e| anyhow!("{}", e))?;
        return Ok(());
    }

    let max_d = dag.nodes.iter().map(|n| n.depth).max().unwrap_or(0);
    for d in (0..=max_d).rev() {
        let st = DeletionService::get_run_json(&run_id, &sys)
            .await?
            .and_then(|j| {
                j.get("status")
                    .and_then(|s| s.as_str().map(|x| x.to_string()))
            });
        if st.as_deref() == Some("cancelled") {
            mark_remaining_queued_skipped(&sys, &run_id).await?;
            return Ok(());
        }

        // Within depth: RemoveEdge → SetNull → CascadeDelete (sub-waves so Boson parallel is safe).
        for wave_ord in 0u8..=2 {
            let wave_nodes: Vec<_> = dag
                .nodes
                .iter()
                .filter(|n| n.depth == d && n.action.wave_order() == wave_ord)
                .collect();
            if wave_nodes.is_empty() {
                continue;
            }
            for node in wave_nodes {
                let step_id = uuid::Uuid::new_v4().to_string();
                let (action, set_null_field, edge_table) = match &node.action {
                    valence::deletion::dag::DeletionAction::CascadeDelete => {
                        (ValenceDeletionStepAction::CascadeDelete, None, None)
                    }
                    valence::deletion::dag::DeletionAction::SetNull { field } => (
                        ValenceDeletionStepAction::SetNull,
                        Some(field.clone()),
                        None,
                    ),
                    valence::deletion::dag::DeletionAction::RemoveEdge { edge_table } => (
                        ValenceDeletionStepAction::RemoveEdge,
                        None,
                        Some(edge_table.clone()),
                    ),
                };
                let row = ValenceDeletionStep::new(
                    run_id.clone(),
                    node.table.clone(),
                    node.record_id.clone(),
                    action,
                    set_null_field,
                    edge_table,
                    ValenceDeletionStepStatus::Queued,
                    node.depth as i64,
                    node.connection_name.clone(),
                    node.from_table.clone(),
                    None,
                    None,
                    None,
                )
                .map_err(|e| anyhow!("{}", e))?;
                ValenceDeletionStep::upsert(&step_id, row, &sys, valence::use_!(r"When **Valence platform iter and deletion** needs to persist work, we **save Valence Deletion Step** so the next step in that feature can continue with the latest values. People and services allowed for **Valence platform iter and deletion** use this data for that workflow—not as a general export of unrelated personal fields."))
                    .await
                    .map_err(|e| anyhow!("{}", e))?;

                match dispatch {
                    ValenceDeletionOrchestratorStepDispatch::BosonQueue => {
                        enqueue_step_job(&valence, &run_id, &step_id).await?;
                    }
                    ValenceDeletionOrchestratorStepDispatch::InlineWorker => {
                        run_valence_deletion_step_worker(valence.clone(), run_id.clone(), step_id)
                            .await?;
                    }
                }
            }
            if dispatch == ValenceDeletionOrchestratorStepDispatch::BosonQueue {
                wait_wave(&sys, &run_id, d).await?;
            }
        }
    }

    let run_after = DeletionService::get_run_json(&run_id, &sys)
        .await?
        .context("run missing after orchestration")?;
    if run_after.get("status").and_then(|s| s.as_str()) == Some("cancelled") {
        return Ok(());
    }
    let failed = run_after
        .get("failed_steps")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);
    let terminal = if failed > 0 { "failed" } else { "completed" };
    DeletionService::merge_run(
        &run_id,
        serde_json::json!({
            "status": terminal,
            "completed_at": Utc::now(),
        }),
        &sys,
    )
    .await
    .map_err(|e| anyhow!("{}", e))?;
    Ok(())
}

/// Production path: enqueue Boson step workers depth-by-depth.
pub async fn run_valence_deletion_orchestrator(
    valence: Valence,
    run_id: String,
) -> anyhow::Result<()> {
    run_valence_deletion_orchestrator_impl(
        valence,
        run_id,
        ValenceDeletionOrchestratorStepDispatch::BosonQueue,
    )
    .await
}

/// Embedded / tests: run step worker inline (no Boson).
///
/// After success, poll [`super::run_service::DeletionService::get_run_json`] for terminal
/// `status` (`completed` / `failed` / `cancelled`).
///
/// # Errors
///
/// Propagates DAG build, privacy, claim, or step-worker failures as `anyhow::Error`.
///
/// # Examples
///
/// ```rust,ignore
/// use valence_platform::deletion::orchestrator::run_valence_deletion_orchestrator_inline_steps;
/// use valence_platform::deletion::run_service::DeletionService;
///
/// run_valence_deletion_orchestrator_inline_steps(valence.clone(), run_id.clone()).await?;
/// let run = DeletionService::get_run_json(&run_id, &valence).await?.expect("run");
/// assert_eq!(run.get("status").and_then(|s| s.as_str()), Some("completed"));
/// ```
pub async fn run_valence_deletion_orchestrator_inline_steps(
    valence: Valence,
    run_id: String,
) -> anyhow::Result<()> {
    run_valence_deletion_orchestrator_impl(
        valence,
        run_id,
        ValenceDeletionOrchestratorStepDispatch::InlineWorker,
    )
    .await
}

#[chronon_coordinator_macros::script(
    name = "valence_deletion_orchestrator",
    default_job(job = "valence-deletion-orchestrator", manual)
)]
/// Chronon script entry: forwards to [`run_valence_deletion_orchestrator`].
pub async fn valence_deletion_orchestrator(
    ctx: Box<dyn chronon_core::ScriptContext>,
    run_id: String,
) -> anyhow::Result<()> {
    let valence = chronon_valence_identity::valence_from_context(&*ctx)?;
    run_valence_deletion_orchestrator(valence, run_id).await
}
