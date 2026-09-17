//! Boson worker: performs one [`ValenceDeletionStep`] (physical delete / SetNull / RemoveEdge).

// `#[boson_macros::task]` expands the task params into a `*Params` struct without doc hooks on the
// struct or its fields; every hand-written item in this module already carries its own doc comment.
#![allow(missing_docs)]

use super::run_service::DeletionService;
use crate::{ValenceDeletionError, ValenceDeletionStep, ValenceDeletionStepAction};
use anyhow::{anyhow, Context};
use chrono::Utc;
use valence::deletion::apply_deletion_node;
use valence::deletion::dag::{DeletionAction, DeletionNode};
use valence::Actor;
use valence::Model;

async fn bump_run_counter(
    run_id: &str,
    field: &str,
    delta: i64,
    valence: &valence::Valence,
) -> anyhow::Result<()> {
    let j = DeletionService::get_run_json(run_id, valence)
        .await
        .map_err(|e| anyhow!("{}", e))?
        .context("deletion run missing")?;
    let cur = match field {
        "completed_steps" => j
            .get("completed_steps")
            .and_then(|v| v.as_i64())
            .unwrap_or(0),
        "failed_steps" => j.get("failed_steps").and_then(|v| v.as_i64()).unwrap_or(0),
        _ => return Err(anyhow!("unknown run counter {}", field)),
    };
    DeletionService::merge_run(run_id, serde_json::json!({ field: cur + delta }), valence)
        .await
        .map_err(|e| anyhow!("{}", e))
}

fn step_to_node(step: &ValenceDeletionStep) -> anyhow::Result<DeletionNode> {
    let action = match step.action() {
        ValenceDeletionStepAction::CascadeDelete => DeletionAction::CascadeDelete,
        ValenceDeletionStepAction::SetNull => {
            let field = step
                .set_null_field()
                .cloned()
                .ok_or_else(|| anyhow!("set_null step missing set_null_field"))?;
            DeletionAction::SetNull { field }
        }
        ValenceDeletionStepAction::RemoveEdge => {
            let edge_table = step
                .edge_table()
                .cloned()
                .ok_or_else(|| anyhow!("remove_edge step missing edge_table"))?;
            DeletionAction::RemoveEdge { edge_table }
        }
    };
    Ok(DeletionNode {
        table: step.record_table().clone(),
        record_id: step.record_id().clone(),
        action,
        depth: *step.depth() as u32,
        connection_name: step.connection_name().clone(),
        from_table: step.from_table().clone(),
    })
}

/// Execute one deletion step (same body as the Boson task).
pub async fn run_valence_deletion_step_worker(
    boot_valence: valence::Valence,
    run_id: String,
    step_id: String,
) -> anyhow::Result<()> {
    let requester = DeletionService::requester_valence_from_run(&run_id, &boot_valence)
        .await
        .map_err(|e| anyhow!("requester actor restore failed: {e}"))?;
    let sys = boot_valence.with_actor(Actor::System {
        operation: "valence_deletion_step_worker".into(),
    });

    let run_j = match DeletionService::get_run_json(&run_id, &sys).await? {
        Some(j) => j,
        None => return Ok(()),
    };
    if run_j.get("status").and_then(|s| s.as_str()) == Some("cancelled") {
        let _ = ValenceDeletionStep::merge_used(
            &step_id,
            serde_json::json!({
                "status": "skipped",
                "completed_at": Utc::now().timestamp(),
            }),
            &sys,
            valence::use_!(r"In **Valence platform iter and deletion**, we **update Valence Deletion Step** in place so saved changes apply on the next read. The same actors who can run **Valence platform iter and deletion** use the updated values; this step is not a silent copy to an external marketing system."),
        )
        .await;
        return Ok(());
    }

    let step = match ValenceDeletionStep::get_used(&step_id, &sys, valence::use_!(r"In **Valence platform iter and deletion**, we **load Valence Deletion Step** so the application can decide what to do next in this workflow. The result is used by **Valence platform iter and deletion** logic—not necessarily displayed on a page unless that feature’s UI shows it.")).await? {
        Some(s) => s,
        None => return Ok(()),
    };

    ValenceDeletionStep::merge_used(
        &step_id,
        serde_json::json!({
            "status": "in_progress",
            "started_at": Utc::now().timestamp(),
        }),
        &sys,
        valence::use_!(r"In **Valence platform iter and deletion**, we **update Valence Deletion Step** in place so saved changes apply on the next read. The same actors who can run **Valence platform iter and deletion** use the updated values; this step is not a silent copy to an external marketing system."),
    )
    .await
    .map_err(|e| anyhow!("{}", e))?;

    let tbl = step.record_table().as_str();
    let rid = step.record_id().as_str();
    let node = match step_to_node(&step) {
        Ok(n) => n,
        Err(e) => {
            let msg = e.to_string();
            let err = ValenceDeletionError::new(
                run_id.clone(),
                step_id.clone(),
                tbl.to_string(),
                rid.to_string(),
                msg.clone(),
                Utc::now(),
            )
            .map_err(|e2| anyhow!("{}", e2))?;
            ValenceDeletionError::create_used(err, &sys, valence::use_!(r"When **Valence platform iter and deletion** needs to persist work, we **save Valence Deletion Error** so the next step in that feature can continue with the latest values. People and services allowed for **Valence platform iter and deletion** use this data for that workflow—not as a general export of unrelated personal fields."))
                .await
                .map_err(|e2| anyhow!("{}", e2))?;
            ValenceDeletionStep::merge_used(
                &step_id,
                serde_json::json!({
                    "status": "failed",
                    "error_message": msg,
                    "completed_at": Utc::now().timestamp(),
                }),
                &sys,
                valence::use_!(r"In **Valence platform iter and deletion**, we **update Valence Deletion Step** in place so saved changes apply on the next read. The same actors who can run **Valence platform iter and deletion** use the updated values; this step is not a silent copy to an external marketing system."),
            )
            .await
            .map_err(|e2| anyhow!("{}", e2))?;
            bump_run_counter(&run_id, "failed_steps", 1, &sys).await?;
            return Ok(());
        }
    };

    // Physical apply under the deleting user (Delete privacy / deletion-scoped clears).
    let apply_result = apply_deletion_node(&node, &requester)
        .await
        .map_err(|e| anyhow!("{}", e));

    if let Err(e) = apply_result {
        let msg = e.to_string();
        let err = ValenceDeletionError::new(
            run_id.clone(),
            step_id.clone(),
            tbl.to_string(),
            rid.to_string(),
            msg.clone(),
            Utc::now(),
        )
        .map_err(|e2| anyhow!("{}", e2))?;
        ValenceDeletionError::create_used(err, &sys, valence::use_!(r"When **Valence platform iter and deletion** needs to persist work, we **save Valence Deletion Error** so the next step in that feature can continue with the latest values. People and services allowed for **Valence platform iter and deletion** use this data for that workflow—not as a general export of unrelated personal fields."))
            .await
            .map_err(|e2| anyhow!("{}", e2))?;
        ValenceDeletionStep::merge_used(
            &step_id,
            serde_json::json!({
                "status": "failed",
                "error_message": msg,
                "completed_at": Utc::now().timestamp(),
            }),
            &sys,
            valence::use_!(r"In **Valence platform iter and deletion**, we **update Valence Deletion Step** in place so saved changes apply on the next read. The same actors who can run **Valence platform iter and deletion** use the updated values; this step is not a silent copy to an external marketing system."),
        )
        .await
        .map_err(|e2| anyhow!("{}", e2))?;
        bump_run_counter(&run_id, "failed_steps", 1, &sys).await?;
        return Ok(());
    }

    ValenceDeletionStep::merge_used(
        &step_id,
        serde_json::json!({
            "status": "completed",
            "completed_at": Utc::now().timestamp(),
        }),
        &sys,
        valence::use_!(r"In **Valence platform iter and deletion**, we **update Valence Deletion Step** in place so saved changes apply on the next read. The same actors who can run **Valence platform iter and deletion** use the updated values; this step is not a silent copy to an external marketing system."),
    )
    .await
    .map_err(|e| anyhow!("{}", e))?;
    bump_run_counter(&run_id, "completed_steps", 1, &sys).await?;
    Ok(())
}

#[boson_macros::task(
    name = "valence_deletion_step_worker",
    priority = 50,
    pool = "valence_deletion",
    max_in_flight = 2000,
    max_enqueue_per_second = 200,
    max_attempts = 3,
    base_delay_ms = 1000,
    backoff_multiplier = 2.0,
    max_delay_ms = 30_000
)]
/// Boson task entry: forwards to [`run_valence_deletion_step_worker`].
pub async fn valence_deletion_step_worker(
    ctx: Box<dyn boson_core::ExecutionContext>,
    run_id: String,
    step_id: String,
) -> anyhow::Result<()> {
    let valence = boson_valence_identity::valence_from_context(ctx.as_ref())?;
    run_valence_deletion_step_worker(valence, run_id, step_id).await
}
