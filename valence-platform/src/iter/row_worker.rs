//! Boson worker: executes **one row** of a [`crate::ValenceIterRun`].
//!
//! Resolves [`valence::find_iter_descriptor`], loads the row via privacy-aware [`valence::QueryCore::get_entity`],
//! injects a synthetic `id` into the JSON payload when Surreal omitted it, then runs generated
//! `should_run` / `execute` functions.
//!
//! Updates [`crate::ValenceIterRun`] counters (`processed_rows`, `skipped_rows`, `failed_rows`) and
//! matching [`crate::ValenceIterBatch`] counters; failures create [`crate::ValenceIterRowError`].
//!
//! # Entrypoints
//!
//! - [`run_valence_iter_row_worker`] — full logic; use directly in tests.
//! - `valence_iter_row_worker` — `#[boson::task]` entry (macro may rename the body to `*_impl`).

// `#[boson_macros::task]` expands the task params into a `*Params` struct without doc hooks on the
// struct or its fields; every hand-written item in this module already carries its own doc comment.
#![allow(missing_docs)]

use crate::{
    ValenceIterBatch, ValenceIterRowError, ValenceIterRowErrorErrorKind, ValenceIterRun,
    ValenceIterRunStatus,
};
use anyhow::{anyhow, Context};
use chrono::Utc;
use std::collections::BTreeMap;

use valence::Model;
use valence::QueryCore;
use valence::Valence;

/// Surreal row payloads often omit the primary key from the field map. Iter glue deserializes into
/// generated models that expect `id` for merges; inject `table:id` when missing.
fn inject_row_id_if_missing(
    table_name: &str,
    row_id: &str,
    mut row: serde_json::Value,
) -> serde_json::Value {
    let synthetic = format!("{table_name}:{row_id}");
    if let serde_json::Value::Object(ref mut map) = row {
        let missing_or_null = matches!(map.get("id"), None | Some(serde_json::Value::Null));
        if missing_or_null {
            map.insert("id".to_string(), serde_json::Value::String(synthetic));
        }
    }
    row
}

async fn load_row_json(
    valence: &Valence,
    table_name: &str,
    row_id: &str,
) -> anyhow::Result<serde_json::Value> {
    match QueryCore::get_entity(table_name, row_id, valence).await {
        Ok(Some(entity)) => {
            let mut map: BTreeMap<String, serde_json::Value> = entity.data.clone();
            map.insert(
                "id".to_string(),
                serde_json::Value::String(format!("{table_name}:{row_id}")),
            );
            Ok(serde_json::Value::Object(map.into_iter().collect()))
        }
        Ok(None) => Err(anyhow!("row not found")),
        Err(e) => Err(anyhow!("{}", e)),
    }
}

async fn bump_run_field(
    run_id: &str,
    field: &str,
    delta: i64,
    valence: &Valence,
) -> anyhow::Result<()> {
    let run = ValenceIterRun::get(run_id, valence)
        .await
        .map_err(|e| anyhow!("{}", e))?
        .context("iter run missing")?;
    let cur = match field {
        "processed_rows" => *run.processed_rows(),
        "skipped_rows" => *run.skipped_rows(),
        "failed_rows" => *run.failed_rows(),
        _ => return Err(anyhow!("unknown run counter field {}", field)),
    };
    let patch = serde_json::json!({ field: cur + delta });
    ValenceIterRun::merge(run_id, patch, valence)
        .await
        .map_err(|e| anyhow!("{}", e))?;
    Ok(())
}

async fn bump_batch_field(
    batch_id: &str,
    field: &str,
    delta: i64,
    valence: &Valence,
) -> anyhow::Result<()> {
    let batch = ValenceIterBatch::get(batch_id, valence)
        .await
        .map_err(|e| anyhow!("{}", e))?
        .context("iter batch missing")?;
    let cur = match field {
        "processed" => *batch.processed(),
        "skipped" => *batch.skipped(),
        "failed" => *batch.failed(),
        _ => return Err(anyhow!("unknown batch counter field {}", field)),
    };
    let patch = serde_json::json!({ field: cur + delta });
    ValenceIterBatch::merge(batch_id, patch, valence)
        .await
        .map_err(|e| anyhow!("{}", e))?;
    Ok(())
}

async fn maybe_complete_batch(batch_id: &str, valence: &Valence) -> anyhow::Result<()> {
    let batch = ValenceIterBatch::get(batch_id, valence)
        .await
        .map_err(|e| anyhow!("{}", e))?
        .context("batch missing")?;
    let done = *batch.processed() + *batch.skipped() + *batch.failed();
    if done >= *batch.row_count() {
        ValenceIterBatch::merge(
            batch_id,
            serde_json::json!({
                "status": "completed",
                "completed_at": Utc::now().timestamp(),
            }),
            valence,
        )
        .await
        .map_err(|e| anyhow!("{}", e))?;
    }
    Ok(())
}

/// Directly run row-worker logic (same as the Boson task body). Use this in tests; the
/// `#[boson::task]` entrypoint is renamed to `__valence_iter_row_worker_impl` by the macro.
pub async fn run_valence_iter_row_worker(
    valence: Valence,
    run_id: String,
    batch_id: String,
    row_id: String,
    iter_name: String,
    table_name: String,
) -> anyhow::Result<()> {
    let run = match ValenceIterRun::get(&run_id, &valence)
        .await
        .map_err(|e| anyhow!("{}", e))?
    {
        Some(r) => r,
        None => return Ok(()),
    };

    if *run.status() == ValenceIterRunStatus::Cancelled {
        return Ok(());
    }

    let desc = inventory::iter::<valence::IterDescriptor>
        .into_iter()
        .find(|d| d.table_name == table_name && d.iter_type_name == iter_name)
        .ok_or_else(|| {
            anyhow!(
                "no IterDescriptor for table={} iter={}",
                table_name,
                iter_name
            )
        })?;

    let row_json = match load_row_json(&valence, &table_name, &row_id).await {
        Ok(j) => inject_row_id_if_missing(&table_name, &row_id, j),
        Err(e) => {
            let err = ValenceIterRowError::new(
                run_id.clone(),
                Some(batch_id.clone()),
                row_id.clone(),
                e.to_string(),
                ValenceIterRowErrorErrorKind::ShouldRunError,
                Utc::now(),
            )
            .map_err(|e2| anyhow!("{}", e2))?;
            ValenceIterRowError::create(err, &valence)
                .await
                .map_err(|e2| anyhow!("{}", e2))?;
            bump_run_field(&run_id, "failed_rows", 1, &valence).await?;
            bump_batch_field(&batch_id, "failed", 1, &valence).await?;
            maybe_complete_batch(&batch_id, &valence).await?;
            return Ok(());
        }
    };

    let eval = match (desc.should_run)(valence.clone(), row_json.clone()).await {
        Ok(e) => e,
        Err(e) => {
            let err = ValenceIterRowError::new(
                run_id.clone(),
                Some(batch_id.clone()),
                row_id.clone(),
                e.to_string(),
                ValenceIterRowErrorErrorKind::ShouldRunError,
                Utc::now(),
            )
            .map_err(|e2| anyhow!("{}", e2))?;
            ValenceIterRowError::create(err, &valence)
                .await
                .map_err(|e2| anyhow!("{}", e2))?;
            bump_run_field(&run_id, "failed_rows", 1, &valence).await?;
            bump_batch_field(&batch_id, "failed", 1, &valence).await?;
            maybe_complete_batch(&batch_id, &valence).await?;
            return Ok(());
        }
    };

    if !eval.should_run {
        bump_run_field(&run_id, "skipped_rows", 1, &valence).await?;
        bump_batch_field(&batch_id, "skipped", 1, &valence).await?;
        maybe_complete_batch(&batch_id, &valence).await?;
        return Ok(());
    }

    if let Err(e) = (desc.execute)(valence.clone(), row_json).await {
        let err = ValenceIterRowError::new(
            run_id.clone(),
            Some(batch_id.clone()),
            row_id.clone(),
            e.to_string(),
            ValenceIterRowErrorErrorKind::ExecuteError,
            Utc::now(),
        )
        .map_err(|e2| anyhow!("{}", e2))?;
        ValenceIterRowError::create(err, &valence)
            .await
            .map_err(|e2| anyhow!("{}", e2))?;
        bump_run_field(&run_id, "failed_rows", 1, &valence).await?;
        bump_batch_field(&batch_id, "failed", 1, &valence).await?;
        maybe_complete_batch(&batch_id, &valence).await?;
        return Ok(());
    }

    bump_run_field(&run_id, "processed_rows", 1, &valence).await?;
    bump_batch_field(&batch_id, "processed", 1, &valence).await?;
    maybe_complete_batch(&batch_id, &valence).await?;
    Ok(())
}

#[boson_macros::task(
    name = "valence_iter_row_worker",
    priority = 50,
    pool = "valence_iter",
    max_in_flight = 1000,
    max_enqueue_per_second = 100,
    max_attempts = 3,
    base_delay_ms = 1000,
    backoff_multiplier = 2.0,
    max_delay_ms = 30_000
)]
/// Boson task entry: forwards to [`run_valence_iter_row_worker`].
pub async fn valence_iter_row_worker(
    ctx: Box<dyn boson_core::ExecutionContext>,
    run_id: String,
    batch_id: String,
    row_id: String,
    iter_name: String,
    table_name: String,
) -> anyhow::Result<()> {
    let valence = boson_valence_identity::valence_from_context(ctx.as_ref())?;
    run_valence_iter_row_worker(valence, run_id, batch_id, row_id, iter_name, table_name).await
}
