//! Platform deletion-run helpers (extended beyond uf-valence-core DeletionService).

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use std::time::Duration;
use uuid::Uuid;

use valence::__internal::CompiledQuery;
use valence::Actor;
use valence::Valence;
use valence::{DateTimePredicate, QueryCore, SortDirection, StringPredicate};
use valence::{Error, Result};

fn system_valence(v: &Valence) -> Valence {
    v.with_actor(Actor::System {
        operation: "valence_deletion_run".to_string(),
    })
}

/// Parse `valence_deletion_run.requested_by` (stringified actor JSON or object) into [`Actor`].
///
/// # Errors
///
/// Returns [`Error::Internal`] when the field is missing, empty, or not valid actor JSON.
pub fn parse_requested_by_actor(requested_by: &Value) -> Result<Actor> {
    let actor: Actor = match requested_by {
        Value::String(s) => serde_json::from_str(s).map_err(|e| {
            Error::Internal(format!(
                "invalid deletion run requested_by JSON string: {e}"
            ))
        })?,
        other => serde_json::from_value(other.clone()).map_err(|e| {
            Error::Internal(format!("invalid deletion run requested_by value: {e}"))
        })?,
    };
    Ok(actor)
}

/// CRUD helpers for the `valence_deletion_run` platform table.
pub struct DeletionService;
impl DeletionService {
    /// Rebuild a [`Valence`] handle as the deleting actor stored on the run (`requested_by`).
    ///
    /// Chronon/Boson jobs may boot as System; call this before privacy, DAG work, or physical
    /// delete so those paths run as the requester — not System.
    ///
    /// # Errors
    ///
    /// Missing run, missing/`null` `requested_by`, or unparseable actor JSON.
    pub async fn requester_valence_from_run(run_id: &str, boot: &Valence) -> Result<Valence> {
        let run = Self::get_run_json(run_id, boot)
            .await?
            .ok_or_else(|| Error::NotFound(format!("deletion run {run_id} not found")))?;
        let requested_by = run.get("requested_by").ok_or_else(|| {
            Error::Internal(format!(
                "deletion run {run_id} missing requested_by; refusing System fallback"
            ))
        })?;
        if requested_by.is_null() {
            return Err(Error::Internal(format!(
                "deletion run {run_id} has null requested_by; refusing System fallback"
            )));
        }
        let actor = parse_requested_by_actor(requested_by)?;
        Ok(boot.with_actor(actor))
    }

    /// Create a new deletion run row and return its id.
    ///
    /// # Errors
    ///
    /// Backend create failures for `valence_deletion_run`.
    pub async fn create_run(
        root_table: &str,
        root_record_id: &str,
        actor_json: Value,
        v: &Valence,
    ) -> Result<String> {
        let run_id = Uuid::new_v4().to_string();
        let requested_by = actor_json.to_string();
        let sys = system_valence(v);
        let backend = sys.backend_for_table("valence_deletion_run")?;
        let row = json!({
            "id": run_id,
            "root_table": root_table,
            "root_record_id": root_record_id,
            "status": "queued",
            "total_steps": 0,
            "completed_steps": 0,
            "failed_steps": 0,
            "requested_by": requested_by,
            "requested_at": Utc::now(),
        });
        backend
            .create_record("valence_deletion_run", row)
            .await
            .map_err(|e| Error::database(e.to_string()))?;
        Ok(run_id)
    }

    /// Fetch a run's raw JSON by id, or `None` if it does not exist.
    pub async fn get_run_json(run_id: &str, v: &Valence) -> Result<Option<Value>> {
        let sys = system_valence(v);
        QueryCore::get_record_json("valence_deletion_run", run_id, &sys)
            .await
            .map_err(|e| Error::database(e.to_string()))
    }

    /// Merge `patch` into the run row.
    pub async fn merge_run(run_id: &str, patch: Value, v: &Valence) -> Result<()> {
        let sys = system_valence(v);
        let backend = sys.backend_for_table("valence_deletion_run")?;
        backend
            .merge_record("valence_deletion_run", run_id, patch)
            .await
            .map_err(|e| Error::database(e.to_string()))
            .map(|_| ())
    }

    /// Runs for a specific root entity (most recent first).
    pub async fn list_runs_for_record(
        root_table: &str,
        root_record_id: &str,
        v: &Valence,
    ) -> Result<Vec<Value>> {
        let sys = system_valence(v);
        QueryCore::new("valence_deletion_run".to_string())
            .where_string(
                "root_table".to_string(),
                StringPredicate::Equals(root_table.to_string()),
            )
            .where_string(
                "root_record_id".to_string(),
                StringPredicate::Equals(root_record_id.to_string()),
            )
            .order_by("requested_at".to_string(), SortDirection::Desc)
            .limit(50)
            .execute(&sys)
            .await
            .map_err(|e| Error::database(e.to_string()))
    }

    /// Recent runs for a logical schema (matches `root_table`).
    pub async fn list_runs_for_schema(schema_table: &str, v: &Valence) -> Result<Vec<Value>> {
        let sys = system_valence(v);
        QueryCore::new("valence_deletion_run".to_string())
            .where_string(
                "root_table".to_string(),
                StringPredicate::Equals(schema_table.to_string()),
            )
            .order_by("requested_at".to_string(), SortDirection::Desc)
            .limit(50)
            .execute(&sys)
            .await
            .map_err(|e| Error::database(e.to_string()))
    }

    /// Count in-flight runs requested by the given actor JSON (`requested_by` field).
    pub async fn count_active_runs_for_requester(requested_by: &str, v: &Valence) -> Result<u64> {
        let sys = system_valence(v);
        let q = concat!(
            "SELECT count() AS n FROM valence_deletion_run ",
            "WHERE requested_by = $requested_by ",
            "AND status IN ['queued', 'scanning', 'processing'] GROUP ALL"
        );
        let compiled = CompiledQuery::new(
            q.to_string(),
            vec![("requested_by".to_string(), json!(requested_by))],
        );
        let backend = sys.backend_for_table("valence_deletion_run")?;
        let rows = backend
            .execute_compiled_query(&compiled)
            .await
            .map_err(|e| Error::database(e.to_string()))?;
        Ok(rows
            .first()
            .and_then(|row| {
                row.get("n")
                    .or_else(|| row.get("count"))
                    .and_then(|v| v.as_u64().or_else(|| v.as_f64().map(|f| f as u64)))
            })
            .unwrap_or(0))
    }

    /// Paged listing for admin UI (newest first).
    pub async fn list_runs_recent(limit: u32, v: &Valence) -> Result<Vec<Value>> {
        let sys = system_valence(v);
        QueryCore::new("valence_deletion_run".to_string())
            .order_by("requested_at".to_string(), SortDirection::Desc)
            .limit(limit)
            .execute(&sys)
            .await
            .map_err(|e| Error::database(e.to_string()))
    }

    /// `status = queued` and `requested_at` strictly before `before` (oldest first) — for reconciling
    /// runs where the manual Chronon `run_now` did not start the orchestrator.
    pub async fn list_queued_runs_requested_before(
        before: DateTime<Utc>,
        limit: u32,
        v: &Valence,
    ) -> Result<Vec<Value>> {
        let sys = system_valence(v);
        QueryCore::new("valence_deletion_run".to_string())
            .where_string(
                "status".to_string(),
                StringPredicate::Equals("queued".to_string()),
            )
            .where_datetime(
                "requested_at".to_string(),
                DateTimePredicate::Before(before),
            )
            .order_by("requested_at".to_string(), SortDirection::Asc)
            .limit(limit)
            .execute(&sys)
            .await
            .map_err(|e| Error::database(e.to_string()))
    }

    /// Only-one-wins transition `queued` → `scanning` (sets `started_at`) so two Chronon
    /// `run_now` invocations for the same run cannot each insert `valence_deletion_step` rows.
    /// Returns `true` if this invocation claimed the run; `false` if another instance already
    /// advanced the row or the run is not `queued`.
    pub async fn try_claim_queued_to_scanning(run_id: &str, v: &Valence) -> Result<bool> {
        let sys = system_valence(v);
        let backend = sys.backend_for_table("valence_deletion_run")?;
        if backend.engine_id() == valence::KnownEngines::SURREALDB {
            let q = concat!(
                "UPDATE type::record('valence_deletion_run', $rid) SET ",
                "status = 'scanning',",
                " started_at = time::now()",
                " WHERE status = 'queued' RETURN AFTER"
            );
            let compiled =
                CompiledQuery::new(q.to_string(), vec![("rid".to_string(), json!(run_id))]);
            let rows = backend
                .execute_compiled_query(&compiled)
                .await
                .map_err(|e| Error::database(e.to_string()))?;
            return Ok(!rows.is_empty());
        }

        // SQL / mem: read-then-merge (single-writer tests and sqlite hosts).
        let Some(j) = Self::get_run_json(run_id, &sys).await? else {
            return Ok(false);
        };
        if j.get("status").and_then(|s| s.as_str()) != Some("queued") {
            return Ok(false);
        }
        Self::merge_run(
            run_id,
            json!({
                "status": "scanning",
                "started_at": chrono::Utc::now(),
            }),
            &sys,
        )
        .await?;
        Ok(true)
    }

    /// Most recent deletion run id for `root_table` + `root_record_id` (bare id, no `thing:` prefix).
    pub async fn latest_run_id_for_record(
        root_table: &str,
        root_record_id: &str,
        v: &Valence,
    ) -> Result<Option<String>> {
        let runs = Self::list_runs_for_record(root_table, root_record_id, v).await?;
        let Some(row) = runs.first() else {
            return Ok(None);
        };
        let id = row.get("id").and_then(|x| x.as_str()).map(|s| {
            s.strip_prefix("valence_deletion_run:")
                .unwrap_or(s)
                .to_string()
        });
        Ok(id)
    }

    /// Poll until `run_id` reaches a terminal status or `deadline` passes.
    ///
    /// Returns `Ok(())` for `completed` or `cancelled`. Returns `Err` for `failed` or timeout.
    /// If the run row disappears, returns `Ok(())` (treated as settled).
    pub async fn wait_for_run_terminal(
        run_id: &str,
        deadline: std::time::Instant,
        v: &Valence,
    ) -> Result<()> {
        loop {
            if std::time::Instant::now() >= deadline {
                return Err(Error::Internal(format!(
                    "valence deletion run {run_id} did not reach a terminal status before deadline"
                )));
            }
            let st = Self::get_run_json(run_id, v).await?;
            let Some(doc) = st else {
                return Ok(());
            };
            let status = doc.get("status").and_then(|s| s.as_str()).unwrap_or("");
            match status {
                "completed" => return Ok(()),
                "failed" => {
                    return Err(Error::database(format!(
                        "valence deletion run {run_id} failed"
                    )));
                }
                "cancelled" => return Ok(()),
                _ => tokio::time::sleep(Duration::from_millis(350)).await,
            }
        }
    }
}
