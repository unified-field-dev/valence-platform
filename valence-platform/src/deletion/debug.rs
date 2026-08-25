//! Opt-in Valence **deletion** diagnostics for local / split-process debugging.
//!
//! Routes are **disabled by default**. Set `VALENCE_DEBUG_DELETIONS=1` to activate them;
//! otherwise they return **404**. When enabled, also set a non-empty
//! `VALENCE_DEBUG_ADMIN_TOKEN` and send the same value in request header
//! `x-valence-debug-token`. Missing env token or header mismatch → **401**.
//!
//! # Guide: wire the debug router
//!
//! **Prerequisites:** host Axum state that implements
//! `FromRef<AppState>` for [`DeletionDebugWiring`]; Chronon/Boson optional on the wiring.
//!
//! ```rust,ignore
//! use valence_platform::deletion::debug::{deletion_debug_router, DeletionDebugWiring};
//!
//! // On AppState: impl FromRef<AppState> for DeletionDebugWiring { … }
//! let app = host_router.merge(deletion_debug_router::<AppState>());
//! // Env: VALENCE_DEBUG_DELETIONS=1, VALENCE_DEBUG_ADMIN_TOKEN=<secret>
//! // Header: x-valence-debug-token: <secret>
//! // GET /__debug/valence/deletions → 200 JSON when gated correctly; else 404/401.
//! ```
//!
//! **Outcome:** overview JSON lists recent runs and whether a deletion dispatcher is registered;
//! trace JSON joins Valence run + optional Chronon/Boson.
//!
//! **Failure:** unset env → **404**; empty token env or header mismatch → **401**.
//!
//! **Next:** [`crate::deletion`] cascade guide; SECURITY.md for the operator contract.
//!
//! # Endpoints
//!
//! - `GET /__debug/valence/deletions?limit=50` — recent deletion runs (optional `limit`,
//!   default 50, max 200), non-terminal steps, recent per-step errors, and whether a deletion
//!   [`dispatch`](valence::deletion::dispatch) handler is registered in this process.
//! - `GET /__debug/valence/deletion-trace?deletion_run_id=…` **or**
//!   `?root_table=…&root_record_id=…` **or** `?record=table:id` — one run (Valence) plus optional
//!   **Chronon** orchestrator runs and **Boson** `valence_deletion_step_worker` jobs for the same
//!   `run_id` / idempotency keys. Use the bare id (e.g. `pna_…`) in `root_record_id` to match
//!   [`PendingDeletion`](valence::Error::PendingDeletion) error text.
//!
//! # Wiring
//!
//! Call [`deletion_debug_router`]. Implement
//! `axum::extract::FromRef<AppState> for [`DeletionDebugWiring`]` in the host and merge the
//! returned router into the host Axum router.

use std::sync::Arc;

use super::run_service::DeletionService;
use axum::extract::FromRef;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::Json;
use axum::Router;
use boson_coordinator::BosonCoordinatorBackend;
use chronon_coordinator::ChrononCoordinatorBackend;
use serde_json::{json, Value};
use valence::actor::Actor;
use valence::deletion::is_deletion_dispatcher_registered;
use valence::ownership::OwnershipService;
use valence::query::{QueryCore, SortDirection, StringPredicate};
use valence::Valence;
use valence::ValenceFactory;

use super::boson_setup::VALENCE_DELETION_STEP_WORKER_TASK;
use crate::{ValenceDeletionError, ValenceDeletionStep};

/// Environment variable: when set to `1`, [`deletion_debug_router`] routes are active.
pub const ENV_VALENCE_DEBUG_DELETIONS: &str = "VALENCE_DEBUG_DELETIONS";

/// Environment variable: required non-empty shared secret when debug routes are enabled.
pub const ENV_VALENCE_DEBUG_ADMIN_TOKEN: &str = "VALENCE_DEBUG_ADMIN_TOKEN";

/// Request header that must match [`ENV_VALENCE_DEBUG_ADMIN_TOKEN`] when debug is enabled.
pub const HEADER_VALENCE_DEBUG_TOKEN: &str = "x-valence-debug-token";

const CHRONON_DELETION_JOB_NAME: &str = "valence-deletion-orchestrator";
const VALENCE_DELETION_RUN_TABLE: &str = "valence_deletion_run";

/// List/query JSON may use a full `Thing` id (`valence_deletion_run:<uuid>`) while
/// [`DeletionService::get_run_json`] and Chronon `params.run_id` use the bare id.
fn bare_valence_deletion_run_id(s: &str) -> String {
    let p = format!("{VALENCE_DELETION_RUN_TABLE}:");
    s.strip_prefix(&p).unwrap_or(s).to_string()
}

/// Wires Valence, and optionally Chronon and Boson backends, for [`deletion_debug_router`].
#[derive(Clone)]
pub struct DeletionDebugWiring {
    /// Factory for building a system [`Valence`] (for example from the host's process-local router).
    pub valence_factory: Arc<dyn ValenceFactory>,
    /// If `None`, trace JSON lists `"chronon"` in `unavailable` and skips orchestrator runs.
    pub chronon: Option<Arc<dyn ChrononCoordinatorBackend>>,
    /// If `None`, trace JSON lists `"boson"` in `unavailable` and skips step-worker jobs.
    pub boson: Option<Arc<dyn BosonCoordinatorBackend>>,
}

/// True when `VALENCE_DEBUG_DELETIONS=1` (trimmed, case-sensitive).
pub fn valence_debug_deletions_enabled() -> bool {
    std::env::var(ENV_VALENCE_DEBUG_DELETIONS)
        .map(|s| s.trim() == "1")
        .unwrap_or(false)
}

fn system_valence(factory: &Arc<dyn ValenceFactory>) -> Result<Valence, (StatusCode, Json<Value>)> {
    let actor = serde_json::to_value(Actor::initialize_system_context())
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, json_err(e.to_string())))?;
    factory
        .build(&actor)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, json_err(e.to_string())))
}

fn json_err(s: String) -> Json<Value> {
    Json(json!({ "ok": false, "error": s }))
}

fn not_enabled() -> (StatusCode, Json<Value>) {
    (
        StatusCode::NOT_FOUND,
        Json(
            json!({ "ok": false, "error": "deletion debug disabled; set VALENCE_DEBUG_DELETIONS=1" }),
        ),
    )
}

fn debug_token_unconfigured() -> (StatusCode, Json<Value>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "ok": false,
            "error": "deletion debug requires non-empty VALENCE_DEBUG_ADMIN_TOKEN"
        })),
    )
}

fn debug_token_rejected() -> (StatusCode, Json<Value>) {
    (
        StatusCode::UNAUTHORIZED,
        Json(json!({
            "ok": false,
            "error": "invalid or missing x-valence-debug-token"
        })),
    )
}

/// Constant-time byte equality, used so admin-token verification does not leak the
/// expected token through response-timing side channels. Length is not secret.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    std::hint::black_box(diff) == 0
}

/// Authorize a deletion debug HTTP request (env gate + admin token header).
///
/// Returns **404** when debug is disabled, **401** when enabled but token env/header invalid.
pub fn authorize_deletion_debug_request(
    headers: &HeaderMap,
) -> Result<(), (StatusCode, Json<Value>)> {
    if !valence_debug_deletions_enabled() {
        return Err(not_enabled());
    }
    let expected = std::env::var(ENV_VALENCE_DEBUG_ADMIN_TOKEN)
        .map(|s| s.trim().to_string())
        .unwrap_or_default();
    if expected.is_empty() {
        return Err(debug_token_unconfigured());
    }
    let provided = headers
        .get(HEADER_VALENCE_DEBUG_TOKEN)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
        return Err(debug_token_rejected());
    }
    Ok(())
}

/// Axum routes under `/__debug/valence/*` using [`DeletionDebugWiring`] from state `S`.
///
/// Merge into the host router after implementing `FromRef<S> for DeletionDebugWiring`.
/// Routes stay **404** until `VALENCE_DEBUG_DELETIONS=1` and a matching
/// `x-valence-debug-token` / `VALENCE_DEBUG_ADMIN_TOKEN` pair is configured.
///
/// # Examples
///
/// ```rust,ignore
/// use valence_platform::deletion::debug::deletion_debug_router;
///
/// let app = host_router.merge(deletion_debug_router::<AppState>());
/// ```
pub fn deletion_debug_router<S>() -> Router<S>
where
    S: Send + Sync + Clone + 'static,
    DeletionDebugWiring: FromRef<S>,
{
    Router::new()
        .route(
            "/__debug/valence/deletions",
            get(get_deletion_overview::<S>),
        )
        .route(
            "/__debug/valence/deletion-trace",
            get(get_deletion_trace::<S>),
        )
}

async fn get_deletion_overview<S: Send + Sync + Clone + 'static>(
    axum::extract::State(state): axum::extract::State<S>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<OverviewQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)>
where
    DeletionDebugWiring: FromRef<S>,
{
    authorize_deletion_debug_request(&headers)?;
    let w = DeletionDebugWiring::from_ref(&state);
    let v = system_valence(&w.valence_factory)?;
    let limit = q.effective_limit();
    let runs = DeletionService::list_runs_recent(limit, &v)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "ok": false, "error": e.to_string() })),
            )
        })?;
    let stuck = list_nonterminal_deletion_steps(&v, 100)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "ok": false, "error": e })),
            )
        })?;
    let recent_errs = list_recent_deletion_errors(&v, 30).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e })),
        )
    })?;
    Ok(Json(json!({
        "ok": true,
        "dispatcher_registered": is_deletion_dispatcher_registered(),
        "limit": limit,
        "valence_deletion_runs": runs,
        "valence_deletion_nonterminal_steps": stuck,
        "valence_deletion_errors_recent": recent_errs,
    })))
}

async fn get_deletion_trace<S: Send + Sync + Clone + 'static>(
    axum::extract::State(state): axum::extract::State<S>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<TraceQuery>,
) -> Result<Json<Value>, (StatusCode, Json<Value>)>
where
    DeletionDebugWiring: FromRef<S>,
{
    authorize_deletion_debug_request(&headers)?;
    let w = DeletionDebugWiring::from_ref(&state);
    let v = system_valence(&w.valence_factory)?;
    let (run_id, root_table, root_bare) = resolve_trace(&v, &q).await.map_err(|e| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({ "ok": false, "error": e })),
        )
    })?;
    let run_j = DeletionService::get_run_json(&run_id, &v)
        .await
        .map_err(|e| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "ok": false, "error": e.to_string() })),
            )
        })?
        .ok_or_else(|| {
            (
                StatusCode::NOT_FOUND,
                Json(json!({ "ok": false, "error": "deletion run not found" })),
            )
        })?;
    let steps = list_steps_for_run(&v, &run_id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e })),
        )
    })?;
    let derr = list_errors_for_run(&v, &run_id).await.map_err(|e| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "ok": false, "error": e })),
        )
    })?;
    let ownership = if let (Some(t), Some(b)) = (root_table.as_deref(), root_bare.as_deref()) {
        OwnershipService::get_ownership_json(t, b, &v)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "ok": false, "error": e.to_string() })),
                )
            })?
    } else {
        None
    };
    let mut unavailable: Vec<&'static str> = Vec::new();
    let chronon_json = if let Some(ref ch) = w.chronon {
        match trace_chronon_for_run(ch.as_ref(), &run_id).await {
            Ok(j) => j,
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "ok": false, "error": e })),
                ));
            }
        }
    } else {
        unavailable.push("chronon");
        Value::Null
    };
    let boson_json = if let Some(ref b) = w.boson {
        match trace_boson_for_run(b.as_ref(), &run_id, &steps).await {
            Ok(j) => j,
            Err(e) => {
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(json!({ "ok": false, "error": e })),
                ));
            }
        }
    } else {
        unavailable.push("boson");
        Value::Null
    };
    Ok(Json(json!({
        "ok": true,
        "deletion_run_id": run_id,
        "valence": {
            "deletion_run": run_j,
            "steps": steps,
            "deletion_errors": derr,
            "ownership": ownership,
        },
        "chronon": chronon_json,
        "boson": boson_json,
        "unavailable": unavailable,
    })))
}

/// Query string for `GET /__debug/valence/deletions` (see [`deletion_debug_router`]).
#[derive(Debug, Default, serde::Deserialize)]
pub struct OverviewQuery {
    /// Max recent deletion runs to return (default 50, clamped to 1..=200).
    pub limit: Option<u32>,
}

impl OverviewQuery {
    fn effective_limit(&self) -> u32 {
        self.limit.unwrap_or(50).clamp(1, 200)
    }
}

/// Query string for `GET /__debug/valence/deletion-trace` (see [`deletion_debug_router`]).
#[derive(Debug, Default, serde::Deserialize)]
pub struct TraceQuery {
    /// Bare or `valence_deletion_run:<id>` deletion run id. Takes precedence over
    /// `root_table` / `root_record_id` / `record` when present.
    pub deletion_run_id: Option<String>,
    /// Root entity's table, used with `root_record_id` to find its latest deletion run.
    pub root_table: Option<String>,
    /// Root entity's bare record id, used with `root_table` to find its latest deletion run.
    pub root_record_id: Option<String>,
    /// `table:bare_id` (first `:` splits table vs id tail).
    pub record: Option<String>,
}

async fn resolve_trace(
    v: &Valence,
    q: &TraceQuery,
) -> Result<(String, Option<String>, Option<String>), String> {
    if let Some(rid) = q
        .deletion_run_id
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        let bare = bare_valence_deletion_run_id(rid);
        let j = DeletionService::get_run_json(&bare, v)
            .await
            .map_err(|e| e.to_string())?
            .ok_or_else(|| format!("no valence_deletion_run for id {rid}"))?;
        let t = j
            .get("root_table")
            .and_then(|x| x.as_str())
            .map(String::from);
        let b = j
            .get("root_record_id")
            .and_then(|x| x.as_str())
            .map(String::from);
        return Ok((bare, t, b));
    }
    let (table, bare) = if let Some(r) = q
        .record
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
    {
        if let Some((t, rest)) = r.split_once(':') {
            (t.to_string(), rest.to_string())
        } else {
            return Err("record= must be table:bare_id".to_string());
        }
    } else {
        let t = q
            .root_table
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "root_table or record= required".to_string())?
            .to_string();
        let b = q
            .root_record_id
            .as_ref()
            .map(|s| s.trim())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| "root_record_id or record= required".to_string())?
            .to_string();
        (t, b)
    };
    let rows = DeletionService::list_runs_for_record(&table, &bare, v)
        .await
        .map_err(|e| e.to_string())?;
    let first = rows
        .first()
        .and_then(|j| j.get("id").and_then(|x| x.as_str()))
        .ok_or_else(|| {
            format!("no valence_deletion_run for root_table={table} root_record_id={bare}")
        })?;
    let run_id = bare_valence_deletion_run_id(first);
    Ok((run_id, Some(table), Some(bare)))
}

async fn list_nonterminal_deletion_steps(v: &Valence, cap: u32) -> Result<Value, String> {
    use valence::__internal::CompiledQuery;
    use valence::actor::Actor;
    let sys = v.with_actor(Actor::System {
        operation: "valence_deletion_debug".to_string(),
    });
    let q = concat!(
        "SELECT * FROM valence_deletion_step ",
        "WHERE status = 'queued' OR status = 'in_progress' OR status = 'failed' ",
        "ORDER BY run_id, depth LIMIT $lim"
    );
    let compiled = CompiledQuery::new(
        q.to_string(),
        vec![(
            "lim".to_string(),
            Value::Number(serde_json::Number::from(cap)),
        )],
    );
    let backend = sys
        .backend_for_table("valence_deletion_step")
        .map_err(|e| e.to_string())?;
    let rows = backend
        .execute_compiled_query(&compiled)
        .await
        .map_err(|e| e.to_string())?;
    Ok(Value::Array(rows))
}

async fn list_recent_deletion_errors(v: &Valence, cap: u32) -> Result<Value, String> {
    let sys = v.with_actor(Actor::System {
        operation: "valence_deletion_debug".to_string(),
    });
    let rows = QueryCore::new("valence_deletion_error".to_string())
        .order_by("created_at".to_string(), SortDirection::Desc)
        .limit(cap)
        .execute(&sys)
        .await
        .map_err(|e| e.to_string())?;
    Ok(Value::Array(rows))
}

async fn list_steps_for_run(v: &Valence, run_id: &str) -> Result<Value, String> {
    let sys = v.with_actor(Actor::System {
        operation: "valence_deletion_debug".to_string(),
    });
    let rows = ValenceDeletionStep::query(&sys)
        .where_run_id(StringPredicate::Equals(run_id.to_string()))
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for r in rows {
        out.push(serde_json::to_value(&r).map_err(|e| e.to_string())?);
    }
    Ok(Value::Array(out))
}

async fn list_errors_for_run(v: &Valence, run_id: &str) -> Result<Value, String> {
    let sys = v.with_actor(Actor::System {
        operation: "valence_deletion_debug".to_string(),
    });
    let rows = ValenceDeletionError::query(&sys)
        .where_run_id(StringPredicate::Equals(run_id.to_string()))
        .await
        .map_err(|e| e.to_string())?;
    let mut out = Vec::new();
    for r in rows {
        out.push(serde_json::to_value(&r).map_err(|e| e.to_string())?);
    }
    Ok(Value::Array(out))
}

/// Serialize Chronon + Boson run payloads for a deletion `run_id`.
async fn trace_chronon_for_run(
    ch: &dyn ChrononCoordinatorBackend,
    run_id: &str,
) -> Result<Value, String> {
    let job = ch
        .get_job_by_name(CHRONON_DELETION_JOB_NAME)
        .await
        .ok_or_else(|| "Chronon job valence-deletion-orchestrator not found".to_string())?;
    let runs = ch
        .list_runs(Some(&job.job_id), None, 0, 200)
        .await
        .map_err(|e| e.to_string())?;
    let mut matched: Vec<chronon_coordinator::models::Run> = runs
        .into_iter()
        .filter(|r| {
            r.params_json
                .get("run_id")
                .and_then(|v| v.as_str())
                .map(|s| s == run_id)
                .unwrap_or(false)
        })
        .collect();
    matched.sort_by_key(|r| r.scheduled_for);
    let json: Value = serde_json::to_value(&matched).map_err(|e| e.to_string())?;
    Ok(json!( {
        "job_name": CHRONON_DELETION_JOB_NAME,
        "chronon_job_id": job.job_id,
        "orchestrator_runs": json,
    }))
}

async fn trace_boson_for_run(
    b: &dyn BosonCoordinatorBackend,
    run_id: &str,
    step_rows: &Value,
) -> Result<Value, String> {
    let Some(arr) = step_rows.as_array() else {
        return Ok(
            json!({ "VALENCE_DELETION_STEP_WORKER_TASK": VALENCE_DELETION_STEP_WORKER_TASK, "steps": [] }),
        );
    };
    let mut steps_out = Vec::new();
    for s in arr {
        let step_id = s.get("id").and_then(|x| x.as_str());
        let Some(sid) = step_id else {
            continue;
        };
        let idem = format!("{run_id}:{sid}");
        let jrow = find_boson_job_by_idempotency(b, &idem).await?;
        let job_runs: Vec<boson_core::Run> = if let Some(jid) = jrow
            .as_object()
            .and_then(|o| o.get("job_id"))
            .and_then(|x| x.as_str())
        {
            b.list_runs(Some(jid), 0, 20).await
        } else {
            vec![]
        };
        steps_out.push(json!({
            "idempotency_key": idem,
            "boson_job": jrow,
            "boson_runs": serde_json::to_value(&job_runs).map_err(|e| e.to_string())?,
        }));
    }
    Ok(json!({
        "task": VALENCE_DELETION_STEP_WORKER_TASK,
        "per_step": steps_out,
    }))
}

/// Scan Boson jobs for this deletion step's idempotency key (queue lives in Boson, not Valence).
async fn find_boson_job_by_idempotency(
    b: &dyn BosonCoordinatorBackend,
    idem: &str,
) -> Result<Value, String> {
    // Coordinator list has no idempotency filter; scan a window for debug traces.
    let jobs = b.list_jobs(None, 0, 2_000).await;
    if let Some(job) = jobs.into_iter().find(|j| {
        j.task_name == VALENCE_DELETION_STEP_WORKER_TASK
            && j.idempotency_key.as_deref() == Some(idem)
    }) {
        return serde_json::to_value(&job).map_err(|e| e.to_string());
    }
    Ok(Value::Null)
}
