//! Happy/sad contracts for valence-platform public surface (Layer 1).
//!
//! Uses InMemory for paging/dispatch gates and SQLite `:memory:` for
//! `DeletionService` query paths (mem backend does not filter QueryCore predicates).

#![allow(missing_docs, clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::http::{HeaderMap, HeaderValue, StatusCode};
use serde_json::json;
use valence::{
    register_backend_logical_names, router_key, Actor, DatabaseBackend, DatabaseRouter,
    InMemoryBackend, RegisterBackendLogicalNamesOptions, SqliteBackend, Valence, SQLITE_ENGINE_ID,
};
use valence_platform::deletion::boson_setup::VALENCE_DELETION_STEP_WORKER_TASK;
use valence_platform::deletion::debug::{
    authorize_deletion_debug_request, valence_debug_deletions_enabled,
    ENV_VALENCE_DEBUG_ADMIN_TOKEN, ENV_VALENCE_DEBUG_DELETIONS, HEADER_VALENCE_DEBUG_TOKEN,
};
use valence_platform::deletion::dispatch::{
    force_deletion_chronon_unregistered_for_tests, is_deletion_chronon_registered,
    run_deletion_orchestrator_now_for_registered_backend,
};
use valence_platform::deletion::run_service::DeletionService;
use valence_platform::deletion::sweep::reenqueue_swept_queued_runs;
use valence_platform::iter::boson_setup::VALENCE_ITER_ROW_WORKER_TASK;
use valence_platform::iter::dispatch::{
    force_iter_chronon_unregistered_for_tests, is_iter_chronon_registered,
    run_iter_orchestrator_now_for_registered_backend,
};
use valence_platform::iter::paging::{count_table_rows, page_row_ids};
use valence_platform::iter::run_service::{IterRunOptions, IterService};

/// Serialize env mutations for debug-gate tests.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn mem_valence() -> Valence {
    Valence::builder()
        .add_backend("default", Arc::new(InMemoryBackend::new()))
        .with_actor(Actor::System {
            operation: "valence_platform_contract".to_string(),
        })
        .build()
        .expect("Valence::builder")
}

async fn sqlite_valence() -> Valence {
    if std::env::var_os("VALENCE_OWNERSHIP_UNIFIED_FETCH").is_none() {
        std::env::set_var("VALENCE_OWNERSHIP_UNIFIED_FETCH", "0");
    }
    if std::env::var_os("VALENCE_OWNERSHIP_COLOCATE").is_none() {
        std::env::set_var("VALENCE_OWNERSHIP_COLOCATE", "0");
    }
    let backend: Arc<dyn DatabaseBackend> = Arc::new(
        SqliteBackend::connect_memory()
            .await
            .expect("SqliteBackend::connect_memory"),
    );
    let mut router = DatabaseRouter::new();
    register_backend_logical_names(
        &mut router,
        backend,
        &["default"],
        RegisterBackendLogicalNamesOptions::default(),
    );
    Valence::builder()
        .database_router(Arc::new(router))
        .default_backend_key(router_key("default", SQLITE_ENGINE_ID))
        .with_actor(Actor::System {
            operation: "valence_platform_contract".to_string(),
        })
        .build()
        .expect("Valence::builder sqlite")
}

#[test]
fn task_name_constants_happy() {
    assert_eq!(VALENCE_ITER_ROW_WORKER_TASK, "valence_iter_row_worker");
    assert_eq!(
        VALENCE_DELETION_STEP_WORKER_TASK,
        "valence_deletion_step_worker"
    );
}

#[test]
fn valence_debug_deletions_enabled_happy_and_sad() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    let prev = std::env::var_os(ENV_VALENCE_DEBUG_DELETIONS);
    std::env::remove_var(ENV_VALENCE_DEBUG_DELETIONS);
    assert!(
        !valence_debug_deletions_enabled(),
        "unset env must disable debug routes"
    );

    std::env::set_var(ENV_VALENCE_DEBUG_DELETIONS, "0");
    assert!(
        !valence_debug_deletions_enabled(),
        "non-1 value is sad/disabled"
    );

    std::env::set_var(ENV_VALENCE_DEBUG_DELETIONS, "1");
    assert!(
        valence_debug_deletions_enabled(),
        "VALENCE_DEBUG_DELETIONS=1 is happy/enabled"
    );

    match prev {
        Some(v) => std::env::set_var(ENV_VALENCE_DEBUG_DELETIONS, v),
        None => std::env::remove_var(ENV_VALENCE_DEBUG_DELETIONS),
    }
}

#[test]
fn deletion_debug_admin_token_gate_happy_and_sad() {
    let _guard = ENV_LOCK.lock().expect("env lock");
    let prev_debug = std::env::var_os(ENV_VALENCE_DEBUG_DELETIONS);
    let prev_token = std::env::var_os(ENV_VALENCE_DEBUG_ADMIN_TOKEN);

    std::env::remove_var(ENV_VALENCE_DEBUG_DELETIONS);
    std::env::remove_var(ENV_VALENCE_DEBUG_ADMIN_TOKEN);
    let headers = HeaderMap::new();
    let err = authorize_deletion_debug_request(&headers).expect_err("disabled must 404");
    assert_eq!(err.0, StatusCode::NOT_FOUND);

    std::env::set_var(ENV_VALENCE_DEBUG_DELETIONS, "1");
    let err = authorize_deletion_debug_request(&headers).expect_err("enabled without token env");
    assert_eq!(err.0, StatusCode::UNAUTHORIZED);

    std::env::set_var(ENV_VALENCE_DEBUG_ADMIN_TOKEN, "test-secret");
    let err = authorize_deletion_debug_request(&headers).expect_err("missing header");
    assert_eq!(err.0, StatusCode::UNAUTHORIZED);

    let mut bad_headers = HeaderMap::new();
    bad_headers.insert(
        HEADER_VALENCE_DEBUG_TOKEN,
        HeaderValue::from_static("wrong"),
    );
    let err = authorize_deletion_debug_request(&bad_headers).expect_err("token mismatch");
    assert_eq!(err.0, StatusCode::UNAUTHORIZED);

    let mut ok_headers = HeaderMap::new();
    ok_headers.insert(
        HEADER_VALENCE_DEBUG_TOKEN,
        HeaderValue::from_static("test-secret"),
    );
    authorize_deletion_debug_request(&ok_headers).expect("matching token is happy");

    match prev_debug {
        Some(v) => std::env::set_var(ENV_VALENCE_DEBUG_DELETIONS, v),
        None => std::env::remove_var(ENV_VALENCE_DEBUG_DELETIONS),
    }
    match prev_token {
        Some(v) => std::env::set_var(ENV_VALENCE_DEBUG_ADMIN_TOKEN, v),
        None => std::env::remove_var(ENV_VALENCE_DEBUG_ADMIN_TOKEN),
    }
}

#[tokio::test]
async fn paging_rejects_unsafe_table_name_sad() {
    let v = mem_valence();
    let err = count_table_rows(&v, "evil;drop")
        .await
        .expect_err("unsafe table must fail");
    let msg = err.to_string();
    assert!(msg.contains("unsafe table name"), "unexpected error: {msg}");

    let err = page_row_ids(&v, "x-y", None, 10)
        .await
        .expect_err("hyphenated table must fail");
    assert!(
        err.to_string().contains("unsafe table name"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn paging_count_and_page_empty_table_happy() {
    let v = mem_valence();
    let n = count_table_rows(&v, "contract_empty_tbl")
        .await
        .expect("count empty");
    assert_eq!(n, 0);
    let ids = page_row_ids(&v, "contract_empty_tbl", None, 5)
        .await
        .expect("page empty");
    assert!(ids.is_empty());
}

#[tokio::test]
async fn deletion_service_create_get_list_happy() {
    let v = sqlite_valence().await;
    let actor = json!({"System": {"operation": "delete_root"}});
    let run_id = DeletionService::create_run("widget", "w1", actor, &v)
        .await
        .expect("create_run");
    assert!(!run_id.is_empty());

    let doc = DeletionService::get_run_json(&run_id, &v)
        .await
        .expect("get_run_json")
        .expect("run must exist");
    assert_eq!(doc.get("status").and_then(|s| s.as_str()), Some("queued"));
    assert_eq!(
        doc.get("root_table").and_then(|s| s.as_str()),
        Some("widget")
    );
    assert_eq!(
        doc.get("root_record_id").and_then(|s| s.as_str()),
        Some("w1")
    );

    let listed = DeletionService::list_runs_for_record("widget", "w1", &v)
        .await
        .expect("list_runs_for_record");
    assert!(
        !listed.is_empty(),
        "created run must appear in list_runs_for_record"
    );
    // `latest_run_id_for_record` only accepts string `id` cells; SQLite may return a
    // structured Thing — assert the list row still refers to our create.
    let listed_id = listed[0].get("id").map(|id| id.to_string());
    assert!(
        listed_id
            .as_deref()
            .is_some_and(|s| s.contains(&run_id) || s.contains("valence_deletion_run")),
        "list row id should reference created run, got {listed_id:?}"
    );
}

#[tokio::test]
async fn deletion_service_get_missing_run_returns_none_sad() {
    let v = sqlite_valence().await;
    let missing = DeletionService::get_run_json("does-not-exist-run", &v)
        .await
        .expect("get missing is Ok(None)");
    assert!(missing.is_none());

    let latest = DeletionService::latest_run_id_for_record("no_such_table", "no_id", &v)
        .await
        .expect("latest missing");
    assert!(latest.is_none());
}

#[tokio::test]
async fn deletion_service_wait_terminal_failed_sad() {
    let v = sqlite_valence().await;
    let run_id = DeletionService::create_run("widget", "fail-me", json!({}), &v)
        .await
        .expect("create");
    DeletionService::merge_run(&run_id, json!({"status": "failed"}), &v)
        .await
        .expect("merge failed");
    let err = DeletionService::wait_for_run_terminal(
        &run_id,
        Instant::now() + Duration::from_secs(2),
        &v,
    )
    .await
    .expect_err("failed status must error");
    let msg = err.to_string();
    assert!(
        msg.contains("failed") || msg.contains(&run_id),
        "unexpected: {msg}"
    );
}

#[tokio::test]
async fn deletion_service_wait_terminal_timeout_sad() {
    let v = sqlite_valence().await;
    let run_id = DeletionService::create_run("widget", "stuck", json!({}), &v)
        .await
        .expect("create");
    let err = DeletionService::wait_for_run_terminal(
        &run_id,
        Instant::now() + Duration::from_millis(50),
        &v,
    )
    .await
    .expect_err("queued run must time out");
    assert!(
        err.to_string().contains("terminal status") || err.to_string().contains(&run_id),
        "unexpected: {err}"
    );
}

#[tokio::test]
async fn deletion_service_wait_terminal_completed_happy() {
    let v = sqlite_valence().await;
    let run_id = DeletionService::create_run("widget", "done", json!({}), &v)
        .await
        .expect("create");
    DeletionService::merge_run(&run_id, json!({"status": "completed"}), &v)
        .await
        .expect("merge completed");
    DeletionService::wait_for_run_terminal(&run_id, Instant::now() + Duration::from_secs(2), &v)
        .await
        .expect("completed is terminal Ok");
    let doc = DeletionService::get_run_json(&run_id, &v)
        .await
        .expect("get after wait")
        .expect("run exists");
    assert_eq!(
        doc.get("status").and_then(|s| s.as_str()),
        Some("completed"),
        "wait_for_run_terminal happy path must leave status completed"
    );
}

#[tokio::test]
async fn reenqueue_swept_returns_zero_when_chronon_unregistered_happy() {
    force_deletion_chronon_unregistered_for_tests(true);
    assert!(
        !is_deletion_chronon_registered(),
        "force seam must report unregistered"
    );
    let v = mem_valence();
    let n = reenqueue_swept_queued_runs(&v, 10, 8)
        .await
        .expect("sweep without chronon");
    assert_eq!(n, 0);
    force_deletion_chronon_unregistered_for_tests(false);
}

#[tokio::test]
async fn run_deletion_orchestrator_now_unregistered_sad() {
    force_deletion_chronon_unregistered_for_tests(true);
    let err = run_deletion_orchestrator_now_for_registered_backend("any-run")
        .await
        .expect_err("must require register_deletion_dispatch");
    match err {
        valence::Error::Internal(msg) => {
            assert!(
                msg.contains("not registered") || msg.contains("register_deletion_dispatch"),
                "unexpected Internal: {msg}"
            );
        }
        other => panic!("expected Internal, got {other:?}"),
    }
    force_deletion_chronon_unregistered_for_tests(false);
}

#[tokio::test]
async fn run_iter_orchestrator_now_unregistered_sad() {
    force_iter_chronon_unregistered_for_tests(true);
    let err = run_iter_orchestrator_now_for_registered_backend("any-run")
        .await
        .expect_err("must require register_iter_dispatch");
    match err {
        valence::Error::Internal(msg) => {
            assert!(
                msg.contains("not registered") || msg.contains("register_iter_dispatch"),
                "unexpected Internal: {msg}"
            );
        }
        other => panic!("expected Internal, got {other:?}"),
    }
    force_iter_chronon_unregistered_for_tests(false);
}

#[tokio::test]
async fn iter_service_start_unregistered_sad() {
    force_iter_chronon_unregistered_for_tests(true);
    assert!(!is_iter_chronon_registered());
    let v = mem_valence();
    let err = IterService::start(&v, "AnyIter", "any_table", IterRunOptions::default())
        .await
        .expect_err("start requires register_iter_dispatch");
    match err {
        valence::Error::Internal(msg) => {
            assert!(
                msg.contains("not registered") || msg.contains("register_iter_dispatch"),
                "unexpected Internal: {msg}"
            );
        }
        other => panic!("expected Internal, got {other:?}"),
    }
    force_iter_chronon_unregistered_for_tests(false);
}

#[tokio::test]
async fn iter_service_start_empty_names_sad() {
    let v = mem_valence();
    let err = IterService::start(&v, "", "t", IterRunOptions::default())
        .await
        .expect_err("empty iter_name");
    assert!(matches!(err, valence::Error::Validation(_)));
    let err = IterService::start(&v, "I", "", IterRunOptions::default())
        .await
        .expect_err("empty target_table");
    assert!(matches!(err, valence::Error::Validation(_)));
}
