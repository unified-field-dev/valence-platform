//! Iter control rows: signed-in operators can read them; only System writes them.

#![allow(missing_docs, clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use chrono::Utc;
use valence::{
    register_backend_logical_names, router_key, Actor, DatabaseBackend, DatabaseRouter, Model,
    RegisterBackendLogicalNamesOptions, SqliteBackend, Valence, SQLITE_ENGINE_ID,
};
use valence_platform::{
    ValenceIterBatch, ValenceIterBatchStatus, ValenceIterRowError, ValenceIterRowErrorErrorKind,
    ValenceIterRun, ValenceIterRunStatus,
};

const RUN_ID: &str = "read-policy-run";
const BATCH_ID: &str = "read-policy-batch";
const ROW_ERROR_ID: &str = "read-policy-row-error";

async fn system_valence() -> Valence {
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
            operation: "iter_read_policy_seed".into(),
        })
        .build()
        .expect("Valence::builder sqlite")
}

fn pending_run() -> ValenceIterRun {
    ValenceIterRun::new(
        "ReadPolicyIter".into(),
        "widget".into(),
        ValenceIterRunStatus::Pending,
        0,
        0,
        0,
        0,
        0,
        None,
        None,
        None,
        Utc::now(),
        "admin".into(),
        None,
    )
    .expect("run row")
}

async fn seed_iter_rows(system: &Valence) {
    ValenceIterRun::upsert(
        RUN_ID,
        pending_run(),
        system,
        valence::use_!(r"**Test:** Seeds a fixture **iter run** so the read-policy suite can check which actors may open it. CI and developers running the suite only."),
    )
    .await
    .expect("seed run");
    let batch = ValenceIterBatch::new(
        RUN_ID.into(),
        0,
        ValenceIterBatchStatus::Enqueuing,
        1,
        0,
        0,
        0,
        0,
        None,
        Utc::now(),
        None,
    )
    .expect("batch row");
    ValenceIterBatch::upsert(
        BATCH_ID,
        batch,
        system,
        valence::use_!(r"**Test:** Seeds a fixture **iter batch** so the read-policy suite can check which actors may open it. CI and developers running the suite only."),
    )
    .await
    .expect("seed batch");
    let row_error = ValenceIterRowError::new(
        RUN_ID.into(),
        Some(BATCH_ID.into()),
        "w1".into(),
        "fixture failure".into(),
        ValenceIterRowErrorErrorKind::ExecuteError,
        Utc::now(),
    )
    .expect("row error row");
    ValenceIterRowError::upsert(
        ROW_ERROR_ID,
        row_error,
        system,
        valence::use_!(r"**Test:** Seeds a fixture **iter row error** so the read-policy suite can check which actors may open it. CI and developers running the suite only."),
    )
    .await
    .expect("seed row error");
}

async fn visible_rows(v: &Valence) -> (bool, bool, bool) {
    let run = ValenceIterRun::get(
        RUN_ID,
        v,
        valence::use_!(r"**Test:** Opens the fixture **iter run** as the actor under test to see whether the read policy lets it through. CI and developers running the suite only."),
    )
    .await
    .expect("get run");
    let batch = ValenceIterBatch::get(
        BATCH_ID,
        v,
        valence::use_!(r"**Test:** Opens the fixture **iter batch** as the actor under test to see whether the read policy lets it through. CI and developers running the suite only."),
    )
    .await
    .expect("get batch");
    let row_error = ValenceIterRowError::get(
        ROW_ERROR_ID,
        v,
        valence::use_!(r"**Test:** Opens the fixture **iter row error** as the actor under test to see whether the read policy lets it through. CI and developers running the suite only."),
    )
    .await
    .expect("get row error");
    (run.is_some(), batch.is_some(), row_error.is_some())
}

#[tokio::test]
async fn signed_in_operator_reads_iter_rows_happy() {
    let system = system_valence().await;
    seed_iter_rows(&system).await;
    let operator = system.with_actor(Actor::User {
        user_id: "operator-1".into(),
    });
    assert_eq!(visible_rows(&operator).await, (true, true, true));
}

#[tokio::test]
async fn anonymous_cannot_read_iter_rows_sad() {
    let system = system_valence().await;
    seed_iter_rows(&system).await;
    let anonymous = system.with_actor(Actor::Anonymous);
    assert_eq!(visible_rows(&anonymous).await, (false, false, false));
}

#[tokio::test]
async fn signed_in_operator_cannot_write_iter_runs_sad() {
    let system = system_valence().await;
    let operator = system.with_actor(Actor::User {
        user_id: "operator-1".into(),
    });
    let err = ValenceIterRun::upsert(
        "operator-written-run",
        pending_run(),
        &operator,
        valence::use_!(r"**Test:** Tries to save an **iter run** as a signed-in operator to prove only platform workers can write run rows. CI and developers running the suite only."),
    )
    .await
    .expect_err("operators must not write iter runs");
    assert!(
        err.to_string().to_lowercase().contains("privacy")
            || err.to_string().to_lowercase().contains("denied"),
        "expected a privacy denial, got: {err}"
    );
}
