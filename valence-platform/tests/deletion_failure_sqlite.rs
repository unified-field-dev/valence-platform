//! A deletion run whose orchestrator fails partway ends `failed` with the error, instead of
//! sitting in `scanning` or `processing` for good, and in-flight runs count correctly on
//! SQLite.
//!
//! No Boson runtime is installed in this test binary, so the production orchestrator fails
//! when it enqueues the first step, after it has claimed the run.

#![allow(missing_docs, clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use serde_json::{json, Value};
use valence::evaluator::DEFAULT_IN_MEMORY;
use valence::privacy_policies::common::PUBLIC_READ;
use valence::schema::{SchemaMetadata, SchemaMetadataInit};
use valence::schema_api::{
    Schema, SchemaMeta, SchemaPolicies, SchemaPolicyRule, SchemaPolicyRules, SchemaPrivacy,
};
use valence::{
    register_backend_logical_names, router_key, Actor, DatabaseBackend, DatabaseEvaluator,
    DatabaseRouter, RegisterBackendLogicalNamesOptions, SqliteBackend, Valence, SQLITE_ENGINE_ID,
};
use valence_platform::deletion::orchestrator::run_valence_deletion_orchestrator;
use valence_platform::deletion::run_service::DeletionService;

fn schema_meta(name: &'static str) -> &'static SchemaMetadata {
    let public = Some(SchemaPolicyRules {
        allow: vec![SchemaPolicyRule {
            name: "PUBLIC".into(),
            description: None,
            evaluator: Some(&PUBLIC_READ),
        }],
        ..SchemaPolicyRules::default()
    });
    let schema: &'static Schema = Box::leak(Box::new(Schema {
        name: name.to_string(),
        version: "0.1.0".into(),
        databases: vec![DEFAULT_IN_MEMORY.name().to_string()],
        database_evaluator: &DEFAULT_IN_MEMORY,
        privacy: SchemaPrivacy {
            read: "t".into(),
            write: "t".into(),
        },
        policies: Some(SchemaPolicies {
            read: public.clone(),
            delete: public,
            ..SchemaPolicies::default()
        }),
        fields: vec![],
        edges: Vec::new(),
        connections: Vec::new(),
        side_effects: Vec::new(),
        iters: Vec::new(),
        composite_key: Vec::new(),
        retired_fields: Vec::new(),
        partition_export: None,
        traits: Vec::new(),
        ttl: None,
        ownership: None,
        meta: SchemaMeta {
            retention: "1d".into(),
            row_count: 0,
            owner: "t".into(),
            description: None,
            repository: "https://github.com/unified-field-dev/valence-platform".to_string(),
        },
    }));
    Box::leak(Box::new(SchemaMetadata::from_schema(schema)))
}

valence::inventory::submit! {
    SchemaMetadataInit(|| schema_meta("df_item"))
}

async fn sqlite_boot() -> Valence {
    if std::env::var_os("VALENCE_OWNERSHIP_UNIFIED_FETCH").is_none() {
        std::env::set_var("VALENCE_OWNERSHIP_UNIFIED_FETCH", "0");
    }
    if std::env::var_os("VALENCE_OWNERSHIP_COLOCATE").is_none() {
        std::env::set_var("VALENCE_OWNERSHIP_COLOCATE", "0");
    }
    let backend: Arc<dyn DatabaseBackend> =
        Arc::new(SqliteBackend::connect_memory().await.expect("sqlite"));
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
            operation: "chronon_boot".into(),
        })
        .build()
        .expect("valence")
}

fn requester(user: &str) -> Value {
    serde_json::to_value(Actor::User {
        user_id: user.into(),
    })
    .unwrap()
}

async fn seed_item(boot: &Valence, id: &str) {
    boot.backend_for_table("df_item")
        .unwrap()
        .create_record("df_item", json!({"id": {"table": "df_item", "id": id}}))
        .await
        .unwrap();
}

#[tokio::test]
async fn orchestrator_failure_marks_run_failed_sad() {
    let boot = sqlite_boot().await;
    seed_item(&boot, "i1").await;
    let run_id = DeletionService::create_run("df_item", "i1", requester("alice"), &boot)
        .await
        .expect("create run");

    let err = run_valence_deletion_orchestrator(boot.clone(), run_id.clone())
        .await
        .expect_err("no Boson runtime, so the first enqueue fails");
    assert!(err.to_string().contains("boson"), "{err}");

    let run = DeletionService::get_run_json(&run_id, &boot)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run["status"], "failed", "run: {run}");
    let message = run["error_message"].as_str().unwrap_or_default();
    assert!(message.contains("orchestrator failed"), "{message}");
    assert!(run.get("completed_at").is_some_and(|v| !v.is_null()));
}

#[tokio::test]
async fn orchestrator_failure_leaves_terminal_runs_alone_edge() {
    let boot = sqlite_boot().await;
    seed_item(&boot, "i2").await;
    let run_id = DeletionService::create_run("df_item", "i2", requester("bob"), &boot)
        .await
        .expect("create run");
    // A run that is no longer queued isn't claimed, so the orchestrator does nothing.
    DeletionService::merge_run(&run_id, json!({"status": "cancelled"}), &boot)
        .await
        .unwrap();
    run_valence_deletion_orchestrator(boot.clone(), run_id.clone())
        .await
        .expect("an unclaimed run returns Ok");
    let run = DeletionService::get_run_json(&run_id, &boot)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run["status"], "cancelled");
}

#[tokio::test]
async fn active_runs_count_on_sqlite_happy() {
    let boot = sqlite_boot().await;
    for id in ["c1", "c2", "c3"] {
        seed_item(&boot, id).await;
    }
    let carol = requester("carol");
    let carol_key = serde_json::to_string(&carol).unwrap();
    let first = DeletionService::create_run("df_item", "c1", carol.clone(), &boot)
        .await
        .unwrap();
    DeletionService::create_run("df_item", "c2", carol.clone(), &boot)
        .await
        .unwrap();
    DeletionService::create_run("df_item", "c3", requester("dan"), &boot)
        .await
        .unwrap();

    let count = DeletionService::count_active_runs_for_requester(&carol_key, &boot)
        .await
        .expect("count on SQLite");
    assert_eq!(count, 2);

    DeletionService::merge_run(&first, json!({"status": "completed"}), &boot)
        .await
        .unwrap();
    let count = DeletionService::count_active_runs_for_requester(&carol_key, &boot)
        .await
        .unwrap();
    assert_eq!(count, 1, "a finished run no longer counts");
}
