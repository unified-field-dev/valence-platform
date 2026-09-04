//! Deferred TTL stamp + budgeted sweep delete on SQLite.

#![allow(missing_docs, clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use serde_json::json;
use valence::evaluator::DEFAULT_IN_MEMORY;
use valence::privacy_policies::common::PUBLIC_READ;
use valence::schema::{SchemaMetadata, SchemaMetadataInit};
use valence::schema_api::{
    Schema, SchemaField, SchemaMeta, SchemaPolicies, SchemaPolicyRule, SchemaPolicyRules,
    SchemaPrivacy,
};
use valence::ttl::SchemaTtlPolicy;
use valence::{
    register_backend_logical_names, router_key, Actor, DatabaseBackend, DatabaseEvaluator,
    DatabaseRouter, QueryCore, RegisterBackendLogicalNamesOptions, SqliteBackend, Valence,
    EXPIRE_AT_FIELD, SQLITE_ENGINE_ID,
};
use valence_platform::deletion::run_service::DeletionService;
use valence_platform::ttl::sweep::{run_valence_ttl_sweep_inline, DEFAULT_TTL_SWEEP_CAP};

const TABLE: &str = "ttl_sweep_sqlite_probe";

fn leak_schema(schema: Schema) -> &'static Schema {
    Box::leak(Box::new(schema))
}

fn ttl_schema_meta() -> &'static SchemaMetadata {
    let schema = leak_schema(Schema {
        name: TABLE.to_string(),
        version: "0.1.0".into(),
        databases: vec![DEFAULT_IN_MEMORY.name().to_string()],
        database_evaluator: &DEFAULT_IN_MEMORY,
        privacy: SchemaPrivacy {
            read: "t".into(),
            write: "t".into(),
        },
        policies: Some(SchemaPolicies {
            delete: Some(SchemaPolicyRules {
                allow: vec![SchemaPolicyRule {
                    name: "PUBLIC".into(),
                    description: None,
                    evaluator: Some(&PUBLIC_READ),
                }],
                ..SchemaPolicyRules::default()
            }),
            ..SchemaPolicies::default()
        }),
        fields: vec![SchemaField {
            name: "id".to_string(),
            field_type: "string".to_string(),
            primary: true,
            nullable: false,
            indexed: false,
            unique: false,
            default: None,
            fk: None,
            validations: Vec::new(),
            policies: None,
            encrypted: false,
            enum_variants: Vec::new(),
            enum_type: None,
            model_path: None,
        }],
        edges: Vec::new(),
        connections: Vec::new(),
        side_effects: Vec::new(),
        iters: Vec::new(),
        composite_key: Vec::new(),
        traits: Vec::new(),
        ttl: Some(SchemaTtlPolicy {
            seconds: 3600,
            mode: "backend_capability".into(),
        }),
        ownership: None,
        meta: SchemaMeta {
            retention: "1d".into(),
            row_count: 0,
            owner: "t".into(),
            description: None,
        },
    });
    Box::leak(Box::new(SchemaMetadata::from_schema(schema)))
}

valence::inventory::submit! {
    SchemaMetadataInit(|| ttl_schema_meta())
}

async fn sqlite_system_valence() -> Valence {
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
            operation: "ttl_sweep_test".into(),
        })
        .build()
        .expect("Valence::builder sqlite")
}

async fn seed_expired(v: &Valence, id: &str) {
    let backend = v.backend_for_table(TABLE).unwrap();
    backend
        .create_record(TABLE, json!({"id": {"table": TABLE, "id": id}}))
        .await
        .expect("create");
    backend
        .merge_record(
            TABLE,
            id,
            json!({ EXPIRE_AT_FIELD: "2020-01-01T00:00:00+00:00" }),
        )
        .await
        .expect("backdate expire");
}

#[tokio::test]
async fn tm1_sqlite_expired_row_deleted_after_inline_sweep() {
    let v = sqlite_system_valence().await;
    v.ensure_ttl_for_table(TABLE).await.expect("ensure index");
    // Idempotent second ensure
    v.ensure_ttl_for_table(TABLE).await.expect("ensure again");

    seed_expired(&v, "e1").await;
    assert!(QueryCore::get_record_json(TABLE, "e1", &v)
        .await
        .unwrap()
        .is_some());

    let report = run_valence_ttl_sweep_inline(v.clone(), DEFAULT_TTL_SWEEP_CAP)
        .await
        .expect("inline sweep");
    assert!(
        report.queued_deletes >= 1,
        "expected at least one queued delete, got {report:?}"
    );

    assert!(
        QueryCore::get_record_json(TABLE, "e1", &v)
            .await
            .unwrap()
            .is_none(),
        "expired row must be gone after inline sweep"
    );

    if let Some(run_id) = report.run_ids.first() {
        let run = DeletionService::get_run_json(run_id, &v)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            run.get("status").and_then(|s| s.as_str()),
            Some("completed")
        );
    }
}

#[tokio::test]
async fn tm2_sqlite_future_expire_untouched() {
    let v = sqlite_system_valence().await;
    v.ensure_ttl_for_table(TABLE).await.unwrap();
    let backend = v.backend_for_table(TABLE).unwrap();
    backend
        .create_record(TABLE, json!({"id": {"table": TABLE, "id": "future1"}}))
        .await
        .unwrap();
    // Stamp from create is in the future (3600s TTL) — leave it.

    let _report = run_valence_ttl_sweep_inline(v.clone(), DEFAULT_TTL_SWEEP_CAP)
        .await
        .expect("sweep");
    assert!(
        QueryCore::get_record_json(TABLE, "future1", &v)
            .await
            .unwrap()
            .is_some(),
        "non-expired row must remain"
    );
}

#[tokio::test]
async fn tm14_sqlite_budget_drains_across_ticks() {
    let v = sqlite_system_valence().await;
    v.ensure_ttl_for_table(TABLE).await.unwrap();
    for i in 0..3 {
        seed_expired(&v, &format!("b{i}")).await;
    }

    let first = run_valence_ttl_sweep_inline(v.clone(), 2)
        .await
        .expect("first tick");
    assert_eq!(first.queued_deletes, 2);
    assert!(first.budget_exhausted);

    let mut remaining = 0u32;
    for i in 0..3 {
        if QueryCore::get_record_json(TABLE, &format!("b{i}"), &v)
            .await
            .unwrap()
            .is_some()
        {
            remaining += 1;
        }
    }
    assert_eq!(remaining, 1, "one expired row should remain after cap=2");

    let second = run_valence_ttl_sweep_inline(v.clone(), 2)
        .await
        .expect("second tick");
    assert!(second.queued_deletes >= 1);

    for i in 0..3 {
        assert!(
            QueryCore::get_record_json(TABLE, &format!("b{i}"), &v)
                .await
                .unwrap()
                .is_none(),
            "b{i} should be gone after second tick"
        );
    }
}
