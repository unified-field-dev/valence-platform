//! Deferred TTL sweep delete on hybrid (sqlite primary).

#![allow(missing_docs, clippy::expect_used, clippy::unwrap_used)]
#![cfg(feature = "db-hybrid")]

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
    DatabaseRouter, HybridBackend, QueryCore, RegisterBackendLogicalNamesOptions, SqliteBackend,
    Valence, EXPIRE_AT_FIELD, HYBRID_ENGINE_ID,
};
use valence_platform::deletion::run_service::DeletionService;
use valence_platform::ttl::sweep::{run_valence_ttl_sweep_inline, DEFAULT_TTL_SWEEP_CAP};

const TABLE: &str = "ttl_sweep_hybrid_probe";

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

async fn hybrid_system_valence() -> Valence {
    if std::env::var_os("VALENCE_OWNERSHIP_UNIFIED_FETCH").is_none() {
        std::env::set_var("VALENCE_OWNERSHIP_UNIFIED_FETCH", "0");
    }
    if std::env::var_os("VALENCE_OWNERSHIP_COLOCATE").is_none() {
        std::env::set_var("VALENCE_OWNERSHIP_COLOCATE", "0");
    }
    let primary: Arc<dyn DatabaseBackend> = Arc::new(
        SqliteBackend::connect_memory()
            .await
            .expect("sqlite primary"),
    );
    let hybrid = HybridBackend::builder()
        .primary(primary)
        .warm_edges(false)
        .build()
        .await
        .expect("hybrid");
    let backend: Arc<dyn DatabaseBackend> = Arc::new(hybrid);
    let mut router = DatabaseRouter::new();
    register_backend_logical_names(
        &mut router,
        backend,
        &["default"],
        RegisterBackendLogicalNamesOptions::default(),
    );
    Valence::builder()
        .database_router(Arc::new(router))
        .default_backend_key(router_key("default", HYBRID_ENGINE_ID))
        .with_actor(Actor::System {
            operation: "ttl_sweep_hybrid".into(),
        })
        .build()
        .expect("valence")
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
        .expect("backdate");
}

#[tokio::test]
async fn k_ttl_pg_hybrid_expired_row_deleted_after_inline_sweep() {
    let v = hybrid_system_valence().await;
    v.ensure_ttl_for_table(TABLE).await.expect("ensure");
    seed_expired(&v, "h1").await;
    assert!(QueryCore::get_record_json(TABLE, "h1", &v)
        .await
        .unwrap()
        .is_some());

    let report = run_valence_ttl_sweep_inline(v.clone(), DEFAULT_TTL_SWEEP_CAP)
        .await
        .expect("sweep");
    assert!(
        report.queued_deletes >= 1,
        "expected queued delete, got {report:?}"
    );
    assert!(
        QueryCore::get_record_json(TABLE, "h1", &v)
            .await
            .unwrap()
            .is_none(),
        "expired hybrid row must be gone"
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
