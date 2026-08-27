//! Deferred TTL stamp + sweep delete on in-memory backend.

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
    DatabaseRouter, InMemoryBackend, QueryCore, RegisterBackendLogicalNamesOptions, Valence,
    EXPIRE_AT_FIELD, MEM_ENGINE_ID, SQLITE_ENGINE_ID,
};
use valence_platform::ttl::sweep::run_valence_ttl_sweep_inline;

const TABLE: &str = "ttl_sweep_mem_probe";

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

#[tokio::test]
async fn tm6_mem_expired_row_deleted_after_inline_sweep() {
    // Platform deletion-run schemas route to sqlite:default; alias the mem backend so the
    // deletion DAG and probe table share one store (same dialect: SQL document compiler).
    let backend: Arc<dyn DatabaseBackend> = Arc::new(InMemoryBackend::new());
    let mut router = DatabaseRouter::new();
    register_backend_logical_names(
        &mut router,
        Arc::clone(&backend),
        &["default"],
        RegisterBackendLogicalNamesOptions {
            register_alias_engine_id: Some(SQLITE_ENGINE_ID),
        },
    );
    let v = Valence::builder()
        .database_router(Arc::new(router))
        .default_backend_key(router_key("default", MEM_ENGINE_ID))
        .with_actor(Actor::System {
            operation: "ttl_sweep_mem_test".into(),
        })
        .build()
        .unwrap();

    v.ensure_ttl_for_table(TABLE).await.expect("ensure");

    backend
        .create_record(TABLE, json!({"id": "m1"}))
        .await
        .unwrap();
    backend
        .merge_record(
            TABLE,
            "m1",
            json!({ EXPIRE_AT_FIELD: "2020-01-01T00:00:00+00:00" }),
        )
        .await
        .unwrap();

    let report = run_valence_ttl_sweep_inline(v.clone(), 32)
        .await
        .expect("inline sweep");
    assert!(
        report.queued_deletes >= 1,
        "expected queued deletes, report={report:?}"
    );
    assert!(
        !report.run_ids.is_empty(),
        "expected run_ids for inline drain, report={report:?}"
    );

    assert!(
        QueryCore::get_record_json(TABLE, "m1", &v)
            .await
            .unwrap()
            .is_none(),
        "mem expired row must be deleted; report={report:?}"
    );
}
