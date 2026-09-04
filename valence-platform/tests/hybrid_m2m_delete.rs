//! Hybrid M2M RemoveEdge when deleting the **target** endpoint (incoming edges).
//!
//! Requires `--features db-hybrid` (CI `--all-features`).

#![allow(missing_docs, clippy::expect_used, clippy::unwrap_used)]
#![cfg(feature = "db-hybrid")]

use std::sync::Arc;

use serde_json::json;
use valence::evaluator::DEFAULT_IN_MEMORY;
use valence::privacy_policies::common::PUBLIC_READ;
use valence::schema::{SchemaMetadata, SchemaMetadataInit};
use valence::schema_api::{
    Schema, SchemaConnection, SchemaMeta, SchemaPolicies, SchemaPolicyRule, SchemaPolicyRules,
    SchemaPrivacy,
};
use valence::{
    register_backend_logical_names, router_key, Actor, DatabaseBackend, DatabaseEvaluator,
    DatabaseRouter, HybridBackend, QueryCore, RegisterBackendLogicalNamesOptions, SqliteBackend,
    Valence, HYBRID_ENGINE_ID,
};
use valence_platform::deletion::orchestrator::run_valence_deletion_orchestrator_inline_steps;
use valence_platform::deletion::run_service::DeletionService;

const PARENT: &str = "hyb_m2m_parent";
const PEER: &str = "hyb_m2m_peer";
const EDGE: &str = "hyb_m2m_edge";

fn leak_schema(schema: Schema) -> &'static Schema {
    Box::leak(Box::new(schema))
}

fn schema_meta(name: &'static str, connections: Vec<SchemaConnection>) -> &'static SchemaMetadata {
    let schema = leak_schema(Schema {
        name: name.to_string(),
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
        fields: vec![],
        edges: Vec::new(),
        connections,
        side_effects: Vec::new(),
        iters: Vec::new(),
        composite_key: Vec::new(),
        traits: Vec::new(),
        ttl: None,
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
    SchemaMetadataInit(|| {
        schema_meta(
            PARENT,
            vec![SchemaConnection {
                name: "tags".into(),
                from_table: PARENT.into(),
                from_field: "id".into(),
                to_table: PEER.into(),
                cardinality: "ManyToMany".into(),
                required: false,
                on_delete: "SetNull".into(),
                label: "tags".into(),
                model_path: None,
                reverse_field: None,
                edge_table: Some(EDGE.into()),
                target_trait: None,
            }],
        )
    })
}

valence::inventory::submit! {
    SchemaMetadataInit(|| schema_meta(PEER, vec![]))
}

async fn hybrid_valence() -> Valence {
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
        .expect("hybrid build");
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
            operation: "hybrid_m2m".into(),
        })
        .build()
        .expect("hybrid valence")
}

#[tokio::test]
async fn k_hyb_1_delete_target_clears_incoming_edges_happy() {
    let user = Actor::User {
        user_id: "deleter".into(),
    };
    let boot = hybrid_valence().await;
    let backend = boot.backend_for_table(PARENT).unwrap();
    backend
        .create_record(PARENT, json!({"id": {"table": PARENT, "id": "p1"}}))
        .await
        .unwrap();
    backend
        .create_record(PEER, json!({"id": {"table": PEER, "id": "t1"}}))
        .await
        .unwrap();
    let from = valence::RecordId::new(PARENT, "p1");
    let to = valence::RecordId::new(PEER, "t1");
    boot.relate_edge(EDGE, &from, &to).await.unwrap();

    // Precondition: reverse lookup sees the source (hybrid must implement get_edge_sources).
    let sources = backend.get_edge_sources(&to, EDGE).await.unwrap();
    assert_eq!(
        sources.len(),
        1,
        "incoming edge must be visible via get_edge_sources before delete"
    );

    let run_id =
        DeletionService::create_run(PEER, "t1", serde_json::to_value(&user).unwrap(), &boot)
            .await
            .expect("create_run");

    run_valence_deletion_orchestrator_inline_steps(boot.clone(), run_id.clone())
        .await
        .expect("orchestrator");

    let run = DeletionService::get_run_json(&run_id, &boot)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        run.get("status").and_then(|s| s.as_str()),
        Some("completed")
    );
    assert!(
        backend
            .get_edge_targets(&from, EDGE)
            .await
            .unwrap()
            .is_empty(),
        "outgoing edges from parent must be cleared"
    );
    assert!(
        backend
            .get_edge_sources(&to, EDGE)
            .await
            .unwrap()
            .is_empty(),
        "incoming edges to deleted peer must be cleared"
    );
    assert!(
        QueryCore::get_record_json(PARENT, "p1", &boot)
            .await
            .unwrap()
            .is_some(),
        "parent peer must remain"
    );
    assert!(
        QueryCore::get_record_json(PEER, "t1", &boot)
            .await
            .unwrap()
            .is_none(),
        "target peer row must be gone"
    );
}
