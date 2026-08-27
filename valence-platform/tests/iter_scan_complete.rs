//! Multi-page iter completeness (sqlite + mem).
//!
//! Validates that paging past the default orchestrator batch size (1000) visits every row.

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
use valence::{
    register_backend_logical_names, router_key, Actor, DatabaseBackend, DatabaseEvaluator,
    DatabaseRouter, InMemoryBackend, RegisterBackendLogicalNamesOptions, SqliteBackend, Valence,
    MEM_ENGINE_ID, SQLITE_ENGINE_ID,
};
use valence_platform::iter::paging::{count_table_rows, page_row_ids};

const TABLE: &str = "iter_scan_page_probe";
/// One past the orchestrator default batch size — forces a second page.
const ROW_COUNT: usize = 1001;
const PAGE: usize = 1000;

fn leak_schema(schema: Schema) -> &'static Schema {
    Box::leak(Box::new(schema))
}

fn probe_schema_meta() -> &'static SchemaMetadata {
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
            read: Some(SchemaPolicyRules {
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
    SchemaMetadataInit(|| probe_schema_meta())
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
            operation: "iter_scan_complete".into(),
        })
        .build()
        .expect("sqlite valence")
}

fn mem_valence() -> Valence {
    Valence::builder()
        .add_backend("default", Arc::new(InMemoryBackend::new()))
        .with_actor(Actor::System {
            operation: "iter_scan_complete".into(),
        })
        .build()
        .expect("mem valence")
}

async fn seed_sortable_ids(v: &Valence, n: usize) {
    let backend = v.backend_for_table(TABLE).expect("backend");
    for i in 0..n {
        let id = format!("r{i:04}");
        backend
            .create_record(TABLE, json!({ "id": {"table": TABLE, "id": id}, "n": i }))
            .await
            .expect("create");
    }
}

async fn assert_full_keyset_scan(v: &Valence) {
    let total = count_table_rows(v, TABLE).await.expect("count") as usize;
    assert_eq!(total, ROW_COUNT, "seeded row count");

    let mut seen = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = page_row_ids(v, TABLE, after.as_deref(), PAGE)
            .await
            .expect("page");
        if page.is_empty() {
            break;
        }
        after = page.last().cloned();
        seen.extend(page);
        assert!(
            seen.len() <= ROW_COUNT + 5,
            "paging did not terminate; seen={}",
            seen.len()
        );
    }

    assert_eq!(
        seen.len(),
        ROW_COUNT,
        "multi-page scan must visit every row (engine={})",
        v.active_backend().map(|b| b.engine_id()).unwrap_or("?")
    );
    let mut uniq = seen.clone();
    uniq.sort();
    uniq.dedup();
    assert_eq!(uniq.len(), ROW_COUNT, "no duplicate ids across pages");
}

#[tokio::test]
async fn k_iter_1_sqlite_multipage_scan_complete_happy() {
    let v = sqlite_valence().await;
    seed_sortable_ids(&v, ROW_COUNT).await;
    assert_full_keyset_scan(&v).await;
    let _ = SQLITE_ENGINE_ID;
}

#[tokio::test]
async fn k_iter_2_mem_multipage_scan_complete_happy() {
    let v = mem_valence();
    seed_sortable_ids(&v, ROW_COUNT).await;
    assert_full_keyset_scan(&v).await;
    let _ = MEM_ENGINE_ID;
}
