//! Parent/child CascadeDelete on in-memory SQLite (inline orchestrator; no Chronon/Boson).
//!
//! ## What this runs
//!
//! 1. Register probe schemas `ex_del_parent` → Cascade → `ex_del_child`.
//! 2. Seed parent `p1` and child `c1`.
//! 3. [`DeletionService::create_run`] as a User actor, then
//!    [`run_valence_deletion_orchestrator_inline_steps`].
//! 4. Assert run `status=completed` and both rows gone.
//!
//! ## Host path (not used here)
//!
//! Hosts call [`register_deletion_dispatch`](valence_platform::deletion::dispatch::register_deletion_dispatch)
//! at boot so `Model::delete` `run_now`s Chronon job `valence-deletion-orchestrator`. The
//! orchestrator restores the requester from `requested_by` before privacy and physical delete.
//!
//! ## Command
//! ```bash
//! CARGO_BUILD_JOBS=1 cargo run -p valence-platform --example deletion_cascade_sqlite
//! ```
//!
//! ## Success
//! Stdout prints `status=completed` and `parent+child deleted`.

#![allow(
    dead_code,
    missing_docs,
    clippy::print_stdout,
    clippy::unwrap_used,
    clippy::expect_used
)]

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
    DatabaseRouter, QueryCore, RegisterBackendLogicalNamesOptions, SqliteBackend, Valence,
    SQLITE_ENGINE_ID,
};
use valence_platform::deletion::orchestrator::run_valence_deletion_orchestrator_inline_steps;
use valence_platform::deletion::run_service::DeletionService;

const PARENT: &str = "ex_del_parent";
const CHILD: &str = "ex_del_child";

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
                name: "kids".into(),
                from_table: PARENT.into(),
                from_field: "id".into(),
                to_table: CHILD.into(),
                cardinality: "HasMany".into(),
                required: false,
                on_delete: "Cascade".into(),
                label: "kids".into(),
                model_path: None,
                reverse_field: Some("parent_id".into()),
                edge_table: None,
                target_trait: None,
            }],
        )
    })
}

valence::inventory::submit! {
    SchemaMetadataInit(|| schema_meta(CHILD, vec![]))
}

async fn sqlite_boot() -> Valence {
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
            operation: "deletion_cascade_sqlite_example".into(),
        })
        .build()
        .expect("Valence::builder sqlite")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let user = Actor::User {
        user_id: "deleter".into(),
    };
    let boot = sqlite_boot().await;
    let backend = boot.backend_for_table(PARENT)?;
    backend
        .create_record(
            PARENT,
            json!({"id": {"table": PARENT, "id": "p1"}, "name": "p"}),
        )
        .await?;
    backend
        .create_record(
            CHILD,
            json!({
                "id": {"table": CHILD, "id": "c1"},
                "parent_id": format!("{PARENT}:p1")
            }),
        )
        .await?;

    let run_id =
        DeletionService::create_run(PARENT, "p1", serde_json::to_value(&user)?, &boot).await?;
    run_valence_deletion_orchestrator_inline_steps(boot.clone(), run_id.clone()).await?;

    let run = DeletionService::get_run_json(&run_id, &boot)
        .await?
        .ok_or_else(|| anyhow::anyhow!("missing deletion run"))?;
    let status = run.get("status").and_then(|s| s.as_str()).unwrap_or("?");
    assert_eq!(status, "completed");
    assert!(QueryCore::get_record_json(PARENT, "p1", &boot)
        .await?
        .is_none());
    assert!(QueryCore::get_record_json(CHILD, "c1", &boot)
        .await?
        .is_none());

    println!("deletion run {run_id}: status={status}");
    println!("parent+child deleted");
    Ok(())
}
