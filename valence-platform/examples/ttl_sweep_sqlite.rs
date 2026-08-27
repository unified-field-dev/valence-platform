//! Deferred TTL stamp + budgeted sweep delete on in-memory SQLite.
//!
//! ## What this runs
//!
//! 1. Register Deferred TTL probe table `ex_ttl_probe`.
//! 2. Seed row `e1` with `__valence_expire_at` in the past.
//! 3. [`run_valence_ttl_sweep_inline`] (discover + queue + inline deletion drain).
//! 4. Assert `queued_deletes >= 1` and the row is gone.
//!
//! ## Host path (not used here)
//!
//! Hosts call [`register_ttl_service`](valence_platform::ttl::sweep::register_ttl_service) at boot;
//! Chronon job `valence-ttl-sweep` calls [`sweep_expired_ttl_rows`](valence_platform::ttl::sweep::sweep_expired_ttl_rows).
//! Physical delete uses the deletion path (`register_deletion_dispatch`).
//!
//! ## Command
//! ```bash
//! CARGO_BUILD_JOBS=1 cargo run -p valence-platform --example ttl_sweep_sqlite
//! ```
//!
//! ## Success
//! Stdout prints `queued_deletes=` and `expired row e1 deleted`.

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

const TABLE: &str = "ex_ttl_probe";

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
            operation: "ttl_sweep_sqlite_example".into(),
        })
        .build()
        .expect("Valence::builder sqlite")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let v = sqlite_boot().await;
    v.ensure_ttl_for_table(TABLE).await?;

    let backend = v.backend_for_table(TABLE)?;
    backend
        .create_record(TABLE, json!({"id": {"table": TABLE, "id": "e1"}}))
        .await?;
    backend
        .merge_record(
            TABLE,
            "e1",
            json!({ EXPIRE_AT_FIELD: "2020-01-01T00:00:00+00:00" }),
        )
        .await?;

    let report = run_valence_ttl_sweep_inline(v.clone(), DEFAULT_TTL_SWEEP_CAP).await?;
    assert!(
        report.queued_deletes >= 1,
        "expected queued delete, got {report:?}"
    );
    assert!(QueryCore::get_record_json(TABLE, "e1", &v).await?.is_none());

    if let Some(run_id) = report.run_ids.first() {
        let run = DeletionService::get_run_json(run_id, &v)
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing deletion run"))?;
        assert_eq!(
            run.get("status").and_then(|s| s.as_str()),
            Some("completed")
        );
    }

    println!(
        "ttl sweep: queued_deletes={} tables_considered={} skipped_native={}",
        report.queued_deletes, report.tables_considered, report.skipped_native
    );
    println!("expired row e1 deleted");
    Ok(())
}
