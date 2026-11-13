//! The production deletion path on SQLite: the orchestrator enqueues each step as a Boson
//! job and waits for every wave to finish before starting the next.
//!
//! Earlier, the wave wait sent a SurrealQL count straight to the backend. SQLite rejected it
//! after the first wave's jobs had run, and the run stayed `processing` for good. The inline
//! orchestrator never waits on a wave, so these tests drive real Boson jobs through an
//! in-memory queue and a manual worker.

#![allow(missing_docs, clippy::expect_used, clippy::unwrap_used)]

use std::sync::{Arc, LazyLock};
use std::time::Duration;

use boson_backend_mem::MemQueueBackend;
use boson_runtime::{configure, Boson, ManualWorker};
use boson_valence_identity::ValenceExecutionContextFactory;
use serde_json::{json, Value};
use valence::evaluator::DEFAULT_IN_MEMORY;
use valence::privacy_policies::common::PUBLIC_READ;
use valence::schema::{SchemaMetadata, SchemaMetadataInit};
use valence::schema_api::{
    Schema, SchemaConnection, SchemaMeta, SchemaPolicies, SchemaPolicyRule, SchemaPolicyRules,
    SchemaPrivacy,
};
use valence::{
    register_backend_logical_names, router_key, Actor, DatabaseBackend, DatabaseEvaluator,
    DatabaseRouter, QueryCore, RegisterBackendLogicalNamesOptions, SqliteBackend, StringPredicate,
    Valence, ValenceFactory, SQLITE_ENGINE_ID,
};
use valence_platform::deletion::orchestrator::run_valence_deletion_orchestrator;
use valence_platform::deletion::run_service::DeletionService;

/// Boson's default runtime is process-wide, so the tests in this file take turns.
static BOSON_LOCK: LazyLock<tokio::sync::Mutex<()>> = LazyLock::new(|| tokio::sync::Mutex::new(()));

fn public_rules() -> Option<SchemaPolicyRules> {
    Some(SchemaPolicyRules {
        allow: vec![SchemaPolicyRule {
            name: "PUBLIC".into(),
            description: None,
            evaluator: Some(&PUBLIC_READ),
        }],
        ..SchemaPolicyRules::default()
    })
}

fn cascade_to(from: &str, to: &str) -> SchemaConnection {
    SchemaConnection {
        name: "kids".into(),
        from_table: from.into(),
        from_field: "id".into(),
        to_table: to.into(),
        cardinality: "HasMany".into(),
        required: false,
        on_delete: "Cascade".into(),
        label: "kids".into(),
        model_path: None,
        reverse_field: Some("parent_id".into()),
        edge_table: None,
        target_trait: None,
        edge_policy: None,
    }
}

fn schema_meta(name: &'static str, connections: Vec<SchemaConnection>) -> &'static SchemaMetadata {
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
            read: public_rules(),
            update: public_rules(),
            delete: public_rules(),
            ..SchemaPolicies::default()
        }),
        fields: vec![],
        edges: Vec::new(),
        connections,
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

// Three levels, so the DAG has three waves: leaves, then middles, then the root.
valence::inventory::submit! {
    SchemaMetadataInit(|| schema_meta("bq_root", vec![cascade_to("bq_root", "bq_mid")]))
}
valence::inventory::submit! {
    SchemaMetadataInit(|| schema_meta("bq_mid", vec![cascade_to("bq_mid", "bq_leaf")]))
}
valence::inventory::submit! {
    SchemaMetadataInit(|| schema_meta("bq_leaf", vec![]))
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

/// Hands every Boson job the shared SQLite Valence, as the job's actor.
struct SharedValence(Valence);

impl ValenceFactory for SharedValence {
    fn build(&self, actor_json: &Value) -> valence::Result<Valence> {
        let actor: Actor = serde_json::from_value(actor_json.clone())
            .map_err(|e| valence::Error::Identity(e.to_string()))?;
        Ok(self.0.with_actor(actor))
    }
}

fn install_boson(boot: &Valence) -> ManualWorker {
    let (boson, manual) = Boson::builder()
        .queue_backend(Arc::new(MemQueueBackend::new()))
        .execution_context_factory(ValenceExecutionContextFactory::new(Arc::new(
            SharedValence(boot.clone()),
        )))
        .auto_registry()
        .without_worker()
        .build_manual()
        .expect("boson");
    configure(boson);
    manual
}

/// `root` → `mids` middles → `leaves_per_mid` leaves under each.
async fn seed_tree(boot: &Valence, root: &str, mids: usize, leaves_per_mid: usize) {
    let backend = boot.backend_for_table("bq_root").unwrap();
    backend
        .create_record("bq_root", json!({"id": {"table": "bq_root", "id": root}}))
        .await
        .unwrap();
    for m in 0..mids {
        let mid = format!("{root}-m{m}");
        backend
            .create_record(
                "bq_mid",
                json!({"id": {"table": "bq_mid", "id": mid}, "parent_id": format!("bq_root:{root}")}),
            )
            .await
            .unwrap();
        for l in 0..leaves_per_mid {
            backend
                .create_record(
                    "bq_leaf",
                    json!({"id": {"table": "bq_leaf", "id": format!("{mid}-l{l}")}, "parent_id": format!("bq_mid:{mid}")}),
                )
                .await
                .unwrap();
        }
    }
}

async fn row_exists(boot: &Valence, table: &str, id: &str) -> bool {
    QueryCore::get_record_json(table, id, boot, valence::use_!(r"**Test:** Fixture row load for `deletion_boson_sqlite` so the suite can assert which rows a queued deletion removed. CI and developers running the suite only."))
        .await
        .unwrap()
        .is_some()
}

async fn step_statuses(boot: &Valence, run_id: &str) -> Vec<String> {
    let rows: Vec<Value> = QueryCore::new("valence_deletion_step".to_string())
        .where_string("run_id".to_string(), StringPredicate::Equals(run_id.to_string()))
        .execute(boot, valence::use_!(r"**Test:** Fixture step list for `deletion_boson_sqlite` so the suite can assert every deletion step's final status. CI and developers running the suite only."))
        .await
        .unwrap();
    rows.iter()
        .map(|r| r["status"].as_str().unwrap_or_default().to_string())
        .collect()
}

fn deleter() -> Value {
    serde_json::to_value(Actor::User {
        user_id: "deleter".into(),
    })
    .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn boson_deletion_on_sqlite_completes_every_wave_happy() {
    let _turn = BOSON_LOCK.lock().await;
    let boot = sqlite_boot().await;
    let manual = install_boson(&boot);
    seed_tree(&boot, "r1", 2, 2).await;
    let run_id = DeletionService::create_run("bq_root", "r1", deleter(), &boot)
        .await
        .expect("create run");

    let orchestrator = tokio::spawn(run_valence_deletion_orchestrator(
        boot.clone(),
        run_id.clone(),
    ));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    while !orchestrator.is_finished() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "orchestrator never finished"
        );
        if !manual.try_run_next().await {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    orchestrator
        .await
        .unwrap()
        .expect("orchestrator finishes on SQLite");

    let run = DeletionService::get_run_json(&run_id, &boot)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run["status"], "completed", "run: {run}");
    // One root, two middles, four leaves.
    assert_eq!(run["total_steps"].as_i64(), Some(7));
    assert_eq!(run["completed_steps"].as_i64(), Some(7));
    let statuses = step_statuses(&boot, &run_id).await;
    assert_eq!(statuses.len(), 7);
    assert!(statuses.iter().all(|s| s == "completed"), "{statuses:?}");
    assert!(!row_exists(&boot, "bq_root", "r1").await);
    assert!(!row_exists(&boot, "bq_mid", "r1-m1").await);
    assert!(!row_exists(&boot, "bq_leaf", "r1-m1-l1").await);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelled_boson_deletion_skips_queued_steps_on_sqlite_sad() {
    let _turn = BOSON_LOCK.lock().await;
    let boot = sqlite_boot().await;
    let manual = install_boson(&boot);
    seed_tree(&boot, "r2", 1, 2).await;
    let run_id = DeletionService::create_run("bq_root", "r2", deleter(), &boot)
        .await
        .expect("create run");

    let orchestrator = tokio::spawn(run_valence_deletion_orchestrator(
        boot.clone(),
        run_id.clone(),
    ));
    // Wait until the first wave (the two leaves) is queued, then cancel before any job runs.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
    while step_statuses(&boot, &run_id).await.len() < 2 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "first wave never queued"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    DeletionService::merge_run(&run_id, json!({"status": "cancelled"}), &boot)
        .await
        .unwrap();
    orchestrator
        .await
        .unwrap()
        .expect("a cancelled run returns Ok");
    // Jobs already on the queue see the cancel and skip themselves.
    while manual.try_run_next().await {}

    let run = DeletionService::get_run_json(&run_id, &boot)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run["status"], "cancelled");
    let statuses = step_statuses(&boot, &run_id).await;
    assert_eq!(
        statuses.len(),
        2,
        "only the first wave (two leaves) was queued"
    );
    assert!(statuses.iter().all(|s| s == "skipped"), "{statuses:?}");
    assert!(row_exists(&boot, "bq_root", "r2").await);
    assert!(row_exists(&boot, "bq_leaf", "r2-m0-l1").await);
}
