//! Platform inline orchestrator OnDelete + cascade side effects.

#![allow(missing_docs, clippy::expect_used, clippy::unwrap_used)]

use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock};

use serde_json::json;
use valence::deletion::dag::{DeletionAction, DeletionNode};
use valence::deletion::{apply_deletion_node, DeleteSideEffectDescriptor};
use valence::evaluator::DEFAULT_IN_MEMORY;
use valence::privacy_policies::common::{PUBLIC_READ, SYSTEM_ONLY};
use valence::schema::{SchemaMetadata, SchemaMetadataInit};
use valence::schema_api::{
    Schema, SchemaConnection, SchemaMeta, SchemaPolicies, SchemaPolicyRule, SchemaPolicyRules,
    SchemaPrivacy,
};
use valence::{
    register_backend_logical_names, router_key, Actor, DatabaseBackend, DatabaseRouter,
    RegisterBackendLogicalNamesOptions, SqliteBackend, Valence, SQLITE_ENGINE_ID,
};
use valence::{DatabaseEvaluator, QueryCore};
use valence_platform::deletion::orchestrator::run_valence_deletion_orchestrator_inline_steps;
use valence_platform::deletion::run_service::DeletionService;

static CHILD_SE_CALLS: AtomicUsize = AtomicUsize::new(0);
/// Serializes tests that reset/assert [`CHILD_SE_CALLS`] (parallel runs otherwise race).
static CHILD_SE_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

fn child_se_dispatch(
    _v: valence::Valence,
    _row: serde_json::Value,
) -> Pin<Box<dyn std::future::Future<Output = ()> + Send + 'static>> {
    Box::pin(async move {
        CHILD_SE_CALLS.fetch_add(1, Ordering::SeqCst);
    })
}

valence::inventory::submit! {
    DeleteSideEffectDescriptor {
        table_name: "tm_v3_child",
        dispatch: child_se_dispatch,
    }
}

fn leak_schema(schema: Schema) -> &'static Schema {
    Box::leak(Box::new(schema))
}

fn schema_meta(
    name: &'static str,
    connections: Vec<SchemaConnection>,
    update_system_only: bool,
) -> &'static SchemaMetadata {
    let update = if update_system_only {
        Some(SchemaPolicyRules {
            allow: vec![SchemaPolicyRule {
                name: "SYSTEM_ONLY".into(),
                description: None,
                evaluator: Some(&SYSTEM_ONLY),
            }],
            ..SchemaPolicyRules::default()
        })
    } else {
        Some(SchemaPolicyRules {
            allow: vec![SchemaPolicyRule {
                name: "PUBLIC".into(),
                description: None,
                evaluator: Some(&PUBLIC_READ),
            }],
            ..SchemaPolicyRules::default()
        })
    };
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
            update,
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
            "tm_v3_parent",
            vec![SchemaConnection {
                name: "kids".into(),
                from_table: "tm_v3_parent".into(),
                from_field: "id".into(),
                to_table: "tm_v3_child".into(),
                cardinality: "HasMany".into(),
                required: false,
                on_delete: "Cascade".into(),
                label: "kids".into(),
                model_path: None,
                reverse_field: Some("parent_id".into()),
                edge_table: None,
                target_trait: None,
            }],
            false,
        )
    })
}

valence::inventory::submit! {
    SchemaMetadataInit(|| schema_meta("tm_v3_child", vec![], false))
}

valence::inventory::submit! {
    SchemaMetadataInit(|| {
        schema_meta(
            "tm_s1_parent",
            vec![SchemaConnection {
                name: "kids".into(),
                from_table: "tm_s1_parent".into(),
                from_field: "id".into(),
                to_table: "tm_s1_child".into(),
                cardinality: "HasMany".into(),
                required: false,
                on_delete: "SetNull".into(),
                label: "kids".into(),
                model_path: None,
                reverse_field: Some("parent_id".into()),
                edge_table: None,
                target_trait: None,
            }],
            false,
        )
    })
}

valence::inventory::submit! {
    SchemaMetadataInit(|| schema_meta("tm_s1_child", vec![], true))
}

async fn sqlite_valence(actor: Actor) -> Valence {
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
        .with_actor(actor)
        .build()
        .expect("Valence::builder sqlite")
}

#[tokio::test]
async fn tm_v3_cascade_child_runs_delete_side_effect() {
    let _guard = CHILD_SE_LOCK.lock().await;
    CHILD_SE_CALLS.store(0, Ordering::SeqCst);
    let user = Actor::User {
        user_id: "deleter".into(),
    };
    let boot = sqlite_valence(Actor::System {
        operation: "chronon_boot".into(),
    })
    .await;
    let backend = boot.backend_for_table("tm_v3_parent").unwrap();
    backend
        .create_record(
            "tm_v3_parent",
            json!({"id": {"table":"tm_v3_parent","id":"p1"}, "name": "p"}),
        )
        .await
        .unwrap();
    backend
        .create_record(
            "tm_v3_child",
            json!({"id": {"table":"tm_v3_child","id":"c1"}, "parent_id": "tm_v3_parent:p1"}),
        )
        .await
        .unwrap();

    let run_id = DeletionService::create_run(
        "tm_v3_parent",
        "p1",
        serde_json::to_value(&user).unwrap(),
        &boot,
    )
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
    assert!(QueryCore::get_record_json("tm_v3_child", "c1", &boot)
        .await
        .unwrap()
        .is_none());
    assert_eq!(CHILD_SE_CALLS.load(Ordering::SeqCst), 1);
}

valence::inventory::submit! {
    SchemaMetadataInit(|| {
        schema_meta(
            "tm_v3_restrict_parent",
            vec![SchemaConnection {
                name: "kids".into(),
                from_table: "tm_v3_restrict_parent".into(),
                from_field: "id".into(),
                to_table: "tm_v3_restrict_child".into(),
                cardinality: "HasMany".into(),
                required: false,
                on_delete: "Restrict".into(),
                label: "kids".into(),
                model_path: None,
                reverse_field: Some("parent_id".into()),
                edge_table: None,
                target_trait: None,
            }],
            false,
        )
    })
}

valence::inventory::submit! {
    SchemaMetadataInit(|| schema_meta("tm_v3_restrict_child", vec![], false))
}

#[tokio::test]
async fn tm_v3_restrict_aborts_without_side_effect() {
    let _guard = CHILD_SE_LOCK.lock().await;
    CHILD_SE_CALLS.store(0, Ordering::SeqCst);
    let user = Actor::User {
        user_id: "deleter".into(),
    };
    let boot = sqlite_valence(Actor::System {
        operation: "chronon_boot".into(),
    })
    .await;
    let backend = boot.backend_for_table("tm_v3_restrict_parent").unwrap();
    backend
        .create_record(
            "tm_v3_restrict_parent",
            json!({"id": {"table":"tm_v3_restrict_parent","id":"rp1"}, "name": "p"}),
        )
        .await
        .unwrap();
    backend
        .create_record(
            "tm_v3_restrict_child",
            json!({
                "id": {"table":"tm_v3_restrict_child","id":"rc1"},
                "parent_id": "tm_v3_restrict_parent:rp1"
            }),
        )
        .await
        .unwrap();

    let run_id = DeletionService::create_run(
        "tm_v3_restrict_parent",
        "rp1",
        serde_json::to_value(&user).unwrap(),
        &boot,
    )
    .await
    .expect("create_run");

    run_valence_deletion_orchestrator_inline_steps(boot.clone(), run_id.clone())
        .await
        .expect("orchestrator returns Ok after marking failed");

    let run = DeletionService::get_run_json(&run_id, &boot)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.get("status").and_then(|s| s.as_str()), Some("failed"));
    assert!(
        QueryCore::get_record_json("tm_v3_restrict_child", "rc1", &boot)
            .await
            .unwrap()
            .is_some(),
        "Restrict must not delete the child"
    );
    assert_eq!(CHILD_SE_CALLS.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn tm_s1_set_null_via_apply_under_update_deny() {
    // Update SYSTEM_ONLY on child; deletion-scoped SetNull still clears FK.
    let user = Actor::User {
        user_id: "deleter".into(),
    };
    let v = sqlite_valence(user).await;
    let backend = v.backend_for_table("tm_s1_child").unwrap();
    backend
        .create_record(
            "tm_s1_child",
            json!({"id": {"table":"tm_s1_child","id":"c1"}, "parent_id": "p1", "name": "keep"}),
        )
        .await
        .unwrap();

    apply_deletion_node(
        &DeletionNode {
            table: "tm_s1_child".into(),
            record_id: "c1".into(),
            action: DeletionAction::SetNull {
                field: "parent_id".into(),
            },
            depth: 0,
            connection_name: "kids".into(),
            from_table: "tm_s1_parent".into(),
        },
        &v,
    )
    .await
    .expect("set null despite Update deny");

    let row = QueryCore::get_record_json("tm_s1_child", "c1", &v)
        .await
        .unwrap()
        .unwrap();
    assert!(row.get("parent_id").unwrap().is_null());
    assert_eq!(row.get("name").and_then(|x| x.as_str()), Some("keep"));
}

#[tokio::test]
async fn tm_s1_set_null_via_inline_orchestrator() {
    let user = Actor::User {
        user_id: "deleter".into(),
    };
    let boot = sqlite_valence(Actor::System {
        operation: "chronon_boot".into(),
    })
    .await;
    let backend = boot.backend_for_table("tm_s1_parent").unwrap();
    backend
        .create_record(
            "tm_s1_parent",
            json!({"id": {"table":"tm_s1_parent","id":"p1"}, "name": "p"}),
        )
        .await
        .unwrap();
    backend
        .create_record(
            "tm_s1_child",
            json!({"id": {"table":"tm_s1_child","id":"c1"}, "parent_id": "tm_s1_parent:p1"}),
        )
        .await
        .unwrap();

    let run_id = DeletionService::create_run(
        "tm_s1_parent",
        "p1",
        serde_json::to_value(&user).unwrap(),
        &boot,
    )
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

    let child = QueryCore::get_record_json("tm_s1_child", "c1", &boot)
        .await
        .unwrap()
        .expect("child kept");
    assert!(child.get("parent_id").unwrap().is_null());
    assert!(QueryCore::get_record_json("tm_s1_parent", "p1", &boot)
        .await
        .unwrap()
        .is_none());
}

valence::inventory::submit! {
    SchemaMetadataInit(|| {
        schema_meta(
            "tm_s2_parent",
            vec![SchemaConnection {
                name: "tags".into(),
                from_table: "tm_s2_parent".into(),
                from_field: "id".into(),
                to_table: "tm_s2_peer".into(),
                cardinality: "ManyToMany".into(),
                required: false,
                on_delete: "SetNull".into(),
                label: "tags".into(),
                model_path: None,
                reverse_field: None,
                edge_table: Some("tm_s2_edge".into()),
                target_trait: None,
            }],
            false,
        )
    })
}

valence::inventory::submit! {
    SchemaMetadataInit(|| schema_meta("tm_s2_peer", vec![], false))
}

valence::inventory::submit! {
    SchemaMetadataInit(|| {
        schema_meta(
            "tm_s6_parent",
            vec![
                SchemaConnection {
                    name: "cascade_kids".into(),
                    from_table: "tm_s6_parent".into(),
                    from_field: "id".into(),
                    to_table: "tm_s6_cascade".into(),
                    cardinality: "HasMany".into(),
                    required: false,
                    on_delete: "Cascade".into(),
                    label: "cascade_kids".into(),
                    model_path: None,
                    reverse_field: Some("parent_id".into()),
                    edge_table: None,
                    target_trait: None,
                },
                SchemaConnection {
                    name: "setnull_kids".into(),
                    from_table: "tm_s6_parent".into(),
                    from_field: "id".into(),
                    to_table: "tm_s6_setnull".into(),
                    cardinality: "HasMany".into(),
                    required: false,
                    on_delete: "SetNull".into(),
                    label: "setnull_kids".into(),
                    model_path: None,
                    reverse_field: Some("parent_id".into()),
                    edge_table: None,
                    target_trait: None,
                },
            ],
            false,
        )
    })
}

valence::inventory::submit! {
    SchemaMetadataInit(|| schema_meta("tm_s6_cascade", vec![], false))
}

valence::inventory::submit! {
    SchemaMetadataInit(|| schema_meta("tm_s6_setnull", vec![], false))
}

#[tokio::test]
async fn tm_s2_remove_edge_via_inline_orchestrator() {
    let user = Actor::User {
        user_id: "deleter".into(),
    };
    let boot = sqlite_valence(Actor::System {
        operation: "chronon_boot".into(),
    })
    .await;
    let backend = boot.backend_for_table("tm_s2_parent").unwrap();
    backend
        .create_record(
            "tm_s2_parent",
            json!({"id": {"table":"tm_s2_parent","id":"p1"}}),
        )
        .await
        .unwrap();
    backend
        .create_record(
            "tm_s2_peer",
            json!({"id": {"table":"tm_s2_peer","id":"t1"}}),
        )
        .await
        .unwrap();
    let from = valence::RecordId::new("tm_s2_parent", "p1");
    let to = valence::RecordId::new("tm_s2_peer", "t1");
    boot.relate_edge("tm_s2_edge", &from, &to).await.unwrap();

    let run_id = DeletionService::create_run(
        "tm_s2_parent",
        "p1",
        serde_json::to_value(&user).unwrap(),
        &boot,
    )
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
    assert!(backend
        .get_edge_targets(&from, "tm_s2_edge")
        .await
        .unwrap()
        .is_empty());
    assert!(
        QueryCore::get_record_json("tm_s2_peer", "t1", &boot)
            .await
            .unwrap()
            .is_some(),
        "peer must remain"
    );
}

#[tokio::test]
async fn tm_s6_mixed_cascade_and_set_null() {
    let user = Actor::User {
        user_id: "deleter".into(),
    };
    let boot = sqlite_valence(Actor::System {
        operation: "chronon_boot".into(),
    })
    .await;
    let backend = boot.backend_for_table("tm_s6_parent").unwrap();
    backend
        .create_record(
            "tm_s6_parent",
            json!({"id": {"table":"tm_s6_parent","id":"p1"}}),
        )
        .await
        .unwrap();
    backend
        .create_record(
            "tm_s6_cascade",
            json!({
                "id": {"table":"tm_s6_cascade","id":"c1"},
                "parent_id": "tm_s6_parent:p1"
            }),
        )
        .await
        .unwrap();
    backend
        .create_record(
            "tm_s6_setnull",
            json!({
                "id": {"table":"tm_s6_setnull","id":"s1"},
                "parent_id": "tm_s6_parent:p1"
            }),
        )
        .await
        .unwrap();

    let run_id = DeletionService::create_run(
        "tm_s6_parent",
        "p1",
        serde_json::to_value(&user).unwrap(),
        &boot,
    )
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
    assert!(QueryCore::get_record_json("tm_s6_cascade", "c1", &boot)
        .await
        .unwrap()
        .is_none());
    let kept = QueryCore::get_record_json("tm_s6_setnull", "s1", &boot)
        .await
        .unwrap()
        .expect("setnull child kept");
    assert!(kept.get("parent_id").unwrap().is_null());
}

#[tokio::test]
async fn tm_s7_live_restrict_via_orchestrator() {
    // Same Restrict fixture as the sad Restrict case — live DeletionDag::compute inside orchestrator.
    let user = Actor::User {
        user_id: "deleter".into(),
    };
    let boot = sqlite_valence(Actor::System {
        operation: "chronon_boot".into(),
    })
    .await;
    let backend = boot.backend_for_table("tm_v3_restrict_parent").unwrap();
    backend
        .create_record(
            "tm_v3_restrict_parent",
            json!({"id": {"table":"tm_v3_restrict_parent","id":"rp2"}}),
        )
        .await
        .unwrap();
    backend
        .create_record(
            "tm_v3_restrict_child",
            json!({
                "id": {"table":"tm_v3_restrict_child","id":"rc2"},
                "parent_id": "tm_v3_restrict_parent:rp2"
            }),
        )
        .await
        .unwrap();

    let dag = valence::deletion::dag::DeletionDag::compute("tm_v3_restrict_parent", "rp2", &boot)
        .await
        .unwrap();
    assert!(!dag.restrict_violations.is_empty());
    assert!(dag.nodes.is_empty());

    let run_id = DeletionService::create_run(
        "tm_v3_restrict_parent",
        "rp2",
        serde_json::to_value(&user).unwrap(),
        &boot,
    )
    .await
    .expect("create_run");
    run_valence_deletion_orchestrator_inline_steps(boot.clone(), run_id.clone())
        .await
        .expect("orchestrator");
    let run = DeletionService::get_run_json(&run_id, &boot)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(run.get("status").and_then(|s| s.as_str()), Some("failed"));
}
