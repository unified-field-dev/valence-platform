//! Requester actor restore from `requested_by`.

#![allow(missing_docs, clippy::expect_used, clippy::unwrap_used)]

use std::sync::Arc;

use serde_json::json;
use valence::{
    register_backend_logical_names, router_key, Actor, DatabaseBackend, DatabaseRouter,
    RegisterBackendLogicalNamesOptions, SqliteBackend, Valence, SQLITE_ENGINE_ID,
};
use valence_platform::deletion::run_service::{parse_requested_by_actor, DeletionService};

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
async fn tm_a1_requester_valence_matches_deleting_user() {
    let user = Actor::User {
        user_id: "deleter-42".into(),
    };
    let boot = sqlite_valence(Actor::System {
        operation: "chronon_boot".into(),
    })
    .await;
    let actor_json = serde_json::to_value(&user).unwrap();
    let run_id = DeletionService::create_run("widget", "w1", actor_json, &boot)
        .await
        .expect("create_run");

    let restored = DeletionService::requester_valence_from_run(&run_id, &boot)
        .await
        .expect("restore");
    assert_eq!(*restored.actor(), user);
    assert!(!restored.actor().is_system());
}

#[tokio::test]
async fn tm_a2_restored_user_sees_system_only_delete_deny() {
    use valence::evaluator::DEFAULT_IN_MEMORY;
    use valence::privacy_policies::common::SYSTEM_ONLY;
    use valence::schema::SchemaMetadata;
    use valence::schema_api::{
        Schema, SchemaMeta, SchemaPolicies, SchemaPolicyRule, SchemaPolicyRules, SchemaPrivacy,
    };
    use valence::{DatabaseEvaluator, PrivacyEvaluator, PrivacyOperation};

    let user = Actor::User {
        user_id: "limited".into(),
    };
    let boot = sqlite_valence(Actor::System {
        operation: "chronon_boot".into(),
    })
    .await;
    let actor_json = serde_json::to_value(&user).unwrap();
    let run_id = DeletionService::create_run("widget", "w2", actor_json, &boot)
        .await
        .expect("create_run");
    let restored = DeletionService::requester_valence_from_run(&run_id, &boot)
        .await
        .expect("restore");

    let schema = Box::leak(Box::new(Schema {
        name: "tm_a2_secret".into(),
        version: "0.1.0".into(),
        databases: vec![DEFAULT_IN_MEMORY.name().to_string()],
        database_evaluator: &DEFAULT_IN_MEMORY,
        privacy: SchemaPrivacy {
            read: "x".into(),
            write: "x".into(),
        },
        policies: Some(SchemaPolicies {
            delete: Some(SchemaPolicyRules {
                allow: vec![SchemaPolicyRule {
                    name: "SYSTEM_ONLY".into(),
                    description: None,
                    evaluator: Some(&SYSTEM_ONLY),
                }],
                ..SchemaPolicyRules::default()
            }),
            ..SchemaPolicies::default()
        }),
        fields: vec![],
        edges: Vec::new(),
        connections: Vec::new(),
        side_effects: Vec::new(),
        iters: Vec::new(),
        composite_key: Vec::new(),
        traits: Vec::new(),
        ttl: None,
        ownership: None,
        meta: SchemaMeta {
            retention: "365 days".into(),
            row_count: 0,
            owner: "system".into(),
            description: None,
        },
    }));
    let meta = Box::leak(Box::new(SchemaMetadata::from_schema(schema)));
    let denied = PrivacyEvaluator::check_entity_access(
        meta,
        PrivacyOperation::Delete,
        &serde_json::json!({"id": "1"}),
        &restored,
    )
    .await;
    assert!(
        denied.is_err(),
        "restored user must not pass SYSTEM_ONLY Delete"
    );
    let msg = denied.expect_err("deny").to_string();
    assert!(
        msg.to_lowercase().contains("deny")
            || msg.to_lowercase().contains("privacy")
            || msg.to_lowercase().contains("system")
            || msg.to_lowercase().contains("not allowed")
            || msg.to_lowercase().contains("forbidden"),
        "deny should classify privacy rejection, got: {msg}"
    );
}

#[tokio::test]
async fn tm_a3_missing_requested_by_fails_closed() {
    let boot = sqlite_valence(Actor::System {
        operation: "chronon_boot".into(),
    })
    .await;
    let run_id = "orphan-run".to_string();
    let sys = boot.with_actor(Actor::System {
        operation: "valence_deletion_run".into(),
    });
    let backend = sys.backend_for_table("valence_deletion_run").unwrap();
    backend
        .create_record(
            "valence_deletion_run",
            json!({
                "id": run_id,
                "root_table": "widget",
                "root_record_id": "w1",
                "status": "queued",
                "total_steps": 0,
                "completed_steps": 0,
                "failed_steps": 0,
                "requested_at": chrono::Utc::now(),
            }),
        )
        .await
        .expect("create orphan run");

    let err = DeletionService::requester_valence_from_run(&run_id, &boot)
        .await
        .expect_err("missing requested_by must fail closed");
    let msg = err.to_string();
    assert!(
        msg.contains("requested_by") || msg.contains("missing"),
        "got {msg}"
    );
}

#[test]
fn parse_requested_by_accepts_string_and_object() {
    let actor = Actor::User {
        user_id: "u9".into(),
    };
    let as_value = serde_json::to_value(&actor).unwrap();
    let as_string = serde_json::Value::String(as_value.to_string());
    assert_eq!(parse_requested_by_actor(&as_value).unwrap(), actor);
    assert_eq!(parse_requested_by_actor(&as_string).unwrap(), actor);

    let bad = parse_requested_by_actor(&json!("not-json-actor"));
    let err = bad.expect_err("garbage requested_by must fail");
    let msg = err.to_string();
    assert!(!msg.is_empty(), "parse error must carry a message");
    assert!(
        msg.contains("requested_by")
            || msg.contains("actor")
            || msg.contains("parse")
            || msg.contains("expected")
            || msg.contains("invalid"),
        "unexpected parse error: {msg}"
    );
}
