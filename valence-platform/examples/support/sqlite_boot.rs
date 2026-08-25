//! SQLite Valence boot for iter examples and tests (ownership env + router wiring).

use std::sync::Arc;

use valence::{
    Actor, DatabaseBackend, DatabaseRouter, RegisterBackendLogicalNamesOptions, SqliteBackend,
    Valence, SQLITE_ENGINE_ID,
};

fn ownership_env_defaults() {
    if std::env::var_os("VALENCE_OWNERSHIP_UNIFIED_FETCH").is_none() {
        std::env::set_var("VALENCE_OWNERSHIP_UNIFIED_FETCH", "0");
    }
    if std::env::var_os("VALENCE_OWNERSHIP_COLOCATE").is_none() {
        std::env::set_var("VALENCE_OWNERSHIP_COLOCATE", "0");
    }
}

/// In-memory SQLite Valence on logical `default`.
pub async fn sqlite_valence(operation: &str) -> anyhow::Result<Valence> {
    ownership_env_defaults();
    let backend: Arc<dyn DatabaseBackend> = Arc::new(SqliteBackend::connect_memory().await?);
    let mut router = DatabaseRouter::new();
    valence::register_backend_logical_names(
        &mut router,
        backend,
        &["default"],
        RegisterBackendLogicalNamesOptions::default(),
    );
    Ok(Valence::builder()
        .database_router(Arc::new(router))
        .default_backend_key(valence::router_key("default", SQLITE_ENGINE_ID))
        .with_actor(Actor::System {
            operation: operation.to_string(),
        })
        .build()?)
}
