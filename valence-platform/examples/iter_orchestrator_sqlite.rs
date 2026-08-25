//! Seeded Valence Iter scan on in-memory SQLite (inline row worker; no Boson).
//!
//! ## What this runs
//!
//! 1. `valence_schema!` for `DemoNote` with `iters: [MarkProcessedIter]`, plus
//!    `should_run` / `execute` on that type.
//! 2. Seed three `demo_note` rows (`seq` 1, 2, 3).
//! 3. [`IterService::run_for_tests`] — create a pending [`ValenceIterRun`] and drive the inline
//!    orchestrator (pages the table and runs each row on this task; no Chronon).
//! 4. Even `seq` rows run `execute` (`marker = "done"`); odd rows are skipped.
//!
//! ## Host path (not used here)
//!
//! Hosts call [`valence_platform::iter::dispatch::register_iter_dispatch`] at boot, then
//! [`IterService::start`] (Chronon `run_now` → Boson row workers). Author per-row hooks the same
//! way as this demo: list the type in `iters: [...]` and implement `should_run` / `execute`.
//!
//! Schema crates normally get the model type from Valence / uf-valence codegen. This example
//! declares a serde-compatible `DemoNote` inline so hooks type-check without a separate
//! `build.rs`.
//!
//! ## Command
//! ```bash
//! CARGO_BUILD_JOBS=1 cargo run -p valence-platform --example iter_orchestrator_sqlite
//! ```
//!
//! ## Success
//! Stdout prints processed=1 skipped=2 and even row marker=done.

#![allow(
    dead_code,
    clippy::print_stdout,
    clippy::unwrap_used,
    clippy::expect_used
)]

#[path = "support/sqlite_boot.rs"]
pub mod sqlite_boot;

use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlite_boot::sqlite_valence;
use valence::prelude::*;
use valence::{
    Database, DatabaseFromEngine, IterEvaluation, QueryCore, RecordId, Valence, SQLITE_ENGINE_ID,
};
use valence_platform::iter::run_service::{IterRunOptions, IterService};
use valence_platform::{ValenceIterRun, ValenceIterRunStatus};

/// Logical table the orchestrator pages.
pub const DEMO_TABLE: &str = "demo_note";
/// Must match the type listed in `iters: [MarkProcessedIter]`.
pub const DEMO_ITER: &str = "MarkProcessedIter";

const DEMO_NOTE_DB: DatabaseFromEngine = Database::from_engine("default", SQLITE_ENGINE_ID);

/// Row shape for `demo_note` (mirrors what valence-codegen would emit for this schema).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DemoNote {
    #[serde(default)]
    id: Option<RecordId>,
    seq: i64,
    marker: String,
}

impl DemoNote {
    /// Primary key when present on the loaded row.
    pub fn id(&self) -> Option<&RecordId> {
        self.id.as_ref()
    }

    /// Sequence used by [`MarkProcessedIter::should_run`].
    pub fn seq(&self) -> &i64 {
        &self.seq
    }
}

/// Per-row iter: even `seq` runs; odd skips.
pub struct MarkProcessedIter;

valence_schema! {
    DemoNote {
        table: "demo_note",
        version: "0.1.0",
        description: "Teaching table for valence-platform iter examples",
        database: DEMO_NOTE_DB,
        fields: [
            id: { r#type: FieldType::String, primary_key: true, required: true },
            seq: { r#type: FieldType::Integer, required: true },
            marker: { r#type: FieldType::String, required: true },
        ],
        iters: [MarkProcessedIter],
    }
}

impl MarkProcessedIter {
    /// Even `seq` → run; odd → skip.
    pub async fn should_run(
        &self,
        row: &DemoNote,
        _valence: &Valence,
    ) -> valence::Result<IterEvaluation> {
        if *row.seq() % 2 == 0 {
            Ok(IterEvaluation::run("even seq"))
        } else {
            Ok(IterEvaluation::skip("odd seq"))
        }
    }

    /// Sets `marker = "done"` on the row.
    pub async fn execute(&self, row: &DemoNote, valence: &Valence) -> valence::Result<()> {
        let id = row
            .id()
            .map(|r| r.id().to_string())
            .ok_or_else(|| valence::Error::Internal("demo_note row missing id".into()))?;
        let backend = valence.backend_for_table(DEMO_TABLE)?;
        backend
            .merge_record(DEMO_TABLE, &id, json!({ "marker": "done" }))
            .await?;
        Ok(())
    }
}

/// Insert three notes (`seq` 1, 2, 3); only even seq should execute.
pub async fn seed_demo_notes(v: &Valence) -> anyhow::Result<()> {
    let backend = v.backend_for_table(DEMO_TABLE)?;
    for (id, seq) in [("n1", 1_i64), ("n2", 2), ("n3", 3)] {
        backend
            .create_record(
                DEMO_TABLE,
                json!({
                    "id": { "table": DEMO_TABLE, "id": id },
                    "seq": seq,
                    "marker": "pending",
                }),
            )
            .await?;
    }
    Ok(())
}

/// Happy-path postconditions for the seeded demo.
pub async fn assert_demo_outcomes(v: &Valence, completed: &ValenceIterRun) -> anyhow::Result<()> {
    assert_eq!(*completed.status(), ValenceIterRunStatus::Completed);
    assert_eq!(*completed.total_rows(), 3);
    assert_eq!(*completed.processed_rows(), 1);
    assert_eq!(*completed.skipped_rows(), 2);
    assert_eq!(*completed.failed_rows(), 0);

    let even = QueryCore::get_record_json(DEMO_TABLE, "n2", v)
        .await?
        .ok_or_else(|| anyhow::anyhow!("even row n2 missing"))?;
    assert_eq!(even.get("marker").and_then(|m| m.as_str()), Some("done"));

    for odd in ["n1", "n3"] {
        let row = QueryCore::get_record_json(DEMO_TABLE, odd, v)
            .await?
            .ok_or_else(|| anyhow::anyhow!("odd row {odd} missing"))?;
        assert_eq!(
            row.get("marker").and_then(|m| m.as_str()),
            Some("pending"),
            "{odd} should stay pending"
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Demo-only System actor. Hosts use a real actor from request / job context.
    let valence = sqlite_valence("iter_orchestrator_sqlite_example").await?;

    seed_demo_notes(&valence).await?;
    let completed = IterService::run_for_tests(
        &valence,
        DEMO_ITER,
        DEMO_TABLE,
        IterRunOptions::default()
            .initiated_by("example")
            .run_id("sqlite-example"),
    )
    .await?;
    assert_demo_outcomes(&valence, &completed).await?;

    println!(
        "iter run sqlite-example completed: total={} processed={} skipped={} failed={}",
        completed.total_rows(),
        completed.processed_rows(),
        completed.skipped_rows(),
        completed.failed_rows()
    );
    println!("even row n2 marker=done (odd rows stay pending)");

    Ok(())
}
