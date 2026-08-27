//! Seeded inline iter demo (happy counts+marker; missing descriptor sad).

#![allow(missing_docs, clippy::expect_used, clippy::unwrap_used)]

#[path = "../examples/iter_orchestrator_sqlite.rs"]
mod iter_example;

use iter_example::sqlite_boot::sqlite_valence;
use iter_example::{assert_demo_outcomes, seed_demo_notes, DEMO_ITER, DEMO_TABLE};
use valence_platform::iter::run_service::{IterRunOptions, IterService};

#[tokio::test]
async fn tm1_seeded_iter_updates_counts_and_marker_happy() {
    let v = sqlite_valence("iter_inline_demo_tm1")
        .await
        .expect("sqlite valence");
    seed_demo_notes(&v).await.expect("seed");
    let completed = IterService::run_for_tests(
        &v,
        DEMO_ITER,
        DEMO_TABLE,
        IterRunOptions::default()
            .initiated_by("test")
            .run_id("tm1-run"),
    )
    .await
    .expect("run");
    assert_demo_outcomes(&v, &completed)
        .await
        .expect("outcomes");
}

#[tokio::test]
async fn tm2_missing_iter_descriptor_fails_closed_sad() {
    let v = sqlite_valence("iter_inline_demo_tm2")
        .await
        .expect("sqlite valence");
    seed_demo_notes(&v).await.expect("seed");

    let run_id = IterService::create_run(
        &v,
        "NoSuchIter",
        DEMO_TABLE,
        IterRunOptions::default()
            .initiated_by("test")
            .run_id("tm2-missing-iter"),
    )
    .await
    .expect("create pending run");

    let err = IterService::start_for_tests(&v, &run_id)
        .await
        .expect_err("missing IterDescriptor must fail");
    let msg = err.to_string();
    assert!(
        msg.contains("NoSuchIter") || msg.contains("no IterDescriptor") || msg.contains(DEMO_TABLE),
        "error should name missing iter/table, got: {msg}"
    );
    let _ = DEMO_ITER;
}
