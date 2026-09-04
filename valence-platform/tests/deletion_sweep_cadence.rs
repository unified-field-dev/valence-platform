//! Regression for deletion sweep stale window + cron cadence.

#[test]
fn deletion_sweep_stale_secs_and_cron_expr_parse_happy() {
    assert_eq!(
        valence_platform::deletion::sweep::DEFAULT_STALE_SECS,
        10,
        "sweep should treat queued runs as stale after ~10s"
    );
    chronon_coordinator::CronExpr::parse("*/10 * * * * *", None)
        .expect("deletion sweep cron must parse");
}

#[test]
fn deletion_sweep_cron_expr_rejects_garbage_sad() {
    let err = chronon_coordinator::CronExpr::parse("not a cron", None)
        .expect_err("garbage cron must fail to parse");
    let msg = err.to_string();
    assert!(
        msg.to_lowercase().contains("cron")
            || msg.to_lowercase().contains("parse")
            || msg.to_lowercase().contains("invalid")
            || msg.to_lowercase().contains("expr"),
        "cron sad path should classify parse failure, got: {msg}"
    );
}
