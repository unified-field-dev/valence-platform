//! Cadence constants for the platform TTL sweeper.

#[test]
fn ttl_sweep_cap_and_cron_expr_parse_happy() {
    assert_eq!(
        valence_platform::ttl::sweep::DEFAULT_TTL_SWEEP_CAP,
        32,
        "TTL sweep should queue at most 32 deletes per tick"
    );
    chronon_coordinator::CronExpr::parse("*/30 * * * * *", None)
        .expect("ttl sweep cron must parse");
}

#[test]
fn ttl_sweep_cron_expr_rejects_garbage_sad() {
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

#[test]
fn fair_table_limit_unit() {
    assert_eq!(valence_platform::ttl::sweep::fair_table_limit(32, 3), 11);
    assert_eq!(valence_platform::ttl::sweep::fair_table_limit(1, 10), 1);
}
