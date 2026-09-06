use serde_yaml::Value;

fn load_confidence() -> Value {
    let raw = std::fs::read_to_string("CONFIDENCE.yaml").expect("CONFIDENCE.yaml must exist");
    serde_yaml::from_str(&raw).expect("CONFIDENCE.yaml must be valid YAML")
}

#[test]
fn confidence_yaml_is_valid_yaml() {
    let parsed = load_confidence();
    assert!(parsed.get("system").is_some());
    assert!(parsed.get("artifacts").is_some());
}

/// Internal confidence score for the scoped local engine + Raft subsystem.
/// This score is deliberately not a production-readiness declaration.
#[test]
fn scoped_system_confidence_meets_regression_floor() {
    const MIN_CONFIDENCE: f64 = 0.75;
    let parsed = load_confidence();

    let effective = parsed["system"]["effective_confidence"]
        .as_f64()
        .expect("system.effective_confidence must be a float");

    assert!(
        effective >= MIN_CONFIDENCE,
        "scoped system effective_confidence {effective:.3} is below regression floor {MIN_CONFIDENCE}"
    );
}

#[test]
fn critical_local_execution_artifacts_stay_above_floor() {
    const CRITICAL_ARTIFACTS: &[&str] = &[
        "wire_protocol_v3",
        "sql_parser",
        "binder",
        "physical_executor",
        "storage_engine",
        "mvcc_txn_manager",
        "mvcc_gc",
    ];
    const FLOOR: f64 = 0.65;

    let parsed = load_confidence();
    let artifacts = parsed["artifacts"]
        .as_sequence()
        .expect("artifacts must be a list");

    for item in artifacts {
        let name = item["artifact"].as_str().unwrap_or("");
        if CRITICAL_ARTIFACTS.contains(&name) {
            let eff = item["effective"].as_f64().unwrap_or(0.0);
            assert!(
                eff >= FLOOR,
                "Critical local artifact '{name}' has effective confidence {eff:.3} < {FLOOR}"
            );
        }
    }
}

#[test]
fn confidence_ledger_cannot_claim_replicated_sql_or_production_ready() {
    let parsed = load_confidence();

    assert_eq!(
        parsed["system"]["production_ready"].as_bool(),
        Some(false),
        "NeuralBase must not be marked production-ready while replicated SQL is absent"
    );
    assert_eq!(
        parsed["system"]["distributed_sql_replication"].as_bool(),
        Some(false),
        "distributed_sql_replication must remain false until SQL DDL/DML is committed and applied through Raft"
    );

    let artifacts = parsed["artifacts"]
        .as_sequence()
        .expect("artifacts must be a list");
    let replication = artifacts
        .iter()
        .find(|item| item["artifact"].as_str() == Some("sql_replication"))
        .expect("sql_replication boundary must be explicit");
    assert_eq!(replication["status"].as_str(), Some("not_implemented"));
}
