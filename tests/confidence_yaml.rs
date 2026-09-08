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
fn distributed_claim_tracks_membership_and_identity_without_overclaiming_production() {
    let parsed = load_confidence();
    let system = &parsed["system"];

    assert_eq!(system["production_ready"].as_bool(), Some(false));
    assert_eq!(system["distributed_sql_replication"].as_bool(), Some(true));

    let scope = &system["replication_scope"];
    assert_eq!(scope["follower_writes"].as_str(), Some("reject"));
    assert_eq!(scope["follower_reads_linearizable"].as_bool(), Some(false));
    assert_eq!(scope["sql_snapshots"].as_bool(), Some(true));
    assert_eq!(
        scope["fixed_member_empty_storage_bootstrap"].as_bool(),
        Some(true)
    );
    assert_eq!(scope["dynamic_membership"].as_bool(), Some(true));
    assert_eq!(scope["learner_promotion"].as_bool(), Some(true));
    assert_eq!(scope["joint_consensus"].as_bool(), Some(true));
    assert_eq!(scope["auth_replication"].as_bool(), Some(true));
    assert_eq!(
        scope["replicated_auth_method"].as_str(),
        Some("scram-sha-256")
    );
    assert_eq!(
        scope["automatic_membership_reconciliation"].as_bool(),
        Some(false)
    );
    assert_eq!(scope["hpa_safe"].as_bool(), Some(false));
    assert_eq!(scope["production_ha"].as_bool(), Some(false));

    let ddl = scope["table_ddl"]
        .as_sequence()
        .expect("replication_scope.table_ddl must be a list");
    assert_eq!(ddl.len(), 2);
    assert_eq!(ddl[0].as_str(), Some("create_table"));
    assert_eq!(ddl[1].as_str(), Some("drop_table"));

    let dml = scope["table_dml"]
        .as_sequence()
        .expect("replication_scope.table_dml must be a list");
    assert_eq!(dml.len(), 3);
    assert_eq!(dml[0].as_str(), Some("insert"));
    assert_eq!(dml[1].as_str(), Some("update"));
    assert_eq!(dml[2].as_str(), Some("delete"));

    let artifacts = parsed["artifacts"]
        .as_sequence()
        .expect("artifacts must be a list");

    let membership = artifacts
        .iter()
        .find(|item| item["artifact"].as_str() == Some("membership_changes"))
        .expect("membership_changes boundary must be explicit");
    assert_eq!(
        membership["status"].as_str(),
        Some("learner_joint_consensus_lifecycle_tested")
    );

    let identity = artifacts
        .iter()
        .find(|item| item["artifact"].as_str() == Some("replicated_identity"))
        .expect("replicated_identity boundary must be explicit");
    assert_eq!(
        identity["status"].as_str(),
        Some("scram_verifier_replication_and_migration_tested")
    );
}
