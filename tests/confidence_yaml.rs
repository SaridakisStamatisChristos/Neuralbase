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
fn distributed_claim_tracks_membership_identity_recovery_reads_and_managed_reconciliation_without_overclaiming_production(
) {
    let parsed = load_confidence();
    let system = &parsed["system"];

    assert_eq!(system["production_ready"].as_bool(), Some(false));
    assert_eq!(system["distributed_sql_replication"].as_bool(), Some(true));

    let scope = &system["replication_scope"];
    assert_eq!(scope["follower_writes"].as_str(), Some("reject"));
    assert_eq!(
        scope["strong_reads_require_current_leader"].as_bool(),
        Some(true)
    );
    assert_eq!(
        scope["strong_read_barrier"].as_str(),
        Some("raft_log_quorum_confirmed_apply")
    );
    assert_eq!(scope["strong_read_silent_downgrade"].as_bool(), Some(false));
    assert_eq!(scope["follower_reads_linearizable"].as_bool(), Some(false));
    assert_eq!(
        scope["automatic_strong_read_routing"].as_bool(),
        Some(false)
    );
    assert_eq!(
        scope["readindex_or_lease_optimization"].as_bool(),
        Some(false)
    );
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
    assert_eq!(scope["operator_backup_restore"].as_bool(), Some(true));
    assert_eq!(scope["offline_backup"].as_bool(), Some(true));
    assert_eq!(scope["online_backup"].as_bool(), Some(true));
    assert_eq!(scope["encrypted_backup"].as_bool(), Some(true));
    assert_eq!(scope["fresh_cluster_dr"].as_bool(), Some(true));
    assert_eq!(scope["pitr"].as_bool(), Some(false));
    assert_eq!(scope["automatic_dr"].as_bool(), Some(false));
    assert_eq!(
        scope["automatic_membership_reconciliation"].as_bool(),
        Some(true)
    );
    assert_eq!(scope["hpa_safe"].as_bool(), Some(false));
    assert_eq!(scope["production_ha"].as_bool(), Some(false));

    let modes = scope["read_consistency_modes"]
        .as_sequence()
        .expect("replication_scope.read_consistency_modes must be a list");
    assert_eq!(modes.len(), 3);
    assert_eq!(modes[0].as_str(), Some("local"));
    assert_eq!(modes[1].as_str(), Some("leader"));
    assert_eq!(modes[2].as_str(), Some("linearizable"));

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
        Some("learner_joint_consensus_and_managed_reconciliation_tested")
    );
    let membership_evidence = membership["evidence"]
        .as_sequence()
        .expect("membership evidence must be a list");
    assert!(membership_evidence
        .iter()
        .any(|item| item.as_str() == Some("tests/phase7_process.rs")));
    assert!(membership_evidence
        .iter()
        .any(|item| item.as_str() == Some("tests/phase7_kubernetes.py")));

    let identity = artifacts
        .iter()
        .find(|item| item["artifact"].as_str() == Some("replicated_identity"))
        .expect("replicated_identity boundary must be explicit");
    assert_eq!(
        identity["status"].as_str(),
        Some("scram_verifier_replication_and_migration_tested")
    );

    let recovery = artifacts
        .iter()
        .find(|item| item["artifact"].as_str() == Some("backup_recovery"))
        .expect("backup_recovery boundary must be explicit");
    assert_eq!(
        recovery["status"].as_str(),
        Some("offline_online_encrypted_fresh_cluster_dr_tested")
    );

    let reads = artifacts
        .iter()
        .find(|item| item["artifact"].as_str() == Some("read_consistency"))
        .expect("read_consistency boundary must be explicit");
    assert_eq!(
        reads["status"].as_str(),
        Some("local_leader_linearizable_barrier_tested")
    );
}
