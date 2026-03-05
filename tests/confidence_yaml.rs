use serde_yaml::Value;

#[test]
fn confidence_yaml_is_valid_yaml() {
    let raw = std::fs::read_to_string("CONFIDENCE.yaml").expect("CONFIDENCE.yaml must exist");
    let parsed: Value = serde_yaml::from_str(&raw).expect("CONFIDENCE.yaml must be valid YAML");
    assert!(parsed.get("system").is_some());
}

/// CI gate: effective_confidence must reach >= 0.75 before Session 7 is marked complete.
/// This test fails (blocks "production-ready" label) if the system confidence
/// falls below the Session 7 hard threshold.
#[test]
fn system_effective_confidence_meets_session7_threshold() {
    const MIN_CONFIDENCE: f64 = 0.75;

    let raw = std::fs::read_to_string("CONFIDENCE.yaml").expect("CONFIDENCE.yaml must exist");
    let parsed: Value = serde_yaml::from_str(&raw).expect("CONFIDENCE.yaml must be valid YAML");

    let effective = parsed["system"]["effective_confidence"]
        .as_f64()
        .expect("system.effective_confidence must be a float");

    assert!(
        effective >= MIN_CONFIDENCE,
        "system effective_confidence {effective:.3} is below Session 7 threshold {MIN_CONFIDENCE}. \
         Investigate: check weakest_link in CONFIDENCE.yaml and raise it above 0.65."
    );
}

/// Gate: no artifact in the critical execution path may have effective < 0.65.
/// Blocks "production-ready" claims as defined in agents.md §4.
#[test]
fn no_critical_artifact_below_0_65() {
    // Critical execution path artifacts (anything below this triggers a warning in README).
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

    let raw = std::fs::read_to_string("CONFIDENCE.yaml").expect("CONFIDENCE.yaml must exist");
    let parsed: Value = serde_yaml::from_str(&raw).expect("CONFIDENCE.yaml must be valid YAML");

    let artifacts = parsed["artifacts"]
        .as_sequence()
        .expect("artifacts must be a list");

    for item in artifacts {
        let name = item["artifact"].as_str().unwrap_or("");
        if CRITICAL_ARTIFACTS.contains(&name) {
            let eff = item["effective"].as_f64().unwrap_or(0.0);
            assert!(
                eff >= FLOOR,
                "Critical artifact '{name}' has effective_confidence {eff:.3} < {FLOOR}. \
                 Address before claiming production-readiness."
            );
        }
    }
}
