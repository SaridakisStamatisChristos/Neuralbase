// SPDX-License-Identifier: Apache-2.0
// Phase 8: executable anti-overclaim checks for SQL_COMPATIBILITY.yaml.

use serde_yaml::{Mapping, Value};
use std::collections::HashSet;
use std::path::Path;

fn load_profile() -> Value {
    let raw = std::fs::read_to_string("SQL_COMPATIBILITY.yaml")
        .expect("SQL_COMPATIBILITY.yaml must exist");
    serde_yaml::from_str(&raw).expect("SQL_COMPATIBILITY.yaml must be valid YAML")
}

fn features(profile: &Value) -> &[Value] {
    profile["features"]
        .as_sequence()
        .expect("features must be a sequence")
}

fn feature<'a>(profile: &'a Value, name: &str) -> &'a Mapping {
    features(profile)
        .iter()
        .find(|item| item["feature"].as_str() == Some(name))
        .and_then(Value::as_mapping)
        .unwrap_or_else(|| panic!("missing compatibility feature: {name}"))
}

fn field<'a>(item: &'a Mapping, key: &str) -> &'a Value {
    item.get(Value::String(key.to_string()))
        .unwrap_or_else(|| panic!("feature is missing required field '{key}'"))
}

#[test]
fn compatibility_profile_is_valid_and_explicitly_not_full_postgresql() {
    let profile = load_profile();
    assert_eq!(profile["version"].as_i64(), Some(1));
    assert_eq!(profile["phase"].as_i64(), Some(8));
    assert_eq!(
        profile["full_postgresql_compatibility"].as_bool(),
        Some(false),
        "Phase 8 must not overclaim full PostgreSQL compatibility"
    );
    assert_eq!(profile["postgresql_reference_version"].as_i64(), Some(16));
}

#[test]
fn every_feature_uses_declared_status_and_has_required_semantic_fields() {
    let profile = load_profile();
    let allowed: HashSet<&str> = profile["status_values"]
        .as_sequence()
        .expect("status_values must be a sequence")
        .iter()
        .map(|v| v.as_str().expect("status_values entries must be strings"))
        .collect();

    let required = [
        "feature",
        "family",
        "status",
        "supported_syntax",
        "semantic_scope",
        "postgres_reference_evidence",
        "neuralbase_divergence",
        "clustered_mode_restriction",
        "introduced",
    ];

    for item in features(&profile) {
        let map = item.as_mapping().expect("feature entries must be mappings");
        let name = field(map, "feature")
            .as_str()
            .expect("feature name must be a string");
        for key in required {
            assert!(
                map.contains_key(Value::String(key.to_string())),
                "feature '{name}' is missing required field '{key}'"
            );
        }
        let status = field(map, "status")
            .as_str()
            .expect("feature status must be a string");
        assert!(
            allowed.contains(status),
            "feature '{name}' uses undeclared status '{status}'"
        );
        assert!(
            field(map, "postgres_reference_evidence").is_sequence(),
            "feature '{name}' postgres_reference_evidence must be a sequence"
        );
    }
}

#[test]
fn every_reference_tested_claim_names_existing_executable_evidence() {
    let profile = load_profile();
    for item in features(&profile) {
        let map = item.as_mapping().expect("feature entries must be mappings");
        let name = field(map, "feature").as_str().unwrap();
        if field(map, "status").as_str() != Some("supported_reference_tested") {
            continue;
        }

        let evidence = field(map, "postgres_reference_evidence")
            .as_sequence()
            .expect("postgres_reference_evidence must be a sequence");
        assert!(
            !evidence.is_empty(),
            "reference-tested feature '{name}' must name executable evidence"
        );
        for path in evidence {
            let path = path.as_str().expect("evidence paths must be strings");
            assert!(
                Path::new(path).exists(),
                "reference evidence for '{name}' does not exist: {path}"
            );
            assert!(
                path.starts_with("tests/"),
                "reference evidence for '{name}' must be executable test evidence: {path}"
            );
        }
    }
}

#[test]
fn phase8_anti_overclaim_boundaries_are_locked() {
    let profile = load_profile();

    assert_eq!(
        field(feature(&profile, "transaction.sql_blocks"), "status").as_str(),
        Some("unsupported")
    );
    assert_eq!(
        field(feature(&profile, "wire.bind_parameters"), "status").as_str(),
        Some("partial")
    );
    assert_eq!(
        field(feature(&profile, "wire.result_formats"), "status").as_str(),
        Some("partial")
    );
    assert_eq!(
        field(feature(&profile, "wire.describe"), "status").as_str(),
        Some("partial")
    );
    for name in [
        "wire.bind_parameters",
        "wire.result_formats",
        "wire.describe",
    ] {
        assert_ne!(
            field(feature(&profile, name), "status").as_str(),
            Some("supported_reference_tested"),
            "{name} must remain explicitly narrower than full PostgreSQL semantics"
        );
    }
    assert_ne!(
        field(feature(&profile, "text.collation"), "status").as_str(),
        Some("supported_reference_tested")
    );
    assert_eq!(
        field(feature(&profile, "authorization.sql_privileges"), "status").as_str(),
        Some("unsupported")
    );
}

#[test]
fn persistent_dml_predicate_profile_requires_fail_closed_evidence() {
    let profile = load_profile();
    let item = feature(&profile, "dml.update_delete_predicate");

    assert_eq!(field(item, "status").as_str(), Some("partial"));
    let scope = field(item, "semantic_scope").as_str().unwrap_or_default();
    assert!(
        scope.to_ascii_lowercase().contains("fail closed"),
        "persistent DML predicate scope must explicitly promise fail-closed behavior"
    );

    let evidence = item
        .get(Value::String("neuralbase_evidence".to_string()))
        .and_then(Value::as_sequence)
        .expect("DML predicate safety must name NeuralBase evidence");
    assert!(evidence.iter().any(|v| {
        v.as_str() == Some("tests/phase8_dml_predicate_safety.rs")
            && Path::new("tests/phase8_dml_predicate_safety.rs").exists()
    }));
}
