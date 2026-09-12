// SPDX-License-Identifier: Apache-2.0
#![cfg(unix)]
use neuralbase::operator::DesiredTopology;
use neuralbase::operator_admin::{bind_storage, ManagedNode};
use std::collections::{BTreeMap, BTreeSet};
fn node() -> ManagedNode {
    ManagedNode {
        topology: DesiredTopology {
            version: 1,
            cluster: "g1".into(),
            revision: 1,
            endpoints: BTreeMap::from([
                ("g1.a".into(), "127.0.0.1:7001".into()),
                ("g1.b".into(), "127.0.0.1:7002".into()),
            ]),
            voters: BTreeSet::from(["g1.a".into()]),
            minimum_voters: 1,
        },
        id: "g1.b".into(),
        seeds: vec!["g1.a".into()],
        learner: true,
        socket: "/tmp/g1.b.sock".into(),
    }
}
#[test]
fn restart_preserves_incarnation_and_rejects_old_disk_relabeling() {
    let root = tempfile::tempdir().unwrap();
    let db = root.path().join("db");
    let n = node();
    n.validate().unwrap();
    bind_storage(&n, &db).unwrap();
    std::fs::write(db.join("CURRENT"), "existing rocksdb").unwrap();
    bind_storage(&n, &db).unwrap();
    let mut changed = n.clone();
    changed.id = "g1.a".into();
    assert!(bind_storage(&changed, &db).is_err());
    changed = n.clone();
    changed.topology.cluster = "restored".into();
    assert!(bind_storage(&changed, &db).is_err());
    changed = n;
    changed
        .topology
        .endpoints
        .insert("g1.b".into(), "127.0.0.1:9999".into());
    assert!(bind_storage(&changed, &db).is_err());
}
#[test]
fn unmanaged_restored_or_partially_initialized_storage_fails_closed() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("CURRENT"), "old").unwrap();
    assert!(bind_storage(&node(), root.path()).is_err());
    let partial = tempfile::tempdir().unwrap();
    std::fs::write(partial.path().join("operator-incarnation-v1.json"), "{").unwrap();
    assert!(bind_storage(&node(), partial.path()).is_err());
}
#[test]
fn mixed_generation_and_voting_learner_config_rejected() {
    let mut n = node();
    n.seeds.push(n.id.clone());
    assert!(n.validate().is_err());
    n = node();
    n.topology
        .endpoints
        .insert("old.a".into(), "127.0.0.1:9000".into());
    assert!(n.validate().is_err());
}
