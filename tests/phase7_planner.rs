// SPDX-License-Identifier: Apache-2.0
use neuralbase::consensus::ClusterMembership;
use neuralbase::operator::*;
use std::collections::{BTreeMap, BTreeSet};
fn set(ids: &[&str]) -> BTreeSet<String> {
    ids.iter().map(|s| s.to_string()).collect()
}
fn fixture() -> (DesiredTopology, Observation) {
    let endpoints: BTreeMap<_, _> = ["a", "b", "c", "d", "e"]
        .into_iter()
        .map(|s| (s.into(), format!("{s}:7001")))
        .collect();
    let d = DesiredTopology {
        version: 1,
        cluster: "g1".into(),
        revision: 1,
        endpoints: endpoints.clone(),
        voters: set(&["a", "b", "c"]),
        minimum_voters: 3,
    };
    let o = Observation {
        cluster: "g1".into(),
        accepted_revision: 1,
        leader: "a".into(),
        term: 2,
        authority_index: 10,
        committed: ClusterMembership::bootstrap("a".into(), ["b".into(), "c".into()]),
        transition_pending: false,
        processes: ["a", "b", "c"]
            .into_iter()
            .map(|id| {
                (
                    id.into(),
                    ProcessObservation {
                        cluster: "g1".into(),
                        endpoint: endpoints[id].clone(),
                        learner_bootstrap: false,
                        ready: true,
                        applied: 10,
                    },
                )
            })
            .collect(),
        matched: ["a", "b", "c"]
            .into_iter()
            .map(|id| (id.into(), 10))
            .collect(),
    };
    (d, o)
}
fn add(d: &DesiredTopology, o: &mut Observation, id: &str) {
    o.processes.insert(
        id.into(),
        ProcessObservation {
            cluster: d.cluster.clone(),
            endpoint: d.endpoints[id].clone(),
            learner_bootstrap: true,
            ready: true,
            applied: 10,
        },
    );
    o.matched.insert(id.into(), 10);
}
fn action(d: &DesiredTopology, o: &Observation) -> Action {
    reconcile(d, o).unwrap().action
}
fn blocked(d: &DesiredTopology, o: &Observation) {
    assert!(matches!(action(d, o), Action::Blocked(_)));
}
#[test]
fn noop_determinism_restart() {
    let (d, o) = fixture();
    let p = reconcile(&d, &o).unwrap();
    assert_eq!(p.action, Action::Converged);
    for _ in 0..10 {
        let restored = serde_json::from_slice(&serde_json::to_vec(&o).unwrap()).unwrap();
        assert_eq!(p, reconcile(&d, &restored).unwrap());
        validate_plan(&p, &d, &restored).unwrap();
    }
}
#[test]
fn three_four_three_committed_boundaries() {
    let (mut d, mut o) = fixture();
    d.voters.insert("d".into());
    assert_eq!(action(&d, &o), Action::CreateLearner("d".into()));
    add(&d, &mut o, "d");
    assert_eq!(action(&d, &o), Action::AddLearner("d".into()));
    o.committed = o.committed.add_learner("d".into(), 5).unwrap();
    o.processes.get_mut("d").unwrap().applied = 0;
    blocked(&d, &o);
    o.processes.get_mut("d").unwrap().applied = 10;
    assert_eq!(action(&d, &o), Action::PromoteLearner("d".into()));
    o.committed = o.committed.begin_promotion(&"d".into(), 6).unwrap();
    blocked(&d, &o);
    o.committed = o.committed.finalize_joint(7).unwrap();
    assert_eq!(action(&d, &o), Action::Converged);
    d.voters.remove("d");
    assert_eq!(action(&d, &o), Action::RemoveMember("d".into()));
    o.committed = o.committed.begin_removal(&"d".into(), 8).unwrap();
    blocked(&d, &o);
    o.committed = o.committed.finalize_joint(9).unwrap();
    assert_eq!(action(&d, &o), Action::StopRemoved("d".into()));
    o.processes.remove("d");
    assert_eq!(action(&d, &o), Action::Converged);
    d.voters.insert("d".into());
    blocked(&d, &o);
}
#[test]
fn replacement_before_leader_removal() {
    let (mut d, mut o) = fixture();
    d.voters = set(&["b", "c", "d"]);
    assert_eq!(action(&d, &o), Action::CreateLearner("d".into()));
    add(&d, &mut o, "d");
    o.committed = o
        .committed
        .add_learner("d".into(), 4)
        .unwrap()
        .begin_promotion(&"d".into(), 5)
        .unwrap()
        .finalize_joint(6)
        .unwrap();
    for _ in 0..3 {
        assert_eq!(action(&d, &o), Action::TransferLeadership("b".into()));
    }
    o.leader = "b".into();
    o.term += 1;
    assert_eq!(action(&d, &o), Action::RemoveMember("a".into()));
}
#[test]
fn stale_guards_and_revisions() {
    let (mut d, mut o) = fixture();
    d.voters.insert("d".into());
    let p = reconcile(&d, &o).unwrap();
    o.term += 1;
    assert!(validate_plan(&p, &d, &o).is_err());
    o.accepted_revision += 1;
    assert!(reconcile(&d, &o).is_err());
    o.accepted_revision = d.revision;
    o.cluster = "restored".into();
    assert!(reconcile(&d, &o).is_err());
}
#[test]
fn partitions_unreachable_and_lagging_learners() {
    let (mut d, mut o) = fixture();
    d.voters.insert("d".into());
    o.committed = o.committed.add_learner("d".into(), 5).unwrap();
    assert_eq!(action(&d, &o), Action::RestartMember("d".into()));
    add(&d, &mut o, "d");
    o.matched.insert("d".into(), 0);
    blocked(&d, &o);
    o.processes.remove("b");
    o.processes.remove("c");
    blocked(&d, &o);
    o.authority_index = 0;
    blocked(&d, &o);
}
#[test]
fn desired_changes_during_joint_and_uncommitted_removal() {
    let (mut d, mut o) = fixture();
    o.committed = o
        .committed
        .add_learner("d".into(), 4)
        .unwrap()
        .begin_promotion(&"d".into(), 5)
        .unwrap();
    d.revision = 2;
    o.accepted_revision = 2;
    blocked(&d, &o);
    o.committed = o.committed.finalize_joint(6).unwrap();
    assert_eq!(action(&d, &o), Action::RemoveMember("d".into()));
    o.transition_pending = true;
    blocked(&d, &o);
    o.transition_pending = false;
    o.committed.config_index = 11;
    blocked(&d, &o);
}
#[test]
fn invalid_inventory_and_process_reuse() {
    let (mut d, mut o) = fixture();
    d.endpoints.insert("d".into(), d.endpoints["a"].clone());
    assert!(reconcile(&d, &o).is_err());
    d.endpoints.insert("d".into(), "d:7001".into());
    d.voters.insert("d".into());
    add(&d, &mut o, "d");
    o.processes.get_mut("d").unwrap().learner_bootstrap = false;
    blocked(&d, &o);
    o.processes.get_mut("d").unwrap().endpoint = "bad:7001".into();
    blocked(&d, &o);
}
#[test]
fn withdrawn_creation_obtains_tombstone_before_retirement() {
    let (d, mut o) = fixture();
    add(&d, &mut o, "d");
    assert_eq!(action(&d, &o), Action::AddLearner("d".into()));
    o.committed = o.committed.add_learner("d".into(), 5).unwrap();
    assert_eq!(action(&d, &o), Action::RemoveMember("d".into()));
}
#[test]
fn retry_bounded_and_restart_safe() {
    let mut r = RetryState::default();
    for _ in 0..1000 {
        r.failed(100);
        assert!(r.retry_after_ms <= 30100);
    }
    let restored: RetryState = serde_json::from_str(&serde_json::to_string(&r).unwrap()).unwrap();
    assert_eq!(r, restored);
    r.failed(u64::MAX);
    assert_eq!(r.retry_after_ms, u64::MAX);
    r.reset();
    assert_eq!(r, RetryState::default());
}

#[test]
fn quorum_alone_does_not_report_all_desired_processes_converged() {
    let (d, mut o) = fixture();
    o.processes.get_mut("c").unwrap().ready = false;
    assert!(
        matches!(reconcile(&d, &o).unwrap().action, Action::Blocked(reason) if reason.contains("all confirmed readiness"))
    );
}
