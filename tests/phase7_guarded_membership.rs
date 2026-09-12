// SPDX-License-Identifier: Apache-2.0
use neuralbase::consensus::operator_control::{
    GuardedMembership, MembershipGuard, OperatorHandle, OperatorStatus,
};
use neuralbase::consensus::{
    ChannelTransport, MemPersistenceStore, MembershipChange, RaftNode, RaftTaskHandle,
};
use std::sync::Arc;
use std::time::Duration;
async fn authority(handles: &[OperatorHandle]) -> (usize, OperatorStatus) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        for (i, h) in handles.iter().enumerate() {
            if h.status().await.is_ok_and(|s| s.is_leader && s.ready) {
                if let Ok(s) = h.observe_authoritative().await {
                    return (i, s);
                }
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "no quorum leader");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
fn guard(s: &OperatorStatus) -> MembershipGuard {
    MembershipGuard {
        leader: s.id.clone(),
        term: s.term,
        generation: s.committed.generation,
    }
}
#[tokio::test]
async fn guarded_admission_learner_restart_leader_loss_promotion_transfer_removal() {
    let bus = ChannelTransport::new_bus();
    let ids = ["a", "b", "c", "d"];
    let mut tasks: Vec<Option<RaftTaskHandle>> = Vec::new();
    let mut handles = Vec::new();
    let stores: Vec<_> = (0..4)
        .map(|_| Arc::new(MemPersistenceStore::new()))
        .collect();
    for (i, id) in ids.iter().enumerate() {
        let t = Arc::new(ChannelTransport::register(id.to_string(), bus.clone()).await);
        let seeds = ids[..3]
            .iter()
            .filter(|v| *v != id)
            .map(|v| v.to_string())
            .collect();
        let mut node = if i == 3 {
            RaftNode::new_learner(id.to_string(), seeds, t).unwrap()
        } else {
            RaftNode::new(id.to_string(), seeds, t)
        }
        .with_persistence(stores[i].clone());
        node.set_election_timeout_ms(80);
        let (_, _, task) = node.spawn();
        handles.push(task.operator_handle());
        tasks.push(Some(task));
    }
    let (leader, before) = authority(&handles[..3]).await;
    let stale = guard(&before);
    handles[leader]
        .change_membership(GuardedMembership {
            guard: stale.clone(),
            change: MembershipChange::AddLearner("d".into()),
        })
        .await
        .unwrap();
    assert!(handles[leader]
        .change_membership(GuardedMembership {
            guard: stale,
            change: MembershipChange::AddLearner("e".into())
        })
        .await
        .unwrap_err()
        .contains("stale"));
    tasks[3].take().unwrap().shutdown().await;
    let t = Arc::new(ChannelTransport::register("d".into(), bus.clone()).await);
    let node = RaftNode::new_learner(
        "d".into(),
        ids[..3].iter().map(|s| s.to_string()).collect(),
        t,
    )
    .unwrap()
    .with_persistence(stores[3].clone());
    let (_, _, task) = node.spawn();
    handles[3] = task.operator_handle();
    tasks[3] = Some(task);
    // Lose the serving leader during learner catch-up. New leader observes
    // committed admission and finishes using a fresh guard.
    tasks[leader].take().unwrap().shutdown().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let (i, s) = authority(&handles).await;
        match handles[i]
            .change_membership(GuardedMembership {
                guard: guard(&s),
                change: MembershipChange::PromoteLearner("d".into()),
            })
            .await
        {
            Ok(_) => break,
            Err(e) => {
                assert!(tokio::time::Instant::now() < deadline, "{e}");
                tokio::time::sleep(Duration::from_millis(30)).await;
            }
        }
    }
    let (i, s) = authority(&handles).await;
    assert_eq!(s.committed.voters.len(), 4);
    assert!(!s.committed.is_joint());
    assert!(handles[i]
        .change_membership(GuardedMembership {
            guard: guard(&s),
            change: MembershipChange::RemoveNode(s.id.clone())
        })
        .await
        .is_err());
    assert!(handles[i]
        .transfer(guard(&s), "missing".into())
        .await
        .is_err());
    let target = ids
        .iter()
        .enumerate()
        .find(|(j, id)| *j != leader && **id != s.id)
        .unwrap()
        .1
        .to_string();
    let old_leader = s.id;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(8);
    loop {
        let (_, s) = authority(&handles).await;
        if handles[i].transfer(guard(&s), target.clone()).await.is_ok() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
    loop {
        let (i, s) = authority(&handles).await;
        if s.id != old_leader {
            handles[i]
                .change_membership(GuardedMembership {
                    guard: guard(&s),
                    change: MembershipChange::RemoveNode(old_leader.clone()),
                })
                .await
                .unwrap();
            let (_, final_state) = authority(&handles).await;
            assert_eq!(final_state.committed.voters.len(), 3);
            assert!(final_state.committed.removed.contains(&old_leader));
            break;
        }
        assert!(tokio::time::Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(30)).await;
    }
}
#[tokio::test]
async fn isolated_former_leader_cannot_supply_authority() {
    let bus = ChannelTransport::new_bus();
    let mut tasks = Vec::new();
    let mut handles = Vec::new();
    for id in ["x", "y", "z"] {
        let t = Arc::new(ChannelTransport::register(id.into(), bus.clone()).await);
        let mut node = RaftNode::new(
            id.into(),
            ["x", "y", "z"]
                .into_iter()
                .filter(|p| *p != id)
                .map(str::to_string)
                .collect(),
            t,
        );
        node.set_election_timeout_ms(80);
        let (_, _, task) = node.spawn();
        handles.push(task.operator_handle());
        tasks.push(task);
    }
    let (i, _) = authority(&handles).await;
    bus.lock().await.clear();
    assert!(handles[i].observe_authoritative().await.is_err());
}
