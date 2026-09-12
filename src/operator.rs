// SPDX-License-Identifier: Apache-2.0
//! Pure, bounded reconciliation. A plan is never execution authority: reobserve
//! before applying it, then check Raft guards in the serialized event loop.
use crate::consensus::{ClusterMembership, MembershipChange};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
pub const OPERATOR_VERSION: u8 = 1;
pub const MAX_OPERATOR_NODES: usize = 128;
pub const MAX_OPERATOR_INPUT: usize = 256 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct DesiredTopology {
    pub version: u8,
    /// Deployment incarnation, distinct from Raft membership generation.
    pub cluster: String,
    pub revision: u64,
    /// Immutable incarnation inventory. Stable DNS may resolve to new IPs.
    pub endpoints: BTreeMap<String, String>,
    pub voters: BTreeSet<String>,
    pub minimum_voters: usize,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProcessObservation {
    pub cluster: String,
    pub endpoint: String,
    pub learner_bootstrap: bool,
    pub ready: bool,
    pub applied: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Observation {
    pub cluster: String,
    pub accepted_revision: u64,
    pub leader: String,
    pub term: u64,
    /// Current-term quorum commit plus confirmed durable local apply frontier.
    pub authority_index: u64,
    pub committed: ClusterMembership,
    pub transition_pending: bool,
    pub processes: BTreeMap<String, ProcessObservation>,
    pub matched: BTreeMap<String, u64>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Action {
    Converged,
    Blocked(String),
    CreateLearner(String),
    RestartMember(String),
    AddLearner(String),
    PromoteLearner(String),
    TransferLeadership(String),
    RemoveMember(String),
    /// Process retirement only; storage is always retained.
    StopRemoved(String),
}
impl Action {
    pub fn membership_change(&self) -> Option<MembershipChange> {
        match self {
            Self::AddLearner(id) => Some(MembershipChange::AddLearner(id.clone())),
            Self::PromoteLearner(id) => Some(MembershipChange::PromoteLearner(id.clone())),
            Self::RemoveMember(id) => Some(MembershipChange::RemoveNode(id.clone())),
            _ => None,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct Plan {
    pub version: u8,
    pub cluster: String,
    pub revision: u64,
    pub leader: String,
    pub term: u64,
    pub membership_generation: u64,
    pub action: Action,
}
pub fn valid_token(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|c| c.is_ascii_alphanumeric() || b"-_.".contains(&c))
}
impl DesiredTopology {
    pub fn validate(&self) -> Result<(), String> {
        if self.version != OPERATOR_VERSION || self.revision == 0 || !valid_token(&self.cluster) {
            return Err("invalid operator version, incarnation or revision".into());
        }
        if self.endpoints.is_empty() || self.endpoints.len() > MAX_OPERATOR_NODES {
            return Err("inventory must contain 1..=128 nodes".into());
        }
        if self.minimum_voters == 0 || self.voters.len() < self.minimum_voters {
            return Err("desired topology violates voter floor".into());
        }
        let mut addresses = BTreeSet::new();
        for (id, address) in &self.endpoints {
            if !valid_token(id)
                || address.is_empty()
                || address.len() > 253
                || address
                    .bytes()
                    .any(|c| c.is_ascii_whitespace() || c.is_ascii_control())
                || !addresses.insert(address)
            {
                return Err("invalid/duplicate stable identity or endpoint".into());
            }
        }
        if self
            .voters
            .iter()
            .any(|id| !self.endpoints.contains_key(id))
        {
            return Err("desired voter missing from inventory".into());
        }
        Ok(())
    }
}
/// One next action, lexicographically selected. Replacement promotes before
/// removing the old member; joint transitions always finish first.
pub fn reconcile(d: &DesiredTopology, o: &Observation) -> Result<Plan, String> {
    d.validate()?;
    o.committed.validate()?;
    if o.cluster != d.cluster || o.accepted_revision != d.revision {
        return Err("stale cluster incarnation or desired revision".into());
    }
    let m = &o.committed;
    if o.processes.len() > MAX_OPERATOR_NODES
        || o.matched.len() > MAX_OPERATOR_NODES
        || m.replication_targets().len() > MAX_OPERATOR_NODES
    {
        return Err("observation exceeds operator bounds".into());
    }
    let make = |action| Plan {
        version: OPERATOR_VERSION,
        cluster: d.cluster.clone(),
        revision: d.revision,
        leader: o.leader.clone(),
        term: o.term,
        membership_generation: m.generation,
        action,
    };
    let blocked = |reason: &str| Ok(make(Action::Blocked(reason.into())));
    if o.authority_index == 0
        || o.term == 0
        || !m.is_voter(&o.leader)
        || m.config_index > o.authority_index
    {
        return blocked("current leader quorum/apply authority unavailable");
    }
    if o.transition_pending || m.is_joint() {
        return blocked("Raft must finish the outstanding membership transition");
    }
    for (id, p) in &o.processes {
        if p.cluster != d.cluster || d.endpoints.get(id) != Some(&p.endpoint) {
            return blocked("process incarnation or endpoint conflicts with inventory");
        }
    }
    if m.replication_targets()
        .iter()
        .any(|id| !d.endpoints.contains_key(id))
    {
        return blocked("manual membership includes unmanaged identity");
    }
    if !d.voters.is_disjoint(&m.removed) {
        return blocked("desired topology reuses a tombstoned incarnation");
    }
    let ready: BTreeSet<_> = o
        .processes
        .iter()
        .filter(|(_, p)| p.ready && p.applied >= o.authority_index)
        .map(|(id, _)| id.clone())
        .collect();
    if !m.has_vote_quorum(&ready) {
        return blocked("insufficient confirmed applied voters for current quorum");
    }
    for id in d.voters.intersection(&m.voters) {
        if !o.processes.contains_key(id) {
            return Ok(make(Action::RestartMember(id.clone())));
        }
    }
    for id in d.voters.difference(&m.voters) {
        let Some(p) = o.processes.get(id) else {
            return Ok(make(if m.learners.contains(id) {
                Action::RestartMember(id.clone())
            } else {
                Action::CreateLearner(id.clone())
            }));
        };
        if !m.learners.contains(id) {
            if !p.learner_bootstrap {
                return blocked("unadmitted process did not start non-voting");
            }
            return Ok(make(Action::AddLearner(id.clone())));
        }
        if !ready.contains(id) || o.matched.get(id).copied().unwrap_or(0) < o.authority_index {
            return blocked("learner has not confirmed snapshot/log catch-up");
        }
        let mut next = m.voters.clone();
        next.insert(id.clone());
        if next.intersection(&ready).count() < next.len() / 2 + 1 {
            return blocked("promotion lacks new voter quorum");
        }
        return Ok(make(Action::PromoteLearner(id.clone())));
    }
    for id in m.replication_targets().difference(&d.voters) {
        if m.voters.contains(id) {
            let next: BTreeSet<_> = m.voters.iter().filter(|v| *v != id).cloned().collect();
            if next.len() < d.minimum_voters
                || next.intersection(&ready).count() < next.len() / 2 + 1
            {
                return blocked("removal violates voter floor or surviving quorum");
            }
            if id == &o.leader {
                return match next
                    .intersection(&ready)
                    .find(|id| o.matched.get(*id).copied().unwrap_or(0) >= o.authority_index)
                {
                    Some(target) => Ok(make(Action::TransferLeadership(target.clone()))),
                    None => blocked("no caught-up surviving voter for transfer"),
                };
            }
        }
        return Ok(make(Action::RemoveMember(id.clone())));
    }
    for id in &m.removed {
        if o.processes.contains_key(id) {
            return Ok(make(Action::StopRemoved(id.clone())));
        }
    }
    // A withdrawn creation must be admitted then removed to obtain a durable
    // tombstone. Never silently leave an untracked process or delete a possible
    // in-flight learner based on an in-memory completion flag.
    for (id, p) in &o.processes {
        if !d.voters.contains(id)
            && !m.replication_targets().contains(id)
            && !m.removed.contains(id)
        {
            if !p.learner_bootstrap {
                return blocked("unmanaged bootstrap process outside desired topology");
            }
            return Ok(make(Action::AddLearner(id.clone())));
        }
    }
    Ok(make(Action::Converged))
}
pub fn validate_plan(plan: &Plan, d: &DesiredTopology, o: &Observation) -> Result<(), String> {
    if &reconcile(d, o)? != plan {
        return Err("stale reconciliation action; reobserve".into());
    }
    Ok(())
}
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RetryState {
    pub failures: u32,
    pub retry_after_ms: u64,
}
impl RetryState {
    pub fn failed(&mut self, now_ms: u64) {
        self.failures = self.failures.saturating_add(1);
        self.retry_after_ms =
            now_ms.saturating_add((100_u64 * (1_u64 << self.failures.min(9))).min(30_000));
    }
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}
