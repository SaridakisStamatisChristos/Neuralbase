// SPDX-License-Identifier: Apache-2.0
//! Versioned Raft cluster-membership state and quorum rules.
//!
//! Phase 3 makes committed membership an explicit consensus state rather than
//! deriving quorum from process configuration. `ClusterMembership` is kept in
//! Raft durable state and uses deterministic ordered sets so serialization and
//! diagnostics are stable across nodes.

use std::collections::{BTreeSet, HashMap};

use serde::{Deserialize, Serialize};

use crate::consensus::rpc::NodeId;

pub const MEMBERSHIP_FORMAT_VERSION: u8 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JointConfig {
    pub old_voters: BTreeSet<NodeId>,
    pub new_voters: BTreeSet<NodeId>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterMembership {
    pub format_version: u8,
    pub generation: u64,
    pub config_index: u64,
    /// Stable voters when `joint` is None. During joint consensus this is the
    /// pre-transition stable set; quorum is instead evaluated against both
    /// sets carried by `joint`.
    pub voters: BTreeSet<NodeId>,
    pub learners: BTreeSet<NodeId>,
    pub joint: Option<JointConfig>,
    /// Node IDs are incarnation identities. Once removed they are tombstoned
    /// and may not be silently reused; replacement requires a new NodeId.
    pub removed: BTreeSet<NodeId>,
}

impl ClusterMembership {
    /// Initial fixed-membership bootstrap. This is the only path that turns the
    /// configured peer list into voters without an already-committed dynamic
    /// configuration.
    pub fn bootstrap(local_id: NodeId, peers: impl IntoIterator<Item = NodeId>) -> Self {
        let mut voters = BTreeSet::new();
        voters.insert(local_id);
        voters.extend(peers);
        Self {
            format_version: MEMBERSHIP_FORMAT_VERSION,
            generation: 1,
            config_index: 0,
            voters,
            learners: BTreeSet::new(),
            joint: None,
            removed: BTreeSet::new(),
        }
    }

    /// Safe non-authoritative seed view for a brand-new joining process. The
    /// local ID is intentionally absent from both voters and learners until the
    /// actual committed AddLearner entry or snapshot arrives. That prevents a
    /// fresh process from campaigning while also avoiding a fake local config
    /// that would collide when the real AddLearner entry is replayed.
    pub fn bootstrap_learner(
        local_id: NodeId,
        voters: impl IntoIterator<Item = NodeId>,
    ) -> Result<Self, String> {
        let voters: BTreeSet<_> = voters.into_iter().collect();
        if voters.is_empty() {
            return Err("joining learner requires at least one seed voter".to_string());
        }
        if voters.contains(&local_id) {
            return Err("joining learner id cannot also be a seed voter".to_string());
        }
        let config = Self {
            format_version: MEMBERSHIP_FORMAT_VERSION,
            generation: 1,
            config_index: 0,
            voters,
            learners: BTreeSet::new(),
            joint: None,
            removed: BTreeSet::new(),
        };
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), String> {
        if self.format_version != MEMBERSHIP_FORMAT_VERSION {
            return Err(format!(
                "unsupported membership format version {}",
                self.format_version
            ));
        }
        if self.generation == 0 {
            return Err("membership generation must be non-zero".to_string());
        }
        if self.voters.is_empty() {
            return Err("membership must contain at least one voter".to_string());
        }
        if !self.voters.is_disjoint(&self.learners) {
            return Err("a node cannot be both voter and learner".to_string());
        }
        if !self.voters.is_disjoint(&self.removed) || !self.learners.is_disjoint(&self.removed) {
            return Err("removed node cannot remain active in membership".to_string());
        }
        if let Some(joint) = &self.joint {
            if joint.old_voters.is_empty() || joint.new_voters.is_empty() {
                return Err("joint consensus voter sets must be non-empty".to_string());
            }
            if joint.old_voters != self.voters {
                return Err(
                    "joint old voter set must equal the preceding stable voter set".to_string(),
                );
            }
            if !joint.old_voters.is_disjoint(&self.removed)
                || !joint.new_voters.is_disjoint(&self.removed)
            {
                return Err("joint voter set contains a tombstoned node".to_string());
            }
            if joint
                .old_voters
                .union(&joint.new_voters)
                .any(|id| self.learners.contains(id))
            {
                return Err("joint voter cannot simultaneously remain a learner".to_string());
            }
        }
        Ok(())
    }

    pub fn is_joint(&self) -> bool {
        self.joint.is_some()
    }

    pub fn is_voter(&self, id: &NodeId) -> bool {
        match &self.joint {
            Some(joint) => joint.old_voters.contains(id) || joint.new_voters.contains(id),
            None => self.voters.contains(id),
        }
    }

    pub fn is_stable_voter(&self, id: &NodeId) -> bool {
        self.joint.is_none() && self.voters.contains(id)
    }

    pub fn is_learner(&self, id: &NodeId) -> bool {
        self.learners.contains(id)
    }

    pub fn is_removed(&self, id: &NodeId) -> bool {
        self.removed.contains(id)
    }

    pub fn replication_targets(&self) -> BTreeSet<NodeId> {
        let mut targets = self.learners.clone();
        match &self.joint {
            Some(joint) => {
                targets.extend(joint.old_voters.iter().cloned());
                targets.extend(joint.new_voters.iter().cloned());
            }
            None => targets.extend(self.voters.iter().cloned()),
        }
        targets
    }

    pub fn election_targets(&self) -> BTreeSet<NodeId> {
        match &self.joint {
            Some(joint) => joint.old_voters.union(&joint.new_voters).cloned().collect(),
            None => self.voters.clone(),
        }
    }

    fn majority(set: &BTreeSet<NodeId>) -> usize {
        set.len() / 2 + 1
    }

    fn votes_in(set: &BTreeSet<NodeId>, votes: &BTreeSet<NodeId>) -> usize {
        set.intersection(votes).count()
    }

    pub fn has_vote_quorum(&self, votes: &BTreeSet<NodeId>) -> bool {
        match &self.joint {
            Some(joint) => {
                Self::votes_in(&joint.old_voters, votes) >= Self::majority(&joint.old_voters)
                    && Self::votes_in(&joint.new_voters, votes) >= Self::majority(&joint.new_voters)
            }
            None => Self::votes_in(&self.voters, votes) >= Self::majority(&self.voters),
        }
    }

    pub fn has_match_quorum(
        &self,
        local_id: &NodeId,
        matches: &HashMap<NodeId, u64>,
        index: u64,
    ) -> bool {
        let mut replicated = BTreeSet::new();
        if self.is_voter(local_id) {
            replicated.insert(local_id.clone());
        }
        for (id, matched) in matches {
            if *matched >= index && self.is_voter(id) {
                replicated.insert(id.clone());
            }
        }
        self.has_vote_quorum(&replicated)
    }

    pub fn add_learner(&self, id: NodeId, index: u64) -> Result<Self, String> {
        self.require_stable()?;
        if self.is_removed(&id) {
            return Err(format!(
                "node id {id} was removed and cannot be reused; use a new incarnation id"
            ));
        }
        if self.voters.contains(&id) || self.learners.contains(&id) {
            return Err(format!("node id {id} is already present in membership"));
        }
        let mut next = self.clone();
        next.generation = self.next_generation()?;
        next.config_index = index;
        next.learners.insert(id);
        next.validate()?;
        Ok(next)
    }

    pub fn begin_promotion(&self, id: &NodeId, index: u64) -> Result<Self, String> {
        self.require_stable()?;
        if !self.learners.contains(id) {
            return Err(format!("node id {id} is not a learner"));
        }
        let old_voters = self.voters.clone();
        let mut new_voters = old_voters.clone();
        new_voters.insert(id.clone());
        let mut next = self.clone();
        next.generation = self.next_generation()?;
        next.config_index = index;
        next.learners.remove(id);
        next.joint = Some(JointConfig {
            old_voters,
            new_voters,
        });
        next.validate()?;
        Ok(next)
    }

    pub fn begin_removal(&self, id: &NodeId, index: u64) -> Result<Self, String> {
        self.require_stable()?;
        if self.learners.contains(id) {
            let mut next = self.clone();
            next.generation = self.next_generation()?;
            next.config_index = index;
            next.learners.remove(id);
            next.removed.insert(id.clone());
            next.validate()?;
            return Ok(next);
        }
        if !self.voters.contains(id) {
            return Err(format!("node id {id} is not an active member"));
        }
        if self.voters.len() <= 1 {
            return Err("cannot remove the last voter".to_string());
        }
        let old_voters = self.voters.clone();
        let mut new_voters = old_voters.clone();
        new_voters.remove(id);
        let mut next = self.clone();
        next.generation = self.next_generation()?;
        next.config_index = index;
        next.joint = Some(JointConfig {
            old_voters,
            new_voters,
        });
        next.validate()?;
        Ok(next)
    }

    pub fn finalize_joint(&self, index: u64) -> Result<Self, String> {
        let joint = self
            .joint
            .as_ref()
            .ok_or_else(|| "cannot finalize membership: not in joint consensus".to_string())?;
        let mut next = self.clone();
        next.generation = self.next_generation()?;
        next.config_index = index;
        next.voters = joint.new_voters.clone();
        for removed in joint.old_voters.difference(&joint.new_voters) {
            next.removed.insert(removed.clone());
        }
        next.joint = None;
        next.validate()?;
        Ok(next)
    }

    fn require_stable(&self) -> Result<(), String> {
        self.validate()?;
        if self.joint.is_some() {
            return Err("membership change already in joint consensus".to_string());
        }
        Ok(())
    }

    fn next_generation(&self) -> Result<u64, String> {
        self.generation
            .checked_add(1)
            .ok_or_else(|| "membership generation overflow".to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn three() -> ClusterMembership {
        ClusterMembership::bootstrap("n1".to_string(), ["n2".to_string(), "n3".to_string()])
    }

    #[test]
    fn bootstrap_is_canonical_and_majority_is_voter_only() {
        let cfg = three();
        cfg.validate().unwrap();
        let votes = BTreeSet::from(["n1".to_string(), "n2".to_string()]);
        assert!(cfg.has_vote_quorum(&votes));
        let one = BTreeSet::from(["n1".to_string()]);
        assert!(!cfg.has_vote_quorum(&one));
    }

    #[test]
    fn joining_seed_view_cannot_vote_or_claim_membership() {
        let cfg = ClusterMembership::bootstrap_learner(
            "n4".to_string(),
            ["n1".to_string(), "n2".to_string(), "n3".to_string()],
        )
        .unwrap();
        assert!(!cfg.is_learner(&"n4".to_string()));
        assert!(!cfg.is_voter(&"n4".to_string()));
    }

    #[test]
    fn learner_never_counts_toward_quorum() {
        let cfg = three().add_learner("n4".to_string(), 7).unwrap();
        assert!(cfg.is_learner(&"n4".to_string()));
        let votes = BTreeSet::from(["n1".to_string(), "n4".to_string()]);
        assert!(!cfg.has_vote_quorum(&votes));
    }

    #[test]
    fn promotion_enters_joint_and_requires_both_majorities() {
        let cfg = three().add_learner("n4".to_string(), 7).unwrap();
        let joint = cfg.begin_promotion(&"n4".to_string(), 8).unwrap();
        assert!(joint.is_joint());
        let only_old = BTreeSet::from(["n1".to_string(), "n2".to_string()]);
        assert!(!joint.has_vote_quorum(&only_old));
        let both = BTreeSet::from(["n1".to_string(), "n2".to_string(), "n4".to_string()]);
        assert!(joint.has_vote_quorum(&both));
        let final_cfg = joint.finalize_joint(9).unwrap();
        assert_eq!(final_cfg.voters.len(), 4);
        assert!(!final_cfg.is_learner(&"n4".to_string()));
    }

    #[test]
    fn removal_tombstones_identity_after_joint_finalize() {
        let joint = three().begin_removal(&"n3".to_string(), 10).unwrap();
        let final_cfg = joint.finalize_joint(11).unwrap();
        assert!(!final_cfg.is_voter(&"n3".to_string()));
        assert!(final_cfg.is_removed(&"n3".to_string()));
        let err = final_cfg.add_learner("n3".to_string(), 12).unwrap_err();
        assert!(err.contains("cannot be reused"));
    }

    #[test]
    fn match_quorum_uses_joint_rules() {
        let joint = three()
            .add_learner("n4".to_string(), 4)
            .unwrap()
            .begin_promotion(&"n4".to_string(), 5)
            .unwrap();
        let mut matches = HashMap::new();
        matches.insert("n2".to_string(), 5);
        assert!(!joint.has_match_quorum(&"n1".to_string(), &matches, 5));
        matches.insert("n4".to_string(), 5);
        assert!(joint.has_match_quorum(&"n1".to_string(), &matches, 5));
    }
}
