# Phase 7 operator membership orchestration

Status: implementation in progress; no automatic deployment capability is yet claimed.
Baseline: `4c2b29681a75ce5ee683b2d95546ef8c4afbc2ab` (PR #12, post-merge CI #305 passed all jobs).

## Audited boundary

The Phase-3 membership state machine already admits learners, validates catch-up,
promotes/removes through joint consensus, finalizes on the serving leader and
persists removed incarnation IDs. Its effective configuration can include an
uncommitted log entry; this is correct for consensus but insufficient to authorize
deployment deletion. The existing shared status exposes that effective view.

The server entry point always constructs `RaftNode::new`, even though the library
provides `new_learner`. TCP/TLS transports map stable IDs to addresses from a fixed
startup map. Helm's ordinal peer list bootstraps a fixed topology; changing replicas
does not invoke any membership operation. SQL TCP probes prove neither catch-up
nor voter membership. Phase-5 restore creates a deliberate new recovery topology.

## Reconciliation invariants

1. Desired topology is a versioned document with an explicit cluster incarnation,
   monotonic revision and stable node IDs; process existence is separate from
   committed voters, learners, joint configuration and tombstones.
2. A deterministic planner emits at most one next action. Repeating a plan does
   not execute it. Every execution reobserves authoritative state and rejects
   stale membership, leadership, deployment or desired-state revisions.
3. A new process starts non-voting, is admitted as learner, catches up through
   the existing log/snapshot path, and becomes active only after finalized
   committed promotion. Readiness alone never establishes membership.
4. Replacement adds and promotes a fresh incarnation before removing the old
   member. Tombstoned IDs and old cluster generations cannot be reused.
5. Remove followers through committed consensus; transfer and observe a new
   leader before removing a leader. Deployment deletion requires a finalized
   committed tombstone, never an effective/uncommitted removal.
6. A quorum authority barrier precedes actionable observation. Membership intents
   additionally check their preconditions inside the serialized Raft event loop.
   Minority partitions and stale leaders cannot authorize automation.
7. Incomplete joint transitions block new topology changes until Raft finalizes.
   On controller restart, committed state overrides remembered progress.
8. Controller retries, input sizes, pending actions and deadlines are bounded.
   Timeouts have uncertain outcomes and require observation before retry.
9. Endpoint changes must preserve stable incarnation identity; replacements use
   fresh IDs and storage. Existing static deployment remains explicitly static.
10. HPA remains disabled, `production_ready` remains false. Phase 7 is not closed
    until process/failure evidence and exact PR/post-merge CI satisfy the handoff.

## Evidence checklist

Planner: no-op, deterministic selection, scale-out/in, leader transfer, stale
revisions, impossible transitions, restart replay and desired changes mid-joint.
Integration: guarded real Raft membership, catch-up, snapshot bootstrap, failed
transfer, partitions, leader loss, replacement, SQL/identity convergence.
Deployment: real independent processes driven through the orchestration surface;
partial process/deployment failures and restart recovery.
