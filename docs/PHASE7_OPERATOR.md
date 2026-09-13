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

## Current implementation and supported profile

`src/operator.rs` defines the v1 desired topology, observation and plan. The
`neuralbase-operator plan` binary parses bounded JSON and emits exactly one next
action. `src/consensus/operator_control.rs` obtains a current-term quorum/apply
barrier and submits term/generation-guarded membership commands through the
serialized Raft event loop. The committed membership view is distinct from the
existing effective configuration. The original persistence and membership codecs
remain unchanged. Managed learners preserve the cluster's original genesis voter
set for full log replay, separately from current routing seeds. Routing seeds
only admit replication to a non-voting, non-removed learner; they never grant
votes or replace committed membership. This allows a newly promoted leader to
bootstrap a later replacement without replaying history against the wrong set.

`ops/neuralbase_operator.py` supplies a Linux local-process deployment adapter.
It manages independent server processes, TCP Raft, SQL listeners and RocksDB
folders using the same planner. The controller is explicitly invoked; it is not
an installed daemon. The managed profile is separate from the existing static
Compose/Helm/Kubernetes examples. The opt-in Kubernetes adapter described below is separate from those static
examples. HPA remains unsupported.

Requirements: Linux with `pidfd_open`/`pidfd_send_signal`, Python 3.9 or later,
Rust 1.88 builds of both server and planner, and one trusted local user. Raft and
SQL bind to unique loopback ports. The development adapter uses plaintext. SQL authentication defaults to disabled;
the explicit identity bootstrap selection below enables required SCRAM authentication.
The adapter does not configure TLS or production authorization. Prometheus currently binds the selected metrics port
on all interfaces, as in the existing server. Protect that listener at the host.

The root directory must be private (0700). The server's management socket is
private (0600). The supervisor verifies Unix peer credentials and the complete
managed configuration. Retirement pins the peer with a Linux process descriptor
before a fresh identity exchange, so a saved or reused numeric PID cannot
authorize a signal. Persisted PIDs are diagnostic. The same user controls all
processes and files; this is not a boundary against a malicious local owner.

## Running a managed local cluster

From the repository root:

```sh
cargo build --locked --bins
cp ops/operator.example.json /tmp/neuralbase-desired.json
python3 ops/neuralbase_operator.py bootstrap \
  --config /tmp/neuralbase-desired.json \
  --server target/debug/neuralbase --planner target/debug/neuralbase-operator
python3 ops/neuralbase_operator.py reconcile --steps 100 \
  --config /tmp/neuralbase-desired.json \
  --server target/debug/neuralbase --planner target/debug/neuralbase-operator
python3 ops/neuralbase_operator.py plan \
  --config /tmp/neuralbase-desired.json \
  --server target/debug/neuralbase --planner target/debug/neuralbase-operator
```

The example reserves five stable identities and starts three voters. To expand,
increment `desired.revision` and add `demo1.d` to `desired.voters`. To replace a
member, increment the revision, add unused `demo1.e`, and remove the old member
from the desired voter list. Run reconciliation again. Its plan promotes the
replacement before removing the old member, transfers leadership when needed,
and retires a process only after finalized committed removal. Storage is retained.

The full endpoint/process inventory, cluster incarnation and voter floor are
immutable. Voter list order is irrelevant; duplicate identities are rejected.
Use fresh IDs for replacement. Tombstoned IDs cannot return to the desired set.
Keep the original per-node configuration and durable controller root across
controller/server restarts. Do not edit the state or relabel a database folder.
Bootstrap is only for a fresh revision-1 topology; repeating it does not resurrect
removed initial members.

`plan` reads live authority and returns its observation and proposed action. It
does not persist accepted revisions, create/stop processes, or change membership.
Obtaining authority appends a read barrier to Raft. For a completely offline
calculation, feed a previously captured `{desired, observed}` document into
`neuralbase-operator plan`; the result is advisory and is revalidated at execution.

## Required SQL authentication

Before the first bootstrap, optionally add an `identity_bootstrap` object with
`path` (an absolute private verifier file, mode 0600) and `sha256` (its exact
lowercase SHA-256 digest) to the controller configuration. The file must contain
valid existing strict SCRAM registry JSON and be at most 128 KiB. No password or
verifier belongs in the desired topology or controller state. The file selection
and digest are immutable for that controller root; preserve the source across
restarts. A digest mismatch fails before member creation.

This enables `NEURALBASE_AUTH_REQUIRED=1` and the existing Phase-4 explicit identity
migration. Connect once to the current leader with a bootstrap credential to
commit initialization; followers fail closed while initialization is pending.
Subsequent user changes use the replicated identity store, which remains
authoritative across replacement and restart. The process adapter creates private
per-node bootstrap copies. Kubernetes creates retained immutable Secrets mounted
read-only in member pods; the controller Role therefore includes Secret read/create.
Treat that principal and namespace as trusted. No Secret data is included in
controller status. Both executable deployment lifecycles select this profile and
check that correct passwords work and incorrect passwords are rejected.

## Failure handling and bounds

The controller serializes writers with `flock`, saves intent before process
creation, and atomically fsyncs its state. Each execution observes and replans
again. A crash or timeout may have an uncertain outcome; the next invocation
reconciles the committed configuration rather than trusting the remembered last
action. Failed creation leaves storage/configuration for safe same-identity retry.
Removed storage stays retained; automatic adoption of unmanaged or Phase-5 restored
storage is rejected. Phase-5 disaster recovery remains a separate deliberate
fresh-topology procedure.

Blocked plans explain absent authority, incomplete joint consensus, insufficient
catch-up, unsafe removal, stale revisions or identity conflicts. Fix the reported
condition and reobserve. A lost quorum requires deliberate operator recovery;
automation does not manufacture authority by rewriting membership. Inspect the
private per-node logs and `status` for attempts, failures, blocked decisions,
completed actions and the last successful plan. These counters are diagnostic,
not consensus evidence.

Limits: 128 reserved nodes, 256 KiB documents/responses, 8 KiB management requests,
16 pending control requests, five-second consensus/control deadlines, a
twelve-second server request deadline, eight concurrent status queries, at most
1000 CLI steps and thirty-second maximum retry delay. Each step performs one
action. Ordinary node logs need host retention management.

## Executable evidence

- `phase7_planner`: deterministic no-op, 3→4→3, stale guards/revisions, quorum and
  catch-up blocks, leader replacement, desired changes during joint consensus,
  withdrawn creation and bounded retry/restart state.
- `phase7_guarded_membership`: actual Raft admission/promotion/removal, learner
  restart, leader loss during catch-up, isolated stale leader, and partitions at
  durable committed-joint promotion and removal boundaries.
- `phase4_identity_membership::phase7_guarded_learner_snapshot_bootstrap_preserves_identity`:
  compacted identity state reaches a fresh learner through snapshot bootstrap,
  then survives guarded promotion, credential rotation and removal.
- `phase7_managed_storage`: immutable incarnation binding, restart and rejection
  of old, unmanaged, restored or partially initialized storage.
- `phase7_process`: independent controller invocations and real server processes,
  failed listener allocation, scale-out/in, leader restart/replacement, retained
  storage, and replicated SQL/SCRAM convergence. Its supervisor checks validate
  process identity before retirement and document bounds.

The phase remains open until the final PR head and post-merge main gates pass;
the Kubernetes gate is required in addition to local process evidence.

## Managed Kubernetes profile

`ops/neuralbase_kubernetes.py` drives the same planner and guarded admin commands
through an explicit `kubectl` context and namespace. Each incarnation receives an
immutable ConfigMap, a headless Raft discovery Service, a readiness-filtered SQL
Service, a PVC, and a **one-replica** StatefulSet. These are new managed objects;
existing Helm/Compose deployments are not adopted. The image must contain both
`/app/neuralbase` and `/app/neuralbase-operator`, as the updated Dockerfile does.
The Python controller and `kubectl` run outside member pods.

Use `ops/operator.kubernetes.example.json` as a starting point. Select the explicit
context, dedicated namespace, available storage class and locally built image,
then run the same `bootstrap`, `plan`, `reconcile` and `status` commands with that
configuration. The `--server` argument remains a required local build path for
CLI compatibility; the Kubernetes adapter runs the image's `/app/neuralbase`.
The desired inventory must use the generated stable pod DNS names and ports.
Do not change adapter settings, image or storage class inside an established
controller root; automated rolling upgrades are outside this phase.

The root holds a unique ownership token and observed object UIDs. Keep it on a
durable filesystem with working `flock` and atomic rename/fsync semantics. Run
exactly one controller root per managed cluster. A new root refuses to adopt
objects belonging to an old controller. The adapter compares managed fields,
rejects external template/configuration changes, and uses JSON Patch tests on
object UID and Kubernetes `resourceVersion` before changing replicas. It never
uses a raw replica count as a voter count. The only managed StatefulSet replica
values are zero and one; expansion creates a fresh member StatefulSet.

Member pods run as an unprivileged UID, drop capabilities, do not mount Kubernetes
API credentials, and use a private Unix socket for administration. Install
`ops/operator-rbac.yaml` in the dedicated namespace and bind that Role to the
chosen external controller principal. Its permissions create/read member objects,
exec the guarded admin binary and patch StatefulSets. It has no PVC deletion
permission. Namespace/principal setup is an explicit administrator operation.
This development profile uses plaintext Raft and optional SQL authentication; it
requires an isolated, trusted test namespace/network and does not claim production
security or multi-tenant controller isolation.

Readiness executes `neuralbase-operator ready /config/node.json` and requires a
serving, finalized voter. Learners remain discoverable for Raft through the
headless Service while the SQL Service excludes them. Reconciliation observes
initializing/unreachable deployment objects separately from ready/applied members.
A committed tombstone must precede scaling the retired StatefulSet to zero;
reconciliation waits for its pod to disappear. The StatefulSet, ConfigMap and PVC
remain retained. PVC quota failures and partial object creation preserve the
original intent for idempotent retry. Changing the desired input during a running
command invalidates further actions; restart reconciliation with the new revision.

The CI gate creates a disposable kind cluster and runs
`tests/phase7_kubernetes.py`: 3→4 expansion, PVC-quota partial failure, external
configuration drift, leader pod loss, fresh-identity leader replacement, 4→3
contraction, retained PVCs and replicated SQL/SCRAM checks. Native process and
consensus tests remain separate gates. HPA and the static Helm scaling contract
remain disabled; only explicit desired-topology reconciliation is supported.
