# Managed membership reconciliation

Status: implemented and lifecycle-tested for the documented opt-in local-process and Kubernetes profiles. NeuralBase remains pre-1.0; this capability is not a production-HA, rolling-upgrade, or HPA-safety claim.

Historical closure evidence is preserved under [`docs/history/`](history/).

## Audited boundary

The Raft membership state machine admits learners, validates catch-up, promotes/removes through joint consensus, finalizes on the serving leader and persists removed incarnation IDs. Its effective configuration can include an uncommitted log entry; that view is correct for consensus but is insufficient to authorize deployment deletion.

The static server/Helm topology does not translate replica-count changes into membership operations. SQL TCP probes prove neither catch-up nor voter membership. Fresh-cluster recovery is a separate deliberate recovery topology and must not be auto-adopted by managed reconciliation.

## Reconciliation invariants

1. Desired topology is versioned with an explicit cluster incarnation, monotonic revision and stable node IDs; process existence is separate from committed voters, learners, joint configuration and tombstones.
2. A deterministic planner emits at most one next action. Repeating a plan does not execute it. Every execution reobserves authoritative state and rejects stale membership, leadership, deployment or desired-state revisions.
3. A new process starts non-voting, is admitted as learner, catches up through the existing log/snapshot path, and becomes active only after finalized committed promotion. Readiness alone never establishes membership.
4. Replacement adds and promotes a fresh incarnation before removing the old member. Tombstoned IDs and old cluster generations cannot be reused.
5. Followers are removed through committed consensus; removing the current leader requires leadership transfer and observation of the new leader first. Deployment deletion requires a finalized committed tombstone, never an effective/uncommitted removal.
6. A quorum authority barrier precedes actionable observation. Membership intents additionally check preconditions inside the serialized Raft event loop. Minority partitions and stale leaders cannot authorize automation.
7. Incomplete joint transitions block new topology changes until Raft finalizes. On controller restart, committed state overrides remembered progress.
8. Controller retries, input sizes, pending actions and deadlines are bounded. Timeouts have uncertain outcomes and require observation before retry.
9. Endpoint changes preserve stable incarnation identity; replacements use fresh IDs and storage. Static deployments remain explicitly static.
10. HPA remains disabled and `production_ready` remains false.

## Current implementation

`src/operator.rs` defines the desired topology, observation and plan. The `neuralbase-operator plan` binary parses bounded JSON and emits exactly one next action. `src/consensus/operator_control.rs` obtains a current-term quorum/apply barrier and submits term/generation-guarded membership commands through the serialized Raft event loop. The committed membership view is distinct from the effective configuration.

Managed learners preserve the cluster's original genesis voter set for full log replay, separately from current routing seeds. Routing seeds only admit replication to a non-voting, non-removed learner; they never grant votes or replace committed membership.

`ops/neuralbase_operator.py` supplies a Linux local-process deployment adapter. It manages independent server processes, TCP Raft, SQL listeners and RocksDB folders using the same planner. The controller is explicitly invoked; it is not an installed daemon. The managed profile is separate from static Compose/Helm/Kubernetes examples.

Requirements for the local-process adapter include Linux with `pidfd_open`/`pidfd_send_signal`, Python 3.9 or later, Rust 1.88 builds of the server and planner, and one trusted local user. The development adapter uses plaintext Raft. It does not configure production authorization or TLS.

The controller root must be private (`0700`). The server management socket is private (`0600`). The supervisor verifies Unix peer credentials and the complete managed configuration. Retirement pins the peer with a Linux process descriptor before a fresh identity exchange, so a saved or reused numeric PID cannot authorize a signal.

## Running a managed local cluster

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

The example reserves five stable identities and starts three voters. Expansion increments `desired.revision` and adds a reserved fresh identity. Replacement increments the revision, adds an unused identity, and removes the old member from the desired voter list. Reconciliation promotes the replacement before removing the old member, transfers leadership when needed, and retires a process only after finalized committed removal. Storage is retained.

The full endpoint/process inventory, cluster incarnation and voter floor are immutable. Voter list order is irrelevant; duplicate identities are rejected. Use fresh IDs for replacement. Tombstoned IDs cannot return to the desired set. Keep original per-node configuration and the durable controller root across controller/server restarts.

`plan` reads live authority and returns its observation and proposed action. It does not persist accepted revisions, create/stop processes, or change membership. Obtaining authority appends a read barrier to Raft. For a fully offline calculation, feed a previously captured `{desired, observed}` document into `neuralbase-operator plan`; the result is advisory and is revalidated at execution.

## Required SQL authentication

Before first bootstrap, optionally add an `identity_bootstrap` object with `path` (an absolute private verifier file, mode `0600`) and `sha256` (its exact lowercase SHA-256 digest). The file must contain valid strict SCRAM registry JSON and be at most 128 KiB. No password or verifier belongs in desired topology or controller state. The file selection and digest are immutable for that controller root; a digest mismatch fails before member creation.

This enables `NEURALBASE_AUTH_REQUIRED=1` and explicit digest-selected identity migration. Connect once to the current leader with a bootstrap credential to commit initialization; followers fail closed while initialization is pending. Subsequent user changes use the replicated identity store, which remains authoritative across replacement and restart.

The process adapter creates private per-node bootstrap copies. Kubernetes creates retained immutable Secrets mounted read-only in member pods; the controller Role therefore includes Secret read/create. Treat that principal and namespace as trusted. No Secret data is included in controller status.

## Failure handling and bounds

The controller serializes writers with `flock`, saves intent before process creation, and atomically fsyncs its state. Each execution observes and replans. A crash or timeout may have an uncertain outcome; the next invocation reconciles committed configuration rather than trusting remembered last action. Failed creation leaves storage/configuration for safe same-identity retry.

Removed storage stays retained. Automatic adoption of unmanaged or restored storage is rejected. Lost quorum requires deliberate operator recovery; automation does not manufacture authority by rewriting membership.

Limits include 128 reserved nodes, 256 KiB documents/responses, 8 KiB management requests, 16 pending control requests, bounded consensus/control deadlines, eight concurrent status queries, at most 1000 CLI steps and a thirty-second maximum retry delay. Each step performs one action.

## Executable evidence

Current capability-named evidence includes:

- `managed_membership_planner`: deterministic no-op, 3→4→3, stale guards/revisions, quorum and catch-up blocks, leader replacement, desired changes during joint consensus, withdrawn creation and bounded retry/restart state;
- `managed_membership_guards`: actual Raft admission/promotion/removal, learner restart, leader loss during catch-up, isolated stale leader, and partitions at durable committed-joint promotion/removal boundaries;
- `replicated_identity_membership`: compacted identity state reaches a fresh learner through snapshot bootstrap and survives guarded promotion, credential rotation and removal;
- `managed_membership_storage`: immutable incarnation binding, restart and rejection of old, unmanaged, restored or partially initialized storage;
- `managed_membership_process`: independent controller invocations and real server processes, listener-allocation failure, scale-out/in, leader restart/replacement, retained storage and replicated SQL/SCRAM convergence;
- `managed_membership_kubernetes.py`: disposable-kind PVC quota failure, managed-field drift, 3→4 expansion, leader pod loss, fresh-identity replacement, 4→3 contraction, retained PVCs and SQL/SCRAM convergence.

## Managed Kubernetes profile

`ops/neuralbase_kubernetes.py` drives the same planner and guarded admin commands through an explicit `kubectl` context and namespace. Each incarnation receives an immutable ConfigMap, a headless Raft discovery Service, a readiness-filtered SQL Service, a PVC, and a **one-replica** StatefulSet. Existing Helm/Compose deployments are not adopted.

Use `ops/operator.kubernetes.example.json` as a starting point. Select the explicit context, dedicated namespace, available storage class and locally built image, then run `bootstrap`, `plan`, `reconcile` and `status`. The desired inventory must use generated stable pod DNS names and ports. Automated rolling upgrades are outside this profile.

The controller root holds a unique ownership token and observed object UIDs. Keep it on a durable filesystem with working `flock` and atomic rename/fsync semantics. Run exactly one controller root per managed cluster. A new root refuses to adopt objects belonging to an old controller. The adapter compares managed fields, rejects external template/configuration changes, and uses JSON Patch tests on object UID and Kubernetes `resourceVersion` before changing replicas. It never uses a raw replica count as a voter count.

Member pods run as an unprivileged UID, drop capabilities, do not mount Kubernetes API credentials, and use a private Unix socket for administration. `ops/operator-rbac.yaml` grants the external controller the limited namespace operations needed for managed objects and guarded admin execution; it has no PVC deletion permission.

Readiness requires a serving, finalized voter. Learners remain discoverable for Raft through the headless Service while the SQL Service excludes them. A committed tombstone must precede scaling a retired StatefulSet to zero. StatefulSet, ConfigMap and PVC remain retained. Partial object creation preserves intent for idempotent retry.

The CI gate uses a disposable kind cluster and `tests/managed_membership_kubernetes.py` to exercise expansion, partial PVC failure, external drift, leader restart/replacement, contraction, retained storage and replicated SQL/SCRAM checks. HPA and raw static-deployment scaling remain unsupported; only explicit desired-topology reconciliation through this managed profile is supported.
