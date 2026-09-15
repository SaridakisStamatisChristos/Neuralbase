from pathlib import Path


def read(path: str) -> str:
    return Path(path).read_text()


def write(path: str, text: str) -> None:
    Path(path).write_text(text)


def replace_once(path: str, old: str, new: str) -> None:
    text = read(path)
    count = text.count(old)
    if count != 1:
        raise SystemExit(f"{path}: expected exactly one anchor, found {count}: {old[:100]!r}")
    write(path, text.replace(old, new, 1))


def insert_before(path: str, anchor: str, block: str) -> None:
    text = read(path)
    count = text.count(anchor)
    if count != 1:
        raise SystemExit(f"{path}: expected exactly one insertion anchor, found {count}: {anchor!r}")
    write(path, text.replace(anchor, block + anchor, 1))


# README.md
replace_once(
    "README.md",
    "PITR/automatic disaster recovery remain open",
    "timestamp-target PITR and automatic disaster recovery remain open; opt-in exact committed-index PITR is implemented",
)
replace_once(
    "README.md",
    "- Versioned NBBK offline/online backup, independent verification, crash-safe fresh-cluster restore, and authenticated NBEC backup encryption.\n",
    "- Versioned NBBK offline/online backup, independent verification, crash-safe fresh-cluster restore, and authenticated NBEC backup encryption.\n- Phase-9 opt-in archived recovery stream with exact committed-Raft-index PITR, authenticated archive encryption, timeline branching, bounded rollover/retirement, and fail-closed replay.\n",
)
replace_once(
    "README.md",
    "| Point-in-time recovery / automatic DR | **Not implemented** |",
    "| Exact committed-index point-in-time recovery | **Implemented and Phase-9 tested** |\n| Timestamp-target PITR / automatic DR | **Not implemented** |",
)
replace_once(
    "README.md",
    "Green CI is evidence for the exact checked commit and tested scopes, not a production-readiness or universal PostgreSQL-compatibility claim. CI #366 passed the complete code-only Phase-8 candidate gate on `5edcfc9b8a5df383a1302f7371bc89cda08f6564`; Phase-8 milestone closure requires this synchronized claim/documentation head to pass again, followed by a green post-merge `main` run.",
    "Green CI is evidence for the exact checked commit and tested scopes, not a production-readiness or universal PostgreSQL-compatibility claim. Phase-9 closure additionally requires the synchronized claim/documentation head to pass the repository CI plus the explicit feature-matrix/PITR process gate, followed by a green post-merge `main` run.",
)
replace_once(
    "README.md",
    "Phases 1–8 establish the current bounded evidence surface:",
    "Phases 1–9 establish the current bounded evidence surface:",
)
replace_once(
    "README.md",
    "Phase 8 does not change the project's pre-1.0 status and does not add a production-HA, full PostgreSQL compatibility, SQL transaction, HPA, PITR, or arbitrary-follower linearizability claim.",
    "Phase 9 does not change the project's pre-1.0 status and does not add a production-HA, full PostgreSQL compatibility, SQL transaction, HPA, timestamp-target PITR, automatic DR, or arbitrary-follower linearizability claim.",
)
replace_once(
    "README.md",
    "- [Operator recovery runbook](ops/RUNBOOK.md)\n",
    "- [Operator recovery runbook](ops/RUNBOOK.md)\n- [Phase-9 PITR operator guide](docs/PITR.md)\n",
)

# docs/README.md
replace_once(
    "docs/README.md",
    "| [PHASE7_CLOSURE.md](PHASE7_CLOSURE.md) | Phase-7 PR-head evidence and final closure conditions |\n",
    "| [PHASE7_CLOSURE.md](PHASE7_CLOSURE.md) | Phase-7 PR-head evidence and final closure conditions |\n| [PITR.md](PITR.md) | Phase-9 archived recovery stream, exact-index recovery, encryption, branching and retention |\n",
)
replace_once(
    "docs/README.md",
    "and the opt-in Phase-7 managed process/Kubernetes membership reconciliation profile are implemented and tested within their documented scopes; this still excludes linearizable reads from arbitrary followers, automatic strong-read routing, PITR/automatic DR, arbitrary Helm/HPA scaling, rolling-upgrade orchestration, and production-HA claims.",
    "the opt-in Phase-7 managed process/Kubernetes membership reconciliation profile, and Phase-9 exact committed-index archived recovery/PITR are implemented and tested within their documented scopes; this still excludes linearizable reads from arbitrary followers, automatic strong-read routing, timestamp-target PITR/automatic DR, arbitrary Helm/HPA scaling, rolling-upgrade orchestration, and production-HA claims.",
)

# AGENTS.md
replace_once(
    "AGENTS.md",
    "arbitrary Helm/HPA scaling remains unsupported, PITR/automatic DR remain open, and the online backup coordinator is currently an in-process API rather than a standalone live-server CLI.",
    "arbitrary Helm/HPA scaling remains unsupported; Phase-9 exact committed-index PITR is implemented through the opt-in archived recovery stream, while timestamp-target PITR and automatic DR remain open; the online backup coordinator is currently an in-process API rather than a standalone live-server CLI.",
)
replace_once(
    "AGENTS.md",
    "10. Update relevant docs with behavioral/configuration changes.\n",
    "10. Preserve the Phase-9 recovery boundary: PITR targets committed Raft indexes exactly; do not claim timestamp recovery or automatic DR, and do not advance confirmed apply past a required archive publication failure.\n11. Update relevant docs with behavioral/configuration changes.\n",
)
replace_once("AGENTS.md", "11. Do not commit build logs, Clippy output, temporary databases, secrets, or private keys.", "12. Do not commit build logs, Clippy output, temporary databases, secrets, or private keys.")
replace_once("AGENTS.md", "- Operator recovery: `ops/RUNBOOK.md`\n", "- Operator recovery: `ops/RUNBOOK.md`\n- Point-in-time recovery: `docs/PITR.md`\n")

# ROADMAP.md
replace_once(
    "ROADMAP.md",
    "### Later recovery extension — PITR\n\nArchived replicated-log/WAL-equivalent streaming and point-in-time recovery remain separate future work. Phase 5 does not infer PITR from retained Raft logs.\n",
    "## Completed Phase 9 — archived recovery stream and exact-index PITR\n\n- [x] Versioned `NBAR` archive segments with explicit committed Raft index/term, timeline, previous-hash link, payload hash, checksum, state-machine compatibility marker and strict fail-closed decoding.\n- [x] Canonical archived SQL, replicated identity, membership and known control records; unknown committed command families fail closed.\n- [x] Opt-in synchronous runtime archive fence: durable logical apply is followed by durable archive publication before confirmed Raft apply completes.\n- [x] Crash-safe staged segment publication, restart verification, gap/overlap/duplicate/conflict detection, bounded stream enumeration and compaction-frontier startup guards.\n- [x] Authenticated `NBPE` ChaCha20-Poly1305 archive encryption with strict out-of-band raw key-file handling and wrong-key/tamper rejection.\n- [x] Exact baseline/index/latest recovery into a fresh single-voter recovery generation with historical source-ID tombstones and independent staged verification before target publication.\n- [x] Timeline branching after earlier-point recovery so old future segments cannot join the new history.\n- [x] Conservative verified rollover/retirement that preserves the parent on ambiguity and requires a child baseline at the old durable frontier.\n- [x] Real OS-process evidence for exact target recovery, replicated identity rollback, branched future writes and restart persistence, plus encrypted replay and archive fault tests.\n- [x] Runtime archive frontier/append/failure/startup-rejection metrics and explicit stream segment limits.\n\nPhase 9 deliberately targets exact committed Raft **indexes**, not wall-clock timestamps. Timestamp-to-index mapping, automatic disaster detection/recovery, remote archive replication/object storage, and production-HA claims remain out of scope.\n",
)

# CHANGELOG.md
insert_before(
    "CHANGELOG.md",
    "### Documentation and deployment\n",
    "### Archived recovery stream / exact-index PITR — Phase 9\n\n- Added versioned, checksummed and hash-linked `NBAR` archive segments for committed SQL, identity, membership and known control records.\n- Added opt-in synchronous runtime archival so confirmed Raft apply waits for required archive publication, preserving the recovery/compaction boundary.\n- Added authenticated `NBPE` archive encryption with strict raw 32-byte key-file handling and wrong-key/tamper rejection.\n- Added exact baseline/index/latest recovery into a fresh recovery generation, deterministic replay, timeline branching, conservative rollover/retirement and fail-closed target publication.\n- Added bounded archive enumeration, canonical membership-byte validation, publication/restart/corruption fault tests, convergent same-record multi-writer publication evidence, encrypted end-to-end replay, and real-process identity/branch/restart evidence.\n- Added operator CLI commands through `neuralbase-pitr`: `init`, `status`, `verify`, `targets`, `recover`, `branch`, `retire` and `diagnose`.\n- Timestamp-target PITR and automatic DR remain unsupported.\n\n",
)
replace_once(
    "CHANGELOG.md",
    "- Manual Phase-5 backup/restore/fresh-cluster DR is implemented and tested; PITR and automatic disaster recovery remain unimplemented.",
    "- Manual Phase-5 backup/restore/fresh-cluster DR and Phase-9 exact committed-index PITR are implemented and tested; timestamp-target PITR and automatic disaster recovery remain unimplemented.",
)

# docs/CONFIGURATION.md
replace_once(
    "docs/CONFIGURATION.md",
    "[`src/tls.rs`](../src/tls.rs) and the replicated identity runtime.",
    "[`src/tls.rs`](../src/tls.rs), [`src/pitr_runtime.rs`](../src/pitr_runtime.rs) and the replicated identity runtime.",
)
insert_before(
    "docs/CONFIGURATION.md",
    "## TLS\n",
    "## Phase-9 PITR archive runtime\n\nPITR archival is opt-in and applies only to configured clustered operation with a previously initialized/verified archive stream. The server does not create a stream implicitly. Initialize one with `neuralbase-pitr init`, then point the runtime at it.\n\n| Variable | Default | Behavior |\n|---|---|---|\n| `NEURALBASE_PITR_ARCHIVE_DIR` | unset | Enables synchronous Phase-9 archive publication into the existing stream directory. Unset leaves the historical non-PITR runtime path unchanged. |\n| `NEURALBASE_PITR_KEY_FILE` | unset | Raw 32-byte archive-encryption key file. Requires `NEURALBASE_PITR_ARCHIVE_DIR`; wrong/missing/insecure key material fails startup. |\n| `NEURALBASE_PITR_MAX_SEGMENTS` | `100000` | Positive per-stream segment limit. Values above the hard maximum `1000000`, zero or malformed text fail startup. |\n\nArchive startup verifies metadata/segments, rejects a frontier newer than durable logical state, and rejects a compacted Raft snapshot ahead of the verified archive frontier. See [PITR.md](PITR.md).\n\n",
)

# docs/DEPLOYMENT.md
replace_once(
    "docs/DEPLOYMENT.md",
    "| `NEURALBASE_IDENTITY_MIGRATION_SHA256` | Exact SHA-256 authorizing the selected clustered legacy registry |\n",
    "| `NEURALBASE_IDENTITY_MIGRATION_SHA256` | Exact SHA-256 authorizing the selected clustered legacy registry |\n| `NEURALBASE_PITR_ARCHIVE_DIR` | Existing verified Phase-9 archive stream; opt-in synchronous archival |\n| `NEURALBASE_PITR_KEY_FILE` | Optional raw 32-byte archive-encryption key file |\n| `NEURALBASE_PITR_MAX_SEGMENTS` | Bounded per-stream archive segment limit |\n",
)
insert_before(
    "docs/DEPLOYMENT.md",
    "## Membership and scaling\n",
    "### Phase-9 exact-index PITR\n\nThe separate `neuralbase-pitr` tool initializes and verifies an archive stream from a verified NBBK/NBEC baseline, lists exact recoverable committed-index targets, recovers into a fresh target, creates a new child timeline after earlier-point recovery, and retires a quiesced parent only after a replacement child is independently verified at the parent frontier. Runtime archival is enabled only with `NEURALBASE_PITR_ARCHIVE_DIR`; optional `NEURALBASE_PITR_KEY_FILE` selects authenticated archive encryption.\n\nThis is an operator-managed recovery path, not automatic DR. Timestamp targets are intentionally unsupported in archive v1; the recovery coordinate is an exact committed Raft index. Deployment automation must provision/archive the baseline, archive directory and key material explicitly. See [PITR.md](PITR.md) and the [runbook](../ops/RUNBOOK.md).\n\n",
)
replace_once(
    "docs/DEPLOYMENT.md",
    "PITR, automatic DR, arbitrary Helm/HPA scaling and production HA remain unimplemented or unclaimed.",
    "Timestamp-target PITR, automatic DR, arbitrary Helm/HPA scaling and production HA remain unimplemented or unclaimed; exact committed-index PITR is implemented only through the explicit Phase-9 archive workflow.",
)

# docs/ARCHITECTURE.md
replace_once(
    "docs/ARCHITECTURE.md",
    "- **Deployment count is not membership.** The Phase-7 managed profile creates/retires incarnations only through guarded committed-state reconciliation; static Helm/HPA replica changes remain outside the contract.\n",
    "- **Deployment count is not membership.** The Phase-7 managed profile creates/retires incarnations only through guarded committed-state reconciliation; static Helm/HPA replica changes remain outside the contract.\n- **PITR is an explicit archive lifecycle.** Phase-9 recovery starts from a verified operator baseline and replays a verified committed-index archive; exact Raft indexes are recovery coordinates, not wall-clock timestamps.\n",
)
insert_before(
    "docs/ARCHITECTURE.md",
    "## Read-consistency boundary\n",
    "## Archived recovery stream and PITR architecture\n\nPhase 9 binds a verified NBBK/NBEC baseline to a distinct archive timeline in `stream.json`. Each post-baseline committed position is represented by one versioned `NBAR` record carrying index/term, record kind, timeline, previous-segment hash, payload hash, compatibility marker and final checksum. Optional `NBPE` wrapping provides authenticated ChaCha20-Poly1305 encryption with an out-of-band raw 32-byte key.\n\n`PitrRuntimeArchiver` is opened only when `NEURALBASE_PITR_ARCHIVE_DIR` is configured. The confirmed-apply task first performs the durable replicated state-machine apply, then publishes the required archive segment, and only then reports confirmed apply to Raft. Publication failure therefore prevents that position from advancing the confirmed apply/compaction boundary. Startup independently verifies the stream and rejects frontier/baseline/compaction relationships that would make required recovery history unavailable.\n\n`pitr_replay` restores the verified baseline into a hidden fresh target, replays records deterministically through the replicated state machine to `baseline`, `latest` or an exact committed index, reconstructs membership at that point, creates a fresh single-voter recovery generation with historical source IDs tombstoned, verifies the staged result, and only then publishes the target. `pitr_branch` creates a new timeline and baseline at an earlier recovered point so segments from the discarded future cannot join the child chain. `pitr_retention` retires a quiesced parent only after a verified replacement begins exactly at the old frontier.\n\nArchive v1 intentionally has no wall-clock timestamp target mapping and no automatic disaster-recovery controller. See [PITR.md](PITR.md).\n\n",
)
replace_once(
    "docs/ARCHITECTURE.md",
    "- `src/restore.rs` — fresh-generation staged restore and atomic target publication.\n",
    "- `src/restore.rs` — fresh-generation staged restore and atomic target publication.\n- `src/pitr.rs` / `pitr_archive.rs` — Phase-9 archive record codec, chain verification, encryption and crash-safe publication.\n- `src/pitr_runtime.rs` — opt-in synchronous archive/confirmed-apply fence and runtime limits/metrics.\n- `src/pitr_replay.rs` / `pitr_branch.rs` / `pitr_retention.rs` — exact-index replay, child timelines and conservative rollover retirement.\n- `src/bin/neuralbase-pitr.rs` — operator PITR initialization, verification, recovery, branching and retirement CLI.\n",
)
replace_once(
    "docs/ARCHITECTURE.md",
    "raw HPA scaling, PITR and automatic DR remain outside the claim.",
    "raw HPA scaling, timestamp-target PITR and automatic DR remain outside the claim; Phase-9 exact committed-index PITR is inside the tested claim.",
)

# docs/DISTRIBUTED.md
replace_once(
    "docs/DISTRIBUTED.md",
    "NeuralBase has tested Raft paths for persistent table replication, SQL-aware snapshot/recovery, coordinated membership changes, strongly consistent clustered identity, and explicit Phase-6 read-consistency modes.",
    "NeuralBase has tested Raft paths for persistent table replication, SQL-aware snapshot/recovery, coordinated membership changes, strongly consistent clustered identity, explicit Phase-6 read-consistency modes, and Phase-9 exact committed-index archived recovery.",
)
insert_before(
    "docs/DISTRIBUTED.md",
    "## Deployment boundary\n",
    "## Phase-9 archived recovery stream and exact-index PITR\n\nA Phase-9 stream starts from a verified operator backup baseline and a unique timeline. Committed post-baseline positions are archived as strict versioned records containing their exact Raft index/term and hash-linked history. SQL, replicated identity, membership transitions and known control entries are represented explicitly; unknown committed command families fail closed. Optional authenticated archive encryption is selected by an out-of-band key file.\n\nWhen archival is enabled, the local replicated state machine applies durably first, the corresponding archive record is then durably published, and only then does the confirmed-apply channel report success to Raft. An archive publication error therefore blocks confirmed apply and prevents compaction from legitimately advancing beyond the missing recovery position. Startup rejects a stream whose baseline/frontier is inconsistent with durable logical state or whose frontier trails an already compacted Raft snapshot.\n\nRecovery accepts `baseline`, `latest` or an exact committed index. It verifies the baseline and complete required archive chain, replays deterministically into a hidden fresh target, reconstructs selected historical membership, creates a new single-voter recovery generation with fresh node identity and source tombstones, verifies the complete staged state, then publishes atomically. Branching creates a distinct child timeline at an earlier recovered point; parent future records cannot join it. Retention retirement is conservative and preserves the parent on ambiguity.\n\nArchive v1 does **not** map wall-clock timestamps to recovery positions and does not perform automatic disaster detection/recovery. Operator details are in [PITR.md](PITR.md).\n\n",
)
replace_once(
    "docs/DISTRIBUTED.md",
    "- versioned offline/online backup, independent verification, authenticated encrypted backup, fresh-target restore and fresh-generation cluster recovery;\n",
    "- versioned offline/online backup, independent verification, authenticated encrypted backup, fresh-target restore and fresh-generation cluster recovery;\n- Phase-9 hash-linked archived recovery, authenticated archive encryption, exact-index replay, branch lineage, bounded retention and real-process PITR recovery;\n",
)
replace_once(
    "docs/DISTRIBUTED.md",
    "arbitrary-follower strong-read routing/optimization, PITR and automatic DR, broader authorization/security, upgrade/storage-chaos evidence and production performance characterization.",
    "arbitrary-follower strong-read routing/optimization, timestamp-target PITR and automatic DR, broader authorization/security, upgrade/storage-chaos evidence and production performance characterization.",
)

# docs/TESTING.md
insert_before(
    "docs/TESTING.md",
    "## Confidence gate\n",
    "## Phase 9 archived recovery / PITR evidence\n\nPhase 9 combines codec, storage, replay and real-process evidence instead of inferring recoverability from retained Raft logs. Unit/focused tests cover strict NBAR/NBPE decoding, unsupported format/state-machine versions, truncation/trailing ambiguity, gap/overlap/hash/timeline failures, canonical membership bytes, restart staging cleanup, corrupt finalized files, duplicate/conflicting publication, convergent same-record publication, encrypted wrong-key/tamper behavior, bounded stream enumeration, target availability, wrong baseline rejection before publication, fresh recovery generations, child-timeline old-future rejection and conservative retention retirement.\n\n`tests/phase9_pitr_process.rs` uses real server/backup/PITR binaries. It captures a baseline, archives later committed mutations, selects an exact target, proves post-target table and password changes are absent after recovery, creates a distinct child timeline before resumed service, writes a new future, restarts, and verifies the recovered identity/data plus child future persist. The replay suite also exercises encrypted end-to-end archive recovery.\n\nThe Phase-9 closure gate additionally runs `cargo test` under default, `tls`, `io_uring`, and combined `tls io_uring` features, full all-feature Clippy with warnings denied, `make test`, and the targeted real-process PITR suite. Timestamp-target recovery and automatic DR are intentionally outside this evidence.\n\n",
)
replace_once(
    "docs/TESTING.md",
    "Production readiness, production HA, HPA safety, arbitrary-follower linearizable reads, automatic strong-read routing, PITR, automatic DR and full PostgreSQL compatibility remain false/not claimed.",
    "Production readiness, production HA, HPA safety, arbitrary-follower linearizable reads, automatic strong-read routing, timestamp-target PITR, automatic DR and full PostgreSQL compatibility remain false/not claimed; exact committed-index PITR is the bounded Phase-9 claim.",
)
replace_once(
    "docs/TESTING.md",
    "For Phase 8 it additionally supports only the capability slices recorded in `SQL_COMPATIBILITY.yaml` and their named executable evidence.\n\nIt still does **not** mean production readiness, complete PostgreSQL compatibility, SQL transaction blocks/distributed transactions, linearizable arbitrary-follower reads, automatic strong-read routing, arbitrary Helm/HPA scaling, PITR or automatic DR, security certification, rolling-upgrade safety, or performance superiority outside measured workloads.",
    "For Phase 8 it additionally supports only the capability slices recorded in `SQL_COMPATIBILITY.yaml` and their named executable evidence. For Phase 9 it supports the exact committed-index archived recovery/PITR scope documented in `PITR.md`.\n\nIt still does **not** mean production readiness, complete PostgreSQL compatibility, SQL transaction blocks/distributed transactions, linearizable arbitrary-follower reads, automatic strong-read routing, arbitrary Helm/HPA scaling, timestamp-target PITR or automatic DR, security certification, rolling-upgrade safety, or performance superiority outside measured workloads.",
)

# docs/THREAT_MODEL.md
replace_once(
    "docs/THREAT_MODEL.md",
    "- NBBK/NBEC operator backups and their separately managed encryption keys;\n",
    "- NBBK/NBEC operator backups and their separately managed encryption keys;\n- Phase-9 NBAR/NBPE archive streams, timeline metadata and separately managed archive keys;\n",
)
insert_before(
    "docs/THREAT_MODEL.md",
    "## Read-consistency risk\n",
    "## PITR archive security and recovery risk\n\nPhase-9 archive streams contain deterministic committed SQL effects, SCRAM verifier identity material, membership transitions and control positions. Plain NBAR streams are integrity-protected but not confidential. NBPE adds authenticated ChaCha20-Poly1305 protection with a raw 32-byte out-of-band key; the key file is treated as sensitive and insecure/wrong key material fails closed. Archive encryption does not encrypt RocksDB, internal Raft state or the baseline unless the baseline itself is NBEC.\n\nThe runtime archive fence is fail-closed: a required archive publication failure prevents confirmed apply from advancing. Operators must therefore provision durable writable archive storage and monitor archive failures/segment limits. A full stream also blocks further archive-required progress until rollover is performed; deleting segments to bypass the limit destroys the recovery chain and is unsupported.\n\nBranching is a history fork, not an in-place rewind. A recovered earlier point must use a distinct child timeline before new future writes. The archive verifier rejects parent-future injection into that child. Timestamp recovery is not supported; selecting an application time requires external mapping to a known committed index. Automatic DR, remote archive replication and key lifecycle automation remain operator responsibilities.\n\n",
)
replace_once(
    "docs/THREAT_MODEL.md",
    "PITR and automatic DR remain unimplemented; Phase-5 backup encryption does not imply general database-at-rest encryption.",
    "Exact committed-index PITR is implemented through the explicit Phase-9 archive workflow, but timestamp-target PITR and automatic DR remain unimplemented; Phase-5/9 artifact encryption does not imply general database-at-rest encryption.",
)
replace_once(
    "docs/THREAT_MODEL.md",
    "- `src/backup*.rs`, `src/offline_backup.rs`, `src/online_backup.rs`, `src/restore.rs` and `tests/phase5_*` — operator recovery/security evidence.\n",
    "- `src/backup*.rs`, `src/offline_backup.rs`, `src/online_backup.rs`, `src/restore.rs` and `tests/phase5_*` — operator recovery/security evidence.\n- `src/pitr*.rs`, `src/bin/neuralbase-pitr.rs` and `tests/phase9_pitr_process.rs` — archived recovery, encryption, replay, branch/retention and real-process PITR evidence.\n",
)

# observability/README.md
replace_once(
    "observability/README.md",
    "| `neuralbase_strong_read_rejections_total{reason}` | Instrumented `not_leader` and `timeout` failures; not every possible strong-read error |\n",
    "| `neuralbase_strong_read_rejections_total{reason}` | Instrumented `not_leader` and `timeout` failures; not every possible strong-read error |\n| `neuralbase_pitr_archive_appends_total` | Successful Phase-9 archive publication calls while PITR runtime archival is enabled |\n| `neuralbase_pitr_archive_failures_total{reason}` | Archive publication or configured segment-limit failures |\n| `neuralbase_pitr_startup_rejections_total{reason}` | PITR-enabled startup rejected because archive/open/frontier/compaction invariants failed |\n| `neuralbase_pitr_archive_frontier_index` | Verified/published archive recovery frontier for the running PITR stream |\n| `neuralbase_pitr_archive_segment_limit` | Configured per-stream runtime segment limit |\n",
)
replace_once(
    "observability/README.md",
    "There is currently no emitted strong-read latency histogram, Raft quorum/lag gauge set, backup-age metric or automatic SLO alert policy.",
    "There is currently no emitted strong-read latency histogram, Raft quorum/lag gauge set, backup/archive-age metric or automatic SLO alert policy. PITR metrics exist only when the relevant startup/archive paths execute.",
)

# ops/RUNBOOK.md
replace_once(
    "ops/RUNBOOK.md",
    "It is not a production-HA or automatic-disaster-recovery guarantee. `Local` reads may lag, strong reads require the current serving leader, membership reconciliation requires the explicit managed profile, and PITR is not implemented.",
    "It is not a production-HA or automatic-disaster-recovery guarantee. `Local` reads may lag, strong reads require the current serving leader, membership reconciliation requires the explicit managed profile, and Phase-9 PITR is limited to exact committed Raft indexes from an explicitly managed archive stream.",
)
replace_once(
    "ops/RUNBOOK.md",
    "- Do not infer PITR, automatic DR, production HA, or linearizable arbitrary-follower reads from this runbook.\n",
    "- Do not infer timestamp-target PITR, automatic DR, production HA, or linearizable arbitrary-follower reads from this runbook. Exact committed-index PITR requires the explicit Phase-9 archive workflow below.\n",
)
insert_before(
    "ops/RUNBOOK.md",
    "## Complete cluster loss\n",
    "## Phase-9 exact-index point-in-time recovery\n\nPhase 9 adds an **opt-in** archived recovery stream. The recovery coordinate is an exact committed Raft index. Wall-clock timestamp targets are intentionally unsupported in archive v1. Build the PITR tool with:\n\n```bash\ncargo build --release --locked --bin neuralbase-pitr\n```\n\n### 1. Initialize a stream from a verified baseline\n\nCreate or select a verified NBBK/NBEC baseline and initialize a distinct archive directory. For a plaintext baseline/archive:\n\n```bash\ntarget/release/neuralbase-pitr init \\\n  --backup /backups/base.nbbk \\\n  --archive /archive/neuralbase-pitr\n```\n\nFor encrypted inputs, pass key **file paths**, never raw key bytes:\n\n```bash\ntarget/release/neuralbase-pitr init \\\n  --backup /backups/base.nbec \\\n  --backup-key-file /secure/backup.key \\\n  --archive /archive/neuralbase-pitr \\\n  --archive-key-file /secure/pitr.key\n```\n\nThe PITR archive key is exactly 32 raw bytes and is managed out of band. On Unix, keep key files mode `0600` or stricter. The stream metadata binds the baseline hash/index/term, state-machine compatibility, source membership metadata, encryption mode and a unique timeline.\n\n### 2. Enable synchronous runtime archival\n\nStart the clustered server with the already initialized stream:\n\n```bash\nexport NEURALBASE_PITR_ARCHIVE_DIR=/archive/neuralbase-pitr\nexport NEURALBASE_PITR_KEY_FILE=/secure/pitr.key   # omit for plaintext NBAR\nexport NEURALBASE_PITR_MAX_SEGMENTS=100000        # optional; default shown\n```\n\nWith PITR enabled, a committed entry is durably applied to the replicated state machine, its required archive segment is durably published and verified, and only then is confirmed apply reported back to Raft. Archive publication failure therefore fails closed rather than allowing the confirmed apply/compaction frontier to pass the missing recovery position. The historical runtime path is unchanged when `NEURALBASE_PITR_ARCHIVE_DIR` is unset.\n\n### 3. Inspect and verify recoverable targets\n\n```bash\ntarget/release/neuralbase-pitr status --archive /archive/neuralbase-pitr --archive-key-file /secure/pitr.key\ntarget/release/neuralbase-pitr verify --archive /archive/neuralbase-pitr --archive-key-file /secure/pitr.key\ntarget/release/neuralbase-pitr targets --archive /archive/neuralbase-pitr --archive-key-file /secure/pitr.key\n```\n\n`targets` reports the baseline, latest frontier and exact inclusive committed-index range. Do not invent a timestamp target. An application/operator that needs time-oriented recovery must maintain its own trustworthy mapping from application time/event to a committed Raft index.\n\n### 4. Recover to an exact point\n\nFence the old topology and choose a **fresh** recovery node ID. The target directory must not exist:\n\n```bash\ntarget/release/neuralbase-pitr recover \\\n  --backup /backups/base.nbbk \\\n  --archive /archive/neuralbase-pitr \\\n  --target 4242 \\\n  --target-dir /var/lib/neuralbase/recovery-pitr \\\n  --node-id recovery-pitr \\\n  --archive-key-file /secure/pitr.key\n```\n\n`--target` also accepts `baseline` or `latest`. Recovery verifies the baseline and required archive chain before authority publication, restores into a hidden staging target, deterministically replays through the selected index, reconstructs selected historical membership, creates a fresh single-voter recovery generation with historical source IDs tombstoned, independently reopens/verifies the staged database, and only then atomically publishes the target. A wrong baseline, missing/corrupt segment, incompatible version or unavailable target fails before the requested target becomes serving authority.\n\n### 5. Branch before creating a new future\n\nAfter recovery to an earlier point, keep the recovered database **stopped** while creating its new child timeline. This is important because starting Raft can append a new current-term control position.\n\n```bash\ntarget/release/neuralbase-pitr branch \\\n  --parent-archive /archive/neuralbase-pitr \\\n  --parent-archive-key-file /secure/pitr.key \\\n  --branch-target 4242 \\\n  --db /var/lib/neuralbase/recovery-pitr \\\n  --baseline-out /backups/branch-4242.nbbk \\\n  --archive /archive/neuralbase-pitr-branch\n```\n\nThe child receives a new timeline and records the parent timeline/branch index. Old parent segments after the branch point are not valid child history. Start the recovered server with the **child** archive directory before accepting new writes.\n\n### 6. Rollover and conservative retirement\n\n`NEURALBASE_PITR_MAX_SEGMENTS` bounds one runtime stream. Before the limit is reached, quiesce the old archive writer and create a verified replacement child whose baseline/branch is exactly the old durable frontier. Only then retire the parent:\n\n```bash\ntarget/release/neuralbase-pitr retire \\\n  --archive /archive/neuralbase-pitr \\\n  --replacement-archive /archive/neuralbase-pitr-next \\\n  --archive-key-file /secure/pitr.key \\\n  --replacement-archive-key-file /secure/pitr-next.key\n```\n\nRetirement is deliberately conservative: mismatched lineage, boundary, term, segment sequence/count or staging ambiguity preserves the old data. Never delete archive segments manually to bypass the configured limit.\n\n### Phase-9 failure boundary\n\n- Unknown committed command encodings, unsupported archive/state-machine versions, gaps, overlaps, bad links, corrupt finalized segments, wrong timelines and wrong keys fail closed.\n- Staging leftovers are not authoritative and are cleaned/revalidated on open; an interrupted publication never advertises an unverified target.\n- Archive verification is bounded by per-payload and per-stream limits.\n- Phase 9 does not provide remote object-store replication, automatic archive copying, timestamp target mapping, automatic disaster detection/failover, or a measured RPO/RTO SLA.\n- `neuralbase_pitr_*` metrics expose runtime publication/frontier/limit/rejection signals; operators still need their own durable storage, alerting, retention and key-lifecycle policy.\n\nSee [`docs/PITR.md`](../docs/PITR.md) for the format and contract summary.\n\n",
)
replace_once(
    "ops/RUNBOOK.md",
    "- point-in-time recovery or archived WAL/Raft-log replay;\n- automatic disaster detection/failover/recovery;",
    "- wall-clock/timestamp-target point-in-time recovery;\n- automatic disaster detection/failover/recovery;",
)

# CONFIDENCE.md
replace_once("CONFIDENCE.md", "**Updated:** 2026-09-14", "**Updated:** 2026-09-15")
replace_once(
    "CONFIDENCE.md",
    "the opt-in Phase-7 managed process/Kubernetes membership reconciliation profile, and the bounded Phase-8 SQL compatibility profile.",
    "the opt-in Phase-7 managed process/Kubernetes membership reconciliation profile, the bounded Phase-8 SQL compatibility profile, and Phase-9 exact committed-index archived recovery/PITR.",
)
replace_once(
    "CONFIDENCE.md",
    "- PostgreSQL 16 TPC-H Q1-Q22 reference comparison remains part of CI at a deterministic small scale.\n",
    "- PostgreSQL 16 TPC-H Q1-Q22 reference comparison remains part of CI at a deterministic small scale.\n- Phase 9 adds strict hash-linked archived committed records, authenticated archive encryption, synchronous archive-before-confirmed-apply fencing, exact baseline/index/latest recovery into a fresh verified generation, child timeline branching and conservative retention retirement.\n- Phase-9 evidence includes corruption/version/gap/overlap/publication/restart/branch/encryption faults plus a real OS-process exact-target recovery that rolls back both SQL data and replicated SCRAM identity, creates a new future, and survives restart.\n",
)
insert_before(
    "CONFIDENCE.md",
    "## What the confidence claim still excludes\n",
    "## Phase-9 PITR scope\n\nThe promoted PITR claim is deliberately narrower than generic time-based recovery. A verified NBBK/NBEC baseline anchors one archive timeline. Each required committed position after the baseline is represented by a strict hash-linked NBAR/NBPE record. When runtime archival is enabled, archive publication is part of the confirmed-apply fence, and startup refuses stream/durable-state/compaction relationships that would invalidate the recovery history.\n\nRecovery accepts `baseline`, `latest` or an exact committed Raft index. It replays into a hidden fresh target, reconstructs state and membership at that exact boundary, establishes a fresh recovery authority, verifies the completed target and only then publishes it. Recovery to an earlier point must branch into a distinct child timeline before new future writes.\n\nThis claim does **not** include timestamp-to-index mapping, automatic DR/failover, remote archive replication/object storage, production retention guarantees or a measured RPO/RTO SLA.\n\n",
)
replace_once(
    "CONFIDENCE.md",
    "- PITR or automatic disaster recovery beyond the documented manual fresh-cluster Phase-5 procedure;",
    "- timestamp-target PITR or automatic disaster recovery; exact committed-index PITR is the bounded Phase-9 capability;",
)

# CONFIDENCE.yaml
replace_once("CONFIDENCE.yaml", "version: 4\nupdated: 2026-09-14", "version: 5\nupdated: 2026-09-15")
replace_once(
    "CONFIDENCE.yaml",
    "scope: local_sql_engine_plus_replicated_tables_snapshots_membership_identity_backup_read_consistency_managed_reconciliation_and_phase8_sql_profile",
    "scope: local_sql_engine_plus_replicated_tables_snapshots_membership_identity_backup_read_consistency_managed_reconciliation_phase8_sql_profile_and_phase9_exact_index_pitr",
)
replace_once(
    "CONFIDENCE.yaml",
    "    pitr: false\n    automatic_dr: false",
    "    pitr: true\n    pitr_target: committed_raft_index\n    pitr_timestamp_targets: false\n    pitr_archive_encryption: true\n    pitr_branching: true\n    pitr_bounded_retention: true\n    automatic_dr: false",
)
replace_once(
    "CONFIDENCE.yaml",
    "    Phase-8 SQL compatibility profile. Phase 8 adds fail-closed unsupported DML/DDL\n    semantics, single-statement request enforcement, selected PostgreSQL-16\n    differential scalar/window evidence, and a typed extended-protocol subset with\n    real postgres-client evidence. Replicated mutation success and strong-read\n    barriers still wait for Raft quorum commit and confirmed durable local\n    state-machine apply. This does not imply production HA, full PostgreSQL\n    compatibility, SQL transaction blocks, arbitrary-follower linearizable reads,\n    automatic strong-read routing, arbitrary Helm/HPA replica scaling, PITR, or\n    automatic disaster recovery.",
    "    Phase-8 SQL compatibility profile, and Phase-9 exact committed-index archived\n    recovery/PITR. Phase 8 adds fail-closed unsupported DML/DDL semantics,\n    single-statement request enforcement, selected PostgreSQL-16 differential\n    scalar/window evidence, and a typed extended-protocol subset with real\n    postgres-client evidence. Phase 9 adds strict hash-linked archive records,\n    optional authenticated archive encryption, synchronous archive-before-confirmed-\n    apply fencing, exact-index replay into a fresh verified recovery generation,\n    child timeline branching and conservative bounded retention. This does not imply\n    production HA, full PostgreSQL compatibility, SQL transaction blocks, arbitrary-\n    follower linearizable reads, automatic strong-read routing, arbitrary Helm/HPA\n    replica scaling, timestamp-target PITR, or automatic disaster recovery.",
)
replace_once(
    "CONFIDENCE.yaml",
    "      committed membership; standalone-only databases are not supported. PITR\n      and automatic DR are not implemented.\n\n  - artifact: deployment_manifests",
    "      committed membership; standalone-only databases are not supported. Exact-\n      index PITR is a separate Phase-9 artifact; automatic DR is not implemented.\n\n  - artifact: point_in_time_recovery\n    effective: 0.74\n    status: exact_index_encrypted_archive_branch_recovery_tested\n    criticality: release_boundary\n    evidence:\n      - src/pitr.rs\n      - src/pitr_archive.rs\n      - src/pitr_runtime.rs\n      - src/pitr_replay.rs\n      - src/pitr_branch.rs\n      - src/pitr_retention.rs\n      - src/bin/neuralbase-pitr.rs\n      - tests/phase9_pitr_process.rs\n      - docs/PITR.md\n    limitation: >-\n      Recovery targets are baseline/latest or an exact committed Raft index. Archive\n      v1 does not map wall-clock timestamps to indexes and does not provide automatic\n      disaster detection/recovery, remote archive replication/object storage, or a\n      production RPO/RTO SLA. Runtime archival is explicit and opt-in; the historical\n      non-PITR path remains unchanged when no archive directory is configured.\n\n  - artifact: deployment_manifests",
)
replace_once(
    "CONFIDENCE.yaml",
    "  - production_ready and production_ha remain false despite membership, identity, recovery, read-consistency, managed-reconciliation, and Phase-8 SQL progress.\n",
    "  - production_ready and production_ha remain false despite membership, identity, recovery, read-consistency, managed-reconciliation, Phase-8 SQL, and Phase-9 exact-index PITR progress.\n  - pitr=true means only the Phase-9 exact committed-Raft-index archive/replay contract; pitr_timestamp_targets and automatic_dr remain false.\n",
)
replace_once(
    "CONFIDENCE.yaml",
    "  - Harden the managed reconciliation profile for upgrades, broader network/storage chaos, and production-grade security without enabling raw HPA scaling.\n",
    "  - Harden the managed reconciliation profile for upgrades, broader network/storage chaos, and production-grade security without enabling raw HPA scaling.\n  - Add timestamp-to-index recovery mapping only with an explicit durable time/index contract; do not infer it from Phase-9 exact-index PITR.\n",
)

# tests/confidence_yaml.rs
replace_once(
    "tests/confidence_yaml.rs",
    "    assert_eq!(scope[\"pitr\"].as_bool(), Some(false));\n    assert_eq!(scope[\"automatic_dr\"].as_bool(), Some(false));",
    "    assert_eq!(scope[\"pitr\"].as_bool(), Some(true));\n    assert_eq!(scope[\"pitr_target\"].as_str(), Some(\"committed_raft_index\"));\n    assert_eq!(scope[\"pitr_timestamp_targets\"].as_bool(), Some(false));\n    assert_eq!(scope[\"pitr_archive_encryption\"].as_bool(), Some(true));\n    assert_eq!(scope[\"pitr_branching\"].as_bool(), Some(true));\n    assert_eq!(scope[\"pitr_bounded_retention\"].as_bool(), Some(true));\n    assert_eq!(scope[\"automatic_dr\"].as_bool(), Some(false));",
)
replace_once(
    "tests/confidence_yaml.rs",
    "    assert_eq!(\n        recovery[\"status\"].as_str(),\n        Some(\"offline_online_encrypted_fresh_cluster_dr_tested\")\n    );\n\n    let reads = artifacts",
    "    assert_eq!(\n        recovery[\"status\"].as_str(),\n        Some(\"offline_online_encrypted_fresh_cluster_dr_tested\")\n    );\n\n    let pitr = artifacts\n        .iter()\n        .find(|item| item[\"artifact\"].as_str() == Some(\"point_in_time_recovery\"))\n        .expect(\"point_in_time_recovery boundary must be explicit\");\n    assert_eq!(\n        pitr[\"status\"].as_str(),\n        Some(\"exact_index_encrypted_archive_branch_recovery_tested\")\n    );\n    let pitr_evidence = pitr[\"evidence\"]\n        .as_sequence()\n        .expect(\"PITR evidence must be a list\");\n    assert!(pitr_evidence\n        .iter()\n        .any(|item| item.as_str() == Some(\"tests/phase9_pitr_process.rs\")));\n\n    let reads = artifacts",
)

# New dedicated PITR guide.
pitr_doc = r'''# Phase-9 point-in-time recovery

NeuralBase Phase 9 implements an **opt-in archived recovery stream with exact committed-Raft-index recovery**. It does not implement wall-clock timestamp targets or automatic disaster recovery.

## Recovery model

A stream consists of:

1. a verified operator backup baseline (`NBBK` or encrypted `NBEC`);
2. `stream.json`, which binds the baseline SHA-256, baseline index/term, source membership generation/config index, state-machine/archive versions, encryption mode and one unique timeline;
3. one archive segment for each required committed position after the baseline.

Plain segments use the versioned `NBAR` envelope. Encrypted segments use authenticated `NBPE` wrapping. Records carry the exact committed Raft index/term and one explicit kind: replicated SQL, replicated identity, membership change, or a known control entry. Unknown non-empty committed command families are rejected so a future command cannot silently become unrecoverable history.

Archive v1 deliberately uses **one logical record per segment**. The format includes a state-machine compatibility marker, timeline ID, previous-segment hash, payload hash, bounded payload length and final SHA-256 checksum. The verifier rejects unsupported versions/flags, malformed lengths, trailing bytes, corruption, gaps, overlaps, duplicate conflicts, wrong timelines and bad hash links.

## Initialize

Build the tool:

```bash
cargo build --release --locked --bin neuralbase-pitr
```

Initialize from a verified plaintext baseline:

```bash
target/release/neuralbase-pitr init \
  --backup /backups/base.nbbk \
  --archive /archive/neuralbase-pitr
```

An NBEC baseline can be supplied with `--backup-key-file`. Add `--archive-key-file /secure/pitr.key` to create an encrypted archive stream. PITR keys are raw 32-byte files; key bytes are never accepted directly on argv.

Initialization does not start archival automatically. It creates and verifies the archive namespace bound to that exact baseline.

## Runtime durability fence

Enable the already initialized stream on a clustered server with:

```bash
export NEURALBASE_PITR_ARCHIVE_DIR=/archive/neuralbase-pitr
export NEURALBASE_PITR_KEY_FILE=/secure/pitr.key   # encrypted streams only
export NEURALBASE_PITR_MAX_SEGMENTS=100000        # optional; default
```

`NEURALBASE_PITR_MAX_SEGMENTS` must be in `1..=1000000`.

When PITR archival is enabled, the confirmed-apply task orders one committed entry as:

1. deterministic durable replicated state-machine apply;
2. durable archive publication and independent record verification;
3. confirmed apply returned to Raft.

Therefore an archive publication failure is a confirmed-apply failure, not a warning. The normal non-PITR path remains unchanged when `NEURALBASE_PITR_ARCHIVE_DIR` is unset.

Startup opens and verifies the complete stream. It fails closed if the archive baseline/frontier is newer than durable logical apply, if the configured segment limit is already exceeded, or if a persisted compacted Raft snapshot is ahead of the verified archive frontier.

## Inspect recovery targets

```bash
target/release/neuralbase-pitr status \
  --archive /archive/neuralbase-pitr \
  --archive-key-file /secure/pitr.key

target/release/neuralbase-pitr verify \
  --archive /archive/neuralbase-pitr \
  --archive-key-file /secure/pitr.key

target/release/neuralbase-pitr targets \
  --archive /archive/neuralbase-pitr \
  --archive-key-file /secure/pitr.key
```

The supported targets are:

- `baseline`;
- `latest`;
- an exact unsigned committed Raft index inside the verified range.

A timestamp such as `2026-09-15T12:00:00Z` is not a valid Phase-9 target. Any time-to-index mapping must come from a separate trustworthy application/operator record.

## Recover

Fence the historical topology before disaster recovery and choose a fresh node ID. The destination must not exist.

```bash
target/release/neuralbase-pitr recover \
  --backup /backups/base.nbbk \
  --archive /archive/neuralbase-pitr \
  --target 4242 \
  --target-dir /var/lib/neuralbase/recovery-pitr \
  --node-id recovery-pitr \
  --archive-key-file /secure/pitr.key
```

Recovery verifies the baseline hash/metadata and requested target before publication. It restores into a hidden staging directory, sequentially verifies and replays required records through `ReplicatedSqlStateMachine`, reconstructs the selected historical membership, then establishes a fresh single-voter recovery generation whose active voter is the requested fresh recovery node. Historical source IDs are tombstoned. The staged database is closed/reopened and independently verified before an atomic target-directory publication.

Wrong baselines, missing/corrupt segments, incompatible versions, unavailable indexes and historical node-ID reuse fail before the requested destination becomes authoritative.

## Branch after an earlier-point recovery

Recovery to an earlier index discards the parent's later logical future. Before starting the recovered database and accepting new writes, create a new child archive timeline while the recovered database is stopped:

```bash
target/release/neuralbase-pitr branch \
  --parent-archive /archive/neuralbase-pitr \
  --parent-archive-key-file /secure/pitr.key \
  --branch-target 4242 \
  --db /var/lib/neuralbase/recovery-pitr \
  --baseline-out /backups/branch-4242.nbbk \
  --archive /archive/neuralbase-pitr-branch
```

The child metadata records `parent_timeline` and `branch_index` but receives a different timeline ID and its own verified baseline. Parent segments after the branch point cannot join the child chain; tests explicitly reject old-future injection.

Use the child archive directory when starting the recovered node. Starting first can append a current-term control/no-op position and move the database beyond the intended branch baseline.

## Retention and rollover

The runtime segment limit bounds one stream; it is not an instruction to delete old files. Rollover requires a verified replacement child whose baseline/branch is exactly the old durable frontier. Quiesce the old writer before retirement, then run:

```bash
target/release/neuralbase-pitr retire \
  --archive /archive/neuralbase-pitr \
  --replacement-archive /archive/neuralbase-pitr-next \
  --archive-key-file /secure/pitr.key \
  --replacement-archive-key-file /secure/pitr-next.key
```

Retirement atomically moves the old stream out of service, revalidates its metadata/segment sequence/frontier/staging state, and deletes it only after all retirement preconditions remain true. Ambiguity preserves data.

## Encryption

NBPE uses ChaCha20-Poly1305 authenticated encryption. Archive metadata records whether encryption is required and the external key ID; raw key bytes are not stored in metadata. Wrong keys or authenticated-byte tampering fail verification/recovery.

Archive encryption protects the archive segments only. Use NBEC independently if the baseline also requires confidentiality. Neither mechanism encrypts RocksDB or internal Raft persistence at rest.

## Runtime metrics

When the relevant paths execute, the runtime emits:

- `neuralbase_pitr_archive_appends_total`;
- `neuralbase_pitr_archive_failures_total{reason}`;
- `neuralbase_pitr_startup_rejections_total{reason}`;
- `neuralbase_pitr_archive_frontier_index`;
- `neuralbase_pitr_archive_segment_limit`.

These are diagnostics, not an automatic SLO/alerting system.

## Failure semantics and tested boundaries

Phase-9 evidence covers strict format/version handling, canonical membership bytes, corruption/truncation/trailing bytes, gap/overlap/hash/timeline failures, restart staging cleanup, corrupt finalized files, duplicate/conflicting and convergent same-record publication, encrypted wrong-key/tamper behavior, bounded directory enumeration, wrong-baseline failure before publication, fresh recovery generations, child old-future rejection, conservative retention, and real-process exact-target SQL/identity recovery plus branched restart.

The real-process test proves an earlier recovery target excludes later rows and a later password rotation, then creates a new child future and verifies it survives restart.

## Explicit non-goals

Phase 9 does not claim:

- timestamp/wall-clock recovery targets;
- automatic disaster detection, failover or recovery;
- automatic remote/object-store archive replication;
- production retention, key-rotation or archive-copy policy;
- a measured RPO/RTO SLA;
- production SQL HA.

Those boundaries remain explicit even when `CONFIDENCE.yaml` reports `pitr: true`: that flag means the exact committed-index Phase-9 contract only.
'''
Path("docs/PITR.md").write_text(pitr_doc)

# Cross-link in the docs index and runbook already handled above.

print("Phase-9 claim synchronization patch applied")
