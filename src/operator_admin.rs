// SPDX-License-Identifier: Apache-2.0
//! Opt-in Unix management socket for a trusted same-user process controller.
use crate::consensus::operator_control::{GuardedMembership, MembershipGuard, OperatorStatus};
use crate::operator::{DesiredTopology, MAX_OPERATOR_INPUT};
use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ManagedNode {
    pub topology: DesiredTopology,
    pub id: String,
    pub seeds: Vec<String>,
    pub learner: bool,
    pub socket: String,
}
impl ManagedNode {
    pub fn validate(&self) -> Result<(), String> {
        self.topology.validate()?;
        let prefix = format!("{}.", self.topology.cluster);
        if !self.topology.endpoints.contains_key(&self.id)
            || self
                .topology
                .endpoints
                .keys()
                .any(|id| !id.starts_with(&prefix))
            || self.seeds.is_empty()
            || self.seeds.len() > 128
            || self
                .seeds
                .iter()
                .any(|id| !self.topology.endpoints.contains_key(id))
            || self
                .seeds
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.seeds.len()
            || (self.learner && self.seeds.contains(&self.id))
            || (!self.learner && !self.seeds.contains(&self.id))
        {
            return Err("invalid managed incarnation or bootstrap seeds".into());
        }
        if self.socket.len() > 100 || !std::path::Path::new(&self.socket).is_absolute() {
            return Err("socket must be absolute and at most 100 bytes".into());
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminRequest {
    pub cluster: String,
    pub command: AdminCommand,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AdminCommand {
    Status,
    Observe,
    Membership(GuardedMembership),
    Transfer {
        guard: MembershipGuard,
        target: String,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminResponse {
    pub pid: u32,
    pub node: ManagedNode,
    pub status: Option<OperatorStatus>,
    pub index: Option<u64>,
    pub error: Option<String>,
}
#[cfg(unix)]
mod local {
    use super::*;
    use crate::consensus::operator_control::OperatorHandle;
    use std::io::{self, Read, Write};
    use std::os::unix::fs::{FileTypeExt, PermissionsExt};
    use std::path::Path;
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{UnixListener, UnixStream};
    pub fn read_config(path: &Path) -> io::Result<ManagedNode> {
        let mut bytes = Vec::new();
        std::fs::File::open(path)?
            .take((MAX_OPERATOR_INPUT + 1) as u64)
            .read_to_end(&mut bytes)?;
        if bytes.len() > MAX_OPERATOR_INPUT {
            return Err(io::Error::other("managed config too large"));
        }
        let node: ManagedNode = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        node.validate().map_err(io::Error::other)?;
        Ok(node)
    }
    /// Bind a fresh directory to an immutable incarnation before RocksDB opens.
    /// A partial marker fails closed. Unmanaged/restored data is never adopted.
    pub fn bind_storage(node: &ManagedNode, db: &Path) -> io::Result<()> {
        let marker = db.join("operator-incarnation-v1.json");
        if marker.exists() {
            if &read_config(&marker)? != node {
                return Err(io::Error::other(
                    "managed storage incarnation/configuration mismatch",
                ));
            }
            return Ok(());
        }
        if db.exists() && std::fs::read_dir(db)?.next().is_some() {
            return Err(io::Error::other(
                "nonempty unmanaged storage cannot be adopted",
            ));
        }
        std::fs::create_dir_all(db)?;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(marker)?;
        file.write_all(&serde_json::to_vec(node).map_err(io::Error::other)?)?;
        file.sync_all()?;
        std::fs::File::open(db)?.sync_all()?;
        Ok(())
    }
    async fn handle(
        mut stream: UnixStream,
        node: &ManagedNode,
        raft: &OperatorHandle,
    ) -> io::Result<()> {
        let size = stream.read_u32().await? as usize;
        if size > 8192 {
            return Err(io::Error::other("admin request too large"));
        }
        let mut bytes = vec![0; size];
        stream.read_exact(&mut bytes).await?;
        let request: AdminRequest = serde_json::from_slice(&bytes).map_err(io::Error::other)?;
        let mut response = AdminResponse {
            pid: std::process::id(),
            node: node.clone(),
            status: None,
            index: None,
            error: None,
        };
        let result = if request.cluster != node.topology.cluster {
            Err("wrong management incarnation".into())
        } else {
            match request.command {
                AdminCommand::Status => raft.status().await.map(|s| response.status = Some(s)),
                AdminCommand::Observe => raft
                    .observe_authoritative()
                    .await
                    .map(|s| response.status = Some(s)),
                AdminCommand::Membership(r) => raft
                    .change_membership(r)
                    .await
                    .map(|i| response.index = Some(i)),
                AdminCommand::Transfer { guard, target } => raft.transfer(guard, target).await,
            }
        };
        if let Err(e) = result {
            response.error = Some(e);
        }
        let bytes = serde_json::to_vec(&response).map_err(io::Error::other)?;
        if bytes.len() > MAX_OPERATOR_INPUT {
            return Err(io::Error::other("admin response too large"));
        }
        stream.write_u32(bytes.len() as u32).await?;
        stream.write_all(&bytes).await?;
        Ok(())
    }
    /// Call after acquiring the exclusive RocksDB lock for this incarnation.
    pub fn spawn(
        node: ManagedNode,
        raft: OperatorHandle,
    ) -> io::Result<tokio::task::JoinHandle<()>> {
        let socket = Path::new(&node.socket);
        let parent = socket
            .parent()
            .ok_or_else(|| io::Error::other("socket parent missing"))?;
        let m = std::fs::symlink_metadata(parent)?;
        if !m.is_dir() || m.permissions().mode() & 0o077 != 0 {
            return Err(io::Error::other(
                "management socket requires private 0700 directory",
            ));
        }
        match std::fs::symlink_metadata(socket) {
            Ok(m) if m.file_type().is_socket() => std::fs::remove_file(socket)?,
            Ok(_) => return Err(io::Error::other("refusing to replace non-socket path")),
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e),
        }
        let listener = UnixListener::bind(socket)?;
        std::fs::set_permissions(socket, std::fs::Permissions::from_mode(0o600))?;
        Ok(tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                // One request at a time bounds task count; total deadline bounds
                // stalled readers and uncertain Raft waits independently.
                let _ = tokio::time::timeout(Duration::from_secs(12), handle(stream, &node, &raft))
                    .await;
            }
        }))
    }
}
#[cfg(unix)]
pub use local::{bind_storage, read_config, spawn};
