// SPDX-License-Identifier: Apache-2.0
// Raft transport abstraction.
//
// Defines the Transport trait and three implementations:
//   - ChannelTransport   — in-process tokio channels; used by tests.
//   - TcpTransport       — real TCP with length-prefixed JSON framing.
//   - TlsTcpTransport    — TLS 1.3 mutual auth over TCP (#[cfg(feature="tls")]).
//
// Wire format (Tcp/TlsTcpTransport):
//   [4-byte big-endian length][JSON payload bytes]
//
// Production transports keep Raft's logical NodeId separate from the socket
// address used to reach that node.  This matters in Docker/Kubernetes where a
// stable Raft identity (for example "node2") is not itself a connectable
// host:port.  `listen_with_peers` accepts the explicit NodeId -> address map;
// the legacy `listen` API is retained and falls back to treating NodeId as an
// address for backward compatibility and focused transport tests.
//
// CONFIDENCE: raw=0.84 effective=0.76
// DEPENDS_ON: rpc
// [HUMAN REVIEW REQUIRED] — see REVIEW_REQUIRED.md §Raft

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

use crate::consensus::rpc::{NodeId, RaftMessage};

// ── Transport trait ────────────────────────────────────────────────────────

/// Abstraction over message delivery. `send` is fire-and-forget.
/// Replies are received via the node's own `recv_channel`.
#[async_trait::async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn send(&self, to: &NodeId, msg: RaftMessage);
    async fn recv(&self) -> Option<(NodeId, RaftMessage)>;
}

// ── ChannelTransport ───────────────────────────────────────────────────────

/// Shared in-memory bus for a single simulated cluster.
/// Each node has a named inbox; `ChannelTransport` for node X delivers
/// messages from X's inbox and sends to other nodes' inboxes.
pub type ChannelBus = Arc<Mutex<HashMap<NodeId, mpsc::Sender<(NodeId, RaftMessage)>>>>;

const CHANNEL_TRANSPORT_CAPACITY: usize = 4_096;
const TCP_TRANSPORT_INBOX_CAPACITY: usize = 4_096;

pub struct ChannelTransport {
    /// This node's ID (the sender identity).
    pub id: NodeId,
    bus: ChannelBus,
    rx: Arc<Mutex<mpsc::Receiver<(NodeId, RaftMessage)>>>,
}

impl ChannelTransport {
    /// Create a new channel bus.
    pub fn new_bus() -> ChannelBus {
        Arc::new(Mutex::new(HashMap::new()))
    }

    /// Register a new node on the bus and return its transport.
    pub async fn register(id: NodeId, bus: ChannelBus) -> Self {
        let (tx, rx) = mpsc::channel(CHANNEL_TRANSPORT_CAPACITY);
        bus.lock().await.insert(id.clone(), tx);
        Self {
            id,
            bus,
            rx: Arc::new(Mutex::new(rx)),
        }
    }
}

#[async_trait::async_trait]
impl Transport for ChannelTransport {
    async fn send(&self, to: &NodeId, msg: RaftMessage) {
        let tx = {
            let guard = self.bus.lock().await;
            guard.get(to).cloned()
        };
        if let Some(tx) = tx {
            let _ = tx.send((self.id.clone(), msg)).await;
        }
        // Drop and ignore if peer not yet registered or has gone away.
    }

    async fn recv(&self) -> Option<(NodeId, RaftMessage)> {
        self.rx.lock().await.recv().await
    }
}

// ── TcpTransport ───────────────────────────────────────────────────────────

/// Length-prefixed JSON TCP transport.
/// Each node listens on its own TCP port and connects to peers on demand.
pub struct TcpTransport {
    pub id: NodeId,
    peer_addrs: Arc<HashMap<NodeId, String>>,
    rx: Arc<Mutex<mpsc::Receiver<(NodeId, RaftMessage)>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    accept_task: Option<JoinHandle<()>>,
}

impl TcpTransport {
    /// Backward-compatible listener. Node IDs are treated as socket addresses.
    pub async fn listen(id: NodeId, addr: &str) -> std::io::Result<Self> {
        Self::listen_with_peers(id, addr, HashMap::new()).await
    }

    /// Start listening on `addr` with an explicit logical-peer address map.
    pub async fn listen_with_peers(
        id: NodeId,
        addr: &str,
        peer_addrs: HashMap<NodeId, String>,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(addr).await?;
        let (tx, rx) = mpsc::channel::<(NodeId, RaftMessage)>(TCP_TRANSPORT_INBOX_CAPACITY);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let tx_clone = tx.clone();
        let accept_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((stream, _peer)) => {
                                let tx2 = tx_clone.clone();
                                tokio::spawn(Self::accept_conn(stream, tx2));
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });
        Ok(Self {
            id,
            peer_addrs: Arc::new(peer_addrs),
            rx: Arc::new(Mutex::new(rx)),
            shutdown_tx: Some(shutdown_tx),
            accept_task: Some(accept_task),
        })
    }

    fn peer_addr(&self, to: &NodeId) -> String {
        self.peer_addrs
            .get(to)
            .cloned()
            .unwrap_or_else(|| to.clone())
    }

    async fn accept_conn(mut stream: TcpStream, tx: mpsc::Sender<(NodeId, RaftMessage)>) {
        while let Ok(Some(bytes)) = read_frame(&mut stream).await {
            if let Ok(envelope) = serde_json::from_slice::<(NodeId, RaftMessage)>(&bytes) {
                if tx.send(envelope).await.is_err() {
                    break;
                }
            }
        }
    }
}

impl Drop for TcpTransport {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(task) = self.accept_task.take() {
            task.abort();
        }
    }
}

#[async_trait::async_trait]
impl Transport for TcpTransport {
    async fn send(&self, to: &NodeId, msg: RaftMessage) {
        let addr = self.peer_addr(to);
        if let Ok(mut stream) = TcpStream::connect(&addr).await {
            if let Ok(payload) = serde_json::to_vec(&(self.id.clone(), msg)) {
                let _ = write_frame(&mut stream, &payload).await;
            }
        } else {
            tracing::debug!(peer = %to, %addr, "Raft TCP peer unavailable");
        }
    }

    async fn recv(&self) -> Option<(NodeId, RaftMessage)> {
        self.rx.lock().await.recv().await
    }
}

// ── Framing helpers ────────────────────────────────────────────────────────

/// Write a length-prefixed frame: [u32 BE length][payload].
async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(payload).await
}

/// Read a length-prefixed frame. Returns `None` on clean EOF.
async fn read_frame(stream: &mut TcpStream) -> std::io::Result<Option<Vec<u8>>> {
    let mut len_buf = [0u8; 4];
    match stream.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

#[cfg(feature = "tls")]
async fn write_frame_generic<W>(w: &mut W, payload: &[u8]) -> std::io::Result<()>
where
    W: tokio::io::AsyncWriteExt + Unpin,
{
    let len = payload.len() as u32;
    w.write_all(&len.to_be_bytes()).await?;
    w.write_all(payload).await
}

#[cfg(feature = "tls")]
async fn read_frame_generic<R>(r: &mut R) -> std::io::Result<Option<Vec<u8>>>
where
    R: tokio::io::AsyncReadExt + Unpin,
{
    let mut len_buf = [0u8; 4];
    match r.read_exact(&mut len_buf).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e),
    }
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(Some(buf))
}

// ── TlsTcpTransport (feature = "tls") ─────────────────────────────────────

#[cfg(feature = "tls")]
pub struct TlsTcpTransport {
    pub id: NodeId,
    peer_addrs: Arc<HashMap<NodeId, String>>,
    connector: tokio_rustls::TlsConnector,
    server_name: tokio_rustls::rustls::pki_types::ServerName<'static>,
    rx: Arc<Mutex<mpsc::Receiver<(NodeId, RaftMessage)>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    accept_task: Option<JoinHandle<()>>,
}

#[cfg(feature = "tls")]
impl TlsTcpTransport {
    /// Backward-compatible TLS listener. Node IDs are treated as addresses.
    pub async fn listen(id: NodeId, addr: &str) -> std::io::Result<Self> {
        Self::listen_with_peers(id, addr, HashMap::new()).await
    }

    /// Start a TLS Raft listener with an explicit logical-peer address map.
    pub async fn listen_with_peers(
        id: NodeId,
        addr: &str,
        peer_addrs: HashMap<NodeId, String>,
    ) -> std::io::Result<Self> {
        let acceptor = crate::tls::node_tls::build_raft_acceptor()?;
        let (connector, server_name) = crate::tls::node_tls::build_raft_connector()?;

        let listener = TcpListener::bind(addr).await?;
        let (tx, rx) = mpsc::channel::<(NodeId, RaftMessage)>(TCP_TRANSPORT_INBOX_CAPACITY);
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
        let tx_clone = tx.clone();

        let accept_task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accepted = listener.accept() => {
                        match accepted {
                            Ok((tcp, _peer)) => {
                                let acc = acceptor.clone();
                                let tx2 = tx_clone.clone();
                                tokio::spawn(async move {
                                    match acc.accept(tcp).await {
                                        Ok(mut tls) => {
                                            while let Ok(Some(bytes)) = read_frame_generic(&mut tls).await {
                                                if let Ok(envelope) =
                                                    serde_json::from_slice::<(NodeId, RaftMessage)>(&bytes)
                                                {
                                                    if tx2.send(envelope).await.is_err() {
                                                        break;
                                                    }
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            tracing::warn!(error = %e, "Raft TLS handshake failed");
                                        }
                                    }
                                });
                            }
                            Err(_) => break,
                        }
                    }
                }
            }
        });

        Ok(Self {
            id,
            peer_addrs: Arc::new(peer_addrs),
            connector,
            server_name,
            rx: Arc::new(Mutex::new(rx)),
            shutdown_tx: Some(shutdown_tx),
            accept_task: Some(accept_task),
        })
    }

    fn peer_addr(&self, to: &NodeId) -> String {
        self.peer_addrs
            .get(to)
            .cloned()
            .unwrap_or_else(|| to.clone())
    }
}

#[cfg(feature = "tls")]
impl Drop for TlsTcpTransport {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.accept_task.take() {
            task.abort();
        }
    }
}

#[cfg(feature = "tls")]
#[async_trait::async_trait]
impl Transport for TlsTcpTransport {
    async fn send(&self, to: &NodeId, msg: RaftMessage) {
        let addr = self.peer_addr(to);
        if let Ok(tcp) = TcpStream::connect(&addr).await {
            let sn = self.server_name.clone();
            match self.connector.connect(sn, tcp).await {
                Ok(mut tls) => {
                    if let Ok(payload) = serde_json::to_vec(&(self.id.clone(), msg)) {
                        let _ = write_frame_generic(&mut tls, &payload).await;
                    }
                }
                Err(e) => {
                    tracing::warn!(peer = %to, %addr, error = %e, "Raft TLS connect failed");
                }
            }
        } else {
            tracing::debug!(peer = %to, %addr, "Raft TLS peer unavailable");
        }
    }

    async fn recv(&self) -> Option<(NodeId, RaftMessage)> {
        self.rx.lock().await.recv().await
    }
}
