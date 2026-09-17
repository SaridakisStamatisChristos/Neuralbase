// SPDX-License-Identifier: Apache-2.0
// Raft transport abstraction.
//
// Defines three implementations:
//   - ChannelTransport   — in-process tokio channels; used by tests.
//   - TcpTransport       — persistent per-peer TCP writers with length-prefixed JSON framing.
//   - TlsTcpTransport    — persistent per-peer TLS 1.3 writers (feature = "tls").
//
// Wire format (Tcp/TlsTcpTransport):
//   [4-byte big-endian length][JSON payload bytes]
//
// Each production peer has one bounded ordered writer queue. The writer reuses
// its connection for successive Raft messages and reconnects when necessary.
// This removes the Phase-11 connect/handshake-per-message bottleneck while
// preserving Raft's per-peer message order and fire-and-forget Transport API.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::task::JoinHandle;

use crate::consensus::rpc::{NodeId, RaftMessage};

#[async_trait::async_trait]
pub trait Transport: Send + Sync + 'static {
    async fn send(&self, to: &NodeId, msg: RaftMessage);
    async fn recv(&self) -> Option<(NodeId, RaftMessage)>;
}

pub type ChannelBus = Arc<Mutex<HashMap<NodeId, mpsc::Sender<(NodeId, RaftMessage)>>>>;

const CHANNEL_TRANSPORT_CAPACITY: usize = 4_096;
const TCP_TRANSPORT_INBOX_CAPACITY: usize = 4_096;
const PEER_WRITER_CAPACITY: usize = 4_096;

pub struct ChannelTransport {
    pub id: NodeId,
    bus: ChannelBus,
    rx: Arc<Mutex<mpsc::Receiver<(NodeId, RaftMessage)>>>,
}

impl ChannelTransport {
    pub fn new_bus() -> ChannelBus {
        Arc::new(Mutex::new(HashMap::new()))
    }

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
    }

    async fn recv(&self) -> Option<(NodeId, RaftMessage)> {
        self.rx.lock().await.recv().await
    }
}

// ── TCP transport ──────────────────────────────────────────────────────────

pub struct TcpTransport {
    pub id: NodeId,
    peer_addrs: Arc<HashMap<NodeId, String>>,
    rx: Arc<Mutex<mpsc::Receiver<(NodeId, RaftMessage)>>>,
    writers: Arc<Mutex<HashMap<NodeId, mpsc::Sender<RaftMessage>>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    accept_task: Option<JoinHandle<()>>,
}

impl TcpTransport {
    pub async fn listen(id: NodeId, addr: &str) -> std::io::Result<Self> {
        Self::listen_with_peers(id, addr, HashMap::new()).await
    }

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
                                let _ = stream.set_nodelay(true);
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
            writers: Arc::new(Mutex::new(HashMap::new())),
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

    async fn writer_for(&self, to: &NodeId) -> mpsc::Sender<RaftMessage> {
        let mut writers = self.writers.lock().await;
        if let Some(writer) = writers.get(to) {
            return writer.clone();
        }

        let (tx, rx) = mpsc::channel(PEER_WRITER_CAPACITY);
        let local_id = self.id.clone();
        let peer_id = to.clone();
        let addr = self.peer_addr(to);
        tokio::spawn(run_tcp_peer_writer(local_id, peer_id.clone(), addr, rx));
        writers.insert(peer_id, tx.clone());
        tx
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
        let writer = self.writer_for(to).await;
        if writer.send(msg).await.is_err() {
            // The writer only exits when its receiver is closed, but remove a
            // stale sender defensively so the next message can recreate it.
            self.writers.lock().await.remove(to);
            tracing::debug!(peer = %to, "Raft TCP writer unavailable");
        }
    }

    async fn recv(&self) -> Option<(NodeId, RaftMessage)> {
        self.rx.lock().await.recv().await
    }
}

async fn connect_tcp(addr: &str) -> std::io::Result<TcpStream> {
    let stream = TcpStream::connect(addr).await?;
    stream.set_nodelay(true)?;
    Ok(stream)
}

async fn run_tcp_peer_writer(
    local_id: NodeId,
    peer_id: NodeId,
    addr: String,
    mut rx: mpsc::Receiver<RaftMessage>,
) {
    let mut stream: Option<TcpStream> = None;

    while let Some(msg) = rx.recv().await {
        let payload = match serde_json::to_vec(&(local_id.clone(), msg)) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::warn!(peer = %peer_id, error = %error, "failed to encode Raft message");
                continue;
            }
        };

        if stream.is_none() {
            match connect_tcp(&addr).await {
                Ok(connected) => stream = Some(connected),
                Err(error) => {
                    tracing::debug!(peer = %peer_id, %addr, error = %error, "Raft TCP peer unavailable");
                    continue;
                }
            }
        }

        let write_result =
            write_frame(stream.as_mut().expect("stream established"), &payload).await;
        if let Err(first_error) = write_result {
            // A peer restart commonly invalidates an otherwise healthy persistent
            // socket. Reconnect once and retry this Raft message. Raft tolerates
            // duplicate RPC delivery if the first write happened to reach the peer.
            stream = None;
            match connect_tcp(&addr).await {
                Ok(mut connected) => match write_frame(&mut connected, &payload).await {
                    Ok(()) => stream = Some(connected),
                    Err(error) => {
                        tracing::debug!(peer = %peer_id, %addr, error = %error, "Raft TCP retry failed");
                    }
                },
                Err(error) => {
                    tracing::debug!(peer = %peer_id, %addr, error = %error, first_error = %first_error, "Raft TCP reconnect failed");
                }
            }
        }
    }
}

// ── Framing helpers ────────────────────────────────────────────────────────

async fn write_frame(stream: &mut TcpStream, payload: &[u8]) -> std::io::Result<()> {
    let len = payload.len() as u32;
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(payload).await
}

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

// ── TLS transport ──────────────────────────────────────────────────────────

#[cfg(feature = "tls")]
pub struct TlsTcpTransport {
    pub id: NodeId,
    peer_addrs: Arc<HashMap<NodeId, String>>,
    connector: tokio_rustls::TlsConnector,
    server_name: tokio_rustls::rustls::pki_types::ServerName<'static>,
    rx: Arc<Mutex<mpsc::Receiver<(NodeId, RaftMessage)>>>,
    writers: Arc<Mutex<HashMap<NodeId, mpsc::Sender<RaftMessage>>>>,
    shutdown_tx: Option<oneshot::Sender<()>>,
    accept_task: Option<JoinHandle<()>>,
}

#[cfg(feature = "tls")]
impl TlsTcpTransport {
    pub async fn listen(id: NodeId, addr: &str) -> std::io::Result<Self> {
        Self::listen_with_peers(id, addr, HashMap::new()).await
    }

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
                                let _ = tcp.set_nodelay(true);
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
            writers: Arc::new(Mutex::new(HashMap::new())),
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

    async fn writer_for(&self, to: &NodeId) -> mpsc::Sender<RaftMessage> {
        let mut writers = self.writers.lock().await;
        if let Some(writer) = writers.get(to) {
            return writer.clone();
        }
        let (tx, rx) = mpsc::channel(PEER_WRITER_CAPACITY);
        let local_id = self.id.clone();
        let peer_id = to.clone();
        let addr = self.peer_addr(to);
        let connector = self.connector.clone();
        let server_name = self.server_name.clone();
        tokio::spawn(run_tls_peer_writer(
            local_id,
            peer_id.clone(),
            addr,
            connector,
            server_name,
            rx,
        ));
        writers.insert(peer_id, tx.clone());
        tx
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
        let writer = self.writer_for(to).await;
        if writer.send(msg).await.is_err() {
            self.writers.lock().await.remove(to);
            tracing::debug!(peer = %to, "Raft TLS writer unavailable");
        }
    }

    async fn recv(&self) -> Option<(NodeId, RaftMessage)> {
        self.rx.lock().await.recv().await
    }
}

#[cfg(feature = "tls")]
async fn connect_tls(
    addr: &str,
    connector: &tokio_rustls::TlsConnector,
    server_name: tokio_rustls::rustls::pki_types::ServerName<'static>,
) -> std::io::Result<tokio_rustls::client::TlsStream<TcpStream>> {
    let tcp = TcpStream::connect(addr).await?;
    tcp.set_nodelay(true)?;
    connector
        .connect(server_name, tcp)
        .await
        .map_err(std::io::Error::other)
}

#[cfg(feature = "tls")]
async fn run_tls_peer_writer(
    local_id: NodeId,
    peer_id: NodeId,
    addr: String,
    connector: tokio_rustls::TlsConnector,
    server_name: tokio_rustls::rustls::pki_types::ServerName<'static>,
    mut rx: mpsc::Receiver<RaftMessage>,
) {
    let mut stream: Option<tokio_rustls::client::TlsStream<TcpStream>> = None;

    while let Some(msg) = rx.recv().await {
        let payload = match serde_json::to_vec(&(local_id.clone(), msg)) {
            Ok(payload) => payload,
            Err(error) => {
                tracing::warn!(peer = %peer_id, error = %error, "failed to encode Raft TLS message");
                continue;
            }
        };

        if stream.is_none() {
            match connect_tls(&addr, &connector, server_name.clone()).await {
                Ok(connected) => stream = Some(connected),
                Err(error) => {
                    tracing::debug!(peer = %peer_id, %addr, error = %error, "Raft TLS peer unavailable");
                    continue;
                }
            }
        }

        let write_result =
            write_frame_generic(stream.as_mut().expect("TLS stream established"), &payload).await;
        if let Err(first_error) = write_result {
            stream = None;
            match connect_tls(&addr, &connector, server_name.clone()).await {
                Ok(mut connected) => match write_frame_generic(&mut connected, &payload).await {
                    Ok(()) => stream = Some(connected),
                    Err(error) => {
                        tracing::debug!(peer = %peer_id, %addr, error = %error, "Raft TLS retry failed");
                    }
                },
                Err(error) => {
                    tracing::debug!(peer = %peer_id, %addr, error = %error, first_error = %first_error, "Raft TLS reconnect failed");
                }
            }
        }
    }
}
