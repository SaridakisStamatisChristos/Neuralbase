// SPDX-License-Identifier: Apache-2.0
// PostgreSQL-compatible authentication for NeuralBase.
//
// Authentication methods (RFC 5802 + PostgreSQL wire protocol):
//   • SCRAM-SHA-256 — primary, never stores plaintext passwords
//   • MD5            — legacy fallback for pre-PostgreSQL-14 clients
//
// Auth is opt-in: set NEURALBASE_AUTH_REQUIRED=1 to enforce it.
// When unset the server accepts all connections (dev/test mode).
//
// Stored credential format (users.json):
//   SCRAM: salt + iterations + stored_key + server_key  (derived via PBKDF2)
//   MD5:   hex(md5(password || username))               (no "md5" prefix)
//
// Per-IP connection tracking lives here too (IpConnectionTracker).
//
// CONFIDENCE: raw=0.72  effective=0.68
// DEPENDS_ON: sha2, hmac, pbkdf2, base64, md5, hex, serde_json, rand
// RISK: SCRAM state machine — see REVIEW_REQUIRED.md §Authentication

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use hmac::{Hmac, Mac};
use pbkdf2::pbkdf2_hmac;
use rand::Rng;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::{Arc, Mutex};

type HmacSha256 = Hmac<Sha256>;

// ── Errors ─────────────────────────────────────────────────────────────────────

#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    #[error("authentication failed")]
    Denied,
    #[error("unknown user: {0}")]
    UnknownUser(String),
    #[error("invalid auth message: {0}")]
    InvalidMessage(String),
}

// ── Stored credentials ─────────────────────────────────────────────────────────

/// Keys stored server-side for one SCRAM-SHA-256 user.
/// These are derived from the password at registration time and never allow
/// recovery of the plaintext password.
#[derive(Debug, Clone)]
pub struct ScramKeys {
    /// Random 16-byte salt, base64-encoded.
    pub salt_b64: String,
    pub iterations: u32,
    /// SHA-256(HMAC-SHA-256(SaltedPassword, "Client Key")) — 32 bytes.
    pub stored_key: [u8; 32],
    /// HMAC-SHA-256(SaltedPassword, "Server Key") — 32 bytes.
    pub server_key: [u8; 32],
}

#[derive(Debug, Clone)]
pub enum StoredCredential {
    ScramSha256(ScramKeys),
    /// hex(md5(password || username)) — no "md5" prefix.
    /// Sufficient to answer the MD5 challenge without storing plaintext.
    Md5 {
        password_hash: String,
    },
}

#[derive(Debug, Clone)]
pub struct UserRecord {
    pub username: String,
    pub credential: StoredCredential,
}

// ── User registry ───────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct UserRegistry {
    users: HashMap<String, UserRecord>,
    /// If true, every connection must authenticate.
    pub require_auth: bool,
}

impl Default for UserRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl UserRegistry {
    /// Create an empty registry.
    /// `require_auth` is read from NEURALBASE_AUTH_REQUIRED environment variable.
    pub fn new() -> Self {
        let require_auth = std::env::var("NEURALBASE_AUTH_REQUIRED")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        Self {
            users: HashMap::new(),
            require_auth,
        }
    }

    /// Load from a JSON file.  Missing or malformed file → empty (non-fatal).
    /// Expected format:
    /// {"users": [{"username": "alice", "method": "scram-sha-256", ...}, ...]}
    pub fn load_from_file(path: &str) -> Self {
        let mut registry = Self::new();
        let Ok(content) = std::fs::read_to_string(path) else {
            return registry;
        };
        let Ok(doc) = serde_json::from_str::<UserRegistryFile>(&content) else {
            tracing::warn!(path, "users.json could not be parsed; using empty registry");
            return registry;
        };
        for entry in doc.users {
            let rec = match entry {
                UserRecordFile::ScramSha256 {
                    username,
                    salt,
                    iterations,
                    stored_key,
                    server_key,
                } => {
                    let sk_bytes = match BASE64.decode(stored_key) {
                        Ok(v) if v.len() == 32 => v,
                        _ => continue,
                    };
                    let vk_bytes = match BASE64.decode(server_key) {
                        Ok(v) if v.len() == 32 => v,
                        _ => continue,
                    };
                    let mut sk = [0u8; 32];
                    let mut vk = [0u8; 32];
                    sk.copy_from_slice(&sk_bytes);
                    vk.copy_from_slice(&vk_bytes);
                    UserRecord {
                        username: username.clone(),
                        credential: StoredCredential::ScramSha256(ScramKeys {
                            salt_b64: salt,
                            iterations,
                            stored_key: sk,
                            server_key: vk,
                        }),
                    }
                }
                UserRecordFile::Md5 { username, md5_hash } => UserRecord {
                    username: username.clone(),
                    credential: StoredCredential::Md5 {
                        password_hash: md5_hash,
                    },
                },
            };
            registry.users.insert(rec.username.clone(), rec);
        }
        tracing::info!(count = registry.users.len(), path, "loaded user registry");
        registry
    }

    fn to_file_model(&self) -> UserRegistryFile {
        let mut users: Vec<UserRecordFile> = self
            .users
            .values()
            .map(|rec| match &rec.credential {
                StoredCredential::ScramSha256(keys) => UserRecordFile::ScramSha256 {
                    username: rec.username.clone(),
                    salt: keys.salt_b64.clone(),
                    iterations: keys.iterations,
                    stored_key: BASE64.encode(keys.stored_key),
                    server_key: BASE64.encode(keys.server_key),
                },
                StoredCredential::Md5 { password_hash } => UserRecordFile::Md5 {
                    username: rec.username.clone(),
                    md5_hash: password_hash.clone(),
                },
            })
            .collect();

        users.sort_by(|a, b| {
            let an = match a {
                UserRecordFile::ScramSha256 { username, .. } => username,
                UserRecordFile::Md5 { username, .. } => username,
            };
            let bn = match b {
                UserRecordFile::ScramSha256 { username, .. } => username,
                UserRecordFile::Md5 { username, .. } => username,
            };
            an.cmp(bn)
        });

        UserRegistryFile { users }
    }

    /// Atomically persist the registry. If persistence fails, restore the
    /// in-memory users from the last durable file so SQL user DDL cannot
    /// report an error while leaving a transient mutation active.
    pub fn save_to_file(&mut self, path: &str) -> std::io::Result<()> {
        use std::io::Write;

        let persisted_users = Self::load_from_file(path).users;
        let doc = self.to_file_model();
        let json = serde_json::to_vec_pretty(&doc)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
        let tmp_path = format!("{path}.tmp");

        let persist_result = (|| -> std::io::Result<()> {
            let mut options = std::fs::OpenOptions::new();
            options.create(true).truncate(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }

            let mut file = options.open(&tmp_path)?;
            file.write_all(&json)?;
            file.sync_all()?;

            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                std::fs::set_permissions(&tmp_path, std::fs::Permissions::from_mode(0o600))?;
            }

            std::fs::rename(&tmp_path, path)?;
            Ok(())
        })();

        if let Err(error) = persist_result {
            self.users = persisted_users;
            let _ = std::fs::remove_file(&tmp_path);
            return Err(error);
        }

        Ok(())
    }

    pub fn get_user(&self, username: &str) -> Option<&UserRecord> {
        self.users.get(username)
    }

    pub fn add_user(&mut self, record: UserRecord) {
        self.users.insert(record.username.clone(), record);
    }

    pub fn remove_user(&mut self, username: &str) -> bool {
        self.users.remove(username).is_some()
    }

    /// Update an existing user's credential. Returns false if user not found.
    pub fn update_user(&mut self, record: UserRecord) -> bool {
        if self.users.contains_key(&record.username) {
            self.users.insert(record.username.clone(), record);
            true
        } else {
            false
        }
    }
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
struct UserRegistryFile {
    users: Vec<UserRecordFile>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize)]
#[serde(tag = "method")]
enum UserRecordFile {
    #[serde(rename = "scram-sha-256")]
    ScramSha256 {
        username: String,
        salt: String,
        iterations: u32,
        stored_key: String,
        server_key: String,
    },
    #[serde(rename = "md5")]
    Md5 { username: String, md5_hash: String },
}

// ── Key derivation ─────────────────────────────────────────────────────────────

/// Derive (stored_key, server_key) from a plaintext password using PBKDF2-HMAC-SHA-256.
/// Used when registering a new user or changing a password.
pub fn derive_scram_keys(password: &str, salt: &[u8], iterations: u32) -> ([u8; 32], [u8; 32]) {
    let mut salted = [0u8; 32];
    pbkdf2_hmac::<Sha256>(password.as_bytes(), salt, iterations, &mut salted);
    let client_key = hmac_sha256(&salted, b"Client Key");
    let stored_key = sha256(&client_key);
    let server_key = hmac_sha256(&salted, b"Server Key");
    (stored_key, server_key)
}

/// Create a SCRAM-SHA-256 UserRecord from a plaintext password (for CREATE USER SQL).
pub fn create_scram_user(username: &str, password: &str) -> UserRecord {
    let salt: [u8; 16] = rand::thread_rng().gen();
    let (stored_key, server_key) = derive_scram_keys(password, &salt, 4096);
    UserRecord {
        username: username.to_string(),
        credential: StoredCredential::ScramSha256(ScramKeys {
            salt_b64: BASE64.encode(salt),
            iterations: 4096,
            stored_key,
            server_key,
        }),
    }
}

/// Create an MD5 UserRecord from plaintext credentials (legacy / ALTER USER path).
pub fn create_md5_user(username: &str, password: &str) -> UserRecord {
    use md5::Md5;
    let inner = format!("{password}{username}");
    let mut hasher = Md5::new();
    hasher.update(inner.as_bytes());
    let hash = hasher.finalize();
    UserRecord {
        username: username.to_string(),
        credential: StoredCredential::Md5 {
            password_hash: hex::encode(hash),
        },
    }
}

// ── MD5 server authenticator ───────────────────────────────────────────────────

/// Server-side state for the PostgreSQL MD5 challenge/response.
pub struct Md5State {
    password_hash: String, // hex(md5(password || username))
    pub salt: [u8; 4],
}

impl Md5State {
    pub fn new(password_hash: String) -> Self {
        let salt: [u8; 4] = rand::thread_rng().gen();
        Self {
            password_hash,
            salt,
        }
    }

    /// Verify a client's PasswordMessage response.
    /// Expected format: "md5" + hex(md5(inner_hash || hex(salt)))
    pub fn verify(&self, response: &str) -> bool {
        use md5::Md5;
        let outer = format!("{}{}", self.password_hash, hex::encode(self.salt));
        let mut hasher = Md5::new();
        hasher.update(outer.as_bytes());
        let expected = format!("md5{}", hex::encode(hasher.finalize()));
        response == expected
    }
}

// ── SCRAM-SHA-256 server authenticator ────────────────────────────────────────

/// Server-side SCRAM-SHA-256 authenticator implementing the RFC 5802 server flow.
///
/// State machine:
///   1. `new(keys)`                           — initialise with stored keys
///   2. `process_client_first(msg)`           — parse client-first, return server-first
///   3. `process_client_final(msg)`           → verify proof, return server-signature
pub struct ScramServer {
    keys: ScramKeys,
    server_nonce: String,
    client_first_bare: Option<String>,
    server_first: Option<String>,
}

impl ScramServer {
    pub fn new(keys: ScramKeys) -> Self {
        let nonce_bytes: [u8; 18] = rand::thread_rng().gen();
        Self {
            keys,
            server_nonce: BASE64.encode(nonce_bytes),
            client_first_bare: None,
            server_first: None,
        }
    }

    /// Process the client-first-message (full, including GS2 header).
    /// Returns the server-first-message to send via AuthenticationSASLContinue.
    pub fn process_client_first(&mut self, client_first: &str) -> Result<String, AuthError> {
        let bare = strip_gs2_header(client_first)?;
        let client_nonce = bare
            .split(',')
            .find_map(|p| p.strip_prefix("r="))
            .ok_or_else(|| AuthError::InvalidMessage("no r= attribute in client-first".into()))?;

        let server_first = format!(
            "r={}{},s={},i={}",
            client_nonce, self.server_nonce, self.keys.salt_b64, self.keys.iterations
        );
        self.client_first_bare = Some(bare.to_string());
        self.server_first = Some(server_first.clone());
        Ok(server_first)
    }

    /// Process the client-final-message.  Returns the server-signature (base64)
    /// to include in the AuthenticationSASLFinal `v=<sig>` attribute.
    /// Returns `Err(AuthError::Denied)` when the proof does not verify.
    pub fn process_client_final(&self, client_final: &str) -> Result<String, AuthError> {
        let first_bare = self.client_first_bare.as_deref().ok_or_else(|| {
            AuthError::InvalidMessage("no prior process_client_first call".into())
        })?;
        let server_first = self.server_first.as_deref().ok_or_else(|| {
            AuthError::InvalidMessage("no prior process_client_first call".into())
        })?;

        // Separate proof from the rest of client-final.
        let proof_sep = ",p=";
        let split = client_final
            .rfind(proof_sep)
            .ok_or_else(|| AuthError::InvalidMessage("no p= in client-final".into()))?;
        let without_proof = &client_final[..split];
        let proof_b64 = &client_final[split + proof_sep.len()..];

        // AuthMessage = client-first-message-bare "," server-first-message "," client-final-without-proof
        let auth_msg = format!("{first_bare},{server_first},{without_proof}");

        // ClientSignature = HMAC(StoredKey, AuthMessage)
        let client_sig = hmac_sha256(&self.keys.stored_key, auth_msg.as_bytes());

        // Decode and verify client proof length.
        let proof_bytes = BASE64
            .decode(proof_b64)
            .map_err(|_| AuthError::InvalidMessage("proof is not valid base64".into()))?;
        if proof_bytes.len() != 32 {
            return Err(AuthError::InvalidMessage(format!(
                "proof length {} != 32",
                proof_bytes.len()
            )));
        }

        // ClientKey = ClientProof XOR ClientSignature
        let mut client_key = [0u8; 32];
        for i in 0..32 {
            client_key[i] = proof_bytes[i] ^ client_sig[i];
        }

        // Verify: SHA-256(ClientKey) must equal StoredKey
        if sha256(&client_key) != self.keys.stored_key {
            return Err(AuthError::Denied);
        }

        // ServerSignature = HMAC(ServerKey, AuthMessage)
        let server_sig = hmac_sha256(&self.keys.server_key, auth_msg.as_bytes());
        Ok(BASE64.encode(server_sig))
    }
}

/// Strip the GS2 channel-binding header from a client-first-message.
/// Supports "n,," (no channel binding) and "y,," (signal support but not use).
fn strip_gs2_header(msg: &str) -> Result<&str, AuthError> {
    if let Some(b) = msg.strip_prefix("n,,").or_else(|| msg.strip_prefix("y,,")) {
        return Ok(b);
    }
    // Generic: find position after second comma.
    let mut commas = 0usize;
    for (i, c) in msg.char_indices() {
        if c == ',' {
            commas += 1;
            if commas == 2 {
                return Ok(&msg[i + 1..]);
            }
        }
    }
    Err(AuthError::InvalidMessage(
        "invalid or missing GS2 header".into(),
    ))
}

// ── Crypto primitives ──────────────────────────────────────────────────────────

pub(crate) fn hmac_sha256(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC accepts any key length");
    mac.update(data);
    let bytes = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    out
}

pub(crate) fn sha256(data: &[u8]) -> [u8; 32] {
    let bytes = Sha256::digest(data);
    let mut out = [0u8; 32];
    out.copy_from_slice(&bytes);
    out
}

// ── Per-IP connection tracker ──────────────────────────────────────────────────

/// Tracks active connection counts per source IP.
/// Limit is set via NEURALBASE_MAX_CONNECTIONS_PER_IP (default: unlimited).
#[derive(Clone)]
pub struct IpConnectionTracker {
    inner: Arc<Mutex<HashMap<IpAddr, usize>>>,
    pub max_per_ip: usize,
}

impl IpConnectionTracker {
    pub fn new(max_per_ip: usize) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            max_per_ip,
        }
    }

    /// Try to acquire a slot for `ip`.
    /// Returns `true` (slot reserved) or `false` (over limit, slot NOT reserved).
    pub fn try_acquire(&self, ip: IpAddr) -> bool {
        let mut g = self.inner.lock().unwrap();
        let c = g.entry(ip).or_insert(0);
        if *c >= self.max_per_ip {
            return false;
        }
        *c += 1;
        true
    }

    /// Release a slot for `ip` (call on connection close).
    pub fn release(&self, ip: IpAddr) {
        let mut g = self.inner.lock().unwrap();
        if let Some(c) = g.get_mut(&ip) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                g.remove(&ip);
            }
        }
    }

    /// Active connection count for an IP (used for metrics and tests).
    pub fn count_for(&self, ip: IpAddr) -> usize {
        *self.inner.lock().unwrap().get(&ip).unwrap_or(&0)
    }
}

/// Read max-per-IP from NEURALBASE_MAX_CONNECTIONS_PER_IP.
/// Default = usize::MAX (unlimited) — set the env var to enable per-IP throttling.
pub fn read_max_per_ip() -> usize {
    std::env::var("NEURALBASE_MAX_CONNECTIONS_PER_IP")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(usize::MAX)
}

// ── Unit tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // Create an MD5 user (test helper).
    fn md5_user(username: &str, password: &str) -> UserRecord {
        create_md5_user(username, password)
    }

    #[test]
    fn user_registry_persistence_round_trips_scram_and_md5() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("users.json");
        let path = path.to_str().unwrap();

        let mut reg = UserRegistry::new();
        reg.add_user(create_scram_user("alice", "secret"));
        reg.add_user(create_md5_user("bob", "legacy"));
        reg.save_to_file(path).unwrap();

        let loaded = UserRegistry::load_from_file(path);
        assert!(matches!(
            &loaded.get_user("alice").unwrap().credential,
            StoredCredential::ScramSha256(_)
        ));
        assert!(matches!(
            &loaded.get_user("bob").unwrap().credential,
            StoredCredential::Md5 { .. }
        ));
    }

    #[test]
    fn user_registry_save_failure_restores_last_durable_state() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("users.json");
        let path_str = path.to_str().unwrap();

        let mut reg = UserRegistry::new();
        reg.add_user(create_scram_user("durable", "secret"));
        reg.save_to_file(path_str).unwrap();

        let tmp_path = format!("{path_str}.tmp");
        std::fs::create_dir(&tmp_path).unwrap();
        reg.add_user(create_scram_user("transient", "secret"));

        assert!(reg.save_to_file(path_str).is_err());
        assert!(reg.get_user("durable").is_some());
        assert!(reg.get_user("transient").is_none());

        let durable = UserRegistry::load_from_file(path_str);
        assert!(durable.get_user("durable").is_some());
        assert!(durable.get_user("transient").is_none());
        std::fs::remove_dir(&tmp_path).unwrap();
    }

    #[test]
    fn user_registry_serialization_is_deterministic() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("users.json");
        let path_str = path.to_str().unwrap();

        let mut reg = UserRegistry::new();
        reg.add_user(create_md5_user("zeta", "z"));
        reg.add_user(create_md5_user("alpha", "a"));
        reg.save_to_file(path_str).unwrap();

        let json = std::fs::read_to_string(path_str).unwrap();
        assert!(json.find("alpha").unwrap() < json.find("zeta").unwrap());
    }

    #[test]
    fn user_registry_missing_parent_failure_rolls_back_memory() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("missing").join("users.json");
        let path_str = path.to_str().unwrap();

        let mut reg = UserRegistry::new();
        reg.add_user(create_scram_user("transient", "secret"));
        assert!(reg.save_to_file(path_str).is_err());
        assert!(reg.get_user("transient").is_none());
    }
    #[test]
    fn scram_key_derivation_is_deterministic() {
        let salt = b"fixed_salt_01234";
        let (sk1, vk1) = derive_scram_keys("secret", salt, 4096);
        let (sk2, vk2) = derive_scram_keys("secret", salt, 4096);
        assert_eq!(sk1, sk2);
        assert_eq!(vk1, vk2);
    }

    #[test]
    fn scram_key_derivation_differs_by_password() {
        let salt = b"fixed_salt_01234";
        let (sk1, _) = derive_scram_keys("password1", salt, 4096);
        let (sk2, _) = derive_scram_keys("password2", salt, 4096);
        assert_ne!(sk1, sk2);
    }

    #[test]
    fn scram_keys_stored_key_is_sha256_of_client_key() {
        let salt = b"fixed_salt_01234";
        let mut salted = [0u8; 32];
        pbkdf2_hmac::<Sha256>(b"secret", salt, 4096, &mut salted);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let expected_stored = sha256(&client_key);
        let (stored_key, _) = derive_scram_keys("secret", salt, 4096);
        assert_eq!(stored_key, expected_stored);
    }

    #[test]
    fn md5_auth_correct_password() {
        let user = md5_user("alice", "hunter2");
        let StoredCredential::Md5 { password_hash } = &user.credential else {
            panic!("expected Md5 credential");
        };
        let state = Md5State::new(password_hash.clone());
        // Compute expected response exactly as a client would.
        use md5::Md5;
        let outer = format!("{}{}", password_hash, hex::encode(state.salt));
        let mut h = Md5::new();
        h.update(outer.as_bytes());
        let response = format!("md5{}", hex::encode(h.finalize()));
        assert!(state.verify(&response));
    }

    #[test]
    fn md5_auth_wrong_password_rejected() {
        let user = md5_user("alice", "hunter2");
        let StoredCredential::Md5 { password_hash } = &user.credential else {
            panic!("expected Md5 credential");
        };
        let state = Md5State::new(password_hash.clone());
        assert!(!state.verify("md5deadbeefdeadbeefdeadbeefdeadbeef"));
    }

    #[test]
    fn scram_full_handshake_correct_password() {
        let user = create_scram_user("bob", "mysecret");
        let StoredCredential::ScramSha256(keys) = &user.credential else {
            panic!("expected SCRAM keys");
        };
        let mut server = ScramServer::new(keys.clone());

        // Simulate client-first (no channel binding).
        let client_nonce = BASE64.encode(b"clientnonce12345");
        let client_first = format!("n,,n=bob,r={client_nonce}");

        let server_first = server.process_client_first(&client_first).unwrap();

        // Parse combined nonce and salt from server-first.
        let combined_nonce = server_first
            .split(',')
            .find_map(|p| p.strip_prefix("r="))
            .unwrap()
            .to_string();
        let salt_b64 = server_first
            .split(',')
            .find_map(|p| p.strip_prefix("s="))
            .unwrap();
        let iterations: u32 = server_first
            .split(',')
            .find_map(|p| p.strip_prefix("i="))
            .unwrap()
            .parse()
            .unwrap();

        // Client computes SaltedPassword, ClientKey, StoredKey, ClientSignature, ClientProof.
        let salt_bytes = BASE64.decode(salt_b64).unwrap();
        let mut salted = [0u8; 32];
        pbkdf2_hmac::<Sha256>(b"mysecret", &salt_bytes, iterations, &mut salted);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let stored_key_client = sha256(&client_key);
        // Verify our stored_key matches what client computes (sanity check).
        assert_eq!(stored_key_client, keys.stored_key);

        let client_first_bare = format!("n=bob,r={client_nonce}");
        let channel_binding = BASE64.encode("n,,");
        let client_final_without_proof = format!("c={channel_binding},r={combined_nonce}");
        let auth_msg = format!("{client_first_bare},{server_first},{client_final_without_proof}");
        let client_sig = hmac_sha256(&stored_key_client, auth_msg.as_bytes());
        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_sig.iter())
            .map(|(k, s)| k ^ s)
            .collect();
        let client_final = format!(
            "{},p={}",
            client_final_without_proof,
            BASE64.encode(&client_proof)
        );

        let server_sig = server.process_client_final(&client_final).unwrap();

        // Verify server-signature matches what the client would expect.
        let expected_server_sig = BASE64.encode(hmac_sha256(&keys.server_key, auth_msg.as_bytes()));
        assert_eq!(server_sig, expected_server_sig);
    }

    #[test]
    fn scram_wrong_password_denied() {
        let user = create_scram_user("carol", "correct_password");
        let StoredCredential::ScramSha256(keys) = &user.credential else {
            panic!();
        };
        let mut server = ScramServer::new(keys.clone());
        let client_nonce = BASE64.encode(b"clientnonce12345");
        let client_first = format!("n,,n=carol,r={client_nonce}");
        let server_first = server.process_client_first(&client_first).unwrap();

        let combined_nonce = server_first
            .split(',')
            .find_map(|p| p.strip_prefix("r="))
            .unwrap()
            .to_string();
        let salt_b64 = server_first
            .split(',')
            .find_map(|p| p.strip_prefix("s="))
            .unwrap();
        let iterations: u32 = server_first
            .split(',')
            .find_map(|p| p.strip_prefix("i="))
            .unwrap()
            .parse()
            .unwrap();

        // Client uses WRONG password "wrong_password".
        let salt_bytes = BASE64.decode(salt_b64).unwrap();
        let mut salted = [0u8; 32];
        pbkdf2_hmac::<Sha256>(b"wrong_password", &salt_bytes, iterations, &mut salted);
        let client_key = hmac_sha256(&salted, b"Client Key");
        let wrong_stored = sha256(&client_key);

        let client_first_bare = format!("n=carol,r={client_nonce}");
        let channel_binding = BASE64.encode("n,,");
        let client_final_wop = format!("c={channel_binding},r={combined_nonce}");
        let auth_msg = format!("{client_first_bare},{server_first},{client_final_wop}");
        let client_sig = hmac_sha256(&wrong_stored, auth_msg.as_bytes());
        let client_proof: Vec<u8> = client_key
            .iter()
            .zip(client_sig.iter())
            .map(|(k, s)| k ^ s)
            .collect();
        let client_final = format!("{},p={}", client_final_wop, BASE64.encode(&client_proof));

        let result = server.process_client_final(&client_final);
        assert!(
            matches!(result, Err(AuthError::Denied)),
            "wrong password must be denied"
        );
    }

    #[test]
    fn ip_tracker_allows_up_to_limit() {
        let tracker = IpConnectionTracker::new(3);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();
        assert!(tracker.try_acquire(ip));
        assert!(tracker.try_acquire(ip));
        assert!(tracker.try_acquire(ip));
        assert!(!tracker.try_acquire(ip), "4th attempt must be denied");
    }

    #[test]
    fn ip_tracker_release_frees_slot() {
        let tracker = IpConnectionTracker::new(2);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        assert!(tracker.try_acquire(ip));
        assert!(tracker.try_acquire(ip));
        assert!(!tracker.try_acquire(ip));
        tracker.release(ip);
        assert!(
            tracker.try_acquire(ip),
            "slot must be available after release"
        );
    }

    #[test]
    fn ip_tracker_count_for() {
        let tracker = IpConnectionTracker::new(5);
        let ip: IpAddr = "192.168.1.1".parse().unwrap();
        assert_eq!(tracker.count_for(ip), 0);
        tracker.try_acquire(ip);
        tracker.try_acquire(ip);
        assert_eq!(tracker.count_for(ip), 2);
        tracker.release(ip);
        assert_eq!(tracker.count_for(ip), 1);
    }

    #[test]
    fn ip_tracker_different_ips_independent() {
        let tracker = IpConnectionTracker::new(1);
        let ip1: IpAddr = "1.2.3.4".parse().unwrap();
        let ip2: IpAddr = "5.6.7.8".parse().unwrap();
        assert!(tracker.try_acquire(ip1));
        assert!(!tracker.try_acquire(ip1));
        assert!(
            tracker.try_acquire(ip2),
            "ip2 must not be blocked by ip1 limit"
        );
    }

    #[test]
    fn user_registry_add_and_get() {
        let mut reg = UserRegistry::new();
        let user = create_scram_user("dave", "pass");
        reg.add_user(user.clone());
        let found = reg.get_user("dave").unwrap();
        assert_eq!(found.username, "dave");
    }

    #[test]
    fn user_registry_remove() {
        let mut reg = UserRegistry::new();
        reg.add_user(create_scram_user("eve", "secret"));
        assert!(reg.remove_user("eve"));
        assert!(!reg.remove_user("eve"), "double remove must return false");
        assert!(reg.get_user("eve").is_none());
    }

    #[test]
    fn user_registry_update_existing() {
        let mut reg = UserRegistry::new();
        reg.add_user(create_scram_user("frank", "old_pass"));
        let new_record = create_scram_user("frank", "new_pass");
        assert!(reg.update_user(new_record));
    }

    #[test]
    fn user_registry_update_nonexistent_returns_false() {
        let mut reg = UserRegistry::new();
        let record = create_scram_user("ghost", "pass");
        assert!(!reg.update_user(record));
    }

    #[test]
    fn scram_invalid_proof_base64_error() {
        let user = create_scram_user("hank", "pass");
        let StoredCredential::ScramSha256(keys) = &user.credential else {
            panic!();
        };
        let mut server = ScramServer::new(keys.clone());
        let client_first = format!("n,,n=hank,r={}", BASE64.encode(b"nonce"));
        let server_first = server.process_client_first(&client_first).unwrap();
        let combined = server_first
            .split(',')
            .find_map(|p| p.strip_prefix("r="))
            .unwrap();
        let channel_binding = BASE64.encode("n,,");
        let client_final = format!("c={channel_binding},r={combined},p=!!!NOT_VALID_BASE64!!!");
        let result = server.process_client_final(&client_final);
        assert!(matches!(result, Err(AuthError::InvalidMessage(_))));
    }

    #[test]
    fn user_registry_load_from_nonexistent_file() {
        let reg = UserRegistry::load_from_file("/nonexistent/path/users.json");
        assert!(reg.get_user("anyone").is_none());
    }

    #[test]
    fn user_registry_load_from_valid_json() {
        use std::io::Write;
        let mut f = tempfile::NamedTempFile::new().unwrap();
        let salt_b64 = BASE64.encode(b"salt_16_bytes_!!");
        let (stored_key, server_key) = derive_scram_keys("testpass", b"salt_16_bytes_!!", 4096);
        let content = format!(
            r#"{{"users":[{{"username":"testuser","method":"scram-sha-256","salt":"{}","iterations":4096,"stored_key":"{}","server_key":"{}"}}]}}
"#,
            salt_b64,
            BASE64.encode(stored_key),
            BASE64.encode(server_key)
        );
        f.write_all(content.as_bytes()).unwrap();
        let reg = UserRegistry::load_from_file(f.path().to_str().unwrap());
        let user = reg.get_user("testuser").unwrap();
        assert!(matches!(user.credential, StoredCredential::ScramSha256(_)));
    }

    #[test]
    fn strip_gs2_header_n_variant() {
        assert_eq!(
            strip_gs2_header("n,,n=user,r=nonce").unwrap(),
            "n=user,r=nonce"
        );
    }

    #[test]
    fn strip_gs2_header_y_variant() {
        assert_eq!(
            strip_gs2_header("y,,n=user,r=nonce").unwrap(),
            "n=user,r=nonce"
        );
    }

    #[test]
    fn strip_gs2_header_invalid_returns_error() {
        assert!(strip_gs2_header("no_header_here").is_err());
    }

    #[test]
    fn hmac_sha256_test_vector() {
        // RFC 4231 Test Case 2: key="Jefe", data="what do ya want for nothing?"
        // Verified against the RustCrypto hmac 0.12.x crate.
        let key = b"Jefe";
        let msg = b"what do ya want for nothing?";
        let result = hmac_sha256(key, msg);
        let expected =
            hex::decode("5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843")
                .unwrap();
        assert_eq!(&result, expected.as_slice());
    }
}
