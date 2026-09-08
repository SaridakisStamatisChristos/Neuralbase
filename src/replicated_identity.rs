// SPDX-License-Identifier: Apache-2.0
//! Deterministic, versioned commands for Raft-replicated cluster identity.
//!
//! Password hashing happens before a command is constructed. The replicated
//! representation contains only SCRAM-SHA-256 verifier material: salt,
//! iteration count, StoredKey and ServerKey. Plaintext passwords and PostgreSQL
//! MD5 password hashes are deliberately not representable here.

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine as _;
use thiserror::Error;

use crate::auth::{ScramKeys, StoredCredential, UserRecord};

const MAGIC: &[u8; 4] = b"NBRI";
pub const REPLICATED_IDENTITY_VERSION: u8 = 1;
pub const SCRAM_CREDENTIAL_VERSION: u8 = 1;
pub const MAX_REPLICATED_IDENTITY_BYTES: usize = 1024 * 1024;
const MAX_USERNAME_BYTES: usize = 256;
const MAX_INITIAL_USERS: usize = 100_000;
const SCRAM_SALT_MIN_BYTES: usize = 8;
const SCRAM_SALT_MAX_BYTES: usize = 64;
const SCRAM_MIN_ITERATIONS: u32 = 4_096;
const SCRAM_MAX_ITERATIONS: u32 = 10_000_000;

const OP_INITIALIZE: u8 = 1;
const OP_CREATE_USER: u8 = 2;
const OP_ALTER_USER: u8 = 3;
const OP_DROP_USER: u8 = 4;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedScramCredential {
    pub salt: Vec<u8>,
    pub iterations: u32,
    pub stored_key: [u8; 32],
    pub server_key: [u8; 32],
}

impl ReplicatedScramCredential {
    pub fn from_user_record(record: &UserRecord) -> Result<Self, IdentityCodecError> {
        match &record.credential {
            StoredCredential::ScramSha256(keys) => Self::from_scram_keys(keys),
            StoredCredential::Md5 { .. } => Err(IdentityCodecError::UnsupportedLegacyMd5),
        }
    }

    pub fn from_scram_keys(keys: &ScramKeys) -> Result<Self, IdentityCodecError> {
        let salt = BASE64
            .decode(&keys.salt_b64)
            .map_err(|_| IdentityCodecError::InvalidScramSalt)?;
        let credential = Self {
            salt,
            iterations: keys.iterations,
            stored_key: keys.stored_key,
            server_key: keys.server_key,
        };
        credential.validate()?;
        Ok(credential)
    }

    pub fn to_user_record(&self, username: &str) -> Result<UserRecord, IdentityCodecError> {
        validate_username(username)?;
        self.validate()?;
        Ok(UserRecord {
            username: username.to_string(),
            credential: StoredCredential::ScramSha256(ScramKeys {
                salt_b64: BASE64.encode(&self.salt),
                iterations: self.iterations,
                stored_key: self.stored_key,
                server_key: self.server_key,
            }),
        })
    }

    fn validate(&self) -> Result<(), IdentityCodecError> {
        if !(SCRAM_SALT_MIN_BYTES..=SCRAM_SALT_MAX_BYTES).contains(&self.salt.len()) {
            return Err(IdentityCodecError::InvalidScramSalt);
        }
        if !(SCRAM_MIN_ITERATIONS..=SCRAM_MAX_ITERATIONS).contains(&self.iterations) {
            return Err(IdentityCodecError::InvalidScramIterations(self.iterations));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedIdentityUser {
    pub username: String,
    pub credential: ReplicatedScramCredential,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReplicatedIdentityMutation {
    /// Establish the replicated identity authority from an explicitly selected,
    /// canonical legacy source. This is intentionally separate from CREATE USER
    /// so migration cannot happen implicitly as a side effect of ordinary DDL.
    Initialize { users: Vec<ReplicatedIdentityUser> },
    CreateUser {
        username: String,
        credential: ReplicatedScramCredential,
    },
    AlterUser {
        username: String,
        credential: ReplicatedScramCredential,
    },
    DropUser {
        username: String,
        if_exists: bool,
    },
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum IdentityCodecError {
    #[error("replicated identity mutation exceeds {MAX_REPLICATED_IDENTITY_BYTES} bytes")]
    TooLarge,
    #[error("replicated identity mutation is truncated")]
    UnexpectedEof,
    #[error("invalid replicated identity mutation magic")]
    InvalidMagic,
    #[error("unsupported replicated identity mutation version {0}")]
    UnsupportedVersion(u8),
    #[error("unknown replicated identity mutation opcode {0}")]
    UnknownOpcode(u8),
    #[error("replicated identity mutation contains invalid UTF-8")]
    InvalidUtf8,
    #[error("identity username must be non-empty, at most {MAX_USERNAME_BYTES} bytes, and contain no NUL")]
    InvalidUsername,
    #[error("replicated identity initialization contains too many users")]
    TooManyUsers,
    #[error("replicated identity initialization usernames must be strictly ordered and unique")]
    NonCanonicalUserOrder,
    #[error("invalid SCRAM salt length or encoding")]
    InvalidScramSalt,
    #[error("invalid SCRAM iteration count {0}")]
    InvalidScramIterations(u32),
    #[error("legacy PostgreSQL MD5 password hashes are reusable authentication material and cannot enter the Raft log")]
    UnsupportedLegacyMd5,
    #[error("replicated identity mutation has trailing bytes")]
    TrailingBytes,
    #[error("field length exceeds u32 encoding range")]
    FieldTooLarge,
    #[error("invalid DROP USER if-exists flag {0}")]
    InvalidBoolean(u8),
    #[error("unsupported replicated credential version {0}")]
    UnsupportedCredentialVersion(u8),
}

impl ReplicatedIdentityMutation {
    pub fn encode(&self) -> Result<Vec<u8>, IdentityCodecError> {
        let mut out = Vec::new();
        out.extend_from_slice(MAGIC);
        out.push(REPLICATED_IDENTITY_VERSION);

        match self {
            Self::Initialize { users } => {
                out.push(OP_INITIALIZE);
                let mut canonical = users.clone();
                canonical.sort_by(|a, b| a.username.cmp(&b.username));
                ensure_user_order(&canonical)?;
                if canonical.len() > MAX_INITIAL_USERS {
                    return Err(IdentityCodecError::TooManyUsers);
                }
                put_u32(
                    &mut out,
                    u32::try_from(canonical.len())
                        .map_err(|_| IdentityCodecError::TooManyUsers)?,
                );
                for user in &canonical {
                    put_username(&mut out, &user.username)?;
                    put_credential(&mut out, &user.credential)?;
                }
            }
            Self::CreateUser {
                username,
                credential,
            } => {
                out.push(OP_CREATE_USER);
                put_username(&mut out, username)?;
                put_credential(&mut out, credential)?;
            }
            Self::AlterUser {
                username,
                credential,
            } => {
                out.push(OP_ALTER_USER);
                put_username(&mut out, username)?;
                put_credential(&mut out, credential)?;
            }
            Self::DropUser {
                username,
                if_exists,
            } => {
                out.push(OP_DROP_USER);
                put_username(&mut out, username)?;
                out.push(u8::from(*if_exists));
            }
        }

        if out.len() > MAX_REPLICATED_IDENTITY_BYTES {
            return Err(IdentityCodecError::TooLarge);
        }
        Ok(out)
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, IdentityCodecError> {
        if bytes.len() > MAX_REPLICATED_IDENTITY_BYTES {
            return Err(IdentityCodecError::TooLarge);
        }
        let mut reader = Reader::new(bytes);
        if reader.take(4)? != MAGIC {
            return Err(IdentityCodecError::InvalidMagic);
        }
        let version = reader.u8()?;
        if version != REPLICATED_IDENTITY_VERSION {
            return Err(IdentityCodecError::UnsupportedVersion(version));
        }

        let mutation = match reader.u8()? {
            OP_INITIALIZE => {
                let count = reader.u32()? as usize;
                if count > MAX_INITIAL_USERS {
                    return Err(IdentityCodecError::TooManyUsers);
                }
                let mut users = Vec::with_capacity(count);
                for _ in 0..count {
                    users.push(ReplicatedIdentityUser {
                        username: reader.username()?,
                        credential: reader.credential()?,
                    });
                }
                ensure_user_order(&users)?;
                Self::Initialize { users }
            }
            OP_CREATE_USER => Self::CreateUser {
                username: reader.username()?,
                credential: reader.credential()?,
            },
            OP_ALTER_USER => Self::AlterUser {
                username: reader.username()?,
                credential: reader.credential()?,
            },
            OP_DROP_USER => {
                let username = reader.username()?;
                let if_exists = match reader.u8()? {
                    0 => false,
                    1 => true,
                    other => return Err(IdentityCodecError::InvalidBoolean(other)),
                };
                Self::DropUser {
                    username,
                    if_exists,
                }
            }
            other => return Err(IdentityCodecError::UnknownOpcode(other)),
        };

        if !reader.is_finished() {
            return Err(IdentityCodecError::TrailingBytes);
        }
        Ok(mutation)
    }

    pub fn command_tag(&self) -> &'static str {
        match self {
            Self::Initialize { .. } => "INITIALIZE IDENTITY",
            Self::CreateUser { .. } => "CREATE USER",
            Self::AlterUser { .. } => "ALTER USER",
            Self::DropUser { .. } => "DROP USER",
        }
    }
}

pub fn is_replicated_identity_mutation(bytes: &[u8]) -> bool {
    bytes.starts_with(MAGIC)
}

fn put_username(out: &mut Vec<u8>, username: &str) -> Result<(), IdentityCodecError> {
    validate_username(username)?;
    put_bytes(out, username.as_bytes())
}

fn validate_username(username: &str) -> Result<(), IdentityCodecError> {
    if username.is_empty()
        || username.len() > MAX_USERNAME_BYTES
        || username.as_bytes().contains(&0)
    {
        Err(IdentityCodecError::InvalidUsername)
    } else {
        Ok(())
    }
}

fn ensure_user_order(users: &[ReplicatedIdentityUser]) -> Result<(), IdentityCodecError> {
    for user in users {
        validate_username(&user.username)?;
        user.credential.validate()?;
    }
    for pair in users.windows(2) {
        if pair[0].username >= pair[1].username {
            return Err(IdentityCodecError::NonCanonicalUserOrder);
        }
    }
    Ok(())
}

fn put_credential(
    out: &mut Vec<u8>,
    credential: &ReplicatedScramCredential,
) -> Result<(), IdentityCodecError> {
    credential.validate()?;
    out.push(SCRAM_CREDENTIAL_VERSION);
    put_bytes(out, &credential.salt)?;
    put_u32(out, credential.iterations);
    out.extend_from_slice(&credential.stored_key);
    out.extend_from_slice(&credential.server_key);
    Ok(())
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn put_bytes(out: &mut Vec<u8>, value: &[u8]) -> Result<(), IdentityCodecError> {
    let len = u32::try_from(value.len()).map_err(|_| IdentityCodecError::FieldTooLarge)?;
    put_u32(out, len);
    out.extend_from_slice(value);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Reader<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], IdentityCodecError> {
        let end = self
            .pos
            .checked_add(len)
            .ok_or(IdentityCodecError::UnexpectedEof)?;
        let slice = self
            .bytes
            .get(self.pos..end)
            .ok_or(IdentityCodecError::UnexpectedEof)?;
        self.pos = end;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8, IdentityCodecError> {
        Ok(self.take(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, IdentityCodecError> {
        let raw: [u8; 4] = self
            .take(4)?
            .try_into()
            .map_err(|_| IdentityCodecError::UnexpectedEof)?;
        Ok(u32::from_be_bytes(raw))
    }

    fn bytes(&mut self) -> Result<Vec<u8>, IdentityCodecError> {
        let len = self.u32()? as usize;
        Ok(self.take(len)?.to_vec())
    }

    fn username(&mut self) -> Result<String, IdentityCodecError> {
        let bytes = self.bytes()?;
        let username = String::from_utf8(bytes).map_err(|_| IdentityCodecError::InvalidUtf8)?;
        validate_username(&username)?;
        Ok(username)
    }

    fn credential(&mut self) -> Result<ReplicatedScramCredential, IdentityCodecError> {
        let version = self.u8()?;
        if version != SCRAM_CREDENTIAL_VERSION {
            return Err(IdentityCodecError::UnsupportedCredentialVersion(version));
        }
        let credential = ReplicatedScramCredential {
            salt: self.bytes()?,
            iterations: self.u32()?,
            stored_key: self
                .take(32)?
                .try_into()
                .map_err(|_| IdentityCodecError::UnexpectedEof)?,
            server_key: self
                .take(32)?
                .try_into()
                .map_err(|_| IdentityCodecError::UnexpectedEof)?,
        };
        credential.validate()?;
        Ok(credential)
    }

    fn is_finished(&self) -> bool {
        self.pos == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{derive_scram_keys, StoredCredential};

    fn credential(seed: u8) -> ReplicatedScramCredential {
        ReplicatedScramCredential {
            salt: vec![seed; 16],
            iterations: 4_096,
            stored_key: [seed.wrapping_add(1); 32],
            server_key: [seed.wrapping_add(2); 32],
        }
    }

    #[test]
    fn identity_commands_roundtrip_exactly() {
        let commands = [
            ReplicatedIdentityMutation::Initialize {
                users: vec![
                    ReplicatedIdentityUser {
                        username: "alice".to_string(),
                        credential: credential(1),
                    },
                    ReplicatedIdentityUser {
                        username: "bob".to_string(),
                        credential: credential(2),
                    },
                ],
            },
            ReplicatedIdentityMutation::CreateUser {
                username: "carol".to_string(),
                credential: credential(3),
            },
            ReplicatedIdentityMutation::AlterUser {
                username: "carol".to_string(),
                credential: credential(4),
            },
            ReplicatedIdentityMutation::DropUser {
                username: "carol".to_string(),
                if_exists: false,
            },
        ];

        for command in commands {
            let encoded = command.encode().unwrap();
            assert!(is_replicated_identity_mutation(&encoded));
            assert_eq!(ReplicatedIdentityMutation::decode(&encoded).unwrap(), command);
        }
    }

    #[test]
    fn initialization_encoding_is_canonical() {
        let a = ReplicatedIdentityMutation::Initialize {
            users: vec![
                ReplicatedIdentityUser {
                    username: "bob".to_string(),
                    credential: credential(2),
                },
                ReplicatedIdentityUser {
                    username: "alice".to_string(),
                    credential: credential(1),
                },
            ],
        };
        let b = ReplicatedIdentityMutation::Initialize {
            users: vec![
                ReplicatedIdentityUser {
                    username: "alice".to_string(),
                    credential: credential(1),
                },
                ReplicatedIdentityUser {
                    username: "bob".to_string(),
                    credential: credential(2),
                },
            ],
        };
        assert_eq!(a.encode().unwrap(), b.encode().unwrap());
    }

    #[test]
    fn duplicate_initialization_user_is_rejected() {
        let command = ReplicatedIdentityMutation::Initialize {
            users: vec![
                ReplicatedIdentityUser {
                    username: "alice".to_string(),
                    credential: credential(1),
                },
                ReplicatedIdentityUser {
                    username: "alice".to_string(),
                    credential: credential(2),
                },
            ],
        };
        assert_eq!(
            command.encode().unwrap_err(),
            IdentityCodecError::NonCanonicalUserOrder
        );
    }

    #[test]
    fn leader_materialized_scram_command_does_not_contain_plaintext() {
        let password = "phase4-secret-password";
        let salt = [7u8; 16];
        let (stored_key, server_key) = derive_scram_keys(password, &salt, 4_096);
        let record = UserRecord {
            username: "alice".to_string(),
            credential: StoredCredential::ScramSha256(ScramKeys {
                salt_b64: BASE64.encode(salt),
                iterations: 4_096,
                stored_key,
                server_key,
            }),
        };
        let materialized = ReplicatedScramCredential::from_user_record(&record).unwrap();
        let encoded = ReplicatedIdentityMutation::CreateUser {
            username: record.username,
            credential: materialized,
        }
        .encode()
        .unwrap();

        assert!(!encoded
            .windows(password.len())
            .any(|window| window == password.as_bytes()));
    }

    #[test]
    fn md5_equivalent_reusable_credential_is_not_replicable() {
        let record = UserRecord {
            username: "legacy".to_string(),
            credential: StoredCredential::Md5 {
                password_hash: "0123456789abcdef0123456789abcdef".to_string(),
            },
        };
        assert_eq!(
            ReplicatedScramCredential::from_user_record(&record).unwrap_err(),
            IdentityCodecError::UnsupportedLegacyMd5
        );
    }

    #[test]
    fn decoder_rejects_trailing_bytes_and_invalid_flags() {
        let command = ReplicatedIdentityMutation::DropUser {
            username: "alice".to_string(),
            if_exists: true,
        };
        let mut encoded = command.encode().unwrap();
        encoded.push(0);
        assert_eq!(
            ReplicatedIdentityMutation::decode(&encoded).unwrap_err(),
            IdentityCodecError::TrailingBytes
        );

        let mut invalid_flag = command.encode().unwrap();
        *invalid_flag.last_mut().unwrap() = 2;
        assert_eq!(
            ReplicatedIdentityMutation::decode(&invalid_flag).unwrap_err(),
            IdentityCodecError::InvalidBoolean(2)
        );
    }
}
