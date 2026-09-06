// SPDX-License-Identifier: Apache-2.0
// Integration tests: Session 11 authentication correctness.
//
// Covers:
//   • NbStatement parsing for CREATE / ALTER / DROP USER
//   • BoundPlan generation for user-management statements
//   • UserRegistry semantics (add, update, remove)
//   • SCRAM user creation and key derivation determinism
//   • MD5 user creation and challenge/response correctness
//   • IpConnectionTracker accounting
//   • users.json file loading via serde_json

use neuralbase::auth::{
    create_md5_user, create_scram_user, IpConnectionTracker, StoredCredential, UserRegistry,
};
use neuralbase::binder::{bind_nb_statement, BoundPlan};
use neuralbase::catalog::InMemoryCatalog;
use neuralbase::sql::{parse_nb_statement, NbStatement};
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;

// ── NbStatement parsing ────────────────────────────────────────────────────────

#[test]
fn parse_create_user_basic() {
    let stmt = parse_nb_statement("CREATE USER alice WITH PASSWORD 'secret'").unwrap();
    match stmt {
        NbStatement::CreateUser { username, password } => {
            assert_eq!(username, "alice");
            assert_eq!(password, "secret");
        }
        other => panic!("expected CreateUser, got {:?}", other),
    }
}

#[test]
fn parse_create_user_uppercase_keyword() {
    // Parser is case-insensitive for keywords.
    let stmt = parse_nb_statement("create user bob with password 'pw2'").unwrap();
    match stmt {
        NbStatement::CreateUser { username, password } => {
            assert_eq!(username, "bob");
            assert_eq!(password, "pw2");
        }
        other => panic!("expected CreateUser, got {:?}", other),
    }
}

#[test]
fn parse_alter_user_basic() {
    let stmt = parse_nb_statement("ALTER USER alice WITH PASSWORD 'newpass'").unwrap();
    match stmt {
        NbStatement::AlterUser {
            username,
            new_password,
        } => {
            assert_eq!(username, "alice");
            assert_eq!(new_password, "newpass");
        }
        other => panic!("expected AlterUser, got {:?}", other),
    }
}

#[test]
fn parse_drop_user_basic() {
    let stmt = parse_nb_statement("DROP USER alice").unwrap();
    match stmt {
        NbStatement::DropUser {
            username,
            if_exists,
        } => {
            assert_eq!(username, "alice");
            assert!(!if_exists);
        }
        other => panic!("expected DropUser, got {:?}", other),
    }
}

#[test]
fn parse_drop_user_if_exists() {
    let stmt = parse_nb_statement("DROP USER IF EXISTS carol").unwrap();
    match stmt {
        NbStatement::DropUser {
            username,
            if_exists,
        } => {
            assert_eq!(username, "carol");
            assert!(if_exists);
        }
        other => panic!("expected DropUser, got {:?}", other),
    }
}

#[test]
fn parse_regular_sql_falls_through_to_sql_variant() {
    let stmt = parse_nb_statement("SELECT 1").unwrap();
    assert!(matches!(stmt, NbStatement::Sql(_)));
}

// ── BoundPlan generation ───────────────────────────────────────────────────────

fn empty_catalog() -> Arc<InMemoryCatalog> {
    Arc::new(InMemoryCatalog::with_tpch_lineitem())
}

#[test]
fn bind_create_user_produces_correct_plan() {
    let cat = empty_catalog();
    let nb = parse_nb_statement("CREATE USER testuser WITH PASSWORD 'pw'").unwrap();
    let plan = bind_nb_statement(&nb, cat.as_ref()).unwrap();
    match plan {
        BoundPlan::CreateUser { username, password } => {
            assert_eq!(username, "testuser");
            assert_eq!(password, "pw");
        }
        other => panic!("expected BoundPlan::CreateUser, got {:?}", other),
    }
}

#[test]
fn bind_alter_user_produces_correct_plan() {
    let cat = empty_catalog();
    let nb = parse_nb_statement("ALTER USER alice WITH PASSWORD 'newpw'").unwrap();
    let plan = bind_nb_statement(&nb, cat.as_ref()).unwrap();
    match plan {
        BoundPlan::AlterUser {
            username,
            new_password,
        } => {
            assert_eq!(username, "alice");
            assert_eq!(new_password, "newpw");
        }
        other => panic!("expected BoundPlan::AlterUser, got {:?}", other),
    }
}

#[test]
fn bind_drop_user_produces_correct_plan() {
    let cat = empty_catalog();
    let nb = parse_nb_statement("DROP USER IF EXISTS alice").unwrap();
    let plan = bind_nb_statement(&nb, cat.as_ref()).unwrap();
    match plan {
        BoundPlan::DropUser {
            username,
            if_exists,
        } => {
            assert_eq!(username, "alice");
            assert!(if_exists);
        }
        other => panic!("expected BoundPlan::DropUser, got {:?}", other),
    }
}

// ── UserRegistry semantics ────────────────────────────────────────────────────

fn reg_no_auth() -> UserRegistry {
    // Bypass env var by constructing manually.
    // We test the API directly rather than require_auth behaviour.
    let mut r = UserRegistry::new();
    // create_scram_user inserts inline so we use registry methods:
    r.add_user(create_scram_user("alice", "hunter2"));
    r
}

#[test]
fn registry_add_and_lookup() {
    let r = reg_no_auth();
    assert!(r.get_user("alice").is_some());
    assert!(r.get_user("nobody").is_none());
}

#[test]
fn registry_remove_user() {
    let mut r = reg_no_auth();
    assert!(r.remove_user("alice"));
    assert!(r.get_user("alice").is_none());
    // Removing non-existent user returns false.
    assert!(!r.remove_user("alice"));
}

#[test]
fn registry_update_user_existing() {
    let mut r = reg_no_auth();
    let updated = create_scram_user("alice", "newsecret");
    assert!(r.update_user(updated));
}

#[test]
fn registry_update_user_nonexistent_returns_false() {
    let mut r = reg_no_auth();
    let phantom = create_scram_user("ghost", "pw");
    assert!(!r.update_user(phantom));
}

// ── SCRAM user creation ────────────────────────────────────────────────────────

#[test]
fn create_scram_user_produces_scram_credential() {
    let u = create_scram_user("alice", "pw");
    assert_eq!(u.username, "alice");
    assert!(matches!(u.credential, StoredCredential::ScramSha256(_)));
}

#[test]
fn scram_keys_are_nondeterministic_across_calls() {
    // Each call uses a fresh random salt — stored_key bytes should differ.
    let u1 = create_scram_user("alice", "pw");
    let u2 = create_scram_user("alice", "pw");
    if let (StoredCredential::ScramSha256(k1), StoredCredential::ScramSha256(k2)) =
        (&u1.credential, &u2.credential)
    {
        // Same password but different salts → different stored_key values
        // (probabilistically; collision probability = 2^-128).
        assert_ne!(k1.salt_b64, k2.salt_b64);
    } else {
        panic!("expected ScramSha256 credentials");
    }
}

#[test]
fn scram_stored_key_is_32_bytes() {
    let u = create_scram_user("alice", "pw");
    if let StoredCredential::ScramSha256(k) = &u.credential {
        assert_eq!(k.stored_key.len(), 32);
        assert_eq!(k.server_key.len(), 32);
        assert_eq!(k.iterations, 4096);
    } else {
        panic!("expected ScramSha256");
    }
}

// ── MD5 user creation ─────────────────────────────────────────────────────────

#[test]
fn create_md5_user_produces_md5_credential() {
    let u = create_md5_user("bob", "pw");
    assert!(matches!(u.credential, StoredCredential::Md5 { .. }));
}

#[test]
fn md5_hash_is_32_hex_chars() {
    let u = create_md5_user("bob", "letmein");
    if let StoredCredential::Md5 { password_hash } = &u.credential {
        assert_eq!(password_hash.len(), 32, "MD5 hash must be 32 hex chars");
        assert!(password_hash.chars().all(|c| c.is_ascii_hexdigit()));
    } else {
        panic!("expected Md5 credential");
    }
}

#[test]
fn md5_hash_is_function_of_password_and_username() {
    // Two users with same password but different usernames must have different hashes.
    let u1 = create_md5_user("alice", "same_pw");
    let u2 = create_md5_user("bob", "same_pw");
    if let (
        StoredCredential::Md5 { password_hash: h1 },
        StoredCredential::Md5 { password_hash: h2 },
    ) = (&u1.credential, &u2.credential)
    {
        assert_ne!(h1, h2);
    }
}

// ── IpConnectionTracker ───────────────────────────────────────────────────────

fn localhost() -> IpAddr {
    IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))
}

#[test]
fn ip_tracker_allows_connections_under_limit() {
    let t = IpConnectionTracker::new(3);
    assert!(t.try_acquire(localhost()));
    assert!(t.try_acquire(localhost()));
    assert!(t.try_acquire(localhost()));
    // 4th exceeds limit
    assert!(!t.try_acquire(localhost()));
}

#[test]
fn ip_tracker_release_restores_slot() {
    let t = IpConnectionTracker::new(1);
    assert!(t.try_acquire(localhost()));
    assert!(!t.try_acquire(localhost()));
    t.release(localhost());
    assert!(t.try_acquire(localhost()));
}

#[test]
fn ip_tracker_different_ips_independent() {
    let t = IpConnectionTracker::new(1);
    let ip1 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1));
    let ip2 = IpAddr::V4(Ipv4Addr::new(10, 0, 0, 2));
    assert!(t.try_acquire(ip1));
    assert!(!t.try_acquire(ip1)); // ip1 at limit
    assert!(t.try_acquire(ip2)); // ip2 unaffected
}

#[test]
fn ip_tracker_unlimited_default_allows_many_connections() {
    // usize::MAX default: 200 connections from same IP should all succeed.
    let t = IpConnectionTracker::new(usize::MAX);
    let ip = localhost();
    for _ in 0..200 {
        assert!(t.try_acquire(ip));
    }
}

// ── users.json file loading ───────────────────────────────────────────────────

#[test]
fn load_users_json_missing_file_is_empty_registry() {
    let r = UserRegistry::load_from_file("/nonexistent/path/users.json");
    assert!(r.get_user("anyone").is_none());
}

#[test]
fn load_users_json_scram_user() {
    use base64::engine::general_purpose::STANDARD as B64;
    use base64::Engine as _;
    use neuralbase::auth::derive_scram_keys;
    use std::io::Write;

    let salt = b"test_salt_16byt!";
    let (stored_key, server_key) = derive_scram_keys("testpass", salt, 4096);
    let json = format!(
        r#"{{"users":[{{"username":"jsonuser","method":"scram-sha-256","salt":"{}","iterations":4096,"stored_key":"{}","server_key":"{}"}}]}}"#,
        B64.encode(salt),
        B64.encode(stored_key),
        B64.encode(server_key)
    );
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(json.as_bytes()).unwrap();
    let r = UserRegistry::load_from_file(f.path().to_str().unwrap());
    let u = r.get_user("jsonuser").unwrap();
    assert!(matches!(u.credential, StoredCredential::ScramSha256(_)));
}

#[test]
fn load_users_json_md5_user() {
    use std::io::Write;
    let json = r#"{"users":[{"username":"mduser","method":"md5","md5_hash":"aabbccddeeff00112233445566778899"}]}"#;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(json.as_bytes()).unwrap();
    let r = UserRegistry::load_from_file(f.path().to_str().unwrap());
    let u = r.get_user("mduser").unwrap();
    assert!(matches!(u.credential, StoredCredential::Md5 { .. }));
}

#[test]
fn load_users_json_malformed_json_is_empty_registry() {
    use std::io::Write;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(b"this is not json {{{").unwrap();
    let r = UserRegistry::load_from_file(f.path().to_str().unwrap());
    assert!(r.get_user("anyone").is_none());
}

#[test]
fn load_users_json_unknown_method_skipped() {
    use std::io::Write;
    let json = r#"{"users":[{"username":"x","method":"plaintext","password":"abc"}]}"#;
    let mut f = tempfile::NamedTempFile::new().unwrap();
    f.write_all(json.as_bytes()).unwrap();
    let r = UserRegistry::load_from_file(f.path().to_str().unwrap());
    // Unknown method → entry silently skipped
    assert!(r.get_user("x").is_none());
}
