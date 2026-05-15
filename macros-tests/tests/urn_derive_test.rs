#![cfg(not(target_arch = "wasm32"))]

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use replay_macros::Urn;
use serde::{Deserialize, Serialize};
use urn::Urn;

#[derive(Clone, Debug, Serialize, Deserialize, Urn)]
struct AccountUrn(Urn);

#[derive(Clone, Debug, Serialize, Deserialize, Urn)]
struct BranchUrn(Urn);

// ── Display / FromStr ─────────────────────────────────────────────────────────

#[test]
fn test_display() {
    let urn = AccountUrn::from_str("urn:account:acct-1").unwrap();
    assert_eq!(urn.to_string(), "urn:account:acct-1");
}

#[test]
fn test_from_str_round_trip() {
    let s = "urn:account:acct-42";
    let urn = AccountUrn::from_str(s).unwrap();
    assert_eq!(urn.to_string(), s);
}

#[test]
fn test_from_str_invalid() {
    assert!(AccountUrn::from_str("not-a-urn").is_err());
}

// ── Into<Urn> ────────────────────────────────────────────────────────────────

#[test]
fn test_into_urn() {
    let account = AccountUrn::from_str("urn:account:acct-1").unwrap();
    let raw: Urn = account.into();
    assert_eq!(raw.nid(), "account");
    assert_eq!(raw.nss(), "acct-1");
}

// ── PartialEq / Eq ───────────────────────────────────────────────────────────

#[test]
fn test_eq_same_value() {
    let a = AccountUrn::from_str("urn:account:acct-1").unwrap();
    let b = AccountUrn::from_str("urn:account:acct-1").unwrap();
    assert_eq!(a, b);
}

#[test]
fn test_ne_different_value() {
    let a = AccountUrn::from_str("urn:account:acct-1").unwrap();
    let b = AccountUrn::from_str("urn:account:acct-2").unwrap();
    assert_ne!(a, b);
}

// ── Hash ─────────────────────────────────────────────────────────────────────

#[test]
fn test_hash_equal_values_have_equal_hashes() {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let a = AccountUrn::from_str("urn:account:acct-1").unwrap();
    let b = AccountUrn::from_str("urn:account:acct-1").unwrap();

    let mut ha = DefaultHasher::new();
    let mut hb = DefaultHasher::new();
    a.hash(&mut ha);
    b.hash(&mut hb);

    assert_eq!(ha.finish(), hb.finish());
}

#[test]
fn test_hashset_deduplication() {
    let a = AccountUrn::from_str("urn:account:acct-1").unwrap();
    let b = AccountUrn::from_str("urn:account:acct-1").unwrap();
    let c = AccountUrn::from_str("urn:account:acct-2").unwrap();

    let mut set = HashSet::new();
    set.insert(a);
    set.insert(b); // duplicate — set size must not grow
    set.insert(c);

    assert_eq!(set.len(), 2);
}

#[test]
fn test_hashmap_key() {
    let key = AccountUrn::from_str("urn:account:acct-1").unwrap();
    let mut map: HashMap<AccountUrn, &str> = HashMap::new();
    map.insert(key.clone(), "Alice");

    assert_eq!(map[&key], "Alice");
}
