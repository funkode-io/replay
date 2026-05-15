#![cfg(not(target_arch = "wasm32"))]

use std::collections::{HashMap, HashSet};
use std::str::FromStr;

use replay::prelude::*;
use replay_macros::Urn;
use serde::{Deserialize, Serialize};
use urn::Urn;

// ── Fixtures ──────────────────────────────────────────────────────────────────

/// Basic newtype — no namespace; gets From, Display, FromStr, PartialEq, Eq, Hash.
#[derive(Clone, Debug, Serialize, Deserialize, Urn)]
struct AccountUrn(Urn);

/// With namespace — also gets TryFrom<Urn>, new(), new_random(), parse(),
/// namespace(), nid(), nss(), to_urn().
#[derive(Clone, Debug, Serialize, Deserialize, Urn)]
#[urn(namespace = "branch")]
struct BranchUrn(Urn);

/// With namespace and kebab-case namespace string.
#[derive(Clone, Debug, Serialize, Deserialize, Urn)]
#[urn(namespace = "bank-account")]
struct BankAccountUrn(Urn);

// ── Display / FromStr (base derive) ──────────────────────────────────────────

#[test]
fn test_display() {
    let urn = AccountUrn::from_str("urn:account:acct-1").unwrap();
    assert_eq!(urn.to_string(), "urn:account:acct-1");
}

#[test]
fn test_from_str_round_trip() {
    let s = "urn:account:acct-42";
    assert_eq!(AccountUrn::from_str(s).unwrap().to_string(), s);
}

#[test]
fn test_from_str_invalid() {
    assert!(AccountUrn::from_str("not-a-urn").is_err());
}

// ── Into<Urn> (base derive) ───────────────────────────────────────────────────

#[test]
fn test_into_urn() {
    let account = AccountUrn::from_str("urn:account:acct-1").unwrap();
    let raw: Urn = account.into();
    assert_eq!(raw.nid(), "account");
    assert_eq!(raw.nss(), "acct-1");
}

// ── PartialEq / Eq (base derive) ─────────────────────────────────────────────

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

// ── Hash (base derive) ───────────────────────────────────────────────────────

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
    set.insert(b); // duplicate — must not grow the set
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

// ── new(id) — namespace attribute ────────────────────────────────────────────

#[test]
fn test_new_with_plain_id() {
    let urn = BranchUrn::new("london").unwrap();
    assert_eq!(urn.to_string(), "urn:branch:london");
}

#[test]
fn test_new_with_numeric_id() {
    assert_eq!(BranchUrn::new(42).unwrap().nss(), "42");
}

#[test]
fn test_new_with_full_urn_string() {
    let urn = BranchUrn::new("urn:branch:london").unwrap();
    assert_eq!(urn.nss(), "london");
}

#[test]
fn test_new_with_wrong_namespace_fails() {
    assert!(BranchUrn::new("urn:other:london").is_err());
}

#[test]
fn test_new_unwraps_nested_same_namespace_urn() {
    // urn:branch:urn:branch:london → nss should be "london"
    let urn = BranchUrn::new("urn:branch:urn:branch:london").unwrap();
    assert_eq!(urn.nss(), "london");
}

#[test]
fn test_new_with_kebab_case_namespace() {
    let urn = BankAccountUrn::new("acct-1").unwrap();
    assert_eq!(urn.nid(), "bank-account");
    assert_eq!(urn.nss(), "acct-1");
}

// ── new_random() — namespace attribute ───────────────────────────────────────

#[test]
fn test_new_random_has_correct_namespace() {
    assert_eq!(BranchUrn::new_random().nid(), "branch");
}

#[test]
fn test_new_random_produces_unique_values() {
    assert_ne!(BranchUrn::new_random(), BranchUrn::new_random());
}

// ── parse() — namespace attribute ────────────────────────────────────────────

#[test]
fn test_parse_valid_urn() {
    assert_eq!(BranchUrn::parse("urn:branch:london").unwrap().nss(), "london");
}

#[test]
fn test_parse_wrong_namespace_fails() {
    let err = BranchUrn::parse("urn:other:london").unwrap_err();
    assert!(err.contains("Invalid URN namespace"));
}

#[test]
fn test_parse_invalid_urn_string_fails() {
    assert!(BranchUrn::parse("not-a-urn").is_err());
}

// ── namespace() / nid() / nss() / to_urn() — namespace attribute ─────────────

#[test]
fn test_namespace_static() {
    assert_eq!(BranchUrn::namespace(), "branch");
}

#[test]
fn test_nid_and_nss() {
    let urn = BranchUrn::new("london").unwrap();
    assert_eq!(urn.nid(), "branch");
    assert_eq!(urn.nss(), "london");
}

#[test]
fn test_to_urn_borrows_inner() {
    let urn = BranchUrn::new("london").unwrap();
    let raw = urn.to_urn();
    assert_eq!(raw.nid(), "branch");
    assert_eq!(raw.nss(), "london");
}

// ── TryFrom<Urn> — namespace attribute ───────────────────────────────────────

#[test]
fn test_try_from_urn_valid() {
    let raw = Urn::from_str("urn:branch:london").unwrap();
    let typed = BranchUrn::try_from(raw).unwrap();
    assert_eq!(typed.nss(), "london");
}

#[test]
fn test_try_from_urn_wrong_namespace_fails() {
    let raw = Urn::from_str("urn:other:london").unwrap();
    assert!(BranchUrn::try_from(raw).is_err());
}

// ── ScopedUrn interop (namespace attribute enables TryFrom, which unlocks ScopedUrn) ──

#[test]
fn test_scoped_urns() {
    let account_urn = BankAccountUrn::new("acct-1").unwrap();
    let branch_urn = BranchUrn::new("london").unwrap();

    let scoped = account_urn.at(branch_urn.clone()).unwrap();
    assert_eq!(scoped.extract_scope::<BranchUrn>().unwrap(), branch_urn);
}
