//! Every Postgres advisory-lock key Replay takes, in one place, so a new one is chosen
//! against all the others.
//!
//! Postgres keeps the one-key form (`pg_advisory_lock(bigint)`) and the two-key form
//! (`pg_advisory_lock(int, int)`) in separate spaces: a one-key lock never conflicts with a
//! two-key one, whatever the numbers. Replay uses them so:
//!
//! - **one-key**, `hashtext(policy name)`: a Policy's leadership, held for the session by
//!   the [Policy runner](crate::PolicyRunner)'s lock-manager connection (ADR-0008). It is
//!   the only one-key lock Replay takes;
//! - **two-key**, `(class, key)`, with a class of Replay's own for each purpose. The
//!   classes defined here are spelled as four ASCII letters so a lock seen in
//!   `pg_locks.classid` names itself; the bulk lane's claim lock hashes a name instead:
//!
//! | Class | Value         | Second key                 | What it guards |
//! |-------|---------------|----------------------------|----------------|
//! | `RPJC` | `0x5250_4A43` | `hashtext(projection name)` | an inline projection's catch-up and build |
//! | `RPJR` | `0x5250_4A52` | `0`                         | the inline projection registry as a whole |
//! | `RCGT` | `0x5243_4754` | `0`                         | the commit gate: appends in flight past it |
//! | `hashtext('replay.bulk_claim')` | `0x59C2_65A4` | `hashtext(policy name)` | one bulk-lane claim at a time per Policy (`policy_runner/bulk_lane.rs`) |
//!
//! A class must differ from every other class here; the second key only separates locks
//! within one class.

/// The class every projection lock is taken in: `(PROJECTION_LOCK_CLASS,
/// hashtext(projection name))`.
///
/// The two-key form keeps these locks apart from the policy runner's one-key locks over
/// `hashtext(policy name)`: a Policy and a projection sharing a name must not exclude each
/// other. The value spells `RPJC`.
pub(crate) const PROJECTION_LOCK_CLASS: i32 = 0x5250_4A43;

/// The advisory lock on the registry as a whole: `(REGISTRY_LOCK_CLASS, 0)`. A class of its
/// own so no projection's `hashtext` can land on it. The value spells `RPJR`.
///
/// `build()` holds it shared while it builds, and a deletion of streams takes it
/// exclusively: a projection registered for the first time has no registry row for the
/// deletion to find and lock by name until its build commits (ADR-0033).
pub(crate) const REGISTRY_LOCK_CLASS: i32 = 0x5250_4A52;

/// The advisory lock of the commit gate: `(COMMIT_GATE_LOCK_CLASS, 0)`. The value spells
/// `RCGT`.
///
/// An append whose gate is not the always-continue one takes it shared, for the rest of its
/// transaction, immediately before calling the gate;
/// [`await_in_flight_commits`](crate::PostgresEventStore::await_in_flight_commits) takes it
/// exclusively and releases it at once. So once that call returns, every append that asked
/// its gate before the call began has committed or rolled back (ADR-0035).
pub(crate) const COMMIT_GATE_LOCK_CLASS: i32 = 0x5243_4754;

/// The class the bulk lane's claim lock is taken in, as the server computes
/// `hashtext('replay.bulk_claim')` — the query spells the name, so this value is the
/// reference the other classes are checked against, and an integration test pins it.
#[cfg(test)]
const BULK_CLAIM_LOCK_CLASS: i32 = 0x59C2_65A4;

#[cfg(test)]
mod tests {
    use super::*;

    /// Each class spells its four letters, so a lock seen in `pg_locks` names itself.
    #[test]
    fn each_class_spells_its_name() {
        assert_eq!(&PROJECTION_LOCK_CLASS.to_be_bytes(), b"RPJC");
        assert_eq!(&REGISTRY_LOCK_CLASS.to_be_bytes(), b"RPJR");
        assert_eq!(&COMMIT_GATE_LOCK_CLASS.to_be_bytes(), b"RCGT");
    }

    /// Two purposes sharing a class would exclude each other.
    #[test]
    fn no_two_classes_collide() {
        let classes = [
            PROJECTION_LOCK_CLASS,
            REGISTRY_LOCK_CLASS,
            COMMIT_GATE_LOCK_CLASS,
            BULK_CLAIM_LOCK_CLASS,
        ];
        for (i, a) in classes.iter().enumerate() {
            for b in &classes[i + 1..] {
                assert_ne!(a, b, "two advisory-lock classes share a value");
            }
        }
    }
}
