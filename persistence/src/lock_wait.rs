//! Bounding how long one transaction waits for a row, and recognising the failure
//! that bound produces.
//!
//! Two paths need it for two different rows — the stream row an append and a
//! compaction take ([`crate::PostgresEventStore`], ADR-0022) and the cursor row the
//! liveness beat writes ([`crate::policy_liveness`], ADR-0020) — for the same
//! reason: a wait nobody is still interested in holds a pool connection until the
//! blocker is done, and only the server can end it.

use std::time::Duration;

use sqlx::PgConnection;

/// Postgres `lock_not_available`: a `lock_timeout` expired on a row somebody else
/// holds.
pub(crate) const LOCK_NOT_AVAILABLE: &str = "55P03";

/// Bound how long `conn`'s current transaction waits for a row lock.
///
/// `SET LOCAL`, so the bound dies with the transaction and a connection carries
/// nothing back to the pool; `set_config(…, true)` rather than `SET` because only
/// the former takes a bind parameter. A wait of zero is Postgres's own "no limit",
/// so nothing is sent at all.
pub(crate) async fn bound(conn: &mut PgConnection, wait: Duration) -> Result<(), sqlx::Error> {
    if wait.is_zero() {
        return Ok(());
    }
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(format!("{}ms", wait.as_millis().max(1)))
        .execute(conn)
        .await?;
    Ok(())
}

/// Whether the server gave up waiting for a row somebody else holds, as opposed to
/// any other reason a statement can fail.
pub(crate) fn is_lock_not_available(error: &sqlx::Error) -> bool {
    has_code(error, LOCK_NOT_AVAILABLE)
}

/// Whether `error` is the database refusing a statement with `code`.
pub(crate) fn has_code(error: &sqlx::Error, code: &str) -> bool {
    match error {
        sqlx::Error::Database(db) => db.code().as_deref() == Some(code),
        _ => false,
    }
}
