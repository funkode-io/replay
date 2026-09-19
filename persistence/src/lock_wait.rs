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

/// The largest `lock_timeout` PostgreSQL takes: the setting is an integer number
/// of milliseconds, so anything past `i32::MAX` — about 24.8 days — is refused
/// outright.
const MAX_LOCK_TIMEOUT_MS: u128 = i32::MAX as u128;

/// The millisecond limit `wait` is actually sent as, and therefore the only value
/// worth reporting when it fires.
///
/// Zero stays zero, which is Postgres's "no limit". A sub-millisecond wait is
/// floored to `1` rather than truncated to zero, which would read as the opposite
/// of what the caller asked for. A wait past what the server accepts is clamped to
/// the ceiling rather than sent: at 24.8 days the difference is not one anybody is
/// waiting on, and an unclamped value fails the `set_config` itself — turning a
/// bound on one statement into a failure of every append and compaction.
pub(crate) fn limit_ms(wait: Duration) -> u128 {
    if wait.is_zero() {
        return 0;
    }
    wait.as_millis().clamp(1, MAX_LOCK_TIMEOUT_MS)
}

/// Bound how long `conn`'s current transaction waits for a row lock.
///
/// `SET LOCAL`, so the bound dies with the transaction and a connection carries
/// nothing back to the pool; `set_config(…, true)` rather than `SET` because only
/// the former takes a bind parameter.
///
/// A wait of zero is written out as `0ms` rather than skipped: zero is Postgres's
/// own "no limit", and a consumer who asked for no limit must get it even on a
/// connection whose session carries a `lock_timeout` of its own. Skipping the
/// statement would inherit that setting and report the resulting failure as a wait
/// this library never made.
pub(crate) async fn bound(conn: &mut PgConnection, wait: Duration) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(format!("{}ms", limit_ms(wait)))
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

/// Fakes for the failures this module recognises.
#[cfg(test)]
pub(crate) mod test_support {
    /// A database error carrying `code`, since sqlx exposes no constructor for
    /// one: the SQLSTATE is the only part of its shape this crate reads.
    pub(crate) fn refused(code: &'static str, message: &'static str) -> sqlx::Error {
        // sqlx has no public constructor for a DatabaseError, so the check is
        // exercised through the one shape that matters to it: the SQLSTATE.
        struct Refused {
            code: &'static str,
            message: &'static str,
        }
        impl std::fmt::Debug for Refused {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.message)
            }
        }
        impl std::fmt::Display for Refused {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str(self.message)
            }
        }
        impl std::error::Error for Refused {}
        impl sqlx::error::DatabaseError for Refused {
            fn message(&self) -> &str {
                self.message
            }
            fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
                Some(std::borrow::Cow::Borrowed(self.code))
            }
            fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
                self
            }
            fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
                self
            }
            fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
                self
            }
            fn kind(&self) -> sqlx::error::ErrorKind {
                sqlx::error::ErrorKind::Other
            }
        }
        sqlx::Error::Database(Box::new(Refused { code, message }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Zero is the opt-out, and stays zero all the way to the server.
    #[test]
    fn no_limit_is_written_as_no_limit() {
        assert_eq!(limit_ms(Duration::ZERO), 0);
    }

    /// A wait shorter than the setting's resolution is still a wait: truncating it
    /// to zero would turn the tightest bound anyone can ask for into none at all.
    #[test]
    fn a_sub_millisecond_wait_is_floored_to_a_millisecond() {
        assert_eq!(limit_ms(Duration::from_micros(100)), 1);
        assert_eq!(limit_ms(Duration::from_nanos(1)), 1);
    }

    #[test]
    fn a_wait_the_setting_can_express_is_sent_as_it_is() {
        assert_eq!(limit_ms(Duration::from_secs(30)), 30_000);
    }

    /// Past the ceiling the server refuses the statement, which would fail every
    /// append instead of bounding one.
    #[test]
    fn a_wait_past_what_postgres_accepts_is_clamped_to_its_ceiling() {
        assert_eq!(limit_ms(Duration::MAX), MAX_LOCK_TIMEOUT_MS);
        assert_eq!(
            limit_ms(Duration::from_millis(MAX_LOCK_TIMEOUT_MS as u64 + 1)),
            MAX_LOCK_TIMEOUT_MS
        );
    }
}
