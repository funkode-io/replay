//! Inline-projection catch-up: applying the events of transactions that did not apply a
//! projection themselves (funkode-io/replay#289,
//! [ADR-0032](../../../docs/adr/0032-an-inline-projection-catches-up-on-transactions-that-did-not-apply-it.md)).
//!
//! Two records make it exact without a position that can be passed out of order:
//!
//! - the **catch-up horizon**, `projections.caught_up_below`: every transaction whose id
//!   is below it has been accounted for. It is always the `xmin` of a snapshot, so it never
//!   passes a transaction that was still running;
//! - the **applied marks**, `projection_applied_transactions`: transactions at or above the
//!   horizon whose events this version of the projection has already been handed.
//!
//! A catch-up reads, under one snapshot, the events above the horizon whose transaction
//! has no mark, hands them to `handle` a bounded page at a time, and then **seals**: marks
//! every transaction the snapshot shows as committed at or above its `xmin`, and moves the
//! horizon to that `xmin`. Marking only once the read is over matters: a mark is visible to
//! the transaction that wrote it, so a page-by-page mark would hide the rest of a
//! transaction larger than a page from the very catch-up applying it.
//!
//! Transaction ids are `xid8`, which SQLx does not decode: they travel as text and are
//! cast on the server.

use std::time::Duration;

use serde_json::Value;
use sqlx::{PgConnection, Postgres, QueryBuilder, Row};

use crate::inline_projection::ErasedInlineProjection;
use crate::{PersistedEvent, StreamFilter};

use super::advisory_keys::{PROJECTION_LOCK_CLASS, REGISTRY_LOCK_CLASS};
use super::postgres::PostgresEventStore;

/// A type-erased Postgres inline projection, as the store holds it.
pub(crate) type ErasedPostgresProjection = dyn ErasedInlineProjection<Exec = PgConnection>;

/// Built-in default for how often the background task catches every projection up.
pub(crate) const DEFAULT_CATCH_UP_INTERVAL: Duration = Duration::from_secs(60);

/// Environment variable overriding the catch-up interval, in seconds.
const CATCH_UP_INTERVAL_ENV_VAR: &str = "REPLAY_PROJECTION_CATCH_UP_SECS";

/// Resolve how often the background task catches every projection up.
///
/// Precedence: store override → `REPLAY_PROJECTION_CATCH_UP_SECS` → 60s.
pub(crate) fn resolve_catch_up_interval(store_override: Option<Duration>) -> Duration {
    catch_up_interval_or_default(
        store_override,
        std::env::var(CATCH_UP_INTERVAL_ENV_VAR).ok(),
    )
}

/// The precedence itself, over values rather than the process environment.
///
/// Zero is **disabled**, not unset: it is how a consumer who drives
/// [`PostgresEventStore::catch_up_projections`] itself turns the task off. An unparseable
/// value is a typo, and a typo must not silently turn a safety net off: it falls back to
/// the default.
pub(crate) fn catch_up_interval_or_default(
    declared: Option<Duration>,
    env: Option<String>,
) -> Duration {
    if let Some(interval) = declared {
        return interval;
    }
    env.and_then(|raw| raw.trim().parse::<u64>().ok())
        .map_or(DEFAULT_CATCH_UP_INTERVAL, Duration::from_secs)
}

/// Take every named projection's lock for the rest of this **session**, in name order.
///
/// `build()` needs the locks before its `REPEATABLE READ` transaction takes its snapshot,
/// so that the snapshot is never older than a catch-up that committed while it waited —
/// a transaction-scoped lock would be taken by a statement that has already fixed the
/// snapshot. The caller owns releasing them: it unlocks on success and closes the
/// connection on every path.
///
/// Name order, so two processes building overlapping sets cannot deadlock — byte order,
/// which is what [`lock_every_registered`] asks the database for. The registry lock is
/// taken shared first, so builds run side by side and a deletion of streams waits for
/// them all.
pub(crate) async fn lock_for_build(
    conn: &mut PgConnection,
    names: &[String],
) -> Result<(), replay::Error> {
    sqlx::query("SELECT pg_advisory_lock_shared($1, 0)")
        .bind(REGISTRY_LOCK_CLASS)
        .execute(&mut *conn)
        .await
        .map_err(|e| crate::db_error(e).with_operation("locking the inline projection registry"))?;

    let mut names: Vec<&str> = names.iter().map(String::as_str).collect();
    names.sort_unstable();
    names.dedup();
    for name in names {
        sqlx::query("SELECT pg_advisory_lock($1, hashtext($2))")
            .bind(PROJECTION_LOCK_CLASS)
            .bind(name)
            .execute(&mut *conn)
            .await
            .map_err(|e| {
                crate::db_error(e)
                    .with_operation("locking an inline projection for build")
                    .with_context("projection", name.to_string())
            })?;
    }
    Ok(())
}

/// Release every session lock [`lock_for_build`] took.
pub(crate) async fn unlock_after_build(conn: &mut PgConnection) -> Result<(), replay::Error> {
    sqlx::query("SELECT pg_advisory_unlock_all()")
        .execute(conn)
        .await
        .map_err(crate::db_error)?;
    Ok(())
}

/// Take `name`'s lock for the current transaction. With `wait` false, return `false`
/// instead of waiting when another session holds it.
pub(crate) async fn lock_for_catch_up(
    conn: &mut PgConnection,
    name: &str,
    wait: bool,
) -> Result<bool, replay::Error> {
    if wait {
        sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext($2))")
            .bind(PROJECTION_LOCK_CLASS)
            .bind(name)
            .execute(conn)
            .await
            .map_err(|e| lock_error(e, name))?;
        return Ok(true);
    }
    sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_xact_lock($1, hashtext($2))")
        .bind(PROJECTION_LOCK_CLASS)
        .bind(name)
        .fetch_one(conn)
        .await
        .map_err(|e| lock_error(e, name))
}

/// Take, for the current transaction, the registry lock exclusively and then the lock of
/// every inline projection the registry holds a row for — registered in this process or
/// in any other — in name order, waiting for each.
///
/// What a deletion of streams takes (ADR-0033): a catch-up or a rebuild that read their
/// events before the deletion would otherwise write rows for them after the caller has
/// deleted its own. The registry lock covers a first registration, whose row does not
/// exist until its build commits; the names are read after it is taken, so they include
/// every build it waited for, and are locked for the catch-ups.
///
/// Byte order (`COLLATE "C"`), the order `build()` sorts its names in: under the
/// database's own collation two names can sort the other way round, and a deletion and a
/// build would deadlock. One statement for the names, returning one row however many
/// there are; the ordered subquery feeds the aggregate, which takes the locks in its order.
pub(crate) async fn lock_every_registered(conn: &mut PgConnection) -> Result<(), replay::Error> {
    sqlx::query("SELECT pg_advisory_xact_lock($1, 0)")
        .bind(REGISTRY_LOCK_CLASS)
        .execute(&mut *conn)
        .await
        .map_err(|e| crate::db_error(e).with_operation("locking the inline projection registry"))?;
    sqlx::query(
        "SELECT count(pg_advisory_xact_lock($1, hashtext(name))) \
           FROM (SELECT name FROM projections ORDER BY name COLLATE \"C\") registered",
    )
    .bind(PROJECTION_LOCK_CLASS)
    .execute(conn)
    .await
    .map_err(|e| crate::db_error(e).with_operation("locking every inline projection"))?;
    Ok(())
}

fn lock_error(error: sqlx::Error, name: &str) -> replay::Error {
    crate::db_error(error)
        .with_operation("locking an inline projection for catch-up")
        .with_context("projection", name.to_string())
}

/// The snapshot one catch-up or rebuild reads under, as `pg_snapshot` text.
///
/// Everything it accounts for is decided against this: the events it may apply are the
/// ones visible in it, and the horizon it leaves is its `xmin`.
pub(crate) struct Cut {
    snapshot: String,
}

impl Cut {
    /// The current snapshot of `conn`'s transaction — under `REPEATABLE READ` the one the
    /// transaction reads, under `READ COMMITTED` the one this statement reads.
    pub(crate) async fn current(conn: &mut PgConnection) -> Result<Cut, replay::Error> {
        let snapshot: String = sqlx::query_scalar("SELECT pg_current_snapshot()::text")
            .fetch_one(conn)
            .await
            .map_err(crate::db_error)?;
        Ok(Cut { snapshot })
    }

    pub(crate) fn from_text(snapshot: String) -> Cut {
        Cut { snapshot }
    }
}

/// Which events [`apply_pages`] reads.
pub(crate) enum Scope<'a> {
    /// Everything the projection's filter admits — a rebuild or a first registration. The
    /// caller's transaction must read the cut's snapshot (`REPEATABLE READ`).
    History,
    /// Only the events a catch-up owes: above `horizon`, visible in the cut, and not
    /// marked for this version.
    Gap { horizon: &'a str },
}

/// Hand `projection` every event `scope` selects, a page of at most `chunk_size` at a time
/// in `global_position` order. Returns how many events were read.
///
/// Marks nothing: the caller [`seal`]s once every page has been read.
///
/// Peak retention is one page: each is loaded, handed over and dropped before the next is
/// read. The pages are keyset-paged on `global_position` inside the caller's transaction
/// (ADR-0011), because `handle` writes on the same connection a row stream would hold.
pub(crate) async fn apply_pages(
    conn: &mut PgConnection,
    projection: &mut ErasedPostgresProjection,
    chunk_size: usize,
    cut: &Cut,
    scope: Scope<'_>,
) -> Result<u64, replay::Error> {
    let name = projection.name().to_string();
    let version = projection.version();
    // A replay reads what the projection's filter admits; a catch-up stands in for the
    // live append, which never applies that filter, so it reads what the live path would
    // have been offered. Only the declared event types narrow both.
    let filter = match &scope {
        Scope::History => projection.stream_filter(),
        Scope::Gap { .. } => StreamFilter::All,
    };
    let types = projection.event_types();

    let mut after: Option<i64> = match &scope {
        Scope::History => None,
        Scope::Gap { horizon } => {
            match gap_start(&mut *conn, &name, version, cut, horizon).await? {
                Some(first) => Some(first - 1),
                None => return Ok(0),
            }
        }
    };

    let mut read = 0u64;
    loop {
        let page = load_page(
            &mut *conn,
            PageQuery {
                name: &name,
                version,
                filter: filter.clone(),
                types: types.as_deref(),
                cut,
                scope: &scope,
                after,
                limit: chunk_size,
            },
        )
        .await?;

        if page.events.is_empty() {
            break;
        }
        let fetched = page.events.len();
        read += fetched as u64;
        after = page.last_position;

        projection.handle(&mut *conn, &page.events).await?;
        drop(page);

        // A short page is the last one: the cursor has passed the final row.
        if fetched < chunk_size {
            break;
        }
    }

    Ok(read)
}

/// The lowest `global_position` a catch-up owes `name` at `version`: above `horizon`,
/// visible in the cut, without an applied mark. `None` when nothing is owed.
///
/// The pages walk `global_position` from here, so without it the first page would walk
/// from the beginning of the log to find the recent events above the horizon. It reads
/// the horizon's range — `(commit_txid, global_position)` covers it, though a range that
/// is most of the log may be cheaper to scan — and never walks `global_position`'s own
/// index from the start of the log (funkode-io/replay#306):
///
/// Keep `OFFSET 0`: without the fence, even a custom plan can walk the position index
/// from the start (#306). `persistent(false)` plans the range with the current horizon.
///
/// This predicate must include every row `load_page` can return; extra rows are safe.
/// Excluding marked transactions avoids paging them again under a held-back horizon.
/// A `None` result skips pages, but the caller must still seal (ADR-0032).
async fn gap_start(
    conn: &mut PgConnection,
    name: &str,
    version: i32,
    cut: &Cut,
    horizon: &str,
) -> Result<Option<i64>, replay::Error> {
    sqlx::query_scalar(
        "SELECT MIN(gap.global_position) \
           FROM (SELECT global_position, commit_txid FROM events \
                  WHERE commit_txid >= $1::xid8 OFFSET 0) gap \
          WHERE pg_visible_in_snapshot(gap.commit_txid, $2::pg_snapshot) \
            AND NOT EXISTS (SELECT 1 FROM projection_applied_transactions a \
                             WHERE a.projection = $3 AND a.version = $4 \
                               AND a.txid = gap.commit_txid)",
    )
    .persistent(false)
    .bind(horizon)
    .bind(cut.snapshot.as_str())
    .bind(name)
    .bind(version)
    .fetch_one(conn)
    .await
    .map_err(crate::db_error)
}

/// Catch `projection` up on the gap above `horizon`, or adopt the log when it has no
/// horizon yet, then seal at the cut. Returns how many events were read.
///
/// The caller holds the projection's lock and has checked that the stored version is the
/// projection's code version.
pub(crate) async fn catch_up(
    conn: &mut PgConnection,
    projection: &mut ErasedPostgresProjection,
    chunk_size: usize,
    cut: &Cut,
    horizon: Option<&str>,
) -> Result<u64, replay::Error> {
    let name = projection.name().to_string();
    let version = projection.version();

    let read = match horizon {
        Some(horizon) => {
            apply_pages(
                &mut *conn,
                projection,
                chunk_size,
                cut,
                Scope::Gap { horizon },
            )
            .await?
        }
        None => {
            // No horizon for this version: a row from before the horizon existed, or one
            // an earlier release rebuilt. What was applied to it is unknown, so the log is
            // taken as it stands — sealing marks every transaction visible above the cut's
            // `xmin` without replaying it — and catching up starts from here.
            tracing::info!(
                projection = %name,
                version,
                "inline projection catch-up: adopted the log as it stands (no horizon yet)"
            );
            0
        }
    };

    seal(conn, &name, version, cut).await?;
    Ok(read)
}

/// Account for everything the cut shows, once every page under it has been applied.
///
/// Marks each transaction the cut shows as committed at or above its `xmin` — every one of
/// them was applied by its own append, by the pages just read, or had nothing this
/// projection reads — then moves `name`'s horizon to that `xmin` for `version`, and forgets
/// the marks it no longer needs: those below it, and those of older versions.
///
/// The marking is one statement over the transactions since the oldest one still running
/// when the cut was taken, read from `(commit_txid, global_position)`; nothing of it comes
/// back to this process.
pub(crate) async fn seal(
    conn: &mut PgConnection,
    name: &str,
    version: i32,
    cut: &Cut,
) -> Result<(), replay::Error> {
    sqlx::query(
        "INSERT INTO projection_applied_transactions (projection, version, txid) \
         SELECT DISTINCT $1, $2, commit_txid FROM events \
          WHERE commit_txid >= pg_snapshot_xmin($3::pg_snapshot) \
            AND pg_visible_in_snapshot(commit_txid, $3::pg_snapshot) \
         ON CONFLICT DO NOTHING",
    )
    // Unnamed, like the gap's starting point: a generic plan of this range read is planned
    // blind to the horizon, and nothing would stop it scanning `events` (funkode-io/replay#306).
    .persistent(false)
    .bind(name)
    .bind(version)
    .bind(&cut.snapshot)
    .execute(&mut *conn)
    .await
    .map_err(crate::db_error)?;

    // The horizon is only ever the version's own: a release that does not know about it
    // can change `version` and leave `caught_up_below` behind, and a horizon read for the
    // wrong version is read as none.
    sqlx::query(
        "UPDATE projections \
            SET caught_up_below = CASE WHEN caught_up_version = $3 \
                                       THEN GREATEST(caught_up_below, \
                                                     pg_snapshot_xmin($2::pg_snapshot)) \
                                       ELSE pg_snapshot_xmin($2::pg_snapshot) END, \
                caught_up_version = $3 \
          WHERE name = $1",
    )
    .bind(name)
    .bind(&cut.snapshot)
    .bind(version)
    .execute(&mut *conn)
    .await
    .map_err(crate::db_error)?;

    sqlx::query(
        "DELETE FROM projection_applied_transactions \
          WHERE projection = $1 \
            AND (version < $2 OR (version = $2 AND txid < pg_snapshot_xmin($3::pg_snapshot)))",
    )
    .bind(name)
    .bind(version)
    .bind(&cut.snapshot)
    .execute(&mut *conn)
    .await
    .map_err(crate::db_error)?;

    Ok(())
}

/// Mark the current transaction as having applied each named projection version.
///
/// Called by the append path, in the append transaction, for every projection it
/// registered — whether or not one of its events matched — so a catch-up never reads an
/// append it was offered. One statement, however many projections.
pub(crate) async fn mark_applied_by_append(
    conn: &mut PgConnection,
    names: &[&str],
    versions: &[i32],
) -> Result<(), replay::Error> {
    sqlx::query(
        "INSERT INTO projection_applied_transactions (projection, version, txid) \
         SELECT name, version, pg_current_xact_id() \
           FROM unnest($1::text[], $2::int[]) AS applied(name, version) \
         ON CONFLICT DO NOTHING",
    )
    .bind(names)
    .bind(versions)
    .execute(conn)
    .await
    .map_err(crate::db_error)?;
    Ok(())
}

struct PageQuery<'a> {
    name: &'a str,
    version: i32,
    filter: StreamFilter,
    types: Option<&'a [String]>,
    cut: &'a Cut,
    scope: &'a Scope<'a>,
    after: Option<i64>,
    limit: usize,
}

struct Page {
    events: Vec<PersistedEvent<Value>>,
    last_position: Option<i64>,
}

/// Load one page of the events `query` selects, in `global_position` order.
async fn load_page(conn: &mut PgConnection, query: PageQuery<'_>) -> Result<Page, replay::Error> {
    let mut builder: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT id, data, metadata, stream_id, type, version, created, aggregate_version, \
         compacted_snapshot, global_position \
         FROM events WHERE compacted_snapshot = FALSE AND (",
    );
    PostgresEventStore::add_filters(&mut builder, query.filter);
    builder.push(")");

    if let Some(types) = query.types {
        builder.push(" AND type = ANY(").push_bind(types).push(")");
    }

    if let Scope::Gap { horizon } = query.scope {
        builder
            .push(" AND commit_txid >= ")
            .push_bind(*horizon)
            .push("::xid8 AND pg_visible_in_snapshot(commit_txid, ")
            .push_bind(query.cut.snapshot.as_str())
            .push(
                "::pg_snapshot) AND NOT EXISTS (SELECT 1 FROM projection_applied_transactions a \
                   WHERE a.projection = ",
            )
            .push_bind(query.name)
            .push(" AND a.version = ")
            .push_bind(query.version)
            .push(" AND a.txid = events.commit_txid)");
    }

    if let Some(after) = query.after {
        builder.push(" AND global_position > ").push_bind(after);
    }

    builder
        .push(" ORDER BY global_position ASC LIMIT ")
        // Saturating rather than `as`: a wrapped cast would send a negative LIMIT.
        .push_bind(i64::try_from(query.limit).unwrap_or(i64::MAX));

    // A gap page is unnamed too, so it is planned with its horizon and cursor rather than
    // generically: under a held-back horizon a generic plan could read the whole range above
    // it for every page. A rebuild's pages have no horizon and stay cached.
    let rows = builder
        .build()
        .persistent(!matches!(query.scope, Scope::Gap { .. }))
        .fetch_all(&mut *conn)
        .await
        .map_err(crate::db_error)?;

    let mut last_position = None;
    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        last_position = Some(row.get::<i64, _>("global_position"));
        events.push(PersistedEvent::<Value>::try_from(row)?);
    }

    Ok(Page {
        events,
        last_position,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{catch_up_interval_or_default, DEFAULT_CATCH_UP_INTERVAL};

    #[test]
    fn the_catch_up_interval_falls_back_to_the_built_in_default() {
        assert_eq!(
            catch_up_interval_or_default(None, None),
            DEFAULT_CATCH_UP_INTERVAL
        );
    }

    #[test]
    fn the_catch_up_interval_reads_the_env_var_in_seconds() {
        assert_eq!(
            catch_up_interval_or_default(None, Some("15".into())),
            Duration::from_secs(15)
        );
    }

    #[test]
    fn a_store_override_beats_the_env_var_for_the_catch_up_interval() {
        assert_eq!(
            catch_up_interval_or_default(Some(Duration::from_millis(200)), Some("15".into())),
            Duration::from_millis(200)
        );
    }

    /// Zero is the documented opt-out, from either source.
    #[test]
    fn zero_disables_the_catch_up_task_rather_than_reading_as_unset() {
        assert_eq!(
            catch_up_interval_or_default(Some(Duration::ZERO), None),
            Duration::ZERO
        );
        assert_eq!(
            catch_up_interval_or_default(None, Some("0".into())),
            Duration::ZERO
        );
    }

    /// A typo must not silently turn the safety net off.
    #[test]
    fn an_unparseable_catch_up_interval_falls_back_to_the_default() {
        for env in ["not-a-number", "-1", "60s", ""] {
            assert_eq!(
                catch_up_interval_or_default(None, Some(env.into())),
                DEFAULT_CATCH_UP_INTERVAL,
                "{env:?} must not disable the catch-up"
            );
        }
    }
}
