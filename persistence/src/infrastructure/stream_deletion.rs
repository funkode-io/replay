//! Deleting streams with what Replay keeps about them (funkode-io/replay#292,
//! [ADR-0033](../../../docs/adr/0033-a-deleted-stream-takes-what-replay-keeps-about-it.md)).
//!
//! Each step is one statement over the whole set of ids, run in the caller's transaction.
//! The only rows that come back are the ids [`lock_streams`] locked — a subset of the ids
//! the caller passed — so what the deletion holds is bounded by the caller's request, never
//! by the events the streams carry; event, place and dead-letter data stay on the server.

use sqlx::PgConnection;

/// What [`PostgresEventStore::delete_streams`](crate::PostgresEventStore::delete_streams)
/// removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct DeletedStreams {
    /// Streams deleted: the ids asked for that named a stream.
    pub streams: u64,
    /// Events deleted with them, the archived versions of a compacted stream included.
    pub events: u64,
    /// Policy places deleted with them: one per Policy that had a place in a stream.
    pub places: u64,
    /// Parked dead letters whose triggering event was deleted, deleted with it.
    pub dead_letters: u64,
}

/// Lock the `streams` rows of `ids`, in id order, and return the ids it locked.
///
/// The same row an append and a compaction take, so an append in flight to one of these
/// streams finishes first and is deleted with it, and one that starts later waits and
/// then creates the stream anew. Id order, so two deletions of overlapping sets queue
/// rather than deadlock.
///
/// An id with no row locks nothing, and an append can create that stream while the
/// deletion runs. Every later step works on the ids returned here only: a stream created
/// after the caller's read models were deleted must not lose its events to a deletion
/// whose caller never saw it.
pub(crate) async fn lock_streams(
    conn: &mut PgConnection,
    ids: &[String],
) -> Result<Vec<String>, sqlx::Error> {
    // At most `ids.len()` rows: bounded by the caller's request, not by stored data.
    sqlx::query_scalar(
        "SELECT id FROM streams WHERE id = ANY($1) ORDER BY id \
          LIMIT cardinality($1::text[]) FOR UPDATE",
    )
    .bind(ids)
    .fetch_all(conn)
    .await
}

/// Lock every event of `ids` for deletion, in `global_position` order.
///
/// A Policy writes a place or parks a dead letter only while it holds a key-share lock on
/// the event that write is about, so this waits for any such write in flight, and every
/// one that starts later finds the event gone and writes nothing. The statements after
/// this one therefore see every place and dead letter there will ever be for these
/// events. Position order is the order a checkpoint locks the events of its batch in, so
/// the two cannot deadlock.
///
/// It writes a lock on every row the delete below writes again: the cost of a deletion
/// is twice its events, paid once, by an operation an operator runs.
pub(crate) async fn lock_events(
    conn: &mut PgConnection,
    ids: &[String],
) -> Result<(), sqlx::Error> {
    sqlx::query(
        "SELECT count(*) FROM (SELECT 1 FROM events WHERE stream_id = ANY($1) \
                                ORDER BY global_position FOR UPDATE) locked",
    )
    .bind(ids)
    .execute(conn)
    .await?;
    Ok(())
}

/// Delete every parked dead letter whose triggering event belongs to `ids`, and return
/// how many there were.
///
/// Deleted, not archived: a deletion is a cleanup, and a dead letter whose event is gone
/// can never be retried. The archive of dead letters that already left the active set is
/// history and is not touched. A dead letter *addressed to* one of these streams by an
/// event of another stays: its trigger is still there, so it can still be retried.
pub(crate) async fn delete_dead_letters(
    conn: &mut PgConnection,
    ids: &[String],
) -> Result<u64, sqlx::Error> {
    let deleted = sqlx::query(
        "DELETE FROM policy_dead_letters dl \
          USING events e \
          WHERE e.id = dl.event_id AND e.stream_id = ANY($1)",
    )
    .bind(ids)
    .execute(conn)
    .await?;
    Ok(deleted.rows_affected())
}

/// Delete every Policy's place in `ids`.
///
/// A place outliving its stream is a place in the stream recreated under that id, whose
/// events start again at 1: the Policy would skip every one of them up to where it had
/// got in the old stream.
pub(crate) async fn delete_places(
    conn: &mut PgConnection,
    ids: &[String],
) -> Result<u64, sqlx::Error> {
    let deleted = sqlx::query("DELETE FROM policy_stream_cursors WHERE stream_id = ANY($1)")
        .bind(ids)
        .execute(conn)
        .await?;
    Ok(deleted.rows_affected())
}

/// Delete the events of `ids`, then the streams themselves.
pub(crate) async fn delete_events_and_streams(
    conn: &mut PgConnection,
    ids: &[String],
) -> Result<(u64, u64), sqlx::Error> {
    let events = sqlx::query("DELETE FROM events WHERE stream_id = ANY($1)")
        .bind(ids)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    let streams = sqlx::query("DELETE FROM streams WHERE id = ANY($1)")
        .bind(ids)
        .execute(&mut *conn)
        .await?
        .rows_affected();
    Ok((events, streams))
}
