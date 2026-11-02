//! Deleting streams with what Replay keeps about them (funkode-io/replay#292,
//! [ADR-0033](../../../docs/adr/0033-a-deleted-stream-takes-what-replay-keeps-about-it.md)).
//!
//! Each step is one statement over the whole set of ids, run in the caller's transaction:
//! nothing comes back to this process but a lock count and the rows each statement
//! touched, so what the deletion holds is the ids the caller passed and four numbers,
//! however many events the streams carry.

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
    /// Parked dead letters whose triggering event was deleted, archived with reason
    /// `stream_deleted`.
    pub dead_letters: u64,
}

/// Lock the `streams` rows of `ids`, in id order.
///
/// The same row an append and a compaction take, so an append in flight to one of these
/// streams finishes first and is deleted with it, and one that starts later waits and
/// then creates the stream anew. Id order, so two deletions of overlapping sets queue
/// rather than deadlock.
pub(crate) async fn lock_streams(
    conn: &mut PgConnection,
    ids: &[String],
) -> Result<(), sqlx::Error> {
    sqlx::query("SELECT id FROM streams WHERE id = ANY($1) ORDER BY id FOR UPDATE")
        .bind(ids)
        .execute(conn)
        .await?;
    Ok(())
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

/// Archive every parked dead letter whose triggering event belongs to `ids`, with reason
/// `stream_deleted`, and return how many there were.
///
/// Archived rather than deleted: a parked failure leaves the active set with a record of
/// it, whatever takes it out. A dead letter *addressed to* one of these streams by an
/// event of another stays: its trigger is still there, so it can still be retried.
pub(crate) async fn archive_dead_letters(
    conn: &mut PgConnection,
    ids: &[String],
) -> Result<u64, sqlx::Error> {
    let archived = sqlx::query(
        "WITH moved AS ( \
             DELETE FROM policy_dead_letters dl \
              USING events e \
              WHERE e.id = dl.event_id AND e.stream_id = ANY($1) \
             RETURNING dl.id, dl.policy_name, dl.global_position, dl.event_id, \
                       dl.error_kind, dl.error_message, dl.created_at, dl.aggregate_name, \
                       dl.target_stream_id, dl.command_name, dl.retry_count, \
                       dl.last_retried_at, dl.dispatch_ordinal, dl.deliveries, \
                       dl.last_parked_at \
         ) \
         INSERT INTO discarded_dead_letters \
             (dead_letter_id, policy_name, global_position, event_id, error_kind, \
              error_message, created_at, reason, aggregate_name, target_stream_id, \
              command_name, retry_count, last_retried_at, dispatch_ordinal, \
              deliveries, last_parked_at) \
         SELECT id, policy_name, global_position, event_id, error_kind, error_message, \
                created_at, 'stream_deleted', aggregate_name, target_stream_id, \
                command_name, retry_count, last_retried_at, dispatch_ordinal, \
                deliveries, last_parked_at \
         FROM moved",
    )
    .bind(ids)
    .execute(conn)
    .await?;
    Ok(archived.rows_affected())
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
