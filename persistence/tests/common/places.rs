//! Reconciling a stream's place counter after a fixture has written events by hand.
//!
//! `write_event` hands out the next place from `streams.stream_seq`. A fixture that
//! inserts into `events` directly numbers the rows itself and leaves that counter behind,
//! so the next append through the store would hand out a place the fixture has used and
//! fail on `uidx_events_stream_seq` — a failure that looks like a bug in `write_event`.
//! Settling the counter is the part of the store's job a hand-written insert still owes.
//!
//! `allow(dead_code)`: every test binary that says `mod common;` compiles this module.
#![allow(dead_code)]

use sqlx::PgPool;

/// Move every stream's counter up to the last place its events hold.
pub async fn settle(pool: &PgPool) {
    sqlx::query(
        "UPDATE streams AS s \
            SET stream_seq = numbered.last_seq \
           FROM (SELECT stream_id, MAX(stream_seq) AS last_seq FROM events GROUP BY stream_id) \
                AS numbered \
          WHERE numbered.stream_id = s.id AND numbered.last_seq > s.stream_seq",
    )
    .execute(pool)
    .await
    .expect("settling the place counters must succeed");
}

/// Whether `policy` has passed the event at `global_position`.
///
/// A Policy's progress is a place per stream, so "how far has it got" is only a question
/// about a particular event: has this Policy passed the place that event holds in its own
/// stream. A position no event carries has been passed by nobody, which is the point —
/// it is not work anyone is waiting on.
pub async fn passed(pool: &PgPool, policy: &str, global_position: i64) -> bool {
    sqlx::query_scalar::<_, bool>(
        "SELECT EXISTS ( \
             SELECT 1 FROM events e \
             JOIN policy_stream_cursors c \
               ON c.policy = $1 AND c.stream_id = e.stream_id AND c.stream_seq >= e.stream_seq \
             WHERE e.global_position = $2)",
    )
    .bind(policy)
    .bind(global_position)
    .fetch_one(pool)
    .await
    .expect("reading the policy's places must succeed")
}

/// Move `policy` back to just before the event at `global_position`, the way an operator
/// forces a redelivery (ADR-0012).
pub async fn rewind_to_before(pool: &PgPool, policy: &str, global_position: i64) {
    sqlx::query(
        "INSERT INTO policy_stream_cursors (policy, stream_id, stream_seq) \
         SELECT $1, e.stream_id, e.stream_seq - 1 FROM events e WHERE e.global_position = $2 \
         ON CONFLICT (policy, stream_id) \
         DO UPDATE SET stream_seq = EXCLUDED.stream_seq, updated_at = now()",
    )
    .bind(policy)
    .bind(global_position)
    .execute(pool)
    .await
    .expect("the operator's rewind must succeed");
}

/// Every place `policy` holds, by stream — the whole of what it has processed.
pub async fn all(pool: &PgPool, policy: &str) -> Vec<(String, i64)> {
    sqlx::query_as::<_, (String, i64)>(
        "SELECT stream_id, stream_seq FROM policy_stream_cursors \
         WHERE policy = $1 ORDER BY stream_id",
    )
    .bind(policy)
    .fetch_all(pool)
    .await
    .expect("reading the policy's places must succeed")
}
