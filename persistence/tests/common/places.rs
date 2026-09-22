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
