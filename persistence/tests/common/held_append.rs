//! An append frozen mid-flight: the state a Policy feed has to be right about.
//!
//! The feed's whole order rests on what happens between a write taking its position and
//! that write becoming visible ([ADR-0020](../../../docs/adr/0020-policy-feed-reads-below-the-commit-watermark.md)),
//! and the only way to observe that window is to stop a write inside it.
//!
//! The write goes through `append_event`, the store's own writer: it takes the stream
//! lock, bumps `streams.version` and draws the `global_position`, so what the tests see
//! is an append that has genuinely started. A hand-written `INSERT INTO events` would
//! leave the stream's version behind the log and prove nothing about the real path.
//!
//! `allow(dead_code)` module-wide: every test binary that says `mod common;` compiles
//! this module, including the ones that never hold an append open.
#![allow(dead_code)]

use sqlx::{PgPool, Postgres, Row, Transaction};
use uuid::Uuid;

/// One append, started and not finished, and the position it took.
///
/// The transaction is open: hold it to keep the append in flight, commit it to land it,
/// roll it back to burn the position the way an aborted append does.
pub struct HeldAppend {
    pub tx: Transaction<'static, Postgres>,
    pub position: i64,
}

/// Start the next append of the stream the event at `source` belongs to, and stop it
/// before it commits.
///
/// The payload is that event's, so the Policy under test reacts to it as it would to any
/// other event of that stream; what is new is the version, which `append_event` assigns.
///
/// Nothing else may append to that stream while the returned transaction is open: the
/// second append would wait on the stream lock this one holds until the test commits it,
/// which is the test waiting for itself. Hold the write on a stream of its own.
pub async fn hold_an_append_open(pool: &PgPool, source: i64) -> HeldAppend {
    let mut tx = pool.begin().await.expect("beginning must succeed");
    let position = append_inside(&mut tx, pool, source).await;
    HeldAppend { tx, position }
}

/// Append the next event of `source`'s stream inside a transaction the caller already
/// holds, returning the position it took.
///
/// Separate from [`hold_an_append_open`] so a test can control what the transaction did
/// *before* it appended: Postgres assigns a transaction id at the first write of any
/// kind, so a transaction that acts elsewhere first carries a lower id than one that
/// appends earlier. That is how a committed event comes to sit at a low position under a
/// high `commit_txid`.
pub async fn append_inside(
    tx: &mut Transaction<'static, Postgres>,
    pool: &PgPool,
    source: i64,
) -> i64 {
    let shape = sqlx::query(
        "SELECT e.data, e.metadata, e.type, e.stream_id, s.type AS stream_type \
         FROM events e JOIN streams s ON s.id = e.stream_id \
         WHERE e.global_position = $1",
    )
    .bind(source)
    .fetch_one(pool)
    .await
    .expect("the event to model the append on must exist");

    let id = Uuid::new_v4();
    sqlx::query("SELECT id FROM append_event($1, $2, $3, $4, $5, $6, NULL)")
        .bind(id)
        .bind(shape.get::<serde_json::Value, _>("data"))
        .bind(shape.get::<serde_json::Value, _>("metadata"))
        .bind(shape.get::<String, _>("type"))
        .bind(shape.get::<String, _>("stream_id"))
        .bind(shape.get::<String, _>("stream_type"))
        .fetch_one(&mut **tx)
        .await
        .expect("the append must succeed inside the held transaction");

    // Read back inside the transaction: nobody else can see this row yet.
    sqlx::query_scalar("SELECT global_position FROM events WHERE id = $1")
        .bind(id)
        .fetch_one(&mut **tx)
        .await
        .expect("the append's own row is visible to itself")
}
