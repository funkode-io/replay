//! Which streams a Policy looks at this poll, and how far the search got.
//!
//! Discovery is deliberately allowed to be wrong in one direction. It sweeps the log by
//! `global_position` for streams with new events, and a write that had not committed when
//! the sweep passed its position is missed — permanently, because the sweep never looks
//! back. That is the whole reason it is fast: it never waits for a position to fill, so a
//! write held open in one stream delays nothing else (funkode-io/replay#164).
//!
//! What makes the miss safe is that discovery decides nothing. It nominates streams; what
//! each stream is owed is read from `policy_stream_cursors` against the stream's own
//! sequence, which has no holes. A stream the sweep missed is found by the reconciliation
//! ([`crate::policy_runner`]), late rather than never
//! ([ADR-0026](../../docs/adr/0026-a-policy-tracks-its-position-per-stream.md)).
//!
//! Pure, so it needs no database.

/// The streams to look at, and how far the log was swept to find them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Discovered {
    /// Each stream once, in the order its first new event appears in the log. A Policy
    /// that walks them in this order walks the log roughly in order, which is what an
    /// operator reading a trace expects, without anything depending on it.
    pub(crate) streams: Vec<String>,
    /// The position the sweep reached. Its only use is where the next sweep starts.
    pub(crate) swept_through: i64,
}

/// Reduce a sweep of `(global_position, stream_id)` rows to the streams it nominates.
///
/// `swept_through` is the last position actually read, not the last one asked for: a
/// sweep cut short by its own limit must not claim the rest of the log.
pub(crate) fn discovered_from_sweep(from: i64, rows: Vec<(i64, String)>) -> Discovered {
    let mut streams: Vec<String> = Vec::with_capacity(rows.len());
    let mut swept_through = from;

    for (position, stream_id) in rows {
        swept_through = position;
        if !streams.contains(&stream_id) {
            streams.push(stream_id);
        }
    }

    Discovered {
        streams,
        swept_through,
    }
}

#[cfg(test)]
mod tests {
    use super::{discovered_from_sweep, Discovered};

    fn sweep(rows: &[(i64, &str)]) -> Vec<(i64, String)> {
        rows.iter()
            .map(|(position, stream)| (*position, (*stream).to_string()))
            .collect()
    }

    #[test]
    fn a_sweep_that_found_nothing_leaves_the_search_where_it_was() {
        assert_eq!(
            discovered_from_sweep(7, Vec::new()),
            Discovered {
                streams: Vec::new(),
                swept_through: 7,
            }
        );
    }

    #[test]
    fn a_stream_is_nominated_once_however_many_events_it_has() {
        let discovered = discovered_from_sweep(
            0,
            sweep(&[(1, "a"), (2, "b"), (3, "a"), (4, "a"), (5, "b")]),
        );

        assert_eq!(discovered.streams, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(discovered.swept_through, 5);
    }

    /// The order is where each stream *first* appears, so a stream whose backlog starts
    /// early is looked at before one that only just began.
    #[test]
    fn streams_are_nominated_in_the_order_the_log_first_mentions_them() {
        let discovered = discovered_from_sweep(0, sweep(&[(9, "late"), (10, "later")]));

        assert_eq!(
            discovered.streams,
            vec!["late".to_string(), "later".to_string()]
        );
    }

    /// A sweep stops where its limit stopped it. Claiming further would skip the streams
    /// whose events sit in the part it never read.
    #[test]
    fn the_sweep_reaches_the_last_position_it_read() {
        let discovered = discovered_from_sweep(100, sweep(&[(101, "a"), (102, "b")]));

        assert_eq!(discovered.swept_through, 102);
    }

    /// Positions the sweep never saw are not holes to it: it is looking for streams, and
    /// a position that no event carries names no stream.
    #[test]
    fn a_missing_position_is_not_something_the_sweep_stops_at() {
        let discovered = discovered_from_sweep(0, sweep(&[(1, "a"), (40, "b")]));

        assert_eq!(discovered.streams, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(discovered.swept_through, 40);
    }
}
