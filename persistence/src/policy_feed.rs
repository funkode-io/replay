//! The feed-window decision: given the positions read past a Policy's cursor,
//! how far may the cursor advance, and which of those positions are delivered?
//!
//! Contiguity is a property of the **unfiltered** `global_position` stream, not of
//! the rows a Policy asked for. A position whose event the Policy's
//! [`StreamFilter`](crate::StreamFilter) excludes still exists in the log, so it
//! advances the cursor without firing a reaction — the same shape a compaction
//! snapshot uses (ADR-0004). Deciding otherwise is what used to wedge every Policy
//! whose filter was narrower than `all()` on its first poll.
//!
//! The decision is a pure function over a window that has already been read, so it
//! is exercised without a database, and it is the single place where the rule about
//! how far a cursor may move is written down.

/// One position from the window read past a Policy's cursor.
///
/// `delivered` is `None` when the cursor must advance past the position without
/// firing a reaction: a synthetic compaction snapshot, or an event the Policy's
/// stream filter excludes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowPosition<E> {
    pub(crate) global_position: i64,
    pub(crate) delivered: Option<E>,
}

impl<E> WindowPosition<E> {
    /// A position whose event the Policy reacts to.
    #[cfg(test)]
    pub(crate) fn delivered(global_position: i64, event: E) -> Self {
        Self {
            global_position,
            delivered: Some(event),
        }
    }

    /// A position that only advances the cursor: filtered out, or synthetic.
    #[cfg(test)]
    pub(crate) fn skipped(global_position: i64) -> Self {
        Self {
            global_position,
            delivered: None,
        }
    }
}

/// The prefix of `window` the cursor may advance over.
///
/// `window` is every position greater than `cursor` that was read, in ascending
/// order, **before** the Policy's filter removes anything — filtered-out positions
/// are present with `delivered: None`.
///
/// The cursor may only advance over positions that were actually read and are
/// contiguous from `cursor + 1`: a hole means an event may still be in flight
/// (BIGSERIAL positions are assigned at INSERT and become visible at COMMIT), and
/// skipping it would break the skip-safety rule of ADR-0003. The window is therefore
/// truncated at the first hole, and everything before it is returned unchanged.
pub(crate) fn feed_from_window<E>(
    cursor: i64,
    window: impl IntoIterator<Item = WindowPosition<E>>,
) -> Vec<WindowPosition<E>> {
    window
        .into_iter()
        .zip(cursor + 1..)
        .take_while(|(position, expected)| position.global_position == *expected)
        .map(|(position, _)| position)
        .collect()
}

#[cfg(test)]
mod tests {
    use super::{feed_from_window, WindowPosition};

    #[test]
    fn an_empty_window_advances_nothing() {
        let window: Vec<WindowPosition<&str>> = Vec::new();

        assert_eq!(feed_from_window(7, window), Vec::new());
    }

    #[test]
    fn a_contiguous_window_is_delivered_whole() {
        let window = vec![
            WindowPosition::delivered(8, "a"),
            WindowPosition::delivered(9, "b"),
        ];

        assert_eq!(feed_from_window(7, window.clone()), window);
    }

    /// The bug this module exists for: a Policy whose filter excludes the event at
    /// position 8 must still receive the one at 9, with its cursor walking over both.
    #[test]
    fn filtered_out_positions_advance_the_cursor_without_being_delivered() {
        let window = vec![
            WindowPosition::skipped(8),
            WindowPosition::delivered(9, "b"),
            WindowPosition::skipped(10),
            WindowPosition::delivered(11, "d"),
        ];

        let feed = feed_from_window(7, window.clone());

        assert_eq!(feed, window);
        assert_eq!(feed.last().map(|p| p.global_position), Some(11));
        assert_eq!(
            feed.iter().filter_map(|p| p.delivered).collect::<Vec<_>>(),
            vec!["b", "d"]
        );
    }

    /// Skip-safety (ADR-0003): a position that was not read may still be in flight,
    /// so nothing past it may be consumed on this poll.
    #[test]
    fn a_hole_truncates_the_window() {
        let window = vec![
            WindowPosition::delivered(8, "a"),
            WindowPosition::delivered(10, "c"),
            WindowPosition::delivered(11, "d"),
        ];

        assert_eq!(
            feed_from_window(7, window),
            vec![WindowPosition::delivered(8, "a")]
        );
    }

    #[test]
    fn a_hole_at_the_head_of_the_window_advances_nothing() {
        let window = vec![
            WindowPosition::delivered(9, "b"),
            WindowPosition::delivered(10, "c"),
        ];

        assert_eq!(feed_from_window(7, window), Vec::new());
    }

    /// A hole is a hole whether or not the Policy wanted the events behind it: the
    /// window is read unfiltered, so a filtered-out position is never itself a hole,
    /// and a genuine hole still stops the feed.
    #[test]
    fn a_hole_behind_filtered_out_positions_still_truncates() {
        let window = vec![
            WindowPosition::skipped(8),
            WindowPosition::skipped(9),
            WindowPosition::delivered(11, "d"),
        ];

        assert_eq!(
            feed_from_window(7, window),
            vec![WindowPosition::skipped(8), WindowPosition::skipped(9)]
        );
    }

    /// The window never grows with the log: whatever the reader hands over is the
    /// most the cursor can move in one poll.
    #[test]
    fn the_feed_is_never_longer_than_the_window() {
        let window: Vec<WindowPosition<&str>> =
            (1..=100).map(WindowPosition::<&str>::skipped).collect();

        assert_eq!(feed_from_window(0, window).len(), 100);
    }
}
