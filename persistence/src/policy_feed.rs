//! How far a Policy's cursor may advance over the positions read past it, and which
//! of them are delivered.
//!
//! Contiguity is decided on the unfiltered `global_position` stream: a position the
//! Policy's filter excludes advances the cursor and fires nothing, like a compaction
//! snapshot (ADR-0004, ADR-0012). Pure, so it needs no database.

/// One position from the window read past a Policy's cursor. `delivered` is `None`
/// for a compaction snapshot or an event the Policy's filter excludes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowPosition<E> {
    pub(crate) global_position: i64,
    pub(crate) delivered: Option<E>,
}

impl<E> WindowPosition<E> {
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
/// `window` is every position past `cursor` that was read, ascending, with
/// filtered-out ones present as `delivered: None`. Truncated at the first hole: a
/// missing position may be an append still in flight (ADR-0003 skip-safety).
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

    /// The bug this module exists for: a filter that excludes position 8 must not
    /// hide position 9.
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

    /// ADR-0003 skip-safety: a position that was not read may still be in flight.
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

    /// Filtered-out positions are read, so they are never themselves holes.
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

    /// Whatever the reader hands over is the most the cursor can move in one poll.
    #[test]
    fn the_feed_is_never_longer_than_the_window() {
        let window: Vec<WindowPosition<&str>> =
            (1..=100).map(WindowPosition::<&str>::skipped).collect();

        assert_eq!(feed_from_window(0, window).len(), 100);
    }
}
