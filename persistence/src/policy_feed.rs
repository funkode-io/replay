//! How far a Policy's cursor may advance over the positions read past it, which
//! of them are delivered, and the hole that stopped it.
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

/// The hole a feed stopped at: the position expected next, and the one found instead.
///
/// Carried out of the decision rather than dropped inside it, because "the feed stops
/// here" is the one fact an operator of a blocked Policy never had
/// (funkode-io/replay#164).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Gap {
    /// The position the cursor would have advanced to next.
    pub(crate) expected: i64,
    /// The lowest position past `expected` that actually exists.
    pub(crate) found: i64,
}

/// How far the cursor may advance this poll, and why it stops there.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Feed<E> {
    /// The prefix of the window the cursor may advance over, in position order.
    pub(crate) positions: Vec<WindowPosition<E>>,
    /// Set when the window ended at a hole rather than at its own end.
    pub(crate) gap: Option<Gap>,
}

/// The prefix of `window` the cursor may advance over, and the hole that ends it.
///
/// `window` is every position past `cursor` that was read, ascending, with
/// filtered-out ones present as `delivered: None`. Truncated at the first hole: a
/// missing position may be an append still in flight (ADR-0003 skip-safety). The
/// window is truncated in place, so deciding costs no allocation.
pub(crate) fn feed_from_window<E>(cursor: i64, mut window: Vec<WindowPosition<E>>) -> Feed<E> {
    let gap = window
        .iter()
        .zip(cursor + 1..)
        .find(|(position, expected)| position.global_position != *expected)
        .map(|(position, expected)| Gap {
            expected,
            found: position.global_position,
        });

    if let Some(gap) = gap {
        // Everything from `expected` on is past the hole: unreachable this poll.
        window.truncate((gap.expected - cursor - 1) as usize);
    }

    Feed {
        positions: window,
        gap,
    }
}

#[cfg(test)]
mod tests {
    use super::{feed_from_window, Gap, WindowPosition};

    #[test]
    fn an_empty_window_advances_nothing() {
        let window: Vec<WindowPosition<&str>> = Vec::new();

        let feed = feed_from_window(7, window);

        assert_eq!(feed.positions, Vec::new());
        assert_eq!(feed.gap, None);
    }

    #[test]
    fn a_contiguous_window_is_delivered_whole() {
        let window = vec![
            WindowPosition::delivered(8, "a"),
            WindowPosition::delivered(9, "b"),
        ];

        let feed = feed_from_window(7, window.clone());

        assert_eq!(feed.positions, window);
        assert_eq!(feed.gap, None, "a window that simply ran out is not a gap");
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

        assert_eq!(feed.positions, window);
        assert_eq!(feed.gap, None);
        assert_eq!(feed.positions.last().map(|p| p.global_position), Some(11));
        assert_eq!(
            feed.positions
                .iter()
                .filter_map(|p| p.delivered)
                .collect::<Vec<_>>(),
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

        let feed = feed_from_window(7, window);

        assert_eq!(feed.positions, vec![WindowPosition::delivered(8, "a")]);
        assert_eq!(
            feed.gap,
            Some(Gap {
                expected: 9,
                found: 10
            }),
            "the hole the feed stopped at is what an operator needs named"
        );
    }

    #[test]
    fn a_hole_at_the_head_of_the_window_advances_nothing() {
        let window = vec![
            WindowPosition::delivered(9, "b"),
            WindowPosition::delivered(10, "c"),
        ];

        let feed = feed_from_window(7, window);

        assert_eq!(feed.positions, Vec::new());
        assert_eq!(
            feed.gap,
            Some(Gap {
                expected: 8,
                found: 9
            }),
            "an empty feed with a gap is a blocked Policy; without one it is idle"
        );
    }

    /// Filtered-out positions are read, so they are never themselves holes.
    #[test]
    fn a_hole_behind_filtered_out_positions_still_truncates() {
        let window = vec![
            WindowPosition::skipped(8),
            WindowPosition::skipped(9),
            WindowPosition::delivered(11, "d"),
        ];

        let feed = feed_from_window(7, window);

        assert_eq!(
            feed.positions,
            vec![WindowPosition::skipped(8), WindowPosition::skipped(9)]
        );
        assert_eq!(
            feed.gap,
            Some(Gap {
                expected: 10,
                found: 11
            })
        );
    }

    /// A multi-position hole reports its first missing position, which is the one
    /// the cursor is parked in front of.
    #[test]
    fn a_wide_hole_reports_its_first_missing_position() {
        let window = vec![WindowPosition::delivered(20, "t")];

        let feed = feed_from_window(7, window);

        assert_eq!(
            feed.gap,
            Some(Gap {
                expected: 8,
                found: 20
            })
        );
    }

    /// Whatever the reader hands over is the most the cursor can move in one poll.
    #[test]
    fn the_feed_is_never_longer_than_the_window() {
        let window: Vec<WindowPosition<&str>> =
            (1..=100).map(WindowPosition::<&str>::skipped).collect();

        assert_eq!(feed_from_window(0, window).positions.len(), 100);
    }
}
