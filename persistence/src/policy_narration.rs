//! When a Policy gets to say something, and when it stays quiet.
//!
//! A Policy's output is proportional to how often it changes state, not to how
//! much work it does: a burst is bracketed by the record that starts it and the
//! [Caught up] record that ends it, however many events fell in between. An idle
//! Policy at zero lag writes nothing at any level, which is what keeps silence
//! worth reading (ADR-0021).
//!
//! This module holds only the decision. What the records say, and at what level,
//! belongs to the caller ([`crate::PolicyRunner`]), so wording stays free to
//! change without touching a test.
//!
//! What it cannot see is a worker held inside one reaction: the narration runs on
//! the worker's own thread of control, so a hung dispatch stops it along with
//! everything else. That is the question [`crate::Liveness`] answers.
//!
//! [Caught up]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#caught-up

use std::time::{Duration, Instant};

/// The floor on the spacing between progress records: the first cursor advance
/// at least this long after the previous record writes the next one.
///
/// Inside any alerting window, so "moving slowly" separates from "not moving"
/// within one; and two records a minute is a rate an hour-long backlog can carry.
pub(crate) const PROGRESS_EVERY: Duration = Duration::from_secs(30);

/// What the narration decides is worth a record.
///
/// Every variant is an edge. There is deliberately no variant for "still caught
/// up" and none for "polled": those are the states a Policy spends its life in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Record {
    /// A Policy at zero lag found work. Opens the bracket.
    Working,
    /// Still working: the first advance at least [`PROGRESS_EVERY`] after the
    /// previous record. The only record that repeats, and the only evidence that
    /// a long backlog is moving at all.
    Progress { events: u64, elapsed: Duration },
    /// The feed ended after work: the cursor has reached the end of it. Closes
    /// the bracket, and carries what the burst cost.
    CaughtUp { events: u64, elapsed: Duration },
}

/// Where a Policy is between the two edges.
#[derive(Debug)]
enum State {
    /// At zero lag, or not yet leading. The silent state.
    CaughtUp,
    /// Draining a backlog.
    Draining {
        /// When the window that opened the burst was read, which precedes its
        /// first reaction: a burst is timed from the work appearing, not from
        /// the first of it finishing.
        since: Instant,
        /// Positions advanced over since `since`.
        events: u64,
        /// The last advance. The wait before the empty poll that discovers the
        /// catch-up is idle time, and is not charged to the burst.
        worked_until: Instant,
        /// The advance that wrote the last record, which paces the progress
        /// records against the clock rather than against the work.
        reported: Instant,
    },
}

/// One Policy's transitions, as its worker sees them.
///
/// Held by the worker for the length of one election: a burst interrupted by a
/// lost leadership or a restart belongs to the worker that was draining it, and
/// the replica that takes over opens its own bracket.
#[derive(Debug)]
pub(crate) struct Narration {
    progress_every: Duration,
    state: State,
}

/// What one read of the feed found, as the narration needs to hear it.
///
/// The distinction that matters is between the two ways a poll can advance
/// nothing. Only one of them is a catch-up, and conflating them would announce
/// that a Policy parked in front of a hole has reached the end of its feed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Poll {
    /// A non-empty window was read at `at`: there is work, before any of it has
    /// been done. Opens the bracket, so the first reaction of a burst runs
    /// inside it rather than before it.
    Found { at: Instant },
    /// The cursor advanced over `events` positions, as of `at` — told as the
    /// cursor moves, so a batch that takes ten minutes of dispatches reports
    /// while it runs.
    ///
    /// `events` counts positions advanced over rather than reactions executed:
    /// a window a Policy's filter excludes entirely, and one whose reactions all
    /// park, are both work.
    Advanced { events: u64, at: Instant },
    /// The feed ended: nothing to read, and nothing in the way of reading more.
    /// The only observation that closes a burst.
    Exhausted,
    /// Nothing advanced, and the feed did not end — it stops at a hole, or the
    /// cursor was moved under the poll. No edge either way: a
    /// [Blocked policy](https://github.com/funkode-io/replay/blob/main/CONTEXT.md#blocked-policy)
    /// has not caught up, and its own record says so
    /// ([`crate::policy_blocked`]).
    Stalled,
}

impl Narration {
    /// A worker starts caught up, not working: a Policy elected with nothing to
    /// do must reach its first poll without having said anything.
    pub(crate) fn new(progress_every: Duration) -> Self {
        Self {
            progress_every,
            state: State::CaughtUp,
        }
    }

    /// Hear what a read of the feed found, and return the record it earns, if
    /// any.
    ///
    /// A poll that *failed* is not an observation: the caller reports the error
    /// and says nothing here, so a database outage cannot be narrated as a
    /// catch-up.
    pub(crate) fn polled(&mut self, poll: Poll) -> Option<Record> {
        match (&mut self.state, poll) {
            (_, Poll::Stalled) => None,
            (State::CaughtUp, Poll::Exhausted) => None,
            // A window read while a burst is open is that burst carrying on:
            // the bracket is already where it belongs.
            (State::Draining { .. }, Poll::Found { .. }) => None,
            (State::CaughtUp, Poll::Found { at }) => {
                self.state = State::Draining {
                    since: at,
                    events: 0,
                    worked_until: at,
                    reported: at,
                };
                Some(Record::Working)
            }
            // Unreachable through the runner, which reads a window before it can
            // advance over it. Opening here rather than dropping the edge keeps
            // the bracket balanced for any other caller.
            (State::CaughtUp, Poll::Advanced { events, at }) => {
                self.state = State::Draining {
                    since: at,
                    events,
                    worked_until: at,
                    reported: at,
                };
                Some(Record::Working)
            }
            (
                State::Draining {
                    since,
                    events: total,
                    worked_until,
                    ..
                },
                Poll::Exhausted,
            ) => {
                let record = Record::CaughtUp {
                    events: *total,
                    elapsed: worked_until.saturating_duration_since(*since),
                };
                self.state = State::CaughtUp;
                Some(record)
            }
            (
                State::Draining {
                    since,
                    events: total,
                    worked_until,
                    reported,
                },
                Poll::Advanced { events, at },
            ) => {
                *total = total.saturating_add(events);
                *worked_until = at;
                if at.saturating_duration_since(*reported) < self.progress_every {
                    return None;
                }
                *reported = at;
                Some(Record::Progress {
                    events: *total,
                    elapsed: at.saturating_duration_since(*since),
                })
            }
        }
    }

    /// The worker stopped leading — a lost lock, a shutdown, a restart.
    ///
    /// Silent: the burst it was in the middle of is not finished, and the next
    /// election must not close it with a duration measured across the gap.
    /// Reached between polls, which is where a worker sees a revocation: a
    /// demoted worker narrates the batch it is inside as it finishes it.
    pub(crate) fn stood_down(&mut self) {
        self.state = State::CaughtUp;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// One whole poll, as the worker performs it: a window read `after` the
    /// reference instant, advanced over by `took` later. `events == 0` is the
    /// poll that finds the feed exhausted, and the records are whatever that
    /// poll earned.
    fn poll(
        narration: &mut Narration,
        start: Instant,
        after: Duration,
        took: Duration,
        events: u64,
    ) -> Vec<Record> {
        if events == 0 {
            return narration.polled(Poll::Exhausted).into_iter().collect();
        }
        [
            narration.polled(Poll::Found { at: start + after }),
            narration.polled(Poll::Advanced {
                events,
                at: start + after + took,
            }),
        ]
        .into_iter()
        .flatten()
        .collect()
    }

    /// A slow first reaction must not hold the opening record behind it: the
    /// bracket is opened by the window, so everything the burst does — including
    /// the dead letter a hung first dispatch parks — falls inside it.
    #[test]
    fn the_bracket_opens_when_the_work_is_found_not_when_the_first_reaction_returns() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let start = Instant::now();

        assert_eq!(
            narration.polled(Poll::Found { at: start }),
            Some(Record::Working)
        );
        assert_eq!(
            narration.polled(Poll::Advanced {
                events: 1,
                at: start + Duration::from_secs(150),
            }),
            Some(Record::Progress {
                events: 1,
                elapsed: Duration::from_secs(150),
            }),
            "two and a half minutes on one event is the burst moving slowly, \
             which is the record for it"
        );
    }

    /// A window read while a burst is open is that burst carrying on, not a
    /// second one.
    #[test]
    fn a_window_read_mid_burst_does_not_open_a_second_bracket() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let start = Instant::now();

        poll(&mut narration, start, Duration::ZERO, Duration::ZERO, 10);

        assert_eq!(narration.polled(Poll::Found { at: start }), None);
    }

    /// A burst is not closed by a poll that read nothing because something is in
    /// the way: a Policy parked in front of a hole has not reached the end of its
    /// feed, and announcing a catch-up would say the opposite of what happened.
    #[test]
    fn a_stalled_poll_leaves_the_burst_open() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let start = Instant::now();

        poll(&mut narration, start, Duration::ZERO, Duration::ZERO, 700);

        for minute in 1..10 {
            assert_eq!(
                narration.polled(Poll::Stalled),
                None,
                "nothing is earned by a poll that advanced nothing and ended nothing, \
                 at minute {minute} of the block"
            );
        }

        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(600),
                Duration::ZERO,
                300
            ),
            vec![Record::Progress {
                events: 1_000,
                elapsed: Duration::from_secs(600),
            }],
            "the hole filled and the same burst carries on — a progress record, \
             because the spacing has long since elapsed, and never a second bracket"
        );
        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(601),
                Duration::ZERO,
                0
            ),
            vec![Record::CaughtUp {
                events: 1_000,
                elapsed: Duration::from_secs(600),
            }]
        );
    }

    /// A Policy that was already quiet stays quiet in front of a hole: the block
    /// is reported on the progress axis, by the record that knows how old it is.
    #[test]
    fn a_stalled_poll_says_nothing_about_an_idle_policy() {
        let mut narration = Narration::new(PROGRESS_EVERY);

        assert_eq!(narration.polled(Poll::Stalled), None);
    }

    /// One poll can run for minutes — a batch is dispatched event by event, each
    /// dispatch bounded only by its timeout and its retries. Records are earned
    /// as the cursor moves, so a slow batch is distinguishable from a wedged one
    /// while it is still running rather than after it returns.
    #[test]
    fn a_long_poll_reports_while_it_is_still_running() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let started = Instant::now();
        let advanced = |at: Duration| Poll::Advanced {
            events: 1,
            at: started + at,
        };

        assert_eq!(
            narration.polled(Poll::Found { at: started }),
            Some(Record::Working),
            "the window opens the bracket, before any of it has been dispatched"
        );
        assert_eq!(narration.polled(advanced(Duration::ZERO)), None);
        assert_eq!(narration.polled(advanced(Duration::from_secs(20))), None);
        assert_eq!(
            narration.polled(advanced(PROGRESS_EVERY)),
            Some(Record::Progress {
                events: 3,
                elapsed: PROGRESS_EVERY,
            })
        );
    }

    /// The property the whole module exists for: a Policy at zero lag polls
    /// forever and earns nothing to write.
    #[test]
    fn an_idle_policy_never_earns_a_record() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let start = Instant::now();

        for minute in 0..60 {
            assert_eq!(
                poll(
                    &mut narration,
                    start,
                    Duration::from_secs(60 * minute),
                    Duration::from_millis(1),
                    0
                ),
                Vec::new(),
                "an empty poll an hour into idling still says nothing"
            );
        }
    }

    /// A burst is two records: the edge into work, and the edge out of it.
    #[test]
    fn a_burst_is_bracketed_by_two_records() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let start = Instant::now();

        assert_eq!(
            poll(&mut narration, start, Duration::ZERO, Duration::ZERO, 0),
            Vec::new()
        );
        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(1),
                Duration::from_secs(2),
                500
            ),
            vec![Record::Working]
        );
        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(4),
                Duration::from_millis(1),
                0
            ),
            vec![Record::CaughtUp {
                events: 500,
                elapsed: Duration::from_secs(2),
            }],
            "the burst is timed from the poll that found the work to the poll that \
             finished it, not to the empty poll that noticed"
        );
    }

    /// A backlog drained in one poll is the case an `Instant` taken after the
    /// drain would report as instantaneous.
    #[test]
    fn a_single_poll_burst_is_timed_from_when_the_poll_began() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let start = Instant::now();

        poll(
            &mut narration,
            start,
            Duration::ZERO,
            Duration::from_secs(9),
            4_231,
        );

        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(10),
                Duration::ZERO,
                0
            ),
            vec![Record::CaughtUp {
                events: 4_231,
                elapsed: Duration::from_secs(9),
            }]
        );
    }

    /// Several polls of work are one burst, counted end to end.
    #[test]
    fn consecutive_working_polls_accumulate_into_one_burst() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let start = Instant::now();

        assert_eq!(
            poll(&mut narration, start, Duration::ZERO, Duration::ZERO, 1_000),
            vec![Record::Working]
        );
        for poll_number in 1..5 {
            assert_eq!(
                poll(
                    &mut narration,
                    start,
                    Duration::from_secs(poll_number),
                    Duration::ZERO,
                    1_000
                ),
                Vec::new(),
                "work in progress is not an edge"
            );
        }

        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(5),
                Duration::ZERO,
                0
            ),
            vec![Record::CaughtUp {
                events: 5_000,
                elapsed: Duration::from_secs(4),
            }]
        );
    }

    /// The one record that repeats, and the reason a long import is
    /// distinguishable from a wedged one.
    #[test]
    fn a_long_backlog_reports_progress_on_a_fixed_spacing() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let start = Instant::now();

        poll(&mut narration, start, Duration::ZERO, Duration::ZERO, 1_000);

        assert_eq!(
            poll(
                &mut narration,
                start,
                PROGRESS_EVERY - Duration::from_secs(1),
                Duration::ZERO,
                1_000
            ),
            Vec::new(),
            "a record before the spacing has elapsed would be a flood"
        );
        assert_eq!(
            poll(&mut narration, start, PROGRESS_EVERY, Duration::ZERO, 1_000),
            vec![Record::Progress {
                events: 3_000,
                elapsed: PROGRESS_EVERY,
            }]
        );
        assert_eq!(
            poll(
                &mut narration,
                start,
                PROGRESS_EVERY + Duration::from_secs(1),
                Duration::ZERO,
                1_000
            ),
            Vec::new(),
            "the spacing restarts from the record, not from the burst"
        );
        assert_eq!(
            poll(
                &mut narration,
                start,
                PROGRESS_EVERY + PROGRESS_EVERY,
                Duration::ZERO,
                1_000
            ),
            vec![Record::Progress {
                events: 5_000,
                elapsed: PROGRESS_EVERY + PROGRESS_EVERY,
            }]
        );
    }

    /// A burst longer than the spacing still ends with exactly one catch-up
    /// record, carrying the whole burst rather than what came after the last
    /// progress record.
    #[test]
    fn progress_records_do_not_split_the_burst_they_report_on() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let start = Instant::now();

        poll(&mut narration, start, Duration::ZERO, Duration::ZERO, 1_000);
        poll(&mut narration, start, PROGRESS_EVERY, Duration::ZERO, 1_000);

        assert_eq!(
            poll(
                &mut narration,
                start,
                PROGRESS_EVERY + Duration::from_secs(1),
                Duration::ZERO,
                0
            ),
            vec![Record::CaughtUp {
                events: 2_000,
                elapsed: PROGRESS_EVERY,
            }]
        );
    }

    /// Catching up is not a terminal state: the next append is a new burst, and
    /// it is counted from zero.
    #[test]
    fn the_next_append_opens_a_new_burst() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let start = Instant::now();

        poll(&mut narration, start, Duration::ZERO, Duration::ZERO, 10);
        poll(
            &mut narration,
            start,
            Duration::from_secs(1),
            Duration::ZERO,
            0,
        );

        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(600),
                Duration::ZERO,
                3
            ),
            vec![Record::Working],
            "an hour of silence between bursts is an hour of nothing to say"
        );
        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(601),
                Duration::ZERO,
                0
            ),
            vec![Record::CaughtUp {
                events: 3,
                elapsed: Duration::ZERO,
            }],
            "the second burst carries its own count, not the first one's"
        );
    }

    /// A worker that loses its lock mid-backlog has not caught up, and the
    /// replica that takes over must not inherit its clock.
    #[test]
    fn standing_down_closes_a_burst_without_a_record_and_without_a_carry_over() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let start = Instant::now();

        poll(&mut narration, start, Duration::ZERO, Duration::ZERO, 900);
        narration.stood_down();

        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(300),
                Duration::ZERO,
                0
            ),
            Vec::new(),
            "the abandoned burst is not closed by the next election's first empty poll"
        );
        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(301),
                Duration::ZERO,
                100
            ),
            vec![Record::Working]
        );
        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(302),
                Duration::ZERO,
                0
            ),
            vec![Record::CaughtUp {
                events: 100,
                elapsed: Duration::ZERO,
            }],
            "the new burst counts only its own events"
        );
    }

    /// A poll's start and end are read from a monotonic clock on either side of
    /// an `await`, so a suspended task can produce a pair that looks reversed
    /// against an earlier reading. It costs a duration of zero, never a panic.
    #[test]
    fn a_reversed_clock_reading_costs_nothing() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let start = Instant::now();

        narration.polled(Poll::Found {
            at: start + Duration::from_secs(10),
        });
        narration.polled(Poll::Advanced {
            events: 5,
            at: start,
        });

        assert_eq!(
            narration.polled(Poll::Exhausted),
            Some(Record::CaughtUp {
                events: 5,
                elapsed: Duration::ZERO,
            })
        );
    }
}
