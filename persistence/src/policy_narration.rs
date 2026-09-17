//! When a Policy gets to say something, and when it stays quiet.
//!
//! A Policy's output is proportional to how often it changes state, not to how
//! much work it does: a burst is bracketed by the record that starts it and the
//! [Caught up] record that ends it, however many events fell in between. An idle
//! Policy at zero lag writes nothing at any level, which is what keeps silence
//! worth reading — the property a line per dispatch would destroy, at over a
//! hundred thousand lines for one import.
//!
//! This module holds only the decision. What the records say, and at what level,
//! belongs to the caller ([`crate::PolicyRunner`]), so wording stays free to
//! change without touching a test.
//!
//! [Caught up]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#caught-up

use std::time::{Duration, Instant};

/// How much of a backlog a Policy may work through between progress records.
///
/// Small enough that "moving slowly" and "not moving" are distinguishable within
/// one alerting window, large enough that the whole backlog costs a handful of
/// lines: an import that takes an hour writes two records a minute at worst.
pub(crate) const PROGRESS_EVERY: Duration = Duration::from_secs(30);

/// What the narration decides is worth a record.
///
/// Every variant is an edge. There is deliberately no variant for "still caught
/// up" and none for "polled": those are the states a Policy spends its life in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Record {
    /// A Policy at zero lag found work. Opens the bracket.
    Working,
    /// Still working, `PROGRESS_EVERY` later. The only record that repeats, and
    /// the only evidence that a long backlog is moving at all.
    Progress { events: u64, elapsed: Duration },
    /// The feed came back empty after work: the cursor has reached the end of
    /// it. Closes the bracket, and carries what the burst cost.
    CaughtUp { events: u64, elapsed: Duration },
}

/// Where a Policy is between the two edges.
#[derive(Debug)]
enum State {
    /// At zero lag, or not yet leading. The silent state.
    CaughtUp,
    /// Draining a backlog.
    Draining {
        /// Start of the poll that found the work — not the instant the record
        /// was decided. A backlog small enough for one poll is drained entirely
        /// before anything is decided, and reporting it as instant would be a
        /// lie about the only measurement in the record.
        since: Instant,
        /// Positions advanced over since `since`.
        events: u64,
        /// End of the last poll that had work. The gap before the empty poll
        /// that discovers the catch-up is the poll interval, which the Policy
        /// spent idle and must not be charged for.
        worked_until: Instant,
        /// End of the last poll that wrote a record, which paces the progress
        /// records against wall-clock time rather than against poll count.
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

impl Narration {
    /// A worker starts caught up, not working: a Policy elected with nothing to
    /// do must reach its first poll without having said anything.
    pub(crate) fn new(progress_every: Duration) -> Self {
        Self {
            progress_every,
            state: State::CaughtUp,
        }
    }

    /// A poll that ran from `started` to `ended` advanced over `events`
    /// positions. Returns the record it earns, if any.
    ///
    /// `events` counts positions the cursor advanced over rather than reactions
    /// executed: a Policy chewing through a window its filter excludes entirely
    /// is working, and one whose reactions all park is working too. Both are
    /// "moving", which is the question these records answer.
    ///
    /// A poll that failed is not a poll: the caller reports the error and says
    /// nothing here, so a database outage cannot be narrated as a catch-up.
    pub(crate) fn polled(
        &mut self,
        events: u64,
        started: Instant,
        ended: Instant,
    ) -> Option<Record> {
        match &mut self.state {
            State::CaughtUp if events == 0 => None,
            State::CaughtUp => {
                self.state = State::Draining {
                    since: started,
                    events,
                    worked_until: ended,
                    reported: ended,
                };
                Some(Record::Working)
            }
            State::Draining {
                since,
                events: total,
                worked_until,
                ..
            } if events == 0 => {
                let record = Record::CaughtUp {
                    events: *total,
                    elapsed: worked_until.saturating_duration_since(*since),
                };
                self.state = State::CaughtUp;
                Some(record)
            }
            State::Draining {
                since,
                events: total,
                worked_until,
                reported,
            } => {
                *total = total.saturating_add(events);
                *worked_until = ended;
                if ended.saturating_duration_since(*reported) < self.progress_every {
                    return None;
                }
                *reported = ended;
                Some(Record::Progress {
                    events: *total,
                    elapsed: ended.saturating_duration_since(*since),
                })
            }
        }
    }

    /// The worker stopped leading — a lost lock, a shutdown, a restart.
    ///
    /// Silent by design: the burst it was in the middle of is not finished, and
    /// saying "caught up" would be false. Leaving it open would be worse, since
    /// the next election would close somebody else's bracket with a duration
    /// measured across the gap.
    pub(crate) fn stood_down(&mut self) {
        self.state = State::CaughtUp;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A poll, as the worker times one: it takes `took`, starting `after` the
    /// reference instant.
    fn poll(
        narration: &mut Narration,
        start: Instant,
        after: Duration,
        took: Duration,
        events: u64,
    ) -> Option<Record> {
        narration.polled(events, start + after, start + after + took)
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
                None,
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
            None
        );
        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(1),
                Duration::from_secs(2),
                500
            ),
            Some(Record::Working)
        );
        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(4),
                Duration::from_millis(1),
                0
            ),
            Some(Record::CaughtUp {
                events: 500,
                elapsed: Duration::from_secs(2),
            }),
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
            Some(Record::CaughtUp {
                events: 4_231,
                elapsed: Duration::from_secs(9),
            })
        );
    }

    /// Several polls of work are one burst, counted end to end.
    #[test]
    fn consecutive_working_polls_accumulate_into_one_burst() {
        let mut narration = Narration::new(PROGRESS_EVERY);
        let start = Instant::now();

        assert_eq!(
            poll(&mut narration, start, Duration::ZERO, Duration::ZERO, 1_000),
            Some(Record::Working)
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
                None,
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
            Some(Record::CaughtUp {
                events: 5_000,
                elapsed: Duration::from_secs(4),
            })
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
            None,
            "a record before the spacing has elapsed would be a flood"
        );
        assert_eq!(
            poll(&mut narration, start, PROGRESS_EVERY, Duration::ZERO, 1_000),
            Some(Record::Progress {
                events: 3_000,
                elapsed: PROGRESS_EVERY,
            })
        );
        assert_eq!(
            poll(
                &mut narration,
                start,
                PROGRESS_EVERY + Duration::from_secs(1),
                Duration::ZERO,
                1_000
            ),
            None,
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
            Some(Record::Progress {
                events: 5_000,
                elapsed: PROGRESS_EVERY + PROGRESS_EVERY,
            })
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
            Some(Record::CaughtUp {
                events: 2_000,
                elapsed: PROGRESS_EVERY,
            })
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
            Some(Record::Working),
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
            Some(Record::CaughtUp {
                events: 3,
                elapsed: Duration::ZERO,
            }),
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
            None,
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
            Some(Record::Working)
        );
        assert_eq!(
            poll(
                &mut narration,
                start,
                Duration::from_secs(302),
                Duration::ZERO,
                0
            ),
            Some(Record::CaughtUp {
                events: 100,
                elapsed: Duration::ZERO,
            }),
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

        narration.polled(5, start + Duration::from_secs(10), start);

        assert_eq!(
            narration.polled(0, start, start),
            Some(Record::CaughtUp {
                events: 5,
                elapsed: Duration::ZERO,
            })
        );
    }
}
