//! Which streams a Policy looks at this poll, how far the search got, and what the poll
//! decides between the queries that nominate its streams and the reads that deliver them.
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
//! Pure, so it needs no database — which is what lets the poll's liveness property be
//! decided over its states (`liveness_simulation`, below) rather than one corner at a time.

use std::collections::HashSet;

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

/// What the database returned for one poll, and the knobs the poll is bounded by.
///
/// The three sources a poll shares its slots between, plus the two facts that decide how
/// they are shared. Everything the decision below needs and nothing it cannot be given
/// without a database.
pub(crate) struct Nominations {
    /// What the last poll could not finish, oldest claim first.
    pub(crate) carried: Vec<String>,
    /// What the sweep just found.
    pub(crate) swept: Vec<String>,
    /// What the reconciliation compared, empty when none is due — and empty when one is
    /// due and found nothing, which is a different fact and the one `reconciling` carries.
    pub(crate) examined: Vec<String>,
    /// Whether a reconciliation ran on this poll.
    pub(crate) reconciling: bool,
    /// The poll's candidate cap and its event budget, which are the same knob.
    pub(crate) read_batch: u32,
    /// Which source takes the first slot when the reconciliation is not leading.
    pub(crate) share_from: usize,
}

/// Which source the reconciliation is, in the array a poll is shared between. It leads on
/// the poll it runs, so it is the one index that has to be named.
const RECONCILED: usize = 2;

/// One poll's decision: which streams it reads, in what order, how far its budget got,
/// where that leaves the rotation, and what the next poll starts from.
///
/// Everything a poll decides is here and nothing it does. The caller reads streams and
/// writes rows; it tells this what each read yielded ([`Self::read`]) and asks it what to
/// do next ([`Self::turn`], [`Self::settle`]). The five defects this shape exists for were
/// all in glue between `await`s that no test could call (funkode-io/replay#243).
pub(crate) struct PollPlan {
    /// The candidates, in read order.
    streams: Vec<String>,
    /// What the reconciliation compared, kept whole: the rotation moves through *this*
    /// order, which is stream id order, and not through the order the poll read in.
    page: Vec<String>,
    reconciling: bool,
    read_batch: u32,
    /// The queue this poll started from, whole and in order — not just the part that
    /// found no slot. A stream keeps its seniority when it wins a slot: if the poll then
    /// fails before reading it, it is still the oldest claim there is, and the sweep has
    /// long since passed the events that first nominated it (funkode-io/replay#246
    /// review).
    carried: Vec<String>,
    /// What is left of the poll's event budget.
    budget: u32,
    /// How far down `streams` the poll has been handed a stream to read. Ahead of
    /// `visited` for exactly as long as that read is in flight.
    at: usize,
    /// How far down `streams` the poll got through: read, delivered and done with. A
    /// stream past this was named by a source and the poll never finished with it —
    /// because the budget stopped short of it, or because its read or its delivery failed
    /// — which is the difference the rotation turns on. A slot is not a read, and a read
    /// is not a delivery.
    visited: usize,
    /// Whether the read the poll is in the middle of took the whole budget, so the stream
    /// may have more. Held until the poll is done with the stream: a delivery that fails
    /// half way carries the stream as unfinished business either way.
    filled_the_budget: bool,
    /// Streams this poll read a full budget's worth from, so they may have more. One
    /// entry per stream read, so the batch bounds it.
    unfinished: Vec<String>,
}

/// The next stream to read, and what is left to read it with.
///
/// Carries the slot it came from, so what the poll reports back is about the stream the
/// plan handed out and cannot be about a different one — or, when nothing was handed out
/// at all, about no stream.
pub(crate) struct Turn {
    pub(crate) stream_id: String,
    pub(crate) budget: u32,
    at: usize,
}

/// What a finished poll leaves behind for the next one.
pub(crate) struct Settled {
    /// The queue the next poll starts from, capped at the batch.
    pub(crate) carried: Vec<String>,
    /// Whether a reconciliation ran, which is what stamps the cadence.
    pub(crate) reconciled: bool,
    /// Where the rotation lands: `Some` moves it, `None` leaves it where it was.
    pub(crate) rotation: Option<String>,
}

impl PollPlan {
    /// Decide what this poll looks at.
    ///
    /// The candidate list is shared a slot at a time between the sources rather than
    /// filled from the carried queue first: `read_batch` continuously-busy streams refill
    /// that queue every poll, and a quiet stream the sweep passed would be nominated by
    /// the reconciliation for ever without once being read.
    pub(crate) fn plan(nominations: Nominations) -> Self {
        let Nominations {
            carried,
            swept,
            examined,
            reconciling,
            read_batch,
            share_from,
        } = nominations;

        // Bounded by the batch, like every other collection here: the page is what the
        // reconciliation read, which its own `LIMIT` capped, and the queue is what the
        // last poll handed on, which this one caps again before it hands it on.
        let page = examined.clone();
        let queue = carried.clone();
        let Shared { taken, left } = share_the_poll(
            read_batch,
            // Except on the poll a reconciliation runs, where it leads. Its candidates are
            // the ones no other source will offer again — the sweep has passed them — and
            // it only asks once a cadence, so the cost is one poll's turn to the other
            // two. Leaving it to the turn made the guarantee depend on how the cadence
            // divides into the poll interval: `share_from` moves per poll and a
            // reconciliation samples it per cadence, so a cadence of three poll intervals
            // would sample the same phase for ever (funkode-io/replay#231 review).
            if reconciling { RECONCILED } else { share_from },
            [carried, swept, examined],
        );
        drop(left);

        Self {
            streams: taken,
            page,
            reconciling,
            read_batch,
            carried: queue,
            budget: read_batch,
            at: 0,
            visited: 0,
            filled_the_budget: false,
            unfinished: Vec::new(),
        }
    }

    /// The candidates, in read order. Empty means the poll has nothing to do — every
    /// source with anything to offer wins a slot.
    pub(crate) fn streams(&self) -> &[String] {
        &self.streams
    }

    /// The next stream to read, or `None` when the list is done or the budget is spent.
    ///
    /// Handing a stream out is not reaching it: the read can fail, and the poll settles
    /// even when it does. What counts as reached is what [`Self::read`] reports back, so
    /// a stream whose read never returned is carried and the rotation stops short of it.
    pub(crate) fn turn(&mut self) -> Option<Turn> {
        if self.budget == 0 {
            return None;
        }
        let stream_id = self.streams.get(self.at)?.clone();
        self.at += 1;

        Some(Turn {
            stream_id,
            budget: self.budget,
            at: self.at,
        })
    }

    /// What the read for `turn` yielded. Spends the budget, and nothing else: the poll
    /// still has to deliver these events, and the stream is not one it got through until
    /// it has.
    pub(crate) fn read(&mut self, _turn: &Turn, events: u32) {
        self.filled_the_budget = events == self.budget;
        self.budget -= events;
    }

    /// The poll is done with the stream `turn` was handed out for: its events were read
    /// and delivered.
    ///
    /// A full read means the stream may have more; it is looked at again next poll rather
    /// than drained here, so one busy stream cannot hold up every other. It goes to the
    /// *back* of the queue — ahead of nothing it was ahead of — because a stream written
    /// to faster than it is read would otherwise hold the front of the queue for good and
    /// starve everything behind it.
    pub(crate) fn delivered(&mut self, turn: &Turn) {
        self.visited = turn.at;
        if std::mem::take(&mut self.filled_the_budget) {
            self.unfinished.push(turn.stream_id.clone());
        }
    }

    /// The stream `turn` was handed out for belongs to somebody else now.
    ///
    /// A place that moved under the poll is left where its new owner put it, and the
    /// stream is not carried: the next poll reads the place afresh and resumes from there.
    /// The poll is done with it all the same, so the rotation may pass it.
    pub(crate) fn abandoned(&mut self, turn: &Turn) {
        self.visited = turn.at;
        self.filled_the_budget = false;
    }

    /// What the poll leaves behind.
    ///
    /// The rotation advances through what the poll *read*, not through what it admitted: a
    /// candidate the event budget never reached is carried, and stepping the rotation over
    /// it would leave it for a full pass — or for ever, if the front of the page is always
    /// what the budget spends itself on. Contiguous from the front of the page, because
    /// the rotation is one id and cannot describe a hole in the middle.
    ///
    pub(crate) fn settle(self) -> Settled {
        // Only on the polls a reconciliation ran on: off the cadence there is no page for
        // the rotation to move through, and the set would be hashed for nothing.
        let rotation = self.reconciling.then(|| {
            let read: HashSet<&String> = self.streams[..self.visited].iter().collect();
            let read_through = self
                .page
                .iter()
                .take_while(|stream| read.contains(stream))
                .count();

            rotation_after(&self.page, read_through, self.read_batch)
        });

        // What the next poll starts from, oldest claim first.
        //
        // The stream the poll was in the middle of leads: at most one, handed out and
        // never finished with, its events part delivered and the sweep already past the
        // positions that would nominate it again, so the cap must not be what drops it.
        // Then the queue this poll was given, in the order it was given it and minus what
        // the poll got through — a stream does not lose its place by winning a slot the
        // poll then failed to use. Then the candidates this poll never reached, and last
        // what it read and may not have finished.
        //
        // Each stream once, and capped: this is the one collection here that outlives a
        // poll, and a stream that two of those four name would otherwise spend two of the
        // slots the cap allows and leave another stream out.
        let done: HashSet<&String> = self.streams[..self.visited].iter().collect();
        let mut carried: Vec<String> = Vec::new();
        for stream in self.streams[self.visited..self.at]
            .iter()
            .chain(self.carried.iter().filter(|stream| !done.contains(stream)))
            .chain(self.streams[self.at..].iter())
            .chain(self.unfinished.iter())
        {
            if carried.len() as u32 == self.read_batch {
                break;
            }
            if !carried.contains(stream) {
                carried.push(stream.clone());
            }
        }

        Settled {
            carried,
            reconciled: self.reconciling,
            rotation: rotation.flatten(),
        }
    }
}

/// What one poll takes, and what each source still had to offer when it stopped.
struct Shared<const N: usize> {
    taken: Vec<String>,
    /// Per source, in the order given: the candidates that got no slot. Each source's
    /// caller decides what that means — carried forward, or left for the next pass.
    left: [Vec<String>; N],
}

/// Fill a poll's candidate list from its sources a slot at a time, starting at `from` and
/// wrapping, skipping streams already taken.
///
/// Takes ownership because each source is consumed as far as it was used: a stream named
/// twice costs one slot, not two, and the source that named it first keeps its turn. What
/// is left is handed back rather than dropped, so no caller has to assume its candidates
/// were read.
///
/// `from` moves the first turn between polls. With three sources and a batch of two, a
/// fixed order would give the third source no slot at all — for ever, if the first two
/// always have something to offer.
fn share_the_poll<const N: usize>(limit: u32, from: usize, sources: [Vec<String>; N]) -> Shared<N> {
    let mut sources = sources.map(Vec::into_iter);
    let mut taken: Vec<String> = Vec::new();

    'filling: while (taken.len() as u32) < limit {
        let mut offered = false;
        for turn in 0..N {
            let source = &mut sources[(from + turn) % N];
            for stream_id in source.by_ref() {
                if !taken.contains(&stream_id) {
                    taken.push(stream_id);
                    offered = true;
                    break;
                }
            }
            if (taken.len() as u32) >= limit {
                break 'filling;
            }
        }
        if !offered {
            break;
        }
    }

    Shared {
        taken,
        left: sources.map(Iterator::collect),
    }
}

/// The queue a poll that failed hands to the next one.
///
/// `unwritten` is the places it moved and never wrote — the streams whose progress exists
/// only in the failing poll's memory, and whose events the sweep has passed — so they go
/// ahead of what the plan settled: what the cap drops should cost a re-read, not a
/// redelivery. Each stream once, and capped like the queue a poll that finished hands on.
pub(crate) fn recovering(
    unwritten: impl IntoIterator<Item = String>,
    settled: Vec<String>,
    cap: u32,
) -> Vec<String> {
    let mut carried: Vec<String> = Vec::new();

    for stream in unwritten.into_iter().chain(settled) {
        if carried.len() as u32 == cap {
            break;
        }
        if !carried.contains(&stream) {
            carried.push(stream);
        }
    }

    carried
}

/// Where the rotation lands after a reconciliation compared `page` and the poll read
/// `read` of it, counted from the front. `None` leaves it where it was.
///
/// A page read whole and short of the limit — no page at all included, which is what the
/// last stream id looks like from the far side — is the end of a pass, and the next one
/// starts over: the empty string sorts before every id.
pub(crate) fn rotation_after(page: &[String], read: usize, limit: u32) -> Option<String> {
    if read == page.len() && (page.len() as u32) < limit {
        Some(String::new())
    } else if read > 0 {
        Some(page[read - 1].clone())
    } else {
        None
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
/// How one poll's candidate list is shared between the places it can come from.
///
/// Pure, so the cases that matter — a batch too small to divide between the sources, a
/// source naming what another already named — are one assertion each rather than a
/// database and a daemon.
#[cfg(test)]
mod sharing_tests {
    use super::share_the_poll;

    fn streams(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    /// One slot and three sources: the turn decides, and over three polls each source
    /// gets one. Without the turn the same source takes the only slot for ever, which is
    /// how a quiet stream the sweep passed stays undelivered (funkode-io/replay#231).
    #[test]
    fn a_batch_too_small_to_divide_gives_each_source_a_turn() {
        let taken = |from| {
            share_the_poll(
                1,
                from,
                [
                    streams(&["carried"]),
                    streams(&["swept"]),
                    streams(&["behind"]),
                ],
            )
            .taken
        };

        assert_eq!(taken(0), streams(&["carried"]));
        assert_eq!(taken(1), streams(&["swept"]));
        assert_eq!(taken(2), streams(&["behind"]));
        assert_eq!(taken(3), streams(&["carried"]), "the turn wraps");
    }

    /// Room for everyone: order follows the turn, and nothing is dropped.
    #[test]
    fn a_batch_with_room_takes_from_every_source() {
        let shared = share_the_poll(
            9,
            1,
            [
                streams(&["carried"]),
                streams(&["swept"]),
                streams(&["behind"]),
            ],
        );

        assert_eq!(shared.taken, streams(&["swept", "behind", "carried"]));
        assert!(shared.left.iter().all(Vec::is_empty));
    }

    /// A stream two sources name costs one slot, and what no slot was found for is handed
    /// back rather than dropped — the caller decides whether that means "carry it" or
    /// "leave the rotation where it was".
    #[test]
    fn a_stream_named_twice_costs_one_slot_and_the_rest_is_handed_back() {
        let shared = share_the_poll(
            2,
            0,
            [
                streams(&["both"]),
                streams(&["both", "swept-only"]),
                streams(&["behind-only"]),
            ],
        );

        assert_eq!(shared.taken, streams(&["both", "swept-only"]));
        assert_eq!(
            shared.left,
            [streams(&[]), streams(&[]), streams(&["behind-only"]),],
            "the source that got no slot keeps its candidate"
        );
    }
}

/// What a poll hands to the next one when it stops part way through.
///
/// The queue that outlives a poll is capped, so what the cap drops is a decision, and the
/// states it decides between are reached only by a poll that stopped between a turn and
/// its delivery (funkode-io/replay#246 review).
#[cfg(test)]
mod settling_tests {
    use super::{Nominations, PollPlan};

    /// What a poll that failed hands on: its own unwritten places first, then what its
    /// plan settled, each stream once and capped.
    ///
    /// The order is the whole point. A place this poll moved and never wrote is a
    /// redelivery if it is dropped; a candidate it never reached is a re-read
    /// (funkode-io/replay#246 review).
    #[test]
    fn a_failed_poll_hands_on_its_unwritten_places_ahead_of_its_candidates() {
        let carried = super::recovering(
            [
                "urn:probe:advanced".to_string(),
                "urn:probe:both".to_string(),
            ],
            vec![
                "urn:probe:both".to_string(),
                "urn:probe:candidate".to_string(),
                "urn:probe:dropped".to_string(),
            ],
            3,
        );

        assert_eq!(
            carried,
            vec![
                "urn:probe:advanced".to_string(),
                "urn:probe:both".to_string(),
                "urn:probe:candidate".to_string(),
            ],
            "the unwritten places lead, the repeat costs one slot, and the cap drops the \
             newest claim"
        );
    }

    /// A stream does not lose its place in the queue by winning a slot the poll then
    /// failed to use.
    ///
    /// The poll that fails before its first read is the one that shows it: everything it
    /// admitted is still owed, and what the cap has to drop should be the newest claim,
    /// not the oldest. A carried stream that won a slot has been waiting since some
    /// earlier poll, and the sweep passed the events that first nominated it
    /// (funkode-io/replay#246 review).
    #[test]
    fn a_carried_stream_that_won_a_slot_keeps_its_seniority() {
        let plan = PollPlan::plan(Nominations {
            carried: vec!["urn:probe:a".to_string(), "urn:probe:b".to_string()],
            swept: vec!["urn:probe:c".to_string(), "urn:probe:d".to_string()],
            examined: Vec::new(),
            reconciling: false,
            read_batch: 2,
            share_from: 1,
        });
        assert_eq!(
            plan.streams(),
            ["urn:probe:c".to_string(), "urn:probe:a".to_string()],
            "the sweep leads this poll and the queue takes the other slot"
        );

        // The poll fails before its first read — `places_of`, or the sweep's own write.
        let settled = plan.settle();

        assert_eq!(
            settled.carried,
            vec!["urn:probe:a".to_string(), "urn:probe:b".to_string()],
            "the two the poll was given, in the order it was given them"
        );
    }

    /// The same, with the queue interleaved through the candidate list: its order is the
    /// order it was handed over in, not the order the poll would have read it in.
    #[test]
    fn a_queue_the_poll_never_read_comes_back_in_the_order_it_arrived() {
        let carried = ["urn:probe:c1", "urn:probe:c2", "urn:probe:c3"];
        let plan = PollPlan::plan(Nominations {
            carried: carried.iter().map(|s| (*s).to_string()).collect(),
            swept: ["urn:probe:s1", "urn:probe:s2", "urn:probe:s3"]
                .iter()
                .map(|s| (*s).to_string())
                .collect(),
            examined: Vec::new(),
            reconciling: false,
            read_batch: 3,
            share_from: 1,
        });
        assert_eq!(
            plan.streams(),
            [
                "urn:probe:s1".to_string(),
                "urn:probe:c1".to_string(),
                "urn:probe:s2".to_string()
            ],
        );

        let settled = plan.settle();

        assert_eq!(
            settled.carried,
            carried
                .iter()
                .map(|s| (*s).to_string())
                .collect::<Vec<String>>(),
            "the whole queue, unshuffled, and the newly swept candidates behind it"
        );
    }

    /// A stream two of the queue's four parts name spends one slot, not two.
    ///
    /// The plan's own contract rather than a state `drain_policy_once` reaches today: the
    /// turn order consumes the carried source before the cap bites except when the cap is
    /// one, where the queue is one stream whatever is in it. It is asserted here because
    /// the cap is the only thing standing between a stream and the next pass, and what it
    /// drops should never be decided by a repeat (funkode-io/replay#246 review).
    #[test]
    fn a_stream_named_twice_takes_one_slot_in_the_carried_queue() {
        let mut plan = PollPlan::plan(Nominations {
            carried: vec!["urn:probe:a".to_string(), "urn:probe:z".to_string()],
            swept: vec!["urn:probe:a".to_string()],
            examined: vec!["urn:probe:p".to_string()],
            reconciling: false,
            read_batch: 2,
            share_from: 1,
        });

        let turn = plan.turn().expect("the sweep leads this poll");
        assert_eq!(turn.stream_id, "urn:probe:a");
        // The poll stops here, so `a` is in flight *and* still in what the carried source
        // was not asked for.
        let settled = plan.settle();

        assert_eq!(
            settled.carried,
            vec!["urn:probe:a".to_string(), "urn:probe:z".to_string()],
            "the repeat costs no slot, so the stream behind it keeps its place"
        );
    }

    /// The cap does not get to drop the stream the poll was in the middle of.
    ///
    /// One slot, an older leftover that lost it, and a read that failed: capping from the
    /// front of the queue would drop the in-flight stream and keep the leftover, and the
    /// sweep has already passed the events that would nominate the in-flight one again
    /// (funkode-io/replay#246 review).
    #[test]
    fn the_stream_a_poll_stopped_in_the_middle_of_survives_the_cap() {
        let mut plan = PollPlan::plan(Nominations {
            carried: vec!["urn:probe:old".to_string()],
            swept: vec!["urn:probe:swept".to_string()],
            examined: Vec::new(),
            reconciling: false,
            read_batch: 1,
            share_from: 1,
        });

        let turn = plan.turn().expect("the one slot went to the sweep");
        assert_eq!(turn.stream_id, "urn:probe:swept");
        // No `read` and no `delivered`: the poll stopped here.
        let settled = plan.settle();

        assert_eq!(
            settled.carried,
            vec!["urn:probe:swept".to_string()],
            "the stream the poll was in the middle of keeps the one slot the cap allows"
        );
    }

    /// The stream a failed read was handed out for is one nobody read.
    ///
    /// Settling a poll that failed is what makes this a case at all: the read is in
    /// flight between [`PollPlan::turn`] and [`PollPlan::read`], and counting the stream
    /// as reached at the near end of that window would drop it from the carried queue and
    /// let the rotation past it — a stream the sweep will never nominate again, examined
    /// by nobody, which is the hole ADR-0026's rotation exists to close.
    #[test]
    fn a_read_that_never_came_back_is_carried_and_not_rotated_past() {
        let mut plan = PollPlan::plan(Nominations {
            carried: Vec::new(),
            swept: Vec::new(),
            examined: vec!["urn:probe:a".to_string(), "urn:probe:b".to_string()],
            reconciling: true,
            read_batch: 4,
            share_from: 0,
        });

        let turn = plan.turn().expect("the page's first stream leads the poll");
        assert_eq!(turn.stream_id, "urn:probe:a");
        // No `read`: this is the poll whose `read_stream` returned an error.
        let settled = plan.settle();

        assert_eq!(
            settled.carried,
            vec!["urn:probe:a".to_string(), "urn:probe:b".to_string()],
            "both are still owed to the next poll"
        );
        assert_eq!(
            settled.rotation, None,
            "and the rotation stays where it was, because nothing was read"
        );
    }
}

/// Where the reconciliation's rotation stops, which is a decision about a page of stream
/// ids and nothing else.
///
/// Pure, so the cases live here rather than behind a container: what the rotation does
/// with a page nobody had room for, and with no page at all, is the difference between a
/// stream examined once a pass and a stream never examined again.
#[cfg(test)]
mod rotation_tests {
    use super::rotation_after;

    fn page(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    /// The end of a pass: no page means nothing sorts after the rotation point, so the
    /// next one starts over. Without this a rotation that reached the last stream id
    /// queries past the end for ever, and a stream behind it is never compared again.
    #[test]
    fn a_page_with_nothing_in_it_ends_the_pass() {
        assert_eq!(rotation_after(&page(&[]), 0, 100), Some(String::new()));
    }

    /// A page shorter than the batch is the last of a pass, for the same reason.
    #[test]
    fn a_short_page_taken_whole_ends_the_pass() {
        assert_eq!(
            rotation_after(&page(&["urn:probe:b", "urn:probe:c"]), 2, 100),
            Some(String::new())
        );
    }

    /// A full page taken whole carries on after it.
    #[test]
    fn a_full_page_taken_whole_advances_the_rotation() {
        assert_eq!(
            rotation_after(&page(&["urn:probe:a", "urn:probe:b"]), 2, 2),
            Some("urn:probe:b".to_string())
        );
    }

    /// The case the poll's own limit creates: the page was read, the poll had room for
    /// some of it, and the rotation stops at the last stream that got a slot. Advancing
    /// past the rest would step over them unread whenever the front of the page stays
    /// behind, which is what the rotation exists for.
    #[test]
    fn a_page_the_poll_could_not_fit_stops_where_the_room_ran_out() {
        assert_eq!(
            rotation_after(&page(&["urn:probe:a", "urn:probe:b", "urn:probe:c"]), 1, 3),
            Some("urn:probe:a".to_string())
        );
    }

    /// A page the poll admitted but never read is not stepped over either.
    ///
    /// The distinction the rotation turns on: winning a candidate slot is not being read,
    /// because the poll's event budget can be spent before it reaches the stream. A slot
    /// count is what this used to be given, so a busy stream earlier in the list could
    /// eat the budget while the rotation moved past a quiet stream nobody looked at
    /// (funkode-io/replay#231 review).
    #[test]
    fn a_page_admitted_but_not_read_is_compared_again() {
        assert_eq!(
            rotation_after(&page(&["urn:probe:b", "urn:probe:c"]), 0, 10),
            None,
            "the rotation stays where it was"
        );
    }

    /// And a page nobody had room for leaves it exactly where it was.
    #[test]
    fn a_page_with_no_room_at_all_leaves_the_rotation_alone() {
        assert_eq!(
            rotation_after(&page(&["urn:probe:b", "urn:probe:c"]), 0, 2),
            None
        );
    }
}

/// The liveness property the poll's schedule exists for, decided over its states rather
/// than one corner at a time (funkode-io/replay#243).
///
/// > a stream the Policy is behind on is read within a bounded number of cadences,
/// > whatever the other streams are doing.
///
/// Five defects of one shape were found in this decision by review, each in a different
/// corner of it, and the fix for one reintroduced another. What they have in common is
/// that no one of them is visible from a single poll: the invariant spans three sources,
/// a candidate cap, a rotation cursor, a shared event budget and a carried queue that
/// truncates, and each fix is a local edit that can invalidate a case elsewhere. A test
/// per corner tests the corners somebody thought of.
///
/// So: a deterministic simulation over [`PollPlan`] itself — the code that ships, not a
/// model of it — with a hand-rolled LCG for the schedule (the workspace has no `rand`,
/// and one struct of knobs is cheaper than a dev-dependency) and a fixed set of seeds, so
/// a failure is a seed and a printed schedule rather than a rerun.
///
/// What is modelled outside the decision is only what the database answers: which streams
/// the sweep and the reconciliation would return, and how many events a read yields.
#[cfg(test)]
mod liveness_simulation {
    use super::{Nominations, PollPlan};

    /// How many cadences each seed is run for. Long enough that a starved stream is
    /// starved rather than merely waiting: the bound below is at most `STREAMS + 1`.
    const CADENCES: u32 = 40;

    /// A linear congruential generator, so a seed is a schedule and a schedule replays.
    /// Numbers from Knuth's MMIX; the high bits are the ones with any period worth having.
    struct Lcg(u64);

    impl Lcg {
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            self.0 >> 33
        }

        /// A number in `low..=high`.
        fn between(&mut self, low: u64, high: u64) -> u64 {
            low + self.next() % (high - low + 1)
        }

        fn chance(&mut self, in_n: u64) -> bool {
            self.next().is_multiple_of(in_n)
        }
    }

    /// One stream, as the Policy's two queries see it.
    struct Stream {
        id: String,
        /// Events written to it.
        head: u64,
        /// Events the Policy has read from it.
        place: u64,
        /// Whether its writes are ones the sweep passed — every one of them, for the life
        /// of the schedule. A quiet stream is nominated by the reconciliation and by
        /// nothing else, which is the whole reason the reconciliation exists, and the
        /// reason starving it is a hole rather than a delay.
        quiet: bool,
        /// Whether it is written to on every poll, which is what crowds a batch.
        busy: bool,
        /// How many events one of its writes carries. A number at or above the batch
        /// spends the poll's whole budget on this stream alone.
        burst: u64,
        /// Cadences it has been behind without being read. The number under test.
        waited: u32,
        /// The most streams seen behind at once while it has been waiting, which is the
        /// numerator of ADR-0026's bound for *this* wait.
        behind_while_waiting: u32,
        read_this_cadence: bool,
    }

    /// The states the schedules actually reached, counted as they run.
    ///
    /// Knobs on the generated configuration are not these states and cannot stand in for
    /// them: every stream being quiet does not mean a poll ever had no candidates, because
    /// the carried queue can keep one supplied for ever. What the property is worth
    /// depends on the states it was decided over, so they are observed rather than
    /// assumed.
    #[derive(Default)]
    struct Coverage {
        /// Polls with no candidates at all — the state a finished pass ends on, and the
        /// only one that wraps the rotation.
        empty_polls: u32,
        /// Wraps taken on one of those polls: the rotation was past the last stream id
        /// and nothing was read.
        wraps_on_an_empty_poll: u32,
        /// Polls whose event budget ran out before their candidate list did, which is
        /// where a slot stops meaning a read.
        budgets_spent_early: u32,
        /// Reconciliations whose page was filled to the batch while more streams were
        /// behind than it held, so the pass took more than one page.
        pages_at_the_batch: u32,
        /// Polls no reconciliation ran on, which only exist when a cadence spans several.
        polls_between_cadences: u32,
    }

    impl Coverage {
        fn add(&mut self, other: &Coverage) {
            self.empty_polls += other.empty_polls;
            self.wraps_on_an_empty_poll += other.wraps_on_an_empty_poll;
            self.budgets_spent_early += other.budgets_spent_early;
            self.pages_at_the_batch += other.pages_at_the_batch;
            self.polls_between_cadences += other.polls_between_cadences;
        }
    }

    /// A schedule: the knobs a seed decides, and the state the polls run against.
    struct Schedule {
        seed: u64,
        streams: Vec<Stream>,
        read_batch: u32,
        polls_per_cadence: u32,
        /// The log, as the sweep reads it: one entry per event of a stream it can see.
        log: Vec<usize>,
        /// How far the sweep has read it.
        swept_through: usize,
        /// `reconciled_through`, the reconciliation's rotation.
        rotation: String,
        /// What the last poll could not finish.
        carried: Vec<String>,
        share_from: usize,
        /// One line per poll, printed when the bound is missed. Bounded by the run, which
        /// is `CADENCES * polls_per_cadence` long.
        transcript: Vec<String>,
        seen: Coverage,
    }

    impl Schedule {
        fn from_seed(seed: u64) -> Self {
            let mut rng = Lcg(seed);
            let count = rng.between(3, 9) as usize;
            let read_batch = rng.between(1, 4) as u32;
            // 1 is every poll; the others include cadences that are exact multiples of
            // the poll interval, which is the phase a rotating turn would sample the same
            // way for ever.
            let polls_per_cadence = rng.between(1, 3) as u32;

            let mut schedule = Self {
                seed,
                streams: Vec::new(),
                read_batch,
                polls_per_cadence,
                log: Vec::new(),
                swept_through: 0,
                // Sometimes past the last stream id, which is the state every finished
                // pass leaves it in and the one an empty page has to wrap.
                rotation: if rng.chance(3) {
                    "urn:sim:zz".to_string()
                } else {
                    String::new()
                },
                carried: Vec::new(),
                share_from: 0,
                transcript: Vec::new(),
                seen: Coverage::default(),
            };

            // A Policy whose work is *only* what the sweep passed. Every other source is
            // empty every poll, so its polls are the ones with no candidates at all — the
            // state a finished pass ends on, and the only one that wraps the rotation.
            let all_quiet = rng.chance(4);

            for index in 0..count {
                let quiet = all_quiet || rng.chance(3);
                schedule.streams.push(Stream {
                    id: format!("urn:sim:{index:02}"),
                    head: 0,
                    place: 0,
                    quiet,
                    busy: rng.chance(2),
                    burst: rng.between(1, u64::from(read_batch) + 2),
                    waited: 0,
                    behind_while_waiting: 0,
                    read_this_cadence: false,
                });
                // A backlog to start from, so the first cadences are not all empty.
                let owed = rng.between(0, 3);
                if owed > 0 {
                    schedule.append(index, owed);
                }
            }

            schedule
        }

        /// Write `count` events to a stream. A quiet stream's writes never reach the log
        /// the sweep reads: that is what "the sweep passed it" means, and no future event
        /// will nominate it.
        fn append(&mut self, stream: usize, count: u64) {
            self.streams[stream].head += count;
            if !self.streams[stream].quiet {
                for _ in 0..count {
                    self.log.push(stream);
                }
            }
        }

        /// `sweep_for_streams`, against the modelled log.
        fn swept(&mut self) -> Vec<String> {
            let mut streams: Vec<String> = Vec::new();
            let end = (self.swept_through + self.read_batch as usize).min(self.log.len());
            for position in self.swept_through..end {
                let id = self.streams[self.log[position]].id.clone();
                if !streams.contains(&id) {
                    streams.push(id);
                }
            }
            self.swept_through = end;
            streams
        }

        /// `streams_behind`, against the modelled streams: those owed events, sorting
        /// after the rotation, in id order, capped by the batch.
        fn examined(&self) -> Vec<String> {
            self.streams
                .iter()
                .filter(|stream| stream.head > stream.place && stream.id > self.rotation)
                .map(|stream| stream.id.clone())
                .take(self.read_batch as usize)
                .collect()
        }

        fn behind(&self) -> Vec<&str> {
            self.streams
                .iter()
                .filter(|stream| stream.head > stream.place)
                .map(|stream| stream.id.as_str())
                .collect()
        }

        fn index_of(&self, id: &str) -> usize {
            self.streams
                .iter()
                .position(|stream| stream.id == id)
                .expect("a poll can only read a stream a source named")
        }

        /// One poll: the decision under test, and the two queries and the read it decides
        /// about.
        fn poll(&mut self, reconciling: bool) {
            let carried = std::mem::take(&mut self.carried);
            let swept = self.swept();
            let examined = if reconciling {
                self.examined()
            } else {
                Vec::new()
            };
            let page = examined.join(",");
            let page_len = examined.len();

            let mut plan = PollPlan::plan(Nominations {
                carried: carried.clone(),
                swept: swept.clone(),
                examined,
                reconciling,
                read_batch: self.read_batch,
                share_from: self.share_from,
            });
            self.share_from += 1;

            let candidates = plan.streams().len();
            if candidates == 0 {
                self.seen.empty_polls += 1;
            }
            if reconciling {
                // A full page is only evidence of a page too small for what is behind if
                // something was left out of it.
                if page_len == self.read_batch as usize && self.behind().len() > page_len {
                    self.seen.pages_at_the_batch += 1;
                }
            } else {
                self.seen.polls_between_cadences += 1;
            }

            let mut read: Vec<String> = Vec::new();
            while let Some(turn) = plan.turn() {
                let index = self.index_of(&turn.stream_id);
                let stream = &mut self.streams[index];
                let events = (stream.head - stream.place).min(u64::from(turn.budget));
                stream.place += events;
                stream.read_this_cadence = true;
                stream.waited = 0;
                read.push(format!("{}+{events}", turn.stream_id));
                plan.read(&turn, events as u32);
                plan.delivered(&turn);
            }

            if read.len() < candidates {
                self.seen.budgets_spent_early += 1;
            }

            let was = self.rotation.clone();
            let settled = plan.settle();
            self.carried = settled.carried.clone();
            if settled.reconciled {
                if let Some(through) = settled.rotation {
                    if through.is_empty() && !was.is_empty() && read.is_empty() {
                        self.seen.wraps_on_an_empty_poll += 1;
                    }
                    self.rotation = through;
                }
            }

            self.transcript.push(format!(
                "poll {:>3} {} rotation {was:?}->{:?} carried [{}] swept [{}] page [{}] \
                 read [{}] next [{}] behind [{}]",
                self.transcript.len(),
                if reconciling {
                    "reconciling"
                } else {
                    "           "
                },
                self.rotation,
                carried.join(","),
                swept.join(","),
                page,
                read.join(","),
                settled.carried.join(","),
                self.behind().join(","),
            ));
        }

        /// Write what this poll's schedule says is written. A quiet stream is written to
        /// like any other; what makes it quiet is that its writes never reach the log the
        /// sweep reads.
        fn writes(&mut self, rng: &mut Lcg) {
            for index in 0..self.streams.len() {
                let stream = &self.streams[index];
                if stream.busy || rng.chance(4) {
                    let burst = stream.burst;
                    self.append(index, burst);
                }
            }
        }

        /// Charge a cadence to every stream that was behind and not read during it, and
        /// compare each against [ADR-0026](../../docs/adr/0026-a-policy-tracks-its-position-per-stream.md):
        ///
        /// ```text
        /// cadences ≤ ceil(streams behind / streams read per cadence) + 1
        /// ```
        ///
        /// Taken at its floor, which is the guarantee the ADR gives rather than its best
        /// case: the reconciliation leads the poll it runs on, so **one** behind stream is
        /// read per cadence at worst, and the division is by one. The numerator is the
        /// most streams seen behind at once *during that stream's wait* — the rotation
        /// only spends a cadence on a stream that is behind. The ADR's `+ 1` is the
        /// cadence a finished pass spends wrapping on an empty page.
        fn account(&mut self) -> Result<(), String> {
            let behind_now = self
                .streams
                .iter()
                .filter(|stream| stream.head > stream.place)
                .count() as u32;
            let mut starved: Vec<String> = Vec::new();

            for stream in &mut self.streams {
                if stream.read_this_cadence || stream.head == stream.place {
                    stream.waited = 0;
                    stream.behind_while_waiting = 0;
                } else {
                    stream.waited += 1;
                    stream.behind_while_waiting = stream.behind_while_waiting.max(behind_now);
                    let bound = stream.behind_while_waiting + 1;
                    if stream.waited > bound {
                        starved.push(format!(
                            "{} (owed {}, unread for {} cadences, bound {bound} = at most \
                             {} behind at once while it waited, read one a cadence, plus \
                             the cadence a pass ends on)",
                            stream.id,
                            stream.head - stream.place,
                            stream.waited,
                            stream.behind_while_waiting,
                        ));
                    }
                }
                stream.read_this_cadence = false;
            }

            if starved.is_empty() {
                return Ok(());
            }

            Err(format!(
                "a stream the Policy is behind on went unread for longer than ADR-0026's \
                 bound, ceil(streams behind / streams read per cadence) + 1:\n  \
                 starved: {}\n  \
                 seed: {}\n  streams: {} ({} quiet, {} busy)\n  read_batch: {}\n  \
                 polls per cadence: {}\n  schedule:\n    {}",
                starved.join("\n           "),
                self.seed,
                self.streams.len(),
                self.streams.iter().filter(|s| s.quiet).count(),
                self.streams.iter().filter(|s| s.busy).count(),
                self.read_batch,
                self.polls_per_cadence,
                self.transcript.join("\n    "),
            ))
        }

        /// Run the schedule, and hand back what it reached on the way.
        fn run(mut self) -> Result<Coverage, String> {
            let mut rng = Lcg(self.seed ^ 0x5eed);

            for _ in 0..CADENCES {
                for poll in 0..self.polls_per_cadence {
                    self.writes(&mut rng);
                    self.poll(poll == 0);
                }
                self.account()?;
            }

            Ok(self.seen)
        }
    }

    /// The property, over sixty-four schedules: saturation, empty sweeps, pages larger
    /// than the batch, a cadence that is an exact multiple of the poll interval, and a
    /// rotation sitting past the last stream id.
    #[test]
    fn a_stream_the_policy_is_behind_on_is_read_within_the_bound() {
        for seed in 1..=64 {
            if let Err(violation) = Schedule::from_seed(seed).run() {
                panic!("{violation}");
            }
        }
    }

    /// The schedules are worth the states they reach, so this fails if they stop reaching
    /// them. Each of these is a state one of the five defects needed to show itself; a
    /// seed range or a scheduler change that no longer produces it would leave the
    /// property passing over a world nobody meant to narrow.
    ///
    /// Counted as the schedules run rather than read off the knobs they were generated
    /// from: "every stream is quiet" is not "a poll had no candidates", because the
    /// carried queue outlives the sweep that filled it.
    #[test]
    fn the_schedules_reach_the_states_the_property_is_about() {
        let mut seen = Coverage::default();
        for seed in 1..=64 {
            let reached = Schedule::from_seed(seed)
                .run()
                .expect("the property holds; this test is about what it was decided over");
            seen.add(&reached);
        }

        assert!(
            seen.empty_polls > 0,
            "no poll in any schedule had an empty candidate list, which is the state a \
             finished pass ends on"
        );
        assert!(
            seen.wraps_on_an_empty_poll > 0,
            "no schedule wrapped the rotation on a poll that read nothing: the rotation \
             reached the last stream id and every source was empty, which is the one \
             state the poll's early return used to step over"
        );
        assert!(
            seen.budgets_spent_early > 0,
            "no poll ran out of event budget before its candidate list ran out, so the \
             rotation was never asked to tell a slot from a read"
        );
        assert!(
            seen.pages_at_the_batch > 0,
            "no reconciliation filled a page while more streams were behind than it \
             held, so a pass spanning several pages was never simulated"
        );
        assert!(
            seen.polls_between_cadences > 0,
            "every poll ran a reconciliation, so a cadence spanning several polls \
             — the phase a rotating turn could sample the same way for ever — was never \
             reached"
        );
    }
}
