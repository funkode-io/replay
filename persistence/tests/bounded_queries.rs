//! Guards the library's bounded-memory rule against code nobody wrote a test for.
//!
//! The allocation tests pin the paths we already thought about. They are blind to a
//! *new* path: a `fetch_all` added to a new function allocates as much as the table
//! it reads and no existing test notices. That is exactly how the rebuild path in
//! `load_events_for_replay` survived ADR-0005 — stream-first was decided for the
//! write path, and the replay that came later inherited nothing.
//!
//! So this test inverts the burden. Every `fetch_all` in `persistence/src` must be
//! listed below with a reason it cannot grow without bound. Adding one fails the
//! build until its author writes that sentence — and if they cannot, that is the
//! finding.
//!
//! **This is a review prompt, not a proof.** It cannot tell a bounded query from an
//! unbounded one; only the justification can, and only a reader can judge it. It
//! catches the case that actually happens: a query added without the question being
//! asked at all.

use std::fs;
use std::path::{Path, PathBuf};

/// A reviewed `fetch_all` call site: where it is, and why it cannot grow unbounded.
struct Reviewed {
    /// Path relative to `persistence/`.
    file: &'static str,
    /// The function the call sits in — line numbers rot, names mostly don't.
    function: &'static str,
    /// What bounds the result set. "It is small in practice" is not a bound.
    justification: &'static str,
}

/// Every `fetch_all` in `persistence/src`, with the bound that makes it safe.
const REVIEWED: &[Reviewed] = &[
    Reviewed {
        file: "src/policy_runner.rs",
        function: "sweep_for_streams",
        justification: "SQL carries LIMIT $limit, the policy's resolved read_batch_size \
                        (default 100). Two columns per row, and the rows are reduced to \
                        the streams they name before anything else reads them.",
    },
    Reviewed {
        file: "src/policy_runner.rs",
        function: "streams_behind",
        justification: "SQL carries LIMIT $limit, the policy's resolved read_batch_size \
                        (default 100). Bounded by the tunable rather than by the number \
                        of streams behind, which is why a Policy far behind catches up \
                        over several cadences instead of in one allocation; the batch \
                        resumes after the last id it examined, so the bound costs a \
                        rotation rather than leaving a stream unexamined (ADR-0025).",
    },
    Reviewed {
        file: "src/policy_runner.rs",
        function: "places_of",
        justification: "One row per stream named in the poll's candidate list, which \
                        `drain_policy_once` truncates to read_batch_size whatever the \
                        sweep, the reconciliation and the last poll's leftovers offer.",
    },
    Reviewed {
        file: "src/policy_runner.rs",
        function: "read_stream",
        justification: "SQL carries LIMIT $limit, what is left of the poll's event \
                        budget: the policy's resolved read_batch_size (default 100), \
                        spent across the streams the poll looks at rather than per \
                        stream. One stream's events past its place, and not bounded by \
                        the policy's stream filter, which selects what is delivered from \
                        the batch rather than what is read (ADR-0013).",
    },
    Reviewed {
        file: "src/policy_runner.rs",
        function: "checkpoint_places",
        justification: "RETURNING one row per place written, and the places written are \
                        the streams the poll advanced: bounded by the candidate list, \
                        which `drain_policy_once` truncates to read_batch_size. Read \
                        back rather than counted because the caller needs to know which \
                        of the writes lost their compare-and-set, and at what row \
                        version the ones that won now sit.",
    },
    Reviewed {
        file: "src/policy_runner.rs",
        function: "load_parked_reaction",
        justification: "One reaction's rows: one per command it dispatches (the vector \
                        `react_erased` already materialises), plus the rows parked for \
                        that event before `idx_dead_letters_parked_command` keyed the \
                        table (funkode-io/replay#220). That legacy tail is the old \
                        code's commands times its deliveries, it is fixed at the moment \
                        the migration runs \u{2014} every later park refreshes a row rather \
                        than adding one \u{2014} and it is not a number in the code, which is \
                        why bounding the read over it is tracked by \
                        funkode-io/replay#228. Not bounded by the table: the rows of one \
                        reaction, never of a policy's backlog.",
    },
    Reviewed {
        file: "src/policy_runner.rs",
        function: "load_parked_reactions",
        justification: "SQL carries LIMIT RETRY_PAGE_SIZE (100). One page of a bulk \
                        retry's keyset walk over a policy's parked reactions, settled \
                        before the next is read.",
    },
    Reviewed {
        file: "src/policy_status.rs",
        function: "list",
        justification: "One row per registered policy. Bounded by the code that registers \
                        them, not by data — the per-stream frontier each row carries is \
                        aggregated in SQL, so it costs a scan and not a row.",
    },
    // ── Known unbounded ──────────────────────────────────────────────────────
    Reviewed {
        file: "src/infrastructure/postgres.rs",
        function: "load_events_for_replay",
        justification: "UNBOUNDED — loads all history matching a projection's filter, and \
                        holds the rows and the parsed events at once. Tracked by \
                        funkode-io/replay#157; this entry must be deleted when it is fixed, \
                        not amended.",
    },
];

/// Rust sources under `persistence/src`.
fn sources() -> Vec<PathBuf> {
    fn walk(dir: &Path, out: &mut Vec<PathBuf>) {
        for entry in fs::read_dir(dir).expect("read persistence/src") {
            let path = entry.expect("dir entry").path();
            if path.is_dir() {
                walk(&path, out);
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.push(path);
            }
        }
    }

    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut out = Vec::new();
    walk(&root, &mut out);
    out.sort();
    out
}

/// Whether a line calls `fetch_all`, tolerating whitespace before the paren so that
/// `fetch_all (` is not a way past the guard.
fn calls_fetch_all(line: &str) -> bool {
    line.match_indices("fetch_all")
        .any(|(at, _)| line[at + "fetch_all".len()..].trim_start().starts_with('('))
}

/// Counts `fetch_all` call sites per file, skipping lines whose first non-whitespace
/// is `//` so that prose about the rule — including this file's own doc comment —
/// doesn't trip it. Block comments and trailing `//` are not handled: this is a
/// review prompt, not a parser.
fn fetch_all_sites() -> Vec<(String, usize)> {
    sources()
        .into_iter()
        .filter_map(|path| {
            let text = fs::read_to_string(&path).expect("read source");
            let count = text
                .lines()
                .filter(|line| !line.trim_start().starts_with("//"))
                .filter(|line| calls_fetch_all(line))
                .count();

            if count == 0 {
                return None;
            }

            let relative = path
                .strip_prefix(env!("CARGO_MANIFEST_DIR"))
                .expect("path under manifest dir")
                .to_string_lossy()
                .replace('\\', "/");

            Some((relative, count))
        })
        .collect()
}

/// Every `fetch_all` in the crate must be accounted for in [`REVIEWED`].
///
/// Deliberately compares *counts per file*, not line numbers: line numbers rot on
/// every edit, and the question this asks — "has a new one appeared?" — is answered
/// by the count.
#[test]
fn every_fetch_all_is_reviewed_for_boundedness() {
    for (file, found) in fetch_all_sites() {
        let expected = REVIEWED.iter().filter(|r| r.file == file).count();

        let sites = if found == 1 {
            "call site"
        } else {
            "call sites"
        };
        let verb = if expected == 1 { "is" } else { "are" };

        assert_eq!(
            found, expected,
            "\n{file} has {found} `fetch_all` {sites}, but {expected} {verb} reviewed in \
             persistence/tests/bounded_queries.rs.\n\n\
             If you added one: list it in REVIEWED with the bound that makes it safe — a \
             SQL LIMIT, a fixed set, a chunked loop. If the result set grows with the data, \
             it is unbounded, and this library's rule (AGENTS.md, 'Bounded memory') is to \
             stream it or chunk it instead.\n\n\
             If you removed one: delete its REVIEWED entry.\n"
        );
    }
}

/// A justification that says nothing is worse than none: it looks like the question
/// was asked. Requires enough of an answer to be judged in review.
#[test]
fn every_justification_names_an_actual_bound() {
    for entry in REVIEWED {
        assert!(
            entry.justification.len() > 40,
            "the justification for {}::{} is too short to name a bound",
            entry.file,
            entry.function
        );
    }
}

/// The allowlist must not outlive the code: an entry for a file with no `fetch_all`
/// left is stale, and a stale allowlist is how this test quietly stops working.
#[test]
fn no_reviewed_entry_is_stale() {
    let sites = fetch_all_sites();

    for entry in REVIEWED {
        assert!(
            sites.iter().any(|(file, _)| file == entry.file),
            "REVIEWED lists {}::{}, but {} has no `fetch_all` left — delete the entry",
            entry.file,
            entry.function,
            entry.file
        );
    }
}
