// The PostgreSQL image tag, written once for the two places that pin it: an in-`src`
// test module cannot import from `tests/`, and the hand-kept copies drifted — #223
// raised the floor to 15 and left `cursor_tests` on 13 (funkode-io/replay#226).
//
// `include!`d rather than declared as a `#[path]` module: a `mod` inside an inline
// `mod cursor_tests` resolves its path against `src/policy_runner/cursor_tests/`, a
// directory that does not exist, so the link would be spelled `../../../` and rot on
// the next rename. Line comments only — an included file is spliced in item position,
// where `//!` is an inner attribute and does not compile.

/// Image tag pinning the PostgreSQL release the suite runs against.
///
/// The crate's documented floor (README "Requirements"), not the newest release: the
/// floor is what the crate promises, so the floor is what the suite verifies. A run on
/// a newer server cannot fail on a feature the floor lacks — which is exactly how five
/// tests came to run on `postgres:11-alpine` unnoticed until funkode-io/replay#193
/// needed a type it does not have.
///
/// PostgreSQL 13 and 14 are both out of upstream support. The pin states what the crate
/// promises, not what a deployment should be running; raising the pin is raising the
/// floor, and belongs in the README in the same change.
pub const POSTGRES_TAG: &str = "15-alpine";
