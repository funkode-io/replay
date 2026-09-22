// The PostgreSQL image tag, written once for the two places that pin it.
//
// `include!`d by `tests/common/postgres_image.rs` and by the `cursor_tests` module in
// `src/policy_runner.rs`. An in-`src` test module cannot import from `tests/`, so the
// two pins were kept equal by hand and drifted: #223 raised the floor to 15 and left
// `cursor_tests` on 13 (funkode-io/replay#226). Neither file is the other's parent, so
// the shared definition sits at the crate root.
//
// Not a module — nothing declares `mod postgres_tag;` — and both `include!` sites are
// `cfg(test)`, so this compiles only when the tests do. Line comments throughout: an
// included file is expanded in item position, where `//!` would be an inner attribute
// and fail to compile.

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
