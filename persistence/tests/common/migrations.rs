//! The migration set the Docker-gated tests run, and slices of it.
//!
//! A test that stages a database as it stood *before* some migration needs the set
//! truncated at a version, which `Migrator::run` cannot express on its own.
//!
//! `allow(dead_code)`: every test binary that says `mod common;` compiles this module,
//! including the ones that migrate all the way up and never ask for a slice.
#![allow(dead_code)]

use std::borrow::Cow;

use sqlx::migrate::Migrator;

/// Every migration, in order — the schema a deployment ends up with.
pub static MIGRATOR: Migrator = sqlx::migrate!("./tests/migrations");

/// The migration set truncated at `version`, so a test can populate the schema as it
/// stood before a migration and then run that migration against real data.
pub fn through(version: i64) -> Migrator {
    Migrator {
        migrations: Cow::Owned(
            MIGRATOR
                .iter()
                .filter(|migration| migration.version <= version)
                .cloned()
                .collect(),
        ),
        ..Migrator::DEFAULT
    }
}

/// The SQL of one migration, for a test that has to run it itself rather than through
/// [`Migrator::run`] — holding it open in a transaction, for instance.
pub fn sql(version: i64) -> String {
    MIGRATOR
        .iter()
        .find(|migration| migration.version == version)
        .unwrap_or_else(|| panic!("migration {version} exists"))
        .sql
        .as_str()
        .to_owned()
}
