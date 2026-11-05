//! What a command middleware costs a command, in bytes.
//!
//! `Cqrs::execute` without middleware takes the path it always took; with middleware it
//! builds a command context, runs every hook and hands the metadata on to the store
//! (ADR-0035). This test measures that difference on the in-memory store, so what it
//! sees is `Cqrs`'s own work rather than a database driver's: the bytes one command
//! requests with no middleware, with one middleware that does nothing, and with one
//! that adds a key to the metadata.
//!
//! The budgets bound the *overhead* per command, which is a constant: the context is
//! built once per command and the chain is as long as the consumer made it, so neither
//! grows with the data.

mod common;

use common::alloc::{allocated_so_far, CountingAllocator};
use common::policy_harness::{Probe, ProbeCommand, ProbeUrn};
use common::report::report;

use futures::future::BoxFuture;
use replay::Metadata;
use replay_persistence::{CommandContextMut, CommandMiddleware, Cqrs, InMemoryEventStore, Next};
use serde_json::json;

/// Every test binary registers its own global allocator; the counting itself is
/// shared (`tests/common/alloc.rs`).
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Commands measured per variant: enough that the store's own growth averages out.
const COMMANDS: usize = 200;

/// Implements no hook: what the chain itself costs a command.
struct DoesNothing;

impl CommandMiddleware for DoesNothing {}

/// Adds the acting user to every command's metadata: the enrichment a consumer runs.
struct AddsTheUser;

impl CommandMiddleware for AddsTheUser {
    fn before<'a>(&'a self, mut context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            match context.insert_metadata("user", "alice") {
                Ok(()) => Next::Continue,
                Err(error) => Next::Error(error),
            }
        })
    }
}

/// The average bytes one command requests through `cqrs`, each to a stream of its own
/// so loading the aggregate costs every command the same.
async fn bytes_per_command(cqrs: Cqrs<InMemoryEventStore>) -> usize {
    // Every argument is built before measuring, so what is measured is `execute`.
    let commands: Vec<(ProbeUrn, Metadata, ProbeCommand)> = (0..=COMMANDS)
        .map(|i| {
            (
                ProbeUrn::new(format!("subject-{i}")).expect("a valid URN NSS"),
                Metadata::new(json!({ "tenant": "acme", "request": format!("r-{i}") })),
                ProbeCommand::Ping {
                    tag: format!("tag-{i}"),
                },
            )
        })
        .collect();
    let mut commands = commands.into_iter();

    // The first command warms the store, outside the measurement.
    let (id, metadata, command) = commands.next().expect("one command to warm up");
    cqrs.execute::<Probe>(&id, metadata, command, &(), None)
        .await
        .expect("the warm-up command commits");

    let start = allocated_so_far();
    for (id, metadata, command) in commands {
        cqrs.execute::<Probe>(&id, metadata, command, &(), None)
            .await
            .expect("every measured command commits");
    }
    (allocated_so_far() - start) / COMMANDS
}

/// The bytes a middleware adds to one command stay a small constant.
#[tokio::test]
async fn a_middleware_costs_a_command_a_bounded_number_of_bytes() {
    let plain = bytes_per_command(Cqrs::new(InMemoryEventStore::new())).await;
    let no_op =
        bytes_per_command(Cqrs::new(InMemoryEventStore::new()).with_middleware(DoesNothing)).await;
    let enriching =
        bytes_per_command(Cqrs::new(InMemoryEventStore::new()).with_middleware(AddsTheUser)).await;

    println!("command bytes: plain={plain} no_op={no_op} enriching={enriching}");

    // Measured: 17,522 bytes for a plain command; a no-op middleware adds 138 and one
    // that adds a key 147, under 1%. That is the context (its stream type, stream id and
    // the metadata handed on to the store) and one boxed future per hook. Before `before`
    // took a `CommandContextMut`, `Cqrs` kept a snapshot of the context to check it
    // afterwards, and the enriching middleware cost 826: the budget sits below that, so
    // a per-command copy of the metadata coming back fails it.
    const OVERHEAD_BUDGET: usize = 512;
    for (name, bytes) in [
        ("command_middleware_no_op", no_op),
        ("command_middleware_enriching", enriching),
    ] {
        let overhead = bytes.saturating_sub(plain);
        report(
            name,
            "overhead_per_command",
            overhead as i128,
            OVERHEAD_BUDGET as i128,
            "bytes",
        );
        assert!(
            overhead < OVERHEAD_BUDGET,
            "{name}: a middleware added {overhead} bytes to a command of {plain} \
             (budget {OVERHEAD_BUDGET})"
        );
    }
}
