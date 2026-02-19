/// Example demonstrating proper error chaining with replay::Error
///
/// This example shows how `.with_source()` automatically detects and properly
/// handles replay::Error to maintain a clear "caused by" chain.
use replay::Error;

/// Simulate a database layer that might fail
fn connect_to_database() -> Result<(), Error> {
    // Simulate a connection failure
    Err(Error::unavailable("Connection timeout")
        .with_operation("connect_to_db")
        .with_context("host", "localhost:5432")
        .with_context("timeout_ms", "5000"))
}

/// Repository layer that uses the database
fn fetch_user_from_db(user_id: &str) -> Result<String, Error> {
    // Attempt to connect
    connect_to_database().map_err(|db_err| {
        // with_source() automatically detects replay::Error and preserves the chain
        Error::unavailable("Failed to fetch user from database")
            .with_operation("fetch_user")
            .with_context("user_id", user_id)
            .with_source(db_err)
    })?;

    Ok(format!("User data for {}", user_id))
}

/// Service layer that orchestrates business logic
fn get_user_profile(user_id: &str) -> Result<String, Error> {
    fetch_user_from_db(user_id).map_err(|repo_err| {
        // with_source() works seamlessly - no need to know if it's replay::Error or not
        Error::internal("User profile retrieval failed")
            .with_operation("get_user_profile")
            .with_context("request_id", "abc-123-def-456")
            .with_source(repo_err)
    })
}

fn main() {
    println!("=== Error Chaining Example ===\n");

    // Attempt to get user profile (which will fail in this example)
    match get_user_profile("user-789") {
        Ok(profile) => println!("Success: {}", profile),
        Err(error) => {
            println!("Complete error with full chain:\n");
            println!("{}", error);
            println!("\n{}", "=".repeat(80));

            // Demonstrate error introspection
            println!("\nError Properties:");
            println!("  Kind: {:?}", error.kind());
            println!("  Status: {:?}", error.status());
            println!("  Is Temporary: {}", error.is_temporary());
            println!("  Operation: {}", error.operation());

            if !error.context().is_empty() {
                println!("\n  Context:");
                for (key, value) in error.context() {
                    println!("    - {}: {}", key, value);
                }
            }
        }
    }
}
