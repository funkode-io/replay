fn main() {
    // Enable sqlx offline mode so consumers of this crate don't need DATABASE_URL
    println!("cargo:rustc-env=SQLX_OFFLINE=true");
}
