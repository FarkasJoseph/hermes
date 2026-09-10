//! Hermes-YAMCS bridge library.
//!
//! Split into a library (this crate) and a thin `main.rs` binary so unit tests run under
//! `cargo test --lib`, matching CI's actual gate (`.github/workflows/build-rust-crates.yml`
//! only runs `cargo test --workspace --all-features --lib`, which never exercised a bin-only
//! crate's `#[cfg(test)]` modules).
pub mod convert;
pub mod dp_container;
pub mod file_transfer;
pub mod service;
