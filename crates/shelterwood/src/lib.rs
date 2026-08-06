#![warn(missing_docs)]

//! Structured supervision and actors for asynchronous Rust systems.

// M0 deliberately establishes the complete runtime boundary before its first
// consumer lands in M1.
#[allow(dead_code)]
mod runtime;
