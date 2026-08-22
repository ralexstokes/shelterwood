# Introduction

Shelterwood is a structured-supervision and actor runtime for asynchronous
Rust. A system is declared as a tree of actors, plain tasks, and nested
scopes; the tree owns startup order, readiness, restart policy, bounded
mailboxes, shutdown, and observation.

This book is the narrative on-ramp: it teaches the model in the order you
need it, from a first running system to embedding Shelterwood in a host
process. Every code block is included from a runnable example in the
repository's `crates/shelterwood/examples/` directory — CI compiles and runs
each one, so the book cannot drift from working code.

The book is deliberately not the reference. Per-item contracts, the error
catalog, and the operational guides live in the
[API documentation](https://docs.rs/shelterwood), and the normative
specification lives in the repository's `specs/SPEC.md`. The final appendix,
[Where the prose lives](appendix-where-prose-lives.md), maps that division.

A closing part, [For maintainers](internals-shape.md), turns inward: it
maps how the implementation itself is constructed and how data flows
through it, for readers changing Shelterwood rather than building on it.

If you want to skip straight to code, [A first system](first-system.md) has
you running in a page.
