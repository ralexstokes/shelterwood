# Where the prose lives

Every piece of prose in this project has exactly one home, chosen by
audience, and is tested where it lives. Future writing should find its home
here rather than inventing a new one.

| Home | Audience | Job | How it stays true |
| --- | --- | --- | --- |
| rustdoc (crate docs) | API users at the call site | Per-item contracts, `# Errors`/`# Examples`, the crate front page's map of the flat surface | Doctests compile and run in `cargo test --doc`; `-D warnings` denies broken intra-doc links |
| rustdoc guide pages (`shelterwood::guides`) | API users reading across items | Cross-item contracts: retries and ordering, shutdown and resources, the error catalog | Rendered on docs.rs from `crates/shelterwood/docs/*.md`; their code fences are rustdoc doctests |
| This book (`book/`) | Newcomers and evaluators | The narrative on-ramp, taught in dependency order | Code blocks are `{{#include}}`s of anchored regions from `examples/`; `mdbook build` in CI proves the anchors resolve |
| `crates/shelterwood/examples/` | Everyone above | The tested artifacts the book and README quote | Each ends in assertions; `just examples` runs all of them in CI |
| `specs/SPEC.md` | Maintainers and reviewers | The normative contract the implementation is held to | Conformance obligations map to tests; adjudication protocol in the tracker |
| `README.md` | The front door | Orientation and the quickstart quote | Quotes `examples/quickstart.rs`, which CI runs |
| `AGENTS.md` (`CLAUDE.md` symlinks to it) / code comments | Contributors in the diff | Invariants the code cannot say itself | Reviewed beside the code they constrain |

Two corollaries the pass that created this structure settled:

- **No byte-copies.** The old `doctests/` mirror and its sync script are
  gone. A document needed in two compilation contexts is included by path
  (`include_str!`, `{{#include}}`), never duplicated.
- **The book has no test lane.** Its code cannot drift because it is not
  authored in the book at all; the examples are the single tested source.
