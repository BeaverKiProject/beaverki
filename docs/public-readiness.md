# Public Repository Readiness

This checklist tracks the work needed before or immediately around making BeaverKi publicly visible.

## Current Goal

Make the repository understandable, reproducible, and honest about its security boundaries before broader public use. This is a hardening and documentation track, not a feature-expansion track.

## Required Before Opening

- CI runs the same baseline checks that contributors are expected to run locally:
  - `cargo fmt --all --check`
  - `cargo clippy --workspace --all-targets -- -D warnings`
  - `cargo test --workspace --locked`
- README quick start works from a clean checkout with Rust, `make`, and either an OpenAI API key or a running LM Studio instance with a loaded local model.
- Security policy exists and states supported versus unsupported deployment shapes.
- Release packaging docs describe the tag-driven release flow and generated archive contents.
- Public docs do not imply that the web UI is safe to expose directly to the internet.
- Public docs do not imply that the Discord connector is a general-purpose public bot.
- No raw secrets, host-local paths, or private operational details are committed.

## Nice To Have

- A clean smoke-test script for release archives.
- A short operator checklist for first-time setup validation.
- A docs link check in CI.
- Issue labels synced with the labels described in `CONTRIBUTING.md`.

## Verification Notes

When checking public readiness, record:

- commands run
- operating system
- whether network-backed checks were skipped or exercised
- any required environment variables
- release tag or commit tested

## Out Of Scope

The public-readiness track does not require implementing new providers, new connectors, a skills marketplace, or signed skill distribution before the repository is made public.
