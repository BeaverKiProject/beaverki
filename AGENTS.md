# AGENTS.md

## Commands

- Format: `make fmt`
- Lint: `make lint`
- Test: `make test`
- Type/check build: `make check`
- Run the CLI: `cargo run -p beaverki-cli -- <args>`
- Run the web UI: `make web-ui`

## Instructions

- Keep changes small and scoped to the requested task.
- Prefer existing workspace patterns over new abstractions.
- After implementing code changes, run `make fmt`, `make lint`, and `make test`.
- If a change is narrow and a full test run is too costly, run the most relevant targeted tests and state what was run.
- Update docs when behavior, commands, configuration, or user-facing workflows change.
