# BeaverKI

BeaverKI is an open-source autonomous AI agent framework for running a personal, always-on digital assistant on your own hardware.

The goal is to build a local-first Rust platform that can:

- operate continuously, not only as a chat UI
- act on the local machine through tools
- support multiple human users in one household or deployment
- use different LLM providers
- enforce permissions and approvals
- extend behavior through skills and Lua-based automation

Planned capabilities include:

- terminal command execution
- filesystem access
- browser automation
- messaging integration
- recurring background work
- persistent memory
- dynamic sub-agents

## Current Status

The repository now includes:

- `M0 Foundation`
- `M1 Core Multi-User`

Implemented so far:

- product design document
- V1 technical spec
- GitHub-native contributor workflow
- issue and PR templates
- Rust workspace and crate boundaries for the core runtime
- encrypted local OpenAI credential storage
- SQLite-backed task, memory, tool-invocation, and audit persistence
- multi-user CLI setup and task execution flow
- built-in roles: `owner`, `adult`, `child`, `guest`, `service`
- per-user primary agents
- private versus household memory visibility at retrieval time
- CLI approval flow for risky shell commands
- bounded sub-agent spawning with explicit task slices
- shell and filesystem tools for the primary agent
- CI for formatting, linting, and tests

Still intentionally out of scope:

- always-running daemon/runtime
- Discord
- browser automation
- Lua runtime
- safety-agent review flows

Implementation remains staged. The next platform milestone is `M1.5 Runtime Daemon`.

## Project Direction

Current decisions for V1:

- Rust-based runtime
- SQLite-first operational state
- YAML configuration
- Markdown exports and human-readable summaries
- one persistent primary agent per user
- dynamic ephemeral sub-agents
- Discord as the first messaging connector
- OpenAI as the first model provider
- blocking safety review for Lua scripts
- safety review for medium/high/critical generated shell commands

## Delivery Plan

The milestone plan and pre-issue implementation stories live in:

- [Delivery Plan](docs/delivery-plan.md)

This keeps the README short while letting the delivery plan evolve separately from the product and technical specs.

## Documentation

- [Product Design](docs/design.md)
- [V1 Technical Spec](docs/technical-spec.md)
- [Delivery Plan](docs/delivery-plan.md)
- [Developer Workflow](docs/developer-workflow.md)
- [Contributing Guide](CONTRIBUTING.md)

## Development Workflow

This repository is intended to support a GitHub-native, agent-friendly workflow:

1. Create or refine a GitHub issue.
2. Implement the issue on a dedicated branch.
3. Run local validation.
4. Open a PR linked to the issue.
5. Review, test, and merge after checks pass.

Every material change should start from an issue with clear acceptance criteria.

## Near-Term Next Steps

- add the always-running daemon/runtime milestone (`M1.5`)
- move CLI task execution behind a long-lived local process
- add queued/background execution and heartbeat scaffolding
- add Discord and browser integrations on top of the daemon

## Quick Start

1. Run `make setup`.
2. Enter an OpenAI API key and a local master passphrase when prompted.
3. Run `make user-list`.
4. Run `make run-task OBJECTIVE="Summarize the current README and list the docs folder."`
5. Inspect the recorded task with `make show-task TASK_ID=<task-id>`.

Default storage locations are now platform-standard:

- Linux: `~/.config/beaverki` for config and `~/.local/share/beaverki` or `~/.local/state/beaverki` for runtime state
- macOS: `~/Library/Application Support/beaverki`
- Windows: `%APPDATA%\beaverki` and `%LOCALAPPDATA%\beaverki`

You can also call the CLI directly:

```bash
cargo run -p beaverki-cli -- setup verify-openai
cargo run -p beaverki-cli -- setup init
cargo run -p beaverki-cli -- setup show-models
cargo run -p beaverki-cli -- setup set-models --planner-model gpt-5.4 --executor-model gpt-5.4-mini --summarizer-model gpt-5.4-mini
cargo run -p beaverki-cli -- role list
cargo run -p beaverki-cli -- user list
cargo run -p beaverki-cli -- user add --display-name Casey --role adult
cargo run -p beaverki-cli -- task run --objective "Inspect the repository and summarize it."
cargo run -p beaverki-cli -- task run --user user_casey --objective "Inspect the repository and summarize it."
cargo run -p beaverki-cli -- task show --task-id <task-id>
cargo run -p beaverki-cli -- approval list
cargo run -p beaverki-cli -- approval approve --approval-id <approval-id>
```

Use `--config-dir` if you want a non-default location.

`setup init` verifies the OpenAI API key unless `--skip-openai-check` is passed. The API token is stored in an age-encrypted local secret file under the BeaverKI state directory. `task run` reads the master passphrase from `BEAVERKI_MASTER_PASSPHRASE` when available, otherwise it prompts for it.

The initial defaults are:

- planner: `gpt-5.4`
- executor: `gpt-5.4-mini`
- summarizer: `gpt-5.4-mini`

After setup, use `setup show-models` and `setup set-models` to inspect or change them without editing YAML manually.

Multi-user notes:

- each user gets one persistent primary agent
- `task run --user <user-id>` executes as that user
- task inspection is owner-scoped
- `approval list`, `approval approve`, and `approval deny` operate on the selected user's pending approvals

## Repository

GitHub: <https://github.com/torlenor/beaverki>
