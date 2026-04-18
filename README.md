# BeaverKi

BeaverKi is an open-source, local-first autonomous agent framework for running a personal or household assistant on hardware you control. It is built as a long-lived Rust runtime rather than a chat-only interface, so it can keep operating in the background, act through local tools, and preserve durable state on your machine.

The project is aimed at a practical V1 that can:

- run continuously as a local daemon
- support multiple human users in one household or deployment
- use different model roles for planning, execution, summarization, and safety review
- enforce permissions and approvals for risky actions
- act through shell, filesystem, browser, messaging, and automation capabilities
- keep tasks, memory, approvals, and audit history local and inspectable

The longer-term goal is a trusted digital companion that stays understandable to non-technical users while still giving advanced users full control over data, policy, and automation.

## Releases

BeaverKi release identifiers are driven by Git tags, not by the Cargo package version in the workspace manifests.

- release tags must use `YYYY-MM-DD.N`, for example `2026-04-18.1`
- beta tags may use `YYYY-MM-DD.N-betaX`, for example `2026-04-18.2-beta1`
- pushing a matching tag creates a GitHub Release and uploads packaged CLI binaries for Linux, Windows, and macOS when those builds succeed

This is compatible with Rust because Cargo still uses the workspace crate version for package metadata, while Git tags can use the date-based release scheme independently.

## Current State

BeaverKi already has a usable local runtime foundation. Today the repository includes:

- a Rust workspace with separated crates for runtime, CLI, config, policy, memory, models, tools, automation, and storage
- encrypted local OpenAI credential storage
- SQLite-backed persistence for tasks, memory, approvals, tool invocations, and audit data
- one persistent primary agent per user plus bounded sub-agents for task slices
- built-in household roles: `owner`, `adult`, `child`, `guest`, `service`
- private and household memory scopes enforced at retrieval time
- CLI-first setup, daemon lifecycle, and task execution
- shell, filesystem, and browser tools for the primary agent
- Discord connector setup, identity mapping, DM intake, allowlisted channel intake, and approval routing
- Lua scripting with review and activation flow
- CI for formatting and tests, plus tag-driven GitHub Releases

Still intentionally out of scope for the current milestone:

- safety-agent review flows

## How BeaverKi Works Today

The current V1 direction is:

- a Rust-based local runtime
- SQLite-first operational state with YAML configuration
- one persistent primary agent per user
- dynamic ephemeral sub-agents with bounded context handoff
- OpenAI as the first model provider
- Discord as the first messaging connector
- blocking safety review for Lua scripts
- approval gates for medium, high, and critical generated shell actions

Operationally, that means BeaverKi is already useful as a local, multi-user assistant runtime, but it is still early-stage software and currently optimized for CLI-driven setup and operation.

## Get BeaverKi Running For A Household

The shortest path is to use the Makefile targets for setup and daily operations, then drop into the direct CLI only when you need more control.

1. Install the local prerequisites.

	You need a Rust toolchain with `cargo`, plus `make`. BeaverKi currently runs as a local Rust workspace and uses OpenAI for the initial provider integration.

2. Export an OpenAI API key.

	```bash
	export OPENAI_API_KEY="your-openai-api-key"
	```

3. Initialize BeaverKi on the machine that will host the household runtime.

	```bash
	make setup
	```

	The setup flow creates the runtime config, provider config, integration config, encrypted secret reference, and the initial owner user. It will ask for an instance ID, owner display name, workspace root, and a master passphrase for encrypted local secrets.

4. Start the daemon.

	```bash
	make daemon-start
	```

	BeaverKi keeps decrypted provider credentials only in memory for the runtime process. If `BEAVERKI_MASTER_PASSPHRASE` is not set, the daemon will prompt for it.

5. Review the built-in roles and add the rest of the household.

	```bash
	make role-list
	make user-add DISPLAY_NAME="Casey" USER_ARGS='--role adult'
	make user-add DISPLAY_NAME="Sam" USER_ARGS='--role child'
	make user-list
	```

	Setup creates the owner automatically. Each additional user receives a dedicated user ID and a persistent primary agent.

6. Run a first task to verify the runtime.

	```bash
	make run-task OBJECTIVE="Summarize the repository status and list the docs folder."
	```

	For a household member other than the owner, pass the generated user ID through `TASK_ARGS`, for example `TASK_ARGS='--user user_casey'`.

7. Inspect the recorded task and any pending approvals.

	```bash
	make show-task TASK_ID=<task-id>
	make approval-list
	```

	If a risky action was paused for approval, approve or deny it from the CLI before the task continues.

8. Optionally connect Discord for remote household use.

	Store a bot token, enable the connector, then map each Discord user ID to a BeaverKi user. Direct messages are accepted by default; guild messages are accepted only from allowlisted channel IDs. For the full setup flow, see [Discord Setup Guide](docs/discord-setup.md).

9. Stop the daemon when you are done.

	```bash
	make daemon-stop
	```

For the direct CLI forms of these commands, model configuration, memory inspection, Discord mapping, and automation commands, see [CLI and Operations Guide](docs/cli-operations.md).

## Documentation

- [CLI and Operations Guide](docs/cli-operations.md)
- [Discord Setup Guide](docs/discord-setup.md)
- [Product Design](docs/design.md)
- [V1 Technical Spec](docs/technical-spec.md)
- [Delivery Plan](docs/delivery-plan.md)
- [Memory Design](docs/memory_design.md)
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

- expand the daemon into richer connector-driven and scheduled background work
- deepen Discord and browser workflows on top of the existing runtime
- add safety review flows for risky generated actions and scripts

## Repository

GitHub: <https://github.com/torlenor/beaverki>
