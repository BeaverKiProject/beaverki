# BeaverKi

<img src="docs/assets/logo.png" alt="BeaverKi logo" width="320" />

BeaverKi is an open-source, local-first assistant runtime for a person, family, or small household. It runs on hardware you control, keeps state and history local, and is built as a long-lived Rust daemon instead of a chat-only interface.

## What BeaverKi Does

Today BeaverKi can:

- run as a persistent local daemon
- support multiple users with built-in household roles
- keep tasks, memory, approvals, and audit history in local SQLite storage
- use either OpenAI or LM Studio-backed local models with different roles for planning, execution, summarization, and review
- act through local tools such as shell, filesystem, browser, Discord, and Notion integrations
- offer both a CLI workflow and a loopback-only local web UI

## Current Status

BeaverKi already has a usable runtime foundation, but it is still early-stage software. The current focus is a practical V1: local operation, clear approval boundaries, durable state, and a setup flow that is understandable for a household operator.

Current first-party integrations and assumptions:

- OpenAI and LM Studio are the currently supported model-provider paths
- Discord is the first remote messaging connector
- Notion is available as an optional integration
- the web UI is local-only and intended for use on the same machine

## Quick Start

1. Install the prerequisites.

   You need Rust with `cargo` and `make`.

2. Choose a model provider.

   For OpenAI, export an API key:

   ```bash
   export OPENAI_API_KEY="your-openai-api-key"
   ```

   For LM Studio, start LM Studio locally and load at least one chat-capable model before setup.

3. Initialize BeaverKi.

   ```bash
   make setup
   ```

   `setup init` now lets you choose OpenAI or LM Studio, and for LM Studio it can discover and prompt for the model IDs to use for planning, execution, summarization, and safety review.

4. Start the daemon.

   ```bash
   make daemon-start
   ```

   If `BEAVERKI_MASTER_PASSPHRASE` is not set, BeaverKi will prompt for it.

5. Optionally start the local web UI.

   ```bash
   make web-ui
   ```

   By default it listens on `http://127.0.0.1:7676`.

6. Run a first task.

   ```bash
   make run-task OBJECTIVE="Summarize the repository status and list the docs folder."
   ```

7. Inspect the task and pending approvals.

   ```bash
   make show-task TASK_ID=<task-id>
   make approval-list
   ```

8. Stop the daemon when finished.

   ```bash
   make daemon-stop
   ```

## Common Household Setup Commands

Verify the active configured provider:

```bash
cargo run -p beaverki-cli -- setup verify-provider
```

List roles and users:

```bash
make role-list
make user-list
```

Add another household member:

```bash
make user-add DISPLAY_NAME="Casey" USER_ARGS='--role adult'
```

Run a task as a specific user:

```bash
make run-task OBJECTIVE="Check my recent activity" TASK_ARGS='--user user_casey'
```

## Where To Go Next

- For CLI commands, daemon lifecycle, OpenAI or LM Studio model setup, and approvals, see [CLI and Operations Guide](docs/cli-operations.md).
- For local UI behavior and constraints, see [Product Design](docs/design.md).
- For Discord setup, see [Discord Setup Guide](docs/discord-setup.md).
- For Notion setup, see [Notion Setup Guide](docs/notion-setup.md).
- For workflows, see [Workflow Pipelines](docs/workflow-pipelines.md) and the example in [docs/examples/morning-news-digest-workflow.json](docs/examples/morning-news-digest-workflow.json).
- For architecture and roadmap detail, see [V1 Technical Spec](docs/technical-spec.md), [Delivery Plan](docs/delivery-plan.md), and [Memory Design](docs/memory_design.md).
- For contribution expectations, see [Contributing Guide](CONTRIBUTING.md) and [Developer Workflow](docs/developer-workflow.md).
- For public-readiness and security boundaries, see [Public Repository Readiness](docs/public-readiness.md) and [Security Policy](SECURITY.md).

## Releases

Releases are driven by Git tags rather than the Cargo workspace version.

- stable tags use `YYYY-MM-DD.N`
- beta tags use `YYYY-MM-DD.N-betaX`

Matching tags publish a GitHub Release and package release archives for Linux and macOS.

Each archive includes:

- `beaverki-cli`
- `beaverki-web`
- first-party `skills/`
- `systemd` and `launchd` service templates
- `packaging/install.sh` for curlable installs

Windows users are expected to run BeaverKi through WSL rather than a first-party native Windows package.

Example install command:

```bash
curl -fsSL https://raw.githubusercontent.com/torlenor/beaverki/master/packaging/install.sh | bash -s -- --tag 2026-04-23.1
```

## Repository

GitHub: <https://github.com/torlenor/beaverki>
