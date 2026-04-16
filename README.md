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

The repository is currently in the design and specification phase.

Implemented so far:

- product design document
- V1 technical spec
- GitHub-native contributor workflow
- issue and PR templates

Not implemented yet:

- Rust workspace
- runtime
- providers
- tools
- connectors
- CI

Implementation will be staged. The first shippable milestone is intentionally smaller than the full V1 target.

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

- scaffold the Rust workspace
- define initial SQLite migrations
- implement config loading and setup assistant
- add the first CI workflow

## Repository

GitHub: <https://github.com/torlenor/beaverki>
