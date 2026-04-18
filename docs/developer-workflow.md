# Developer Workflow

## 1. Goal

This repository should support a near-autonomous GitHub workflow where coding agents can:

- pull issues
- understand scope
- implement changes
- run checks
- open PRs
- address review feedback
- merge safely

The workflow must remain human-auditable. Agents accelerate execution, but repository policy defines the guardrails.

## 2. GitHub-Native Loop

Standard loop:

1. A maintainer creates or refines an issue.
2. An agent or human picks up the issue.
3. A branch is created from the issue.
4. The change is implemented locally.
5. Local verification is run.
6. A PR is opened and linked back to the issue.
7. CI validates formatting, linting, tests, and any policy checks.
8. Review feedback is addressed.
9. The PR is merged after approval and green checks.
10. The issue is closed automatically by the PR merge.

## 3. Repository Requirements For Agentic Development

To make this workflow reliable for AI agents, the repository should maintain:

- stable file layout
- explicit docs for architecture and invariants
- deterministic local commands for lint/test/check
- machine-readable issue and PR templates
- narrow, reviewable issues
- labels that map work to subsystems
- CI that gives actionable failures

## 4. Issue Design

Issues should include:

- problem statement
- desired outcome
- acceptance criteria
- out-of-scope notes
- safety or migration concerns
- relevant files or subsystems when known

Well-formed issues are the difference between useful autonomy and noisy autonomy.

## 5. PR Design

PRs should include:

- linked issue
- concise summary
- verification steps
- residual risks
- screenshots/logs/output when relevant

PRs should stay small enough that review remains fast and specific.

## 6. Review Priorities

Review should focus on:

- correctness
- safety and policy regressions
- missing tests
- schema or migration risks
- architectural drift

Style issues matter, but they should not displace correctness and safety.

## 7. Merge Policy

Recommended merge conditions:

- linked issue exists
- required CI checks pass
- at least one review approval exists
- no unresolved blocking comments remain
- branch is up to date with the target branch

Recommended merge method:

- squash merge for most issue-scoped work

## 8. CI Shape

Once code exists, CI should expose predictable top-level jobs such as:

- `fmt`
- `lint`
- `test`
- `docs`
- `policy`

Agent developers should be able to run the same commands locally without guessing.

## 9. Suggested Automation Roadmap

Near-term:

- issue templates
- PR template
- CONTRIBUTING guide
- CI skeleton

After the first code lands:

- rustfmt and test jobs
- test matrix
- schema migration validation
- docs link checker
- PR label checks

Later:

- issue triage automation
- stale issue handling
- auto-assignment rules
- agent-generated PR summary validation

## 10. Branch And Naming Conventions

Branches should be issue-based and predictable:

- `feat/<issue-number>-slug`
- `fix/<issue-number>-slug`
- `chore/<issue-number>-slug`
- `docs/<issue-number>-slug`

This makes it easier for agents and humans to map branches back to issues.

## 11. Human Control Points

Even with strong automation, the following should remain explicit human control points:

- approval of high-risk architecture changes
- approval of policy/security-sensitive changes
- merge authority on protected branches
- release tagging

## 12. Release Tagging

Release publication is tag-driven.

- stable release tags use `YYYY-MM-DD.N`
- beta release tags use `YYYY-MM-DD.N-betaX`
- tag pushes build release binaries for Linux, Windows, and macOS and attach them to the GitHub Release
- Cargo crate versions remain normal semver in `Cargo.toml`; the Git tag is the public release identifier

The current CI workflow intentionally skips clippy until the existing lint failures are addressed.

## 13. Recommended Next Repo Additions

After crate scaffolding starts, the next useful repository additions are:

- `.github/workflows/ci.yml`
- `rust-toolchain.toml`
- `Makefile` or `justfile`
- `README.md`
- migration and schema docs

