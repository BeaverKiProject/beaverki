# Contributing

BeaverKI is intended to support a GitHub-native, agent-friendly development workflow. The repository should be easy for humans and AI coding agents to navigate, plan, implement, review, test, and merge safely.

GitHub uses pull requests, not merge requests. This repository standardizes on `PR` terminology.

## Workflow

1. Pick or create a GitHub issue.
2. Clarify scope, acceptance criteria, and risks in the issue.
3. Create a branch from the issue.
4. Implement the change in small, reviewable commits.
5. Run the relevant checks locally.
6. Open a PR linked to the issue.
7. Review comments, CI results, and follow-up fixes.
8. Merge after approval and passing checks.

## Issue Rules

- Every material code change should start from an issue.
- Issues should describe the problem, intended behavior, and acceptance criteria.
- If a task is too large for one PR, split it into sub-issues.
- Use labels consistently so agents can filter work.

## Branch Naming

Use one of:

- `feat/<issue-number>-short-slug`
- `fix/<issue-number>-short-slug`
- `chore/<issue-number>-short-slug`
- `docs/<issue-number>-short-slug`

Examples:

- `feat/42-discord-connector-bootstrap`
- `fix/57-shell-risk-classification`

## Commit Style

Prefer intentional, scoped commits.

Suggested format:

- `feat: add discord connector identity mapping`
- `fix: block unsafe lua activation on failed review`
- `docs: add technical spec for sqlite memory model`

## Pull Request Rules

- Link the issue using `Closes #<number>` or `Refs #<number>`.
- Keep PRs narrow enough to review quickly.
- Include the problem, change, and verification steps.
- Call out any risks, migrations, or follow-up work.
- Do not mix unrelated refactors into feature PRs.

## Review Rules

- Review for correctness, regression risk, safety, and missing tests first.
- Ask for changes when behavior, safety, or maintainability is unclear.
- Treat AI-generated code exactly like human-written code.
- Do not merge failing CI.

## Agent-Friendly Expectations

To support nearly autonomous agent development:

- issues should have clear acceptance criteria
- repository structure should be explicit and stable
- CI checks should be deterministic and scriptable
- PR templates should force verification notes
- review comments should be actionable and specific
- docs should state architectural boundaries and invariants

## Preferred Issue Labels

Recommended baseline labels:

- `type:feature`
- `type:bug`
- `type:docs`
- `type:chore`
- `area:runtime`
- `area:agent`
- `area:policy`
- `area:memory`
- `area:models`
- `area:tools`
- `area:lua`
- `area:connectors`
- `area:discord`
- `priority:p0`
- `priority:p1`
- `priority:p2`
- `good first issue`
- `blocked`

## Definition Of Done

A change is done when:

- the linked issue scope is satisfied
- tests or validation steps pass
- docs/config/schema changes are updated if needed
- safety implications are called out if relevant
- the PR is reviewed and merged

