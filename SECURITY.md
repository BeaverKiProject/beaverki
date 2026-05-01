# Security Policy

BeaverKi is early-stage local-first software for personal, family, or small-household use. It is not a hosted multi-tenant service and should not be exposed directly to the public internet.

## Supported Usage

The supported security posture is:

- run the daemon on hardware or a VPS you control
- keep the web UI bound to loopback or another trusted local network boundary
- store OpenAI, Discord, and Notion credentials through BeaverKi's encrypted local secret store
- use Discord only with mapped identities and allowlisted channels
- review approval prompts before allowing shell, filesystem, connector, or automation actions

Unsupported usage includes:

- exposing the local web UI as a public website
- running BeaverKi as a shared public bot for unrelated Discord servers
- treating BeaverKi as a hardened sandbox for untrusted users
- storing raw API tokens in committed config files

## Reporting Vulnerabilities

If you find a vulnerability, please open a private security advisory on GitHub if available for this repository. If private advisories are not available, contact the maintainer privately before publishing details.

Please include:

- affected version or commit
- reproduction steps
- impact and affected subsystem
- any known workaround

Do not include real API keys, Discord tokens, Notion tokens, personal data, or household data in reports.

## Public Issues

Use public GitHub issues for ordinary bugs, documentation problems, and feature requests. Do not file public issues that contain exploitable security details before the maintainer has had a chance to respond.

## Maintainer Response

The maintainer should acknowledge credible reports, assess severity, prepare a fix when needed, and publish a clear release note once the issue can be disclosed safely.
