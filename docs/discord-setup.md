# Discord Setup Guide

This guide walks through the complete Discord setup for BeaverKi, from creating the bot to mapping household members and testing the integration.

## What The Discord Connector Does

The current Discord integration lets BeaverKi:

- accept direct messages from mapped Discord users
- accept server messages only from explicitly allowlisted channel IDs
- accept allowlisted server messages when they start with the configured command prefix or a direct mention of the bot
- route each Discord identity to a BeaverKi household user
- send task replies back into the same Discord conversation
- send direct household messages to another mapped household member when an allowed user asks BeaverKi to notify them now
- keep short-lived working indicators active while a task is still running
- route risky approval actions through Discord, with DM-only approval by default

The connector is designed for household remote control, not for a public multi-tenant bot.

## Before You Start

Make sure you already have:

- a working BeaverKi installation created with `make setup` or `beaverki setup init`
- the daemon running locally with `make daemon-start` or `beaverki daemon start`
- at least one BeaverKi user created for each household member you want to map
- a Discord account with permission to create applications and invite bots to your server

Useful commands before you begin:

```bash
make user-list
cargo run -p beaverki-cli -- connector discord show
```

## 1. Create The Discord Application And Bot

1. Open the Discord Developer Portal.
2. Create a new application for BeaverKi.
3. Add a bot user to that application.
4. Copy the bot token. You will store it in BeaverKi during connector configuration.
5. Enable the Message Content intent for the bot.

BeaverKi reads message text to turn Discord messages into task requests. Without Message Content intent, channel and DM requests will not work correctly.

## 2. Invite The Bot To Your Server

If you want to use BeaverKi only in direct messages, you can skip shared server channels. If you want shared household channels, invite the bot to the server first.

When creating the invite in the Discord Developer Portal, grant the bot enough access to:

- view the channels you plan to allowlist
- read messages and history in those channels
- send replies
- add reactions

BeaverKi uses replies, typing indicators, and a temporary working reaction while tasks are in progress.

## 3. Enable Developer Mode In Discord

You will need Discord IDs to map users and allow channels.

1. In Discord, open User Settings.
2. Open Advanced.
3. Enable Developer Mode.
4. Right-click a user and copy their user ID.
5. Right-click a channel and copy its channel ID.

## 4. Enable The Connector In BeaverKi

Export the bot token or let the CLI prompt for it.

```bash
export DISCORD_BOT_TOKEN="your-discord-bot-token"
```

Enable the connector and store the bot token:

```bash
cargo run -p beaverki-cli -- connector discord configure --enable
```

Then add an allowlisted shared channel in the default household mode:

```bash
cargo run -p beaverki-cli -- connector discord add-channel \
  --channel-id 123456789012345678 \
  --mode household
```

If you want the shared channel to behave as a guest room instead, set it explicitly when adding or updating the channel:

```bash
cargo run -p beaverki-cli -- connector discord add-channel \
  --channel-id 123456789012345678 \
  --mode guest
```

If you want a custom prefix in shared channels, set it explicitly without re-entering the token:

```bash
cargo run -p beaverki-cli -- connector discord configure \
  --command-prefix "!beaver"
```

When the connector is enabled, BeaverKi encrypts and stores the Discord bot token locally using the same secret storage flow as the model credentials.

Inspect the resulting configuration:

```bash
cargo run -p beaverki-cli -- connector discord show
```

If you only want the allowlisted shared-channel view, use:

```bash
cargo run -p beaverki-cli -- connector discord list-channels
```

You should see:

- `Discord enabled: true`
- the configured command prefix
- the encrypted token secret reference
- the allowlisted channels with their configured mode

In shared channels, BeaverKi accepts either of these channel triggers:

- the configured command prefix such as `!bk`
- a message that includes a direct mention of the bot such as `@BeaverKi`

## 5. Map Discord Users To Household Users

Each Discord user must be mapped to a BeaverKi household user before BeaverKi can accept their requests.

First list the BeaverKi users so you know the target IDs:

```bash
make user-list
```

Then map each Discord identity:

```bash
cargo run -p beaverki-cli -- connector discord map-user \
  --external-user-id 111111111111111111 \
  --mapped-user-id user_casey
```

Repeat that for every household member who should be able to talk to the bot.

To verify the mappings:

```bash
cargo run -p beaverki-cli -- connector discord list-mappings
```

## 6. Start Or Restart The Daemon

If the daemon was already running before you enabled Discord, restart it so the bot token is loaded into the runtime:

```bash
make daemon-stop
make daemon-start
```

Check that the daemon is reachable:

```bash
make daemon-status
```

## 7. Test Direct Messages And Channel Usage

### Direct messages

Direct messages are accepted by default and do not need the configured command prefix.

Example DM:

```text
Summarize the latest task activity for my account.
```

### Direct household delivery

If the sender has an allowed household role and the recipient has a mapped Discord identity, BeaverKi can send an immediate direct household message on the sender's behalf.

Typical request examples:

```text
Tell Casey dinner is ready.
```

```text
Please let Casey know I am on my way home now.
```

Current delivery behavior:

- BeaverKi resolves the named recipient against active mapped household users
- the send is policy-gated, so untrusted roles are denied rather than silently delivering cross-user messages
- Discord delivery is DM-first and falls back to the recipient's mapped channel when DM creation is unavailable
- delivery is persisted and deduplicated so a retry or restart does not send the same immediate message twice
- audit and task events record who requested the delivery, who received it, and which route was used

If BeaverKi cannot resolve the recipient uniquely or the recipient has no mapped delivery route, it should deny the request and explain why rather than guessing.

### Shared server channels

Shared server channels must be explicitly allowlisted.

Within an allowlisted shared channel, BeaverKi accepts either:

- a message that starts with the configured prefix
- a message that includes a direct bot mention

Example channel message with the default prefix:

```text
!bk Summarize the latest task activity for my account.
```

Equivalent example using a bot mention:

```text
Hey @BeaverKi, summarize the latest task activity for my account.
```

If the message is accepted, BeaverKi will either reply directly or acknowledge that the daemon is still working on it.

## 8. How Approvals Work In Discord

The current default is DM-only approval handling.

That means:

- a risky request may start from a shared channel
- BeaverKi can ask the user to continue the approval flow in a direct message
- approval commands in Discord use one-time action tokens issued by BeaverKi
- critical approvals may require an additional confirmation token

If a request needs approval, follow the instructions from BeaverKi in DM rather than continuing in the public channel.

## 9. Day-To-Day Operations

Useful operational commands:

```bash
cargo run -p beaverki-cli -- connector discord show
cargo run -p beaverki-cli -- connector discord list-mappings
cargo run -p beaverki-cli -- approval list
make daemon-status
```

To change the channel allowlist or command prefix, rerun `connector discord configure` with the new values.
To add or update a specific shared channel later, use `connector discord add-channel`. To remove one, use `connector discord remove-channel`.

## 10. Troubleshooting

### The bot does not answer in DM

Check these first:

- the daemon is running
- the connector is enabled
- the Discord user has been mapped to a BeaverKi user
- the bot token was stored correctly
- Message Content intent is enabled in the Discord Developer Portal

Useful checks:

```bash
make daemon-status
cargo run -p beaverki-cli -- connector discord show
cargo run -p beaverki-cli -- connector discord list-mappings
```

### The bot ignores a message in a server channel

The usual causes are:

- the channel ID is not in the allowlist
- the message does not start with the configured prefix and does not include a direct bot mention
- the user is not mapped to a BeaverKi household user
- the bot lacks permission to read or reply in that channel

### The bot was working before, then stopped after a token change

Rerun the connector configuration command with `--enable` so BeaverKi stores the new token, then restart the daemon.

### A request pauses for approval

That is expected for actions that cross the configured risk threshold. Inspect the pending approval in the CLI if needed:

```bash
cargo run -p beaverki-cli -- approval list
```

## Related Docs

- [README](../README.md)
- [CLI and Operations Guide](cli-operations.md)
- [Technical Spec](technical-spec.md)