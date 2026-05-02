# Packaging Templates

This directory holds release-time packaging assets that are copied into BeaverKi release archives.

Current assets:

- `systemd/`: user-service templates for Linux desktop or server installs
- `launchd/`: LaunchAgent templates for macOS installs
- `install.sh`: curlable installer for tagged release archives

Curlable install example:

```bash
curl -fsSL https://raw.githubusercontent.com/torlenor/beaverki/master/packaging/install.sh | bash -s -- --tag 2026-04-23.1
```

These templates are intentionally shipped with placeholder values so an operator or installer can substitute install-specific paths before enabling them.

Placeholders:

- `__BEAVERKI_INSTALL_DIR__`: unpacked BeaverKi application directory containing `bin/`
- `__BEAVERKI_CONFIG_DIR__`: BeaverKi config directory, usually `~/.config/beaverki` on Linux or `~/Library/Application Support/beaverki` on macOS

Recommended process:

1. Extract the release archive to an installation directory such as `~/.local/share/beaverki/app` or `/opt/beaverki`.
2. Substitute the placeholders in the relevant service template.
3. Create a `beaverki.env` file inside the BeaverKi config directory that exports `BEAVERKI_MASTER_PASSPHRASE` for daemon-managed startup.
4. Run `beaverki setup init` and choose either OpenAI or LM Studio as the model provider.
5. Enable the daemon service first.
6. Optionally enable the web UI service if the local web UI should start automatically.

Provider-specific setup notes:

- OpenAI setup requires an API key. You can export `OPENAI_API_KEY` before running `beaverki setup init`.
- LM Studio setup requires a locally running LM Studio instance with at least one loaded chat-capable model.
- After setup, `beaverki setup verify-provider` checks the active provider configuration.

The templates intentionally run:

- `beaverki-cli daemon serve`
- `beaverki-web --listen-addr 127.0.0.1:7676`

That matches BeaverKi's local-only deployment model.

Example `beaverki.env` content:

```bash
BEAVERKI_MASTER_PASSPHRASE=replace-with-your-passphrase
```
