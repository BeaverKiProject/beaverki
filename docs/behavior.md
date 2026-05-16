# Behavior Layers

BeaverKi builds each agent system prompt from fixed runtime rules plus optional markdown behavior files. Behavior files are meant for personality, tone, style, warmth, humor, verbosity, and per-user interaction preferences.

Behavior markdown cannot override safety, policy, approval, tool-use, filesystem, memory, or permission rules. Those runtime rules always stay authoritative.

## File Locations

Files are read from the configured `workspace_root`.

Instance-wide behavior:

```text
<workspace_root>/behavior/SOUL.md
```

If `SOUL.md` is absent, BeaverKi falls back to:

```text
<workspace_root>/behavior/CHARACTER.md
```

Per-user behavior:

```text
<workspace_root>/behavior/users/<user_id>.md
```

The `<user_id>` is the BeaverKi user ID shown by `make user-list`, for example `user_alex` or `user_casey`.

## Layering Order

Prompt construction applies behavior in this deterministic order:

1. Core runtime instructions
2. Built-in BeaverKi default personality
3. Instance behavior markdown
4. Per-user behavior markdown

Missing optional files are ignored. Empty behavior files are ignored.

## Examples

Global household style in `behavior/SOUL.md`:

```md
# Household Style

Be warm, practical, and concise.
Use concrete next steps for household tasks.
Avoid being overly formal.
```

Per-user style in `behavior/users/user_casey.md`:

```md
# Casey Style

Use simpler language and shorter sentences.
Keep the tone gentle and a little playful.
Explain one step at a time when helping with chores or homework.
```
