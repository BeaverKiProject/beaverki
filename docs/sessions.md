# BeaverKi Sessions

## 1. Purpose

This document explains how conversation sessions work in BeaverKi today.

It is written for users and operators who want to understand:

- when BeaverKi treats a message as part of the same conversation
- when context is shared versus isolated
- what `/new` does
- how sessions differ from durable memory

## 2. What A Session Is

A session is BeaverKi's current conversation context.

It is the part of the system that answers questions like:

- "Does this message continue the previous topic?"
- "Should this room share context with other rooms?"
- "If I switch DM channels, should BeaverKi still remember the recent thread?"

Sessions are for short-term transcript continuity.

They are not the same thing as durable memory:

- session context is recent conversation history
- durable memory is long-lived facts BeaverKi should keep across conversations

Example:

- if you tell BeaverKi your preferred name, that may become durable memory
- if you ask BeaverKi to continue the task from two minutes ago, that usually comes from the session

## 3. Current Session Types

BeaverKi currently uses four session types.

### 3.1 CLI

CLI requests for the same BeaverKi user share one CLI session.

That means:

- a follow-up CLI command can continue the recent CLI conversation
- CLI continuity is separate from Discord continuity

### 3.2 Direct Messages

Direct-message sessions are tied to the mapped BeaverKi user, not to one raw DM channel ID.

That means:

- if the same mapped user sends direct messages through different DM entrypoints, BeaverKi keeps the same DM conversation continuity
- DM history is private to that user-level DM session

### 3.3 Group Rooms

Shared rooms such as Discord channels use a room session.

That means:

- the room has its own shared recent context
- that context is separate from a user's private DM history
- users in the same room can continue the same room conversation
- switching to another room does not bring the old room's recent transcript with it

### 3.4 Scheduled Runs

Scheduled automation runs start with a fresh session every time.

That means:

- one cron run does not inherit transcript continuity from an earlier cron run
- scheduled work may still use durable memory, but it does not continue a prior transcript by default

## 4. What BeaverKi Continues

When a new request arrives, BeaverKi first resolves the session and then looks at recent transcript history inside that session only.

In practice:

- CLI follow-ups continue recent CLI context for that user
- DMs continue recent DM context for that mapped user
- room messages continue recent room context for that room
- scheduled runs start fresh

This is intentionally different from the older behavior where recent tasks could be pulled from a wider user-level history.

## 5. What Sessions Do Not Change

Resetting or changing sessions does not delete:

- task history
- audit history
- durable semantic memory

So if you start a new conversation, BeaverKi may still know stable facts it was meant to remember, such as:

- your name
- long-term preferences
- durable household facts you were allowed to store

What gets cleared is the recent transcript continuity for that session.

## 6. Using `/new`

You can reset the current conversation session with:

```text
/new
```

Current behavior:

- `/new` is handled directly by the runtime
- it does not go through the normal agent workflow
- it clears the session's recent conversation continuity
- it does not forget durable memory
- it does not delete task or audit records

After `/new`, BeaverKi treats the next request in that session as a new conversation unless durable memory is still relevant.

## 7. Shared Rooms And Privacy Caps

Shared rooms can have their own visibility cap.

This means a room can be configured so its session only uses a narrower memory scope than a user might normally have elsewhere.

Example:

- a user may have access to household-level context in general
- a particular shared room can still be capped so BeaverKi only uses private-level context there

This lets shared spaces stay narrower than a user's broader permissions.

## 8. Session Management Today

BeaverKi now supports both manual and policy-driven session management.

Current behavior:

- BeaverKi creates or reuses the correct session automatically
- users can reset the current session with `/new`
- scheduled runs always start fresh
- the runtime can automatically reset or archive inactive sessions based on configured lifecycle policies
- lifecycle policies can match session kind and connector context
- operators can inspect sessions and policies from the CLI

Automatic lifecycle actions stay session-scoped:

- resetting or archiving a session clears recent transcript continuity
- task history and audit history stay intact
- durable memory is not deleted

Current operator controls include:

- `beaverki session list`
- `beaverki session show --session-id ...`
- `beaverki session reset --session-id ...`
- `beaverki session archive --session-id ...`
- `beaverki session policy list`
- `beaverki session policy set ...`

## 9. Practical Examples

### 9.1 CLI Follow-Up

You ask BeaverKi in the CLI:

```text
Summarize the notes in this folder.
```

Then you ask:

```text
Now turn that into a checklist.
```

BeaverKi treats the second command as part of the same recent CLI conversation.

### 9.2 DM Versus Room

You ask something in a shared channel, then open a DM and continue talking there.

Current behavior:

- the channel conversation stays in the room session
- the DM starts or continues the DM session
- recent room transcript does not automatically carry into the DM session

### 9.3 Resetting A Session

You have been talking through one topic for a while and want to start clean.

Send:

```text
/new
```

BeaverKi clears the current session's recent transcript continuity, but it still keeps durable memory and audit history.

## 10. Summary

The current BeaverKi session model is:

- recent transcript continuity is session-scoped
- CLI, DMs, shared rooms, and scheduled runs do not all share one global conversation
- `/new` resets the current session without deleting memory or history
- inactive sessions can be reset or archived automatically by runtime policy
- durable memory survives across sessions
- shared rooms can be capped to a narrower context than a user's broader entitlements
