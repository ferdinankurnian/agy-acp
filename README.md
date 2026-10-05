# agy-acp

An [Agent Client Protocol (ACP)](https://agentclientprotocol.com) stdio adapter for [Google Antigravity CLI](https://github.com/google-antigravity/antigravity-cli) (`agy`). It bridges `agy` into any ACP-compatible host like [Zed](https://zed.dev), enabling you to use Gemini models through `agy` inside Zed's Agent Panel.

## Features

- **Real-Time Streaming**: Directly streams NDJSON events from `agy --output-format stream-json` to deliver fast, incremental text updates.
- **Thinking / Thought Streaming**: Streams model reasoning blocks as ACP thought updates, allowing compatible hosts to render the model's thought process in real time.
- **Rich Tool Execution**: Maps `agy` tool operations (`read`, `edit`, `delete`, `move`, `search`, `execute`, `fetch`, etc.) into structured ACP tool calls with target file paths, line ranges, and formatted outputs (such as directory listings and grep search results).
- **Session Cancellation**: Handles `session/cancel` by cleanly aborting in-flight prompts and terminating the underlying `agy` subprocess.
- **Dynamic Model Selection**: Automatically queries models via `agy models` on startup and exposes them as ACP configuration options. Supports both `session/set_model` and `session/setConfigOption`.
- **Session Persistence & Resume**: Saves conversation mappings to disk with atomic writes and file locking, allowing sessions to resume seamlessly across restarts.
- **Narration Filtering**: Provides a `--skip-naration` CLI flag to filter out leading narrative chatter (e.g., *"I will..."*) before model actions.

## How It Works

`agy-acp` speaks JSON-RPC over stdin/stdout (the ACP transport). When a host sends a prompt, `agy-acp` spawns `agy` in stream-json mode, streams the output incrementally back via `session/update` notifications, and binds the `conversation_id` so subsequent turns or resumed sessions retain context.

```
Zed (ACP host)  <--stdin/stdout JSON-RPC-->  agy-acp  <--subprocess-->  agy  <--API-->  Gemini
```

## Prerequisites

- **Rust** (1.70+) with Cargo
- **`agy`** installed and in your `PATH` — install from [google-antigravity/antigravity-cli releases](https://github.com/google-antigravity/antigravity-cli)
- **Authentication** — either set `GEMINI_API_KEY` or configure auth via `~/.gemini/antigravity-cli/settings.json`

## Build & Install

```bash
cargo build --release
```

The binary is generated at `target/release/agy-acp`. Copy it to a directory in your `PATH`:

```bash
cp target/release/agy-acp /usr/local/bin/
```

## Use with Zed

Add `agy-acp` as a custom agent server in your Zed settings (`~/.config/zed/settings.json`):

```json
{
  "agent_servers": {
    "agy": {
      "type": "custom",
      "command": "agy-acp",
      "args": [],
      "env": {
        "AGY_EXTRA_ARGS": "--dangerously-skip-permissions"
      }
    }
  }
}
```

> [!IMPORTANT]
> **Tool Execution & Permissions:** Antigravity CLI does not natively support the ACP protocol yet, meaning interactive permission prompts from `agy` cannot be answered through ACP hosts. You **must** set `AGY_EXTRA_ARGS="--dangerously-skip-permissions"` in your environment so `agy` auto-approves tool permission requests, enabling tools (file editing, command execution, searching, etc.) to run properly.

Then open the Agent Panel in Zed (`Cmd-?` on macOS, `Ctrl-?` on Linux), select **agy** from the agent dropdown, and start chatting.

### Filtering Narration

To suppress leading narrative chatter from the model, pass `--skip-naration` in the arguments:

```json
{
  "agent_servers": {
    "agy": {
      "type": "custom",
      "command": "agy-acp",
      "args": ["--skip-naration"],
      "env": {}
    }
  }
}
```

### Passing Extra Arguments

Set the `AGY_EXTRA_ARGS` environment variable to pass additional arguments to every `agy` invocation:

```json
{
  "agent_servers": {
    "agy": {
      "type": "custom",
      "command": "agy-acp",
      "args": [],
      "env": {
        "AGY_EXTRA_ARGS": "--some-flag value"
      }
    }
  }
}
```

## Configuration & Environment

| Setting / Variable | Description |
|---|---|
| `--skip-naration` | CLI flag to filter out leading narrative preamble messages |
| `GEMINI_API_KEY` | API key for Gemini (passed through to `agy`) |
| `AGY_EXTRA_ARGS` | Space-separated extra args passed to every `agy` invocation |

## Session Persistence

Sessions are persisted to `~/.openab/agy-acp/sessions.json`. When you resume a session in Zed, `agy-acp` restores the conversation binding and continues it with `agy --conversation <id>`. State persistence uses atomic write-to-temp-and-rename under an exclusive file lock to avoid data corruption.

## Debugging

To inspect the JSON-RPC messages between Zed and `agy-acp`, run `dev: open acp logs` from Zed's Command Palette.

## License

MIT


## Fork additions (effort-split + T3 installer)

This fork adds, on top of upstream:

- **Model/effort split**: `agy` publishes every effort level as a separate
  model (`Gemini 3.8 Flash (High|Medium|Low)`). The adapter now collapses those
  into one base model in the picker and exposes **Effort** as a separate select
  option (Low / Medium / High / … / Default), OpenCode-style. The chosen pair
  is reassembled into `--model "<variant>"` at spawn time. Legacy full-variant
  selections stored in `sessions.json` keep working.
- **`install.sh`**: builds the release binary and installs `agy-acp` plus the
  `agy-acp-t3` wrapper to `$PREFIX/bin` (default `~/.local/bin`):

  ```bash
  ./install.sh
  ```

### Use with T3 Code

T3 Code can drive any ACP Registry agent with a local executable override.
Settings → Providers → Add provider → Enter manually:

| Field | Value |
|---|---|
| Registry agent ID | `antigravity-acp` |
| Executable override | `/home/iydheko/.local/bin/agy-acp-t3` (or wherever you installed it) |
| Authentication | auto (uses your existing `agy` login) |

The wrapper strips T3's registry args (`--uid=` on Linux, which plain
`agy-acp` rejects), ensures `agy` is on `PATH`, and defaults
`AGY_EXTRA_ARGS="--dangerously-skip-permissions"` (required: interactive
permission prompts cannot be answered over ACP).

Note: this exists because Google's official `antigravity-acp` server binary is
compiled with AVX and SIGILLs on CPUs without it (e.g. Intel Celeron N4000).
This adapter shells out to the regular `agy` CLI instead, which runs fine
there.
