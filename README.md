# zcode-session-mcp

Session management MCP server for [ZCode](https://github.com/zai-org/ZCode) — the Z.ai coding agent. Manage the ZCode client's local sessions (list / read / create / rename / pin / archive / delete), check liveness, and run new sessions or send prompts into existing ones, straight from any MCP client.

ZCode 会话管理 MCP server:在任何 MCP 客户端里直接管理 ZCode 客户端的本机会话(增删改查、置顶/归档、判活、注入提示词、新起会话)。

- **Zero dependencies.** Single-file `server.mjs`, pure Node.js (>= 22.5, built-in `node:sqlite`).
- **Source-aligned semantics.** Every write mirrors what the ZCode client itself does — derived from reading the [zai-org/ZCode](https://github.com/zai-org/ZCode) source (see "Semantics" below).
- **Windows-first.** Developed and tested on Windows against ZCode 0.15.x desktop + CLI.

## How it works

ZCode keeps session facts in `~/.zcode/cli/db/db.sqlite` (sessions, messages, parts, model selection, usage) and sidebar organization in `~/.zcode/v2/tasks-index.sqlite` (pinned / archived / deleted / metadata). This server reads both directly and writes with the same shapes the client uses, so changes show up in the desktop app (restart the frontend or reopen the workspace if the sidebar needs a refresh).

```
┌─────────────────┐   stdio/JSON-RPC    ┌──────────────────────┐   reads/writes   ┌────────────────────────────┐
│ any MCP client  │ ◄─────────────────► │ zcode-session-mcp    │ ───────────────► │ ~/.zcode/cli/db/db.sqlite  │  (facts)
└─────────────────┘                     └──────────────────────┘                  ├────────────────────────────┤
                                        └─ optional: zcode CLI headless ──►    │ ~/.zcode/v2/tasks-index…   │  (sidebar org)
                                        └─ optional: desktop scheduler  ──►    └────────────────────────────┘
```

## Install

Requirements: Node.js >= 22.5 (for `node:sqlite`) and an installed ZCode client with at least one session.

```bash
npm install -g zcode-session-mcp
```

Then register with your MCP client.

**ZCode client** (`~/.zcode/cli/config.json`):

```json
{
  "mcpServers": {
    "zcode-session-mcp": {
      "type": "stdio",
      "command": "node",
      "args": ["C:/path/to/global/node_modules/zcode-session-mcp/server.mjs"]
    }
  }
}
```

**Generic MCP client** (Claude Desktop, etc.):

```json
{
  "mcpServers": {
    "zcode-session-mcp": {
      "command": "npx",
      "args": ["-y", "zcode-session-mcp"]
    }
  }
}
```

## Tools

### Query（查）

| Tool | What it does |
| --- | --- |
| `session_list` | Recent sessions with message counts, liveness (active in last 10 min), sidebar state (pinned/archived/deleted), model, automation binding. Filters: keyword, directory, include subagents/archived/deleted. |
| `session_get` | One session in full: fields, message stats, saved model selection (the real source in `session_entry`), token usage, todos, goal heartbeat, child sessions, background agents, model-io/artifact files, sidebar state, running heuristic. |
| `session_messages` | Read messages (client-ordered by `sequence, time_created, id`). `compact` format = role + text + tool summaries; `raw` = original JSON. Tail pagination, role filter, optional reasoning. |
| `session_search` | Keyword search across session titles and/or message bodies with context snippets. |
| `session_status` | Liveness: background agents dir + child sessions + last-message window + sidebar status cross-check. |

### Create / Update / Delete（增删改）

| Tool | What it does |
| --- | --- |
| `session_create` | Create an (empty) session record exactly like the client's deferred persistence does: `proj_` project id derived from the directory, 60-char title, `permission.mode`, optional preset model. The sidebar row is ensured too. Run it later with `session_send`. |
| `session_rename` | Set a custom title (sticky: disables the client's LLM auto-titling), dual-written to both databases with `title_overridden`. |
| `session_pin` | Pin/unpin in the sidebar (`tasks-index.pinned`). |
| `session_archive` | Archive/unarchive, dual-written to `tasks-index.archived` **and** `session.time_archived` (the field that makes CLI `--resume` refuse an archived session, same as the client). |
| `session_delete` | **Soft delete by default** = sidebar tombstone (`tasks.deleted=1`), fully reversible, data kept — this is exactly the client's product-level "delete". `hard: true` + `confirm: true` permanently deletes rows (explicit cascade over every `session_id`-keyed table) plus the sidebar row, optionally artifacts/agents directories. Refuses sessions that look like they're running unless you confirm. |
| `session_model` | Change the session's saved model selection — writes the `runtime/model_selection` entry (the same source the desktop UI and resume use) plus the sidebar display column. |

### Run（运行）

| Tool | What it does |
| --- | --- |
| `session_run` | Start a **new** session with a prompt. Default `native: true` inserts a one-shot row into the desktop scheduler's automations table; the ZCode app (20s polling) picks it up and runs it natively — the session appears live in the sidebar. `native: false` runs the zcode CLI headless. Returns immediately with a `resultPath` you can read later. |
| `session_send` | Inject a prompt into an **existing** session: headless resume (WAL checkpoint → `--resume` → sessionId verification → sidebar visibility write-back) or the native channel (queues natively while the session is busy). `append` queues; `interrupt` refuses while busy. `modelRef` / `matchSessionModel` run it on a specific model (pure JS, no Python needed). |
| `plan_quota` | *Optional.* Zhipu/Z.ai coding-plan usage (5-hour & weekly windows). Requires `session.json` keys; returns guidance when unconfigured. |

## Semantics worth knowing

- **Deletion is soft by default.** The client itself never deletes `db.sqlite` rows; its "delete" is a `deleted=1` tombstone in the sidebar index. `session_delete` matches that by default; hard delete is opt-in, confirmed, and reports exactly what was removed.
- **Renaming is sticky.** `title_source` becomes `custom` and the client's automatic title generation is permanently short-circuited for that session (by design, same as renaming in the app).
- **Liveness is heuristic.** A message in the last 10 minutes is the only reliable "running" signal; `task_status` in the sidebar index is a frozen snapshot and may be stale after a crash.
- **Direct SQLite, best-effort coexistence.** Both databases are WAL-mode and shared with the running client; this server uses busy-timeouts and short transactions. Close writes are best-effort and never leave partial state (transactions + rollback).
- **Rewind/caveat.** `session_messages` does not replay rewind/compaction branch trimming; check `session_get.session.revert` before treating history as linear.

## Environment variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `ZCODE_SESSION_HOME` | `~/.zcode` | ZCode data home. |
| `ZCODE_SESSION_CONFIG` | `<server dir>/session.json` | Optional config (quota keys, providerMap). Falls back to legacy `fleet.json` from `zcode-fleet-mcp`. |
| `ZCODE_SESSION_CLI` | `E:\zcode\resources\glm\zcode.cjs` | zcode CLI entry (headless channel). |
| `ZCODE_SESSION_NODE` | `process.execPath` | Node executable for headless children (>= 22.5). |
| `ZCODE_SESSION_QUOTA_BASE` | Zhipu endpoint | Quota API base. |

## History

This is the renamed, expanded successor of `zcode-fleet-mcp` (a cross-session "fleet" tool). Session CRUD was rebuilt on top of a source-level study of the ZCode client's own session store; old `fleet_*` tool names keep working as aliases but are not advertised.

## Disclaimer

Unofficial, community tool. It manipulates the local ZCode databases directly — keep a backup of `~/.zcode` if you plan to use `hard` deletes. Not affiliated with Z.ai.

## License

MIT
