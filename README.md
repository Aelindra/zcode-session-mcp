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
                                        └─ run/send: desktop scheduler ──►     │ ~/.zcode/v2/tasks-index…   │  (sidebar org + automations)
                                                                                └────────────────────────────┘
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
| `session_run` | Start a **new** session with a prompt. Inserts a one-shot row into the desktop scheduler's automations table; the ZCode app (20s polling) picks it up and runs it natively — the session appears live in the sidebar. Returns immediately with a `resultPath` you can read later. |
| `session_send` | Inject a prompt into an **existing** session via the native channel (desktop `resumeTask`+`sendPrompt`; queues natively while the session is busy). `append` queues; `interrupt` refuses while busy. To run on a specific model, set it first with `session_model` (the native resume honors the session's saved model selection). |

### 额度面（Quota / plan）

| Tool | What it does |
| --- | --- |
| `plan_quota` | *Optional.* Zhipu/Z.ai coding-plan usage (5-hour & weekly windows) via `open.bigmodel.cn`, authenticated with an `apiKey` from `session.json` `keys`. Returns guidance when unconfigured. |
| `plan_start` | View the ZCode **start plan** account face (`zcode.z.ai`): active subscriptions, per-model balance buckets (total/used/remaining/expiry), and currently claimable plans. Credentials are decrypted from the desktop login store `~/.zcode/v2/credentials.json` (same `enc:v1` AES-256-GCM scheme as the client — nothing is sent anywhere except `zcode.z.ai`). |
| `plan_reset_card` | Coding-plan **reset cards**: `status` (inventory of 5-hour/weekly cards + usage history), `use` (consume one card to instantly reset that window — **confirm-gated**, `resetType=FIVE_HOUR\|WEEK`, idempotency key auto-generated), `opportunity` (trigger one card-grant chance; the desktop client already polls this itself — don't loop it), `history_read`. |
| `plan_claim` | Manual-claim plan face: `preview` lists currently claimable plans (safe to poll on a schedule); `claim` requires the Aliyun **captcha** verify param — the server enforces human verification, so fully-automatic claiming is impossible by design; the ZCode client's claim dialog is the easy path. |
| `plan_switch` | View/switch which `session.json` key alias is `current` (the one `plan_quota` queries by default). Local config only — does not touch ZCode client providers. |

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
| `ZCODE_SESSION_CONFIG` | `<server dir>/session.json` | Optional config (quota keys, providerMap). |
| `ZCODE_SESSION_NODE` | `process.execPath` | Node executable for background job children (>= 22.5). |
| `ZCODE_SESSION_QUOTA_BASE` | Zhipu endpoint | Quota API base. |
| `ZCODE_SESSION_PLAN_BASE` | `https://zcode.z.ai` | start plan / reset card API base. |
| `ZCODE_SESSION_APP_VERSION` | `3.14.4` | `app_version` sent to zcode.z.ai (server validates it — keep in sync with the client). |
| `ZCODE_CREDENTIAL_SECRET` | fallback string | Only if the desktop was launched with a custom credential secret. |

## History

This is the renamed, expanded successor of `zcode-fleet-mcp` (a cross-session "fleet" tool). Session CRUD was rebuilt on top of a source-level study of the ZCode client's own session store; the old `fleet_*` tool names and legacy config fallback were removed — use the `session_*` tools and `session.json`.

**v0.3.0** — the zcode CLI headless channel (`ZCODE_SESSION_CLI` / `native: false`) was removed entirely. `session_run` / `session_send` now go through the desktop scheduler's native channel only (real `createTask`/`resumeTask`+`sendPrompt`, live sidebar status, per-model injection via `session_model`).

## Disclaimer

Unofficial, community tool. It manipulates the local ZCode databases directly — keep a backup of `~/.zcode` if you plan to use `hard` deletes. Not affiliated with Z.ai.

## License

MIT
