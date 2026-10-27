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
| `session_export` | Export a session: `format=jsonl` structured verbatim (one message per line, raw `data` + `parts` JSON) or `format=txt` plain conversational (full text, tool previews, optional reasoning). `chunk` splits at ~100k estimated tokens (default on, message-atomic); `chunk=false` writes a single file. Output defaults to `<server dir>/exports/<sessionId>/<format>/` with a `manifest.json`; refuses a non-empty dir unless `overwrite` (which only clears this tool's own files). |
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
| `session_send` | Inject a prompt into an **existing** session via the native channel (desktop `resumeTask`+`sendPrompt`; queues natively while the session is busy). `mode=append` queues; `mode=interrupt` aborts-and-injects: queues the automation row first, then kills the workspace's app-server host (CPU-probe identified) to cut the running turn — the scheduler (main process, survives the kill) reopens the host in ≤20s and delivers the new prompt. Other in-flight sessions in the same workspace die with it unless `force: true`; if the caller lives in the same workspace its connection drops too — verify via `resultPath`. `modelRef` (`providerId/modelId` + optional `thoughtLevel`) pins the model **for this dispatch** on the automation row itself — race-free vs the desktop rewriting session-level selections; use it to switch keys/providers per injection. `session_model` remains the session-level persistent choice. |

### 额度面（Quota / plan）— **optional, disabled by default**（v0.6.0 起）

The five `plan_*` tools are **hidden from `tools/list` and rejected with `QUOTA_FACE_DISABLED` unless enabled** — a session binds to a `providerId`, not to an API key alias, so quota results can't be attributed to sessions anyway, and the credentials/captcha upkeep outweighs the value. Enable with env `ZCODE_SESSION_QUOTA_FACE=1` or `"quotaFace": true` in `session.json`.

| Tool | What it does |
| --- | --- |
| `plan_quota` | *Optional.* Zhipu/Z.ai coding-plan usage (5-hour & weekly windows) via `open.bigmodel.cn`, authenticated with an `apiKey` from `session.json` `keys`. Returns guidance when unconfigured. |
| `plan_start` | View the ZCode **start plan** account face (`zcode.z.ai`): active subscriptions, per-model balance buckets (total/used/remaining/expiry), and currently claimable plans. Credentials are decrypted from the desktop login store `~/.zcode/v2/credentials.json` (same `enc:v1` AES-256-GCM scheme as the client — nothing is sent anywhere except `zcode.z.ai`). |
| `plan_reset_card` | Coding-plan **reset cards** — **desktop login required** (reads `zcodejwttoken` from `~/.zcode/v2/credentials.json`; the account is bound to that login — `session.json` API keys can neither query this nor switch accounts). `status`: inventory with per-card `expire_at` + latest usage history; `use`: consume one card to instantly reset that window (**confirm-gated**, `resetType=FIVE_HOUR\|WEEK`, idempotency key auto-generated); `opportunity`: trigger one card-grant chance (the desktop client already polls this itself — don't loop it); `history_read`. |
| `plan_claim` | Manual-claim plan face: `preview` lists currently claimable plans (safe to poll on a schedule); `claim` requires the Aliyun **captcha** verify param — the server enforces human verification, so fully-automatic claiming is impossible by design; the ZCode client's claim dialog is the easy path. |
| `plan_switch` | View/switch which `session.json` key alias is `current` (the one `plan_quota` queries by default). Local config only — does not touch ZCode client providers. |

**Two auth domains in this section** — `plan_quota` / `plan_switch` talk to `open.bigmodel.cn` with plain API keys from `session.json` (multi-account friendly: one alias per account). `plan_start` / `plan_reset_card` / `plan_claim` talk to `zcode.z.ai` with the **desktop login's** JWTs (`zcodejwttoken` + bigmodel oauth token) and always follow whichever account is logged in on the desktop — API keys do not work there. To inspect another account's reset cards, log that account in on the desktop and query, or snapshot its decrypted `zcodejwttoken` for offline reuse (login JWTs carry no `exp` claim; on 401, log in again to refresh).

### Providers（模型 Provider 管理）

| Tool | What it does |
| --- | --- |
| `provider_list` | List model providers: `personal` = the desktop "管理模型" panel's source of truth (`~/.zcode/v2/provider_config.json`, custom providers, apiKey masked to last 4) plus the resolved `registry` (read-only). |
| `provider_add` | Add a custom provider (template path via `templateId` e.g. `zai-api` — just an `apiKey`; or custom path with `baseUrl`+`apiType`+`models`). Atomic write, original file backed up first. Route sessions onto it with `session_model {modelRef: "<providerId>/<modelId>"}`; takes effect on the next dispatched session (runtime providers arrive via provisioning at dispatch). |
| `provider_set` | Update a custom provider: rotate `apiKey`, edit `baseUrl`/`apiType`/`models`, rename, enable/disable. Backed up. |
| `provider_remove` | Remove a custom provider (`confirm: true` required; also cleans its model rules). In-flight sessions keep their dispatch-time snapshot. |

Schema-aligned with [zai-org/ZCode](https://github.com/zai-org/ZCode) `packages/provider/src/config/*.ts` (zod strict) and `packages/provider-node/src/provider-config-file-codec.ts` (schemaVersion 1). Caveat: if the desktop UI saves model settings afterwards it writes from its own in-memory state and may override external edits — re-apply via `provider_set` if needed.

Routing note for model refs (`session_send.modelRef` / `session_model`): custom providers route by their own id (`zai-api`, UUIDs); the logged-in plan faces route as `account:bigmodel-individual-coding-plan` / `account:zai-coding-plan`-style ids — the `builtin:*` registry form is rejected at dispatch with「模型选择不可用」. A per-dispatch `modelRef` on the automation row is authoritative (the host pins the first valid selection); session-level `session_model` writes can be overwritten by the desktop's own selection bookkeeping between turns.

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
| `ZCODE_SESSION_QUOTA_FACE` | *(unset = disabled)* | Set to `1` to expose the five `plan_*` quota tools (also enabled by `"quotaFace": true` in `session.json`). |
| `ZCODE_SESSION_APP_VERSION` | `3.14.4` | `app_version` sent to zcode.z.ai (server validates it — keep in sync with the client). |
| `ZCODE_CREDENTIAL_SECRET` | fallback string | Only if the desktop was launched with a custom credential secret. |

## History

This is the renamed, expanded successor of `zcode-fleet-mcp` (a cross-session "fleet" tool). Session CRUD was rebuilt on top of a source-level study of the ZCode client's own session store; the old `fleet_*` tool names and legacy config fallback were removed — use the `session_*` tools and `session.json`.

**v0.3.0** — the zcode CLI headless channel (`ZCODE_SESSION_CLI` / `native: false`) was removed entirely. `session_run` / `session_send` now go through the desktop scheduler's native channel only (real `createTask`/`resumeTask`+`sendPrompt`, live sidebar status, per-model injection via `session_model`).

**v0.4.0** — new `session_export` tool (structured raw JSONL / plain conversational txt, optional ~100k-token chunking). `plan_reset_card` now returns per-card `expire_at` in `status`, and its docs make the desktop-login requirement explicit (API keys cannot query or switch accounts).

**v0.5.1** — desktop 3.16.x compatibility (externally created sessions became invisible and dispatch was silently killed):

- `session_create` stamps `session.version` with the client's real core version (detected from the newest non-zero row in `db.sqlite`) instead of `0.0.0`.
- Task-index rows are now written fully-formed in the modern shape (`host_id='local'`, `root_path`, `cli_*` columns). The new desktop host validates `host_id==='local'` per `(workspace_key, task_id)` on every dispatch and rejects otherwise with `project.host-mismatch`; it only backfills `NULL` rows at host startup, so rows created while the app is running never become dispatchable without this.
- Workspace paths are canonicalized to native separators (Windows backslashes). The client's `resolveWorkspaceKey` does zero normalization — forward-slash paths landed under a different `workspace_key`, so the sidebar grouping and the dispatch router never matched.
- Session ids in both formats are accepted (`sess_*` legacy/MCP and `session_<hex>` native).
- One-shot dispatches keep their `next_run_at` inside the scheduler's 5-minute misfire window while unclaimed (15-minute keepalive for fire-and-forget jobs), and a scheduler misfire skip (`computer_asleep_or_app_not_running`) is now reported as `MISFIRE_SKIPPED` instead of surfacing as a generic claim timeout.

**v0.6.0** — the quota face (`plan_quota` / `plan_start` / `plan_reset_card` / `plan_claim` / `plan_switch`) is now **optional and disabled by default**: the five tools disappear from `tools/list` and calls return `QUOTA_FACE_DISABLED` unless enabled via `ZCODE_SESSION_QUOTA_FACE=1` or `session.json "quotaFace": true`. Rationale: sessions bind to a `providerId` (e.g. `account:bigmodel-start-plan/GLM-5.3-Flash`), not to a `keyAlias`, so quota numbers can't be attributed to any session — and key/captcha upkeep isn't worth an opt-in face that mostly duplicates what the desktop client already shows.

## Disclaimer

Unofficial, community tool. It manipulates the local ZCode databases directly — keep a backup of `~/.zcode` if you plan to use `hard` deletes. Not affiliated with Z.ai.

## License

MIT
