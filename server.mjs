#!/usr/bin/env node
// zcode-session-mcp —— ZCode (zai-org/ZCode) 本机会话管理 MCP server (stdio, JSON-RPC 2.0)
//
// 纯 Node.js 单文件、零依赖；要求 node >= 22.5（node:sqlite；headless 子进程需 >= 22.13 更稳）。
// 能力对照 zcode 客户端源码（apps/zcode-cli）的会话管理面：
//   查: session_list / session_get / session_messages / session_search / session_status
//   增: session_create（落 session 行，复刻 ensureSessionPersisted 语义）/ session_run（跑一个新会话）
//   改: session_rename / session_pin / session_archive / session_model / session_send
//   删: session_delete（默认软删=tasks-index tombstone；hard=true 真删，FK/显式级联）
// 数据层（源码事实层/索引层，均为 WAL 多进程共享）:
//   ~/.zcode/cli/db/db.sqlite          session/message/part/session_entry/usage 等会话事实
//   ~/.zcode/v2/tasks-index.sqlite     侧栏组织态（pinned/archived/deleted/meta_json）+ automations
// 关键源码依据（写入/删除语义均对齐，勿凭感觉改）:
//   createSession SQL        apps/zcode-cli/packages/adapters/src/storage/session-store/repositories/sessions.ts:20
//   projectIdFromDirectory   apps/zcode-cli/packages/core/src/runtime/helpers/project.ts:17（proj_ + slugify(dir)[0..80]）
//   titleFromInput           同上 :4（压缩空白，<=60 字符否则 57+"..."）
//   rename=custom 粘性       apps/zcode-cli/packages/core/src/runtime/methods/session-title.ts:264
//   删除=软删 tombstone      packages/services/src/zcode-agent/zcodeTaskServiceAdapter.ts:2855（tasks.deleted=1）
//   模型选择真源             db.sqlite session_entry(runtime/model_selection)，resume 侧按条目选模型
// 可选配置 session.json（兼容旧 zcode-fleet-mcp/fleet.json）：智谱额度 keyAlias 与 providerMap。

import { DatabaseSync } from "node:sqlite";
import { spawn } from "node:child_process";
import { existsSync, readFileSync, writeFileSync, readdirSync, statSync, mkdirSync, rmSync } from "node:fs";
import { homedir } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import process from "node:process";

const VERSION = "0.1.0";
const DB_TASKS_TIMEOUT_MS = 5000;
// scheduler 每 20s 轮询 automations（packages/desktop/src/scheduler）；misfire 宽限 5 分钟，
// 插行的 next_run_at 必须 ≈ now，否则被判"错过窗口"直接跳过。
const SCHEDULER_POLL_MS = 20000;
const HOME = process.env.USERPROFILE || homedir();
const ZCODE_HOME = process.env.ZCODE_SESSION_HOME || join(HOME, ".zcode");
const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const DB_MAIN = join(ZCODE_HOME, "cli", "db", "db.sqlite");
const DB_TASKS = join(ZCODE_HOME, "v2", "tasks-index.sqlite");
const AGENTS_DIR = join(ZCODE_HOME, "cli", "agents");
const ROLLOUT_DIR = join(ZCODE_HOME, "cli", "rollout");
const ARTIFACTS_DIR = join(ZCODE_HOME, "cli", "artifacts");
const V2_CONFIG = join(ZCODE_HOME, "v2", "config.json");
const CONFIG_PATH = process.env.ZCODE_SESSION_CONFIG || join(SCRIPT_DIR, "session.json");
const CONFIG_LEGACY = join(SCRIPT_DIR, "fleet.json"); // 旧 zcode-fleet-mcp 同目录 fleet.json 兼容读取
const QUOTA_BASE = process.env.ZCODE_SESSION_QUOTA_BASE || "https://open.bigmodel.cn/api/monitor/usage";
const ZCODE_CJS = process.env.ZCODE_SESSION_CLI || "E:\\zcode\\resources\\glm\\zcode.cjs";
const NODE_EXE = process.env.ZCODE_SESSION_NODE || process.execPath;
const DEFAULT_CWD = join(ZCODE_HOME, "workspace");
const ACTIVE_WINDOW_MS = 10 * 60 * 1000; // 近10分钟有消息 = 此刻在飞（最可靠判活信号；task_status 可能是崩溃残留）

// ---------------------------------------------------------------- 基础设施

function log(...args) { process.stderr.write("[zcode-session-mcp] " + args.join(" ") + "\n"); }

function errText(e) { return (e && e.message) ? e.message : String(e); }

function sleepMs(ms) { Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms); }

function fetchWithTimeout(url, opts = {}, timeoutMs = 15000) {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(new Error(`timeout after ${timeoutMs}ms`)), timeoutMs);
  return fetch(url, { ...opts, signal: ac.signal }).finally(() => clearTimeout(t));
}

function openDbRo(path, retries = 2) {
  // 只读 URI 打开；桌面端持锁时 SQLITE_BUSY → 短暂重试
  let lastErr;
  for (let i = 0; i <= retries; i++) {
    try {
      const db = new DatabaseSync(`file:${path.replace(/\\/g, "/")}?mode=ro`);
      try { db.exec("PRAGMA busy_timeout=3000"); } catch { /* 老版本可能不支持 */ }
      return db;
    } catch (e) { lastErr = e; }
    if (i < retries) sleepMs(1500);
  }
  throw new Error(`sqlite 只读打开失败(${path}): ${errText(lastErr)}`);
}

function openDbRw(path, busyTimeoutMs = DB_TASKS_TIMEOUT_MS) {
  // 读写打开：写操作与桌面端（同为 WAL 多进程写者）靠 busy_timeout 排队；
  // foreign_keys=ON 对齐客户端打开方式（migration-runner 强制），DELETE session 才能级联。
  const db = new DatabaseSync(`file:${path.replace(/\\/g, "/")}`);
  db.exec(`PRAGMA busy_timeout=${busyTimeoutMs}`);
  try { db.exec("PRAGMA foreign_keys=ON"); } catch { /* 老版本忽略 */ }
  return db;
}

function okResult(obj) {
  return { content: [{ type: "text", text: JSON.stringify(obj, null, 2) }] };
}
function errResult(code, message, extra = {}) {
  return { content: [{ type: "text", text: JSON.stringify({ ok: false, error: { code, message }, ...extra }, null, 2) }], isError: true };
}

// ---------------------------------------------------------------- 会话 ID / 标题 / 项目（对齐源码 helpers/project.ts + shared.ts）

function slugify(value) {
  const slug = String(value || "").toLowerCase().replace(/[^a-z0-9._-]+/g, "-").replace(/^-+|-+$/g, "");
  return slug || "session";
}

function projectIdFromDirectory(directory) {
  return "proj_" + (slugify(directory).slice(0, 80) || "default");
}

function titleFromInput(input) {
  const compact = String(input || "").trim().replace(/\s+/g, " ");
  if (!compact) return "Untitled session";
  return compact.length <= 60 ? compact : `${compact.slice(0, 57)}...`;
}

function newSessionId() { return `sess_${randomUUID()}`; }

const SESSION_ID_RE = /^sess_[A-Za-z0-9._-]+$/;

// ---------------------------------------------------------------- session.json 配置（可选；兼容旧 fleet.json）

function loadConfig() {
  for (const p of [CONFIG_PATH, CONFIG_LEGACY]) {
    if (existsSync(p)) {
      try {
        return { config: JSON.parse(readFileSync(p, "utf8")), path: p, legacy: p === CONFIG_LEGACY };
      } catch (e) { log("配置解析失败:", p, errText(e)); }
    }
  }
  return { config: null, path: CONFIG_PATH, legacy: false };
}

function configTemplate() {
  return {
    version: 1,
    note: "zcode-session-mcp 可选配置。keys: 智谱 coding plan 额度查询凭据（alias -> {apiKey, plan, current}）；providerMap: tasks.model 的 provider 前缀 -> keyAlias。不需要额度查询可整文件删除。",
    keys: {},
    providerMap: {},
  };
}

function ensureConfig() {
  const loaded = loadConfig();
  if (loaded.config) return loaded;
  const cfg = configTemplate();
  try {
    writeFileSync(CONFIG_PATH, JSON.stringify(cfg, null, 2), "utf8");
    log("已生成模板配置", CONFIG_PATH);
  } catch (e) { log("写模板配置失败:", errText(e)); }
  return { config: cfg, path: CONFIG_PATH, legacy: false, created: true };
}

// ---------------------------------------------------------------- 模型选择（纯 JS，对齐桌面端真源 session_entry）

function providerRegistry() {
  // providerId -> [modelId...]，来自桌面端 v2/config.json 的 /provider/ 子树（与 zcode_continuer.py 同源）
  const reg = {};
  try {
    const cfg = JSON.parse(readFileSync(V2_CONFIG, "utf8"));
    for (const [pid, pcfg] of Object.entries(cfg.provider || {})) {
      const models = Object.keys((pcfg && pcfg.models) || {});
      if (models.length) reg[pid] = models;
    }
  } catch (e) { log("v2/config.json 读取失败(模型解析降级):", errText(e)); }
  return reg;
}

function matchProvider(pidPart, reg) {
  if (reg[pidPart]) return pidPart;
  const hits = Object.keys(reg).filter((p) => p.startsWith(pidPart));
  if (hits.length === 1) return hits[0];
  const sub = Object.keys(reg).filter((p) => p.includes(pidPart));
  if (sub.length === 1) return sub[0];
  return null;
}

function resolveModelRef(ref) {
  // "providerId/modelId"（provider 前缀/模型大小写不敏感）或裸 "modelId"（全注册表查唯一）
  if (!ref || typeof ref !== "string") return null;
  const reg = providerRegistry();
  const s = ref.trim();
  if (s.includes("/")) {
    const slashIdx = s.indexOf("/");
    const pidPart = s.slice(0, slashIdx);
    const mid = s.slice(slashIdx + 1);
    const pid = matchProvider(pidPart.trim(), reg) || pidPart.trim();
    if (reg[pid]) {
      const exact = reg[pid].find((m) => m.toLowerCase() === mid.trim().toLowerCase());
      if (exact) return { providerId: pid, modelId: exact };
    }
    return { providerId: pid, modelId: mid.trim() }; // 注册表缺失时放行（headless 可能动态注册）
  }
  for (const pid of Object.keys(reg).sort()) {
    const exact = reg[pid].find((m) => m.toLowerCase() === s.toLowerCase());
    if (exact) return { providerId: pid, modelId: exact };
  }
  return null;
}

function getSessionModelEntry(db, sessionId) {
  const row = db.prepare(
    "SELECT data FROM session_entry WHERE session_id = ? AND type = 'runtime/model_selection' ORDER BY time_updated DESC LIMIT 1"
  ).get(sessionId);
  if (!row) return null;
  try { return JSON.parse(row.data); } catch { return null; }
}

function setSessionModelEntry(db, sessionId, providerId, modelId, thoughtLevel) {
  // 形状对齐桌面端 runtime/model_selection 条目（zcode_continuer.set_session_model 同款）
  const now = Date.now();
  const payload = {
    modelId, providerId, thoughtLevel,
    modelSelection: { providerId, modelId, options: { reasoningLevel: thoughtLevel } },
  };
  db.prepare(
    "INSERT OR REPLACE INTO session_entry (id, session_id, type, time_created, time_updated, data) VALUES (?,?,?,?,?,?)"
  ).run(`${sessionId}:runtime-model-selection`, sessionId, "runtime/model_selection", now, now, JSON.stringify(payload));
}

// ---------------------------------------------------------------- tasks-index 组织态（对齐 zcodeTaskServiceAdapter 写法）

function taskRow(dbt, sessionId) {
  return dbt.prepare("SELECT * FROM tasks WHERE task_id = ?").get(sessionId) || null;
}

function ensureTaskRow(dbt, sessionId, { title, directory }) {
  // 无行先收编（headless/手工建的会话默认不在侧栏）——字段形状照抄桌面端现有行，前端渲染依赖 meta_json
  const nowMs = Date.now();
  const existing = taskRow(dbt, sessionId);
  if (existing) return existing;
  const last = dbt.prepare("SELECT provider, model FROM tasks WHERE provider IS NOT NULL ORDER BY updated_at DESC LIMIT 1").get() || {};
  const provider = last.provider || "glm";
  const model = last.model || null;
  const meta = JSON.stringify({ taskId: sessionId, traceId: randomUUID(), title: title || "", titleOverridden: false, workspacePath: directory, createdAt: nowMs, updatedAt: nowMs, mode: "build", model, thoughtLevel: "max", provider, status: "idle", target: null });
  dbt.prepare(`INSERT OR IGNORE INTO tasks (workspace_key, workspace_path, workspace_identity, task_id, title, task_status, provider, mode, model, created_at, updated_at, unread_at, pinned, archived, deleted, title_overridden, meta_json, searchable_text, cron_automation_id, last_unread_at, off_peak_task_id)
    VALUES (?, ?, NULL, ?, ?, 'idle', ?, 'build', ?, ?, ?, NULL, 0, 0, 0, 0, ?, ?, NULL, 0, NULL)`)
    .run(directory, directory, sessionId, title || "", provider, model, nowMs, nowMs, meta, title || "");
  return taskRow(dbt, sessionId);
}

function patchTaskMeta(dbt, sessionId, patch) {
  // meta_json 深度合并（桌面端把 mode/model/status 等放 meta 里，前端渲染读它）
  const row = taskRow(dbt, sessionId);
  if (!row) return null;
  let meta = {};
  if (row.meta_json) { try { meta = JSON.parse(row.meta_json); } catch { meta = {}; } }
  Object.assign(meta, patch, { taskId: meta.taskId || sessionId, updatedAt: Date.now() });
  const cols = { meta_json: JSON.stringify(meta), updated_at: Date.now() };
  if (patch.title !== undefined) { cols.title = patch.title; cols.searchable_text = patch.title; }
  if (patch.status !== undefined) cols.task_status = patch.status;
  const sets = Object.keys(cols).map((c) => `${c} = ?`).join(", ");
  dbt.prepare(`UPDATE tasks SET ${sets} WHERE task_id = ?`).run(...Object.values(cols), sessionId);
  return taskRow(dbt, sessionId);
}

// ---------------------------------------------------------------- session_list

function listSessions(args) {
  const limit = Math.min(Math.max(Number(args.limit) || 20, 1), 100);
  const includeSubagents = Boolean(args.includeSubagents);
  const includeArchived = args.includeArchived === undefined ? true : Boolean(args.includeArchived);
  const includeDeleted = Boolean(args.includeDeleted);
  const keyword = String(args.keyword || "").trim();
  const directory = String(args.directory || "").trim();

  if (!existsSync(DB_MAIN)) return errResult("DB_NOT_FOUND", `未找到 ${DB_MAIN}`);

  let db;
  try { db = openDbRo(DB_MAIN); } catch (e) { return errResult("DB_OPEN_FAILED", errText(e)); }
  try {
    const where = [];
    const params = [];
    if (!includeSubagents) where.push("s.task_type != 'subagent_child'");
    if (directory) { where.push("(s.directory LIKE ? OR s.directory LIKE ?)"); params.push(directory, directory.replace(/[\\/]$/, "") + "%"); }
    if (keyword) { where.push("s.title LIKE ?"); params.push(`%${keyword}%`); }
    const rows = db.prepare(`
      SELECT s.id AS session_id, s.title, s.directory, s.task_type, s.parent_id, s.project_id,
             s.title_source, s.time_created, s.time_updated, s.time_archived,
             COALESCE(a.msg_count, 0) AS msg_count, a.last_msg_at
      FROM session s
      LEFT JOIN (SELECT session_id, COUNT(*) AS msg_count, MAX(time_created) AS last_msg_at
                 FROM message GROUP BY session_id) a ON a.session_id = s.id
      ${where.length ? "WHERE " + where.join(" AND ") : ""}
      ORDER BY COALESCE(a.last_msg_at, s.time_updated) DESC LIMIT ?`).all(...params, limit);

    // 组织态/自动化绑定（tasks-index 只读；失败则降级为空）
    const taskMeta = new Map();
    const boundSet = new Set();
    try {
      const dbt = openDbRo(DB_TASKS);
      try {
        for (const r of dbt.prepare("SELECT task_id, task_status, model, mode, provider, pinned, archived, deleted FROM tasks").all())
          taskMeta.set(r.task_id, r);
        for (const r of dbt.prepare("SELECT session_id FROM automation_runs WHERE session_id IS NOT NULL").all()) boundSet.add(r.session_id);
        for (const r of dbt.prepare("SELECT session_id FROM off_peak_tasks WHERE session_id IS NOT NULL").all()) boundSet.add(r.session_id);
      } finally { dbt.close(); }
    } catch (e) { log("tasks-index 读取失败(组织态降级):", errText(e)); }

    const now = Date.now();
    const sessions = [];
    for (const r of rows) {
      const tm = taskMeta.get(r.session_id);
      const deleted = tm ? Boolean(tm.deleted) : false;
      const archived = tm ? Boolean(tm.archived) || Boolean(r.time_archived) : Boolean(r.time_archived);
      if (deleted && !includeDeleted) continue;
      if (archived && !includeArchived) continue;
      const lastAt = r.last_msg_at || r.time_updated;
      sessions.push({
        session_id: r.session_id,
        title: r.title,
        directory: r.directory,
        project_id: r.project_id,
        msg_count: r.msg_count,
        time_created: r.time_created ? new Date(r.time_created).toISOString() : null,
        time_updated: lastAt ? new Date(lastAt).toISOString() : null,
        active_now: Boolean(lastAt && now - lastAt < ACTIVE_WINDOW_MS),
        task_type: r.task_type,
        is_subagent: r.task_type === "subagent_child",
        parent_id: r.parent_id,
        title_source: r.title_source,
        pinned: tm ? Boolean(tm.pinned) : false,
        archived,
        deleted,
        task_status: tm ? tm.task_status : null,
        model: tm ? tm.model : null,
        automation_bound: boundSet.has(r.session_id),
      });
    }
    return okResult({
      ok: true, count: sessions.length, generatedAt: new Date().toISOString(),
      activeWindowMinutes: ACTIVE_WINDOW_MS / 60000,
      note: "active_now=近10分钟有新消息（最可靠判活）；task_status 是侧栏最后一次快照，可能是崩溃残留，仅供对照。deleted 行是软删 tombstone。",
      sessions,
    });
  } catch (e) {
    return errResult("QUERY_FAILED", errText(e));
  } finally { try { db.close(); } catch { /* noop */ } }
}

// ---------------------------------------------------------------- 会话存在性与基础读取

function getSessionRow(db, sessionId) {
  return db.prepare("SELECT * FROM session WHERE id = ?").get(sessionId) || null;
}

function requireSession(sessionId) {
  if (!sessionId || !SESSION_ID_RE.test(sessionId)) return { error: errResult("BAD_ARGS", `sessionId 必填且需匹配 sess_*（收到: ${sessionId}）`) };
  let db;
  try { db = openDbRo(DB_MAIN); } catch (e) { return { error: errResult("DB_OPEN_FAILED", errText(e)) }; }
  const row = getSessionRow(db, sessionId);
  db.close();
  if (!row) return { error: errResult("SESSION_NOT_FOUND", `db.sqlite 中不存在会话 ${sessionId}`) };
  return { row };
}

// ---------------------------------------------------------------- session_get

function sessionGet(args) {
  const need = requireSession(args.sessionId);
  if (need.error) return need.error;
  const sessionId = args.sessionId;
  const now = Date.now();

  let db;
  try { db = openDbRo(DB_MAIN); } catch (e) { return errResult("DB_OPEN_FAILED", errText(e)); }
  try {
    const s = getSessionRow(db, sessionId);
    const out = { ok: true, sessionId, checkedAt: new Date(now).toISOString() };

    out.session = {
      title: s.title, title_source: s.title_source,
      directory: s.directory, project_id: s.project_id,
      parent_id: s.parent_id, task_type: s.task_type, version: s.version,
      time_created: s.time_created ? new Date(s.time_created).toISOString() : null,
      time_updated: s.time_updated ? new Date(s.time_updated).toISOString() : null,
      time_archived: s.time_archived ? new Date(s.time_archived).toISOString() : null,
      permission: (() => { try { return JSON.parse(s.permission || "null"); } catch { return null; } })(),
      revert: s.revert ? "有 rewind 状态（读消息时注意分支裁剪）" : null,
    };

    // 消息统计 + 判活
    const agg = db.prepare("SELECT COUNT(*) AS c, MIN(time_created) AS first_at, MAX(time_created) AS last_at FROM message WHERE session_id = ?").get(sessionId);
    out.activity = {
      msg_count: agg.c,
      first_message_at: agg.first_at ? new Date(agg.first_at).toISOString() : null,
      last_message_at: agg.last_at ? new Date(agg.last_at).toISOString() : null,
      active_now: Boolean(agg.last_at && now - agg.last_at < ACTIVE_WINDOW_MS),
    };

    // 模型选择真源
    const sel = getSessionModelEntry(db, sessionId);
    out.model_selection = sel ? { providerId: sel.providerId, modelId: sel.modelId, thoughtLevel: sel.thoughtLevel || (sel.modelSelection && sel.modelSelection.options && sel.modelSelection.options.reasoningLevel) || null } : null;

    // 用量（表可能不存在/为空，全部降级）
    try {
      const tu = db.prepare("SELECT COUNT(*) AS turns, SUM(input_tokens) AS in_tok, SUM(output_tokens) AS out_tok, SUM(reasoning_tokens) as reason_tok FROM turn_usage WHERE session_id = ?").get(sessionId);
      if (tu && tu.turns) out.usage = { turns: tu.turns, input_tokens: tu.in_tok || 0, output_tokens: tu.out_tok || 0, reasoning_tokens: tu.reason_tok || 0 };
      const mu = db.prepare("SELECT model_id, COUNT(*) AS requests, SUM(computed_total_tokens) AS total_tokens FROM model_usage WHERE session_id = ? GROUP BY model_id ORDER BY requests DESC LIMIT 5").all(sessionId);
      if (mu.length) out.usage = Object.assign(out.usage || {}, { models: mu });
    } catch (e) { out.usageError = errText(e); }

    // todo 列表
    try {
      out.todos = db.prepare("SELECT content, status, priority, position FROM todo WHERE session_id = ? ORDER BY position").all(sessionId);
    } catch { out.todos = []; }

    // 运行目标心跳（session_target，goal-run 粒度）
    try {
      const t = db.prepare("SELECT status, objective, token_budget, tokens_used, active_run_started_at, active_run_last_seen_at FROM session_target WHERE session_id = ?").get(sessionId);
      if (t) {
        out.target = {
          status: t.status, objective: t.objective,
          token_budget: t.token_budget, tokens_used: t.tokens_used,
          active_run_last_seen_at: t.active_run_last_seen_at ? new Date(t.active_run_last_seen_at).toISOString() : null,
          heartbeat_active: Boolean(t.active_run_last_seen_at && now - t.active_run_last_seen_at < ACTIVE_WINDOW_MS),
        };
      }
    } catch { /* 无表则无 target */ }

    // 子会话
    out.childSessions = db.prepare(`
      SELECT s.id, s.title, s.task_type, COUNT(m.id) AS msg_count, MAX(m.time_created) AS last_msg_at
      FROM session s LEFT JOIN message m ON m.session_id = s.id
      WHERE s.parent_id = ? GROUP BY s.id ORDER BY s.time_created DESC`).all(sessionId)
      .map((r) => ({
        session_id: r.id, title: r.title, task_type: r.task_type, msg_count: r.msg_count,
        last_message_at: r.last_msg_at ? new Date(r.last_msg_at).toISOString() : null,
        running: Boolean(r.last_msg_at && now - r.last_msg_at < ACTIVE_WINDOW_MS),
      }));

    // 后台代理目录
    try {
      const adir = join(AGENTS_DIR, sessionId);
      if (existsSync(adir)) {
        out.agents = readdirSync(adir).filter((n) => n.startsWith("agent_")).map((name) => {
          let meta = {};
          try { meta = JSON.parse(readFileSync(join(adir, name, "metadata.json"), "utf8")); } catch { /* 缺 metadata */ }
          const files = {};
          for (const f of ["transcript.jsonl", "task.output", "output.txt"]) {
            const fp = join(adir, name, f);
            if (existsSync(fp)) { const st = statSync(fp); files[f] = { bytes: st.size, mtime: st.mtime.toISOString() }; }
          }
          return { agentId: meta.agentId || name, childSessionId: meta.childSessionId || null, description: meta.description || null, files };
        });
      } else out.agents = [];
    } catch (e) { out.agents = []; out.agentsError = errText(e); }

    // 文件面：model-io 轨迹 + artifacts
    const sanitized = String(sessionId).replace(/[^a-zA-Z0-9._-]+/g, "-").slice(0, 80);
    const rolloutPath = join(ROLLOUT_DIR, `model-io-${sanitized}.jsonl`);
    if (existsSync(rolloutPath)) {
      const st = statSync(rolloutPath);
      out.files = { modelIo: { path: rolloutPath, bytes: st.size, mtime: st.mtime.toISOString() } };
    } else out.files = { modelIo: null };
    out.files.artifactsDir = existsSync(join(ARTIFACTS_DIR, sessionId)) ? join(ARTIFACTS_DIR, sessionId) : null;
    out.files.agentsDir = existsSync(join(AGENTS_DIR, sessionId)) ? join(AGENTS_DIR, sessionId) : null;

    // 侧栏组织态
    try {
      const dbt = openDbRo(DB_TASKS);
      try {
        const t = taskRow(dbt, sessionId);
        if (t) out.taskIndex = { task_status: t.task_status, model: t.model, mode: t.mode, provider: t.provider, pinned: Boolean(t.pinned), archived: Boolean(t.archived), deleted: Boolean(t.deleted), title_overridden: Boolean(t.title_overridden), updated_at: new Date(t.updated_at).toISOString() };
      } finally { dbt.close(); }
    } catch (e) { out.taskIndexError = errText(e); }

    out.running_heuristic = Boolean(
      out.activity.active_now
      || (out.target && out.target.heartbeat_active)
      || out.childSessions.some((c) => c.running)
    );
    out.summary = `${out.activity.msg_count} 条消息，${out.running_heuristic ? "疑似在飞" : "空闲"}${out.taskIndex ? `，侧栏状态 ${out.taskIndex.task_status}` : ""}。`;
    return okResult(out);
  } catch (e) {
    return errResult("QUERY_FAILED", errText(e));
  } finally { try { db.close(); } catch { /* noop */ } }
}

// ---------------------------------------------------------------- session_messages

function compactParts(parts) {
  // parts: [{type:'text'|'reasoning'|'tool'|'step-start'|'step-finish', ...}]
  const out = { text: [], reasoning: [], tools: [] };
  for (const p of parts) {
    if (!p || typeof p !== "object") continue;
    if (p.type === "text" && p.text) out.text.push(p.text);
    else if (p.type === "reasoning" && p.text) out.reasoning.push(p.text);
    else if (p.type === "tool") {
      const st = p.state || {};
      out.tools.push({ name: p.tool || p.toolName || null, status: st.status || null, input: st.input ?? null, outputPreview: typeof st.output === "string" ? st.output.slice(0, 500) : null });
    }
  }
  return out;
}

function sessionMessages(args) {
  const need = requireSession(args.sessionId);
  if (need.error) return need.error;
  const sessionId = args.sessionId;
  const limit = Math.min(Math.max(Number(args.limit) || 20, 1), 200);
  const offset = Math.max(Number(args.offset) || 0, 0);
  const tail = args.tail === undefined ? true : Boolean(args.tail); // 默认取最新一段（按时间正序返回）
  const role = String(args.role || "").trim().toLowerCase();
  const format = String(args.format || "compact").toLowerCase();
  const includeReasoning = Boolean(args.includeReasoning);

  let db;
  try { db = openDbRo(DB_MAIN); } catch (e) { return errResult("DB_OPEN_FAILED", errText(e)); }
  try {
    // 排序对齐源码 messages()：(sequence, time_created, id)
    const total = db.prepare("SELECT COUNT(*) AS c FROM message WHERE session_id = ?").get(sessionId).c;
    const rows = db.prepare(`
      SELECT id, data, sequence, time_created FROM message
      WHERE session_id = ? ${role ? "AND json_extract(data,'$.role') = ?" : ""}
      ORDER BY sequence, time_created, id`).all(...(role ? [sessionId, role] : [sessionId]));
    const sliced = tail ? rows.slice(Math.max(0, rows.length - offset - limit), rows.length - offset) : rows.slice(offset, offset + limit);

    const partStmt = db.prepare("SELECT id, data FROM part WHERE message_id = ? ORDER BY sequence, time_created, id");
    const messages = sliced.map((r) => {
      let data = {};
      try { data = JSON.parse(r.data); } catch { /* 坏行保底 */ }
      const parts = partStmt.all(r.id).map((p) => { try { return JSON.parse(p.data); } catch { return null; } }).filter(Boolean);
      const base = {
        message_id: r.id, role: data.role || null,
        time_created: r.time_created ? new Date(r.time_created).toISOString() : null,
        sequence: r.sequence,
      };
      if (format === "raw") {
        return { ...base, data, parts };
      }
      const cp = compactParts(parts);
      const text = cp.text.join("\n");
      const out = { ...base, text: text || null, text_truncated: false };
      if (cp.text.length === 1 && cp.text[0].length > 8000) { out.text = cp.text[0].slice(0, 8000); out.text_truncated = true; }
      if (includeReasoning && cp.reasoning.length) out.reasoning = cp.reasoning.join("\n").slice(0, 4000);
      if (cp.tools.length) out.tools = cp.tools;
      if (data.modelId) out.modelId = data.modelId;
      if (data.tokens) out.tokens = data.tokens;
      if (data.error) out.error = data.error;
      return out;
    });
    return okResult({
      ok: true, sessionId, total, returned: messages.length,
      offset: tail ? Math.max(0, rows.length - offset - messages.length) : offset,
      format, note: "compact 只含 text/tool 概要与角色；format=raw 返回原始 data+parts（可能很大）。rewind/compact 的逻辑删除分支未裁剪，session.session.revert 有值时注意。",
      messages,
    });
  } catch (e) {
    return errResult("QUERY_FAILED", errText(e));
  } finally { try { db.close(); } catch { /* noop */ } }
}

// ---------------------------------------------------------------- session_search

function sessionSearch(args) {
  const keyword = String(args.keyword || "").trim();
  if (!keyword) return errResult("BAD_ARGS", "缺少必填参数 keyword");
  const limit = Math.min(Math.max(Number(args.limit) || 20, 1), 50);
  const scope = String(args.scope || "all").toLowerCase(); // title | messages | all

  let db;
  try { db = openDbRo(DB_MAIN); } catch (e) { return errResult("DB_OPEN_FAILED", errText(e)); }
  try {
    const hits = new Map(); // session_id -> {session, matches:[]}
    const addHit = (sid, match) => {
      if (!hits.has(sid)) {
        const s = getSessionRow(db, sid);
        if (!s) return;
        hits.set(sid, { session: s, matches: [] });
      }
      hits.get(sid).matches.push(match);
    };
    if (scope === "title" || scope === "all") {
      for (const r of db.prepare("SELECT id FROM session WHERE title LIKE ? LIMIT ?").all(`%${keyword}%`, limit)) {
        addHit(r.id, { where: "title" });
      }
    }
    if (scope === "messages" || scope === "all") {
      // LIKE 直查 message.data JSON 文本（大小写对 ASCII 不敏感；够用的粗搜）
      const rows = db.prepare(`
        SELECT session_id, id, time_created, substr(data, max(1, instr(data, ?) - 80), 240) AS ctx
        FROM message WHERE data LIKE ?
        ORDER BY time_created DESC LIMIT ?`).all(keyword, `%${keyword}%`, 200);
      for (const r of rows) {
        if (hits.size >= limit && !hits.has(r.session_id)) continue;
        addHit(r.session_id, { where: "message", message_id: r.id, time: r.time_created ? new Date(r.time_created).toISOString() : null, context: (r.ctx || "").replace(/\s+/g, " ").slice(0, 240) });
      }
    }
    const results = [...hits.values()].slice(0, limit).map((h) => ({
      session_id: h.session.id,
      title: h.session.title,
      directory: h.session.directory,
      task_type: h.session.task_type,
      time_updated: h.session.time_updated ? new Date(h.session.time_updated).toISOString() : null,
      matches: h.matches.slice(0, 5),
      match_count: h.matches.length,
    }));
    return okResult({ ok: true, keyword, scope, count: results.length, results, note: "命中按消息时间取最新 200 条采样，非全量精确计数；要读上下文用 session_messages。" });
  } catch (e) {
    return errResult("QUERY_FAILED", errText(e));
  } finally { try { db.close(); } catch { /* noop */ } }
}

// ---------------------------------------------------------------- session_create（增）

function sessionCreate(args) {
  const directory = String(args.directory || "").trim();
  if (!directory || !existsSync(directory)) return errResult("BAD_ARGS", `directory 必填且必须存在（收到: ${directory}）`);
  let sessionId = String(args.sessionId || "").trim() || newSessionId();
  if (!SESSION_ID_RE.test(sessionId)) return errResult("BAD_ARGS", `sessionId 需匹配 sess_*（收到: ${sessionId}）`);
  const title = args.title ? titleFromInput(args.title) : "Untitled session";
  const titleSource = args.title ? "custom" : "first_input";
  const taskType = String(args.taskType || "interactive");
  const mode = String(args.mode || "build");
  const version = String(args.version || "0.0.0"); // 本地老 schema version NOT NULL；0.0.0 = 未知，首跑由 runtime 覆写
  const now = Date.now();
  let modelInfo = null;
  const projectId = projectIdFromDirectory(directory);
  const slug = slugify(sessionId);

  if (existsSync(DB_MAIN) === false) return errResult("DB_NOT_FOUND", `未找到 ${DB_MAIN}`);
  let db;
  try { db = openDbRw(DB_MAIN); } catch (e) { return errResult("DB_OPEN_FAILED", errText(e)); }
  try {
    if (getSessionRow(db, sessionId)) return errResult("SESSION_EXISTS", `会话已存在: ${sessionId}`, { session_id: sessionId });
    db.exec("BEGIN IMMEDIATE");
    try {
      // INSERT 形状对齐 createSession（repositories/sessions.ts:20），version 留空（由首次运行时 runtime 补）
      db.prepare(`INSERT INTO session (
        id, project_id, workspace_id, parent_id, trace_id, task_type, slug, directory, path,
        title, title_source, title_message_id, version,
        share_url, summary_additions, summary_deletions, summary_files, summary_diffs,
        revert, permission, time_created, time_updated, time_title_updated,
        time_compacting, time_archived
      ) VALUES (?, ?, NULL, ?, NULL, ?, ?, ?, ?, ?, ?, NULL, ?,
        NULL, NULL, NULL, NULL, NULL,
        NULL, ?, ?, ?, ?, NULL, NULL)`)
        .run(sessionId, projectId, args.parentId || null, taskType, slug, directory, directory,
          title, titleSource, version, JSON.stringify({ mode }), now, now,
          (titleSource === "custom") ? now : null);
      // 可选：预设模型选择（resume 侧按条目选模型）
      if (args.modelRef) {
        const r = resolveModelRef(String(args.modelRef));
        if (!r) throw new Error(`无法解析模型引用 "${args.modelRef}"`);
        setSessionModelEntry(db, sessionId, r.providerId, r.modelId, String(args.thoughtLevel || "high"));
        modelInfo = { providerId: r.providerId, modelId: r.modelId, thoughtLevel: args.thoughtLevel || "high" };
      }
      db.exec("COMMIT");
    } catch (e) {
      try { db.exec("ROLLBACK"); } catch { /* noop */ }
      throw e;
    }

    // 侧栏收编（best-effort：失败不影响落库）
    let taskIndex = null;
    try {
      const dbt = openDbRw(DB_TASKS);
      try { taskIndex = ensureTaskRow(dbt, sessionId, { title, directory }) ? { ensured: true } : { ensured: false }; }
      finally { dbt.close(); }
    } catch (e) { taskIndex = { ensured: false, error: errText(e) }; }

    return okResult({
      ok: true, session_id: sessionId, title, project_id: projectId, directory,
      mode, model: modelInfo, taskIndex,
      limitations: [
        "这是空会话记录（无消息）：可用于组织/归档/改名/预选模型；要让它干活请用 session_send 注入首条提示词（headless resume 会补 runtime entry）。",
        "客户端侧栏若未立即出现，重启前端或打开该 workspace 即可（tasks-index 已收编）。",
      ],
      next: `session_send { sessionId: "${sessionId}", prompt: "..." }`,
    });
  } catch (e) {
    return errResult("CREATE_FAILED", errText(e));
  } finally { try { db.close(); } catch { /* noop */ } }
}

// ---------------------------------------------------------------- session_rename（改：custom 粘性标题，双写两库）

function sessionRename(args) {
  const need = requireSession(args.sessionId);
  if (need.error) return need.error;
  const title = titleFromInput(args.title === undefined ? "" : String(args.title));
  if (!args.title || !String(args.title).trim()) return errResult("BAD_ARGS", "缺少必填参数 title");
  const sessionId = args.sessionId;
  const now = Date.now();

  try {
    const db = openDbRw(DB_MAIN);
    try {
      // 对齐 setCustomSessionTitle：title + title_source='custom' + time_title_updated；time_updated 单调 max
      const info = db.prepare(`
        UPDATE session SET title = ?, title_source = 'custom', time_title_updated = ?,
          time_updated = MAX(time_updated, ?) WHERE id = ?`).run(title, now, now, sessionId);
      if (info.changes === 0) return errResult("SESSION_NOT_FOUND", `db.sqlite 中不存在会话 ${sessionId}`);
    } finally { db.close(); }
  } catch (e) { return errResult("RENAME_DB_FAILED", errText(e)); }

  let taskIndex = null;
  try {
    const dbt = openDbRw(DB_TASKS);
    try {
      const r = patchTaskMeta(dbt, sessionId, { title, titleOverridden: true });
      taskIndex = r ? { updated: true } : { updated: false, note: "tasks-index 无该行（未在侧栏），仅 db 已改" };
    } finally { dbt.close(); }
  } catch (e) { taskIndex = { updated: false, error: errText(e) }; }

  return okResult({
    ok: true, sessionId, title, taskIndex,
    note: "title_source 已置 custom（粘性）：客户端 LLM 自动命名被永久短路，只有再次 rename 能改。侧栏若未立即刷新，重启前端即可。",
  });
}

// ---------------------------------------------------------------- session_pin / session_archive（组织态）

function sessionPin(args) {
  const need = requireSession(args.sessionId);
  if (need.error) return need.error;
  const sessionId = args.sessionId;
  const pinned = args.pinned === undefined ? true : Boolean(args.pinned);
  try {
    const dbt = openDbRw(DB_TASKS);
    try {
      ensureTaskRow(dbt, sessionId, { title: need.row.title, directory: need.row.directory });
      const nowMs = Date.now();
      dbt.prepare("UPDATE tasks SET pinned = ?, updated_at = ? WHERE task_id = ?").run(pinned ? 1 : 0, nowMs, sessionId);
      patchTaskMeta(dbt, sessionId, { pinned });
    } finally { dbt.close(); }
  } catch (e) { return errResult("PIN_FAILED", errText(e)); }
  return okResult({ ok: true, sessionId, pinned, note: "pinned 是侧栏组织态（tasks-index），不改会话内容。" });
}

function sessionArchive(args) {
  const need = requireSession(args.sessionId);
  if (need.error) return need.error;
  const sessionId = args.sessionId;
  const archived = args.archived === undefined ? true : Boolean(args.archived);
  const now = Date.now();
  // 双写：tasks-index.archived（产品组织态） + session.time_archived（CLI resume 拒绝恢复已归档会话的字段）
  try {
    const db = openDbRw(DB_MAIN);
    try {
      db.prepare("UPDATE session SET time_archived = ?, time_updated = MAX(time_updated, ?) WHERE id = ?")
        .run(archived ? now : null, now, sessionId);
    } finally { db.close(); }
  } catch (e) { return errResult("ARCHIVE_DB_FAILED", errText(e)); }
  let taskIndex = null;
  try {
    const dbt = openDbRw(DB_TASKS);
    try {
      ensureTaskRow(dbt, sessionId, { title: need.row.title, directory: need.row.directory });
      dbt.prepare("UPDATE tasks SET archived = ?, updated_at = ? WHERE task_id = ?").run(archived ? 1 : 0, nowMsSafe(), sessionId);
      patchTaskMeta(dbt, sessionId, { archived });
      taskIndex = { updated: true };
    } finally { dbt.close(); }
  } catch (e) { taskIndex = { updated: false, error: errText(e) }; }
  return okResult({
    ok: true, sessionId, archived, taskIndex,
    note: archived
      ? "已归档：侧栏进归档区，且 CLI/注入式 resume 会被 time_archived 拒绝（对齐客户端行为）。取消归档用 archived=false。"
      : "已取消归档。",
  });
}

function nowMsSafe() { return Date.now(); }

// ---------------------------------------------------------------- session_delete（删：软删为默认，hard 需 confirm）

function sessionDelete(args) {
  const need = requireSession(args.sessionId);
  if (need.error) return need.error;
  const sessionId = args.sessionId;
  const hard = Boolean(args.hard);
  const cleanFiles = Boolean(args.files);

  // 防误删在飞会话：看最后一条消息时间（判活唯一可靠信号），不看 time_updated（新建空会话也会很新）
  {
    let lastMsgAt = 0, msgCount = 0;
    try {
      const dbr = openDbRo(DB_MAIN);
      try {
        const a = dbr.prepare("SELECT MAX(time_created) AS t, COUNT(*) AS c FROM message WHERE session_id = ?").get(sessionId);
        lastMsgAt = (a && a.t) || 0;
        msgCount = (a && a.c) || 0;
      } finally { dbr.close(); }
    } catch { /* 读失败不拦截删除路径 */ }
    if (msgCount > 0 && lastMsgAt && Date.now() - lastMsgAt < 2 * 60 * 1000 && !args.confirm) {
      return errResult("REFUSED_ACTIVE",
        `该会话 2 分钟内仍有新消息，疑似在飞。请先确认不在跑（session_status），再带 confirm=true 重试；软删（默认）只是侧栏隐藏，随时可逆。`);
    }
  }

  // 软删：tasks.deleted=1 tombstone（产品级删除语义，源码 deleteTask；db.sqlite 行保留）
  let taskIndex = null;
  try {
    const dbt = openDbRw(DB_TASKS);
    try {
      ensureTaskRow(dbt, sessionId, { title: need.row.title, directory: need.row.directory });
      dbt.prepare("UPDATE tasks SET deleted = 1, pinned = 0, updated_at = ? WHERE task_id = ?").run(Date.now(), sessionId);
      patchTaskMeta(dbt, sessionId, { status: "deleted", deleted: true });
      taskIndex = { tombstoned: true };
    } finally { dbt.close(); }
  } catch (e) { taskIndex = { tombstoned: false, error: errText(e) }; }

  if (!hard) {
    return okResult({
      ok: true, sessionId, hard: false, taskIndex,
      note: "软删完成：侧栏 tombstone（客户端冷启动也不会复活该行），db.sqlite 数据全部保留。真删需要 hard=true 且 confirm=true（不可逆，FK/显式级联清 message/part/entry/usage，可选连 artifacts/agents 文件）。",
    });
  }

  if (!args.confirm) {
    return errResult("HARD_DELETE_REQUIRES_CONFIRM",
      "hard=true 会永久删除会话数据（不可逆）。请确认后带 confirm=true 重新调用。",
      { sessionId, taskIndex });
  }

  // 硬删：显式清所有带 session_id 列的表（比依赖 FK 更确定），再删 session 行
  const deletedCounts = {};
  try {
    const db = openDbRw(DB_MAIN, 10000);
    try {
      const tables = db.prepare("SELECT name FROM sqlite_master WHERE type = 'table'").all().map((r) => r.name);
      db.exec("BEGIN IMMEDIATE");
      try {
        for (const t of tables) {
          let cols;
          try { cols = db.prepare(`PRAGMA table_info(${JSON.stringify(t).replace(/"/g, "")})`).all(); } catch { continue; }
          if (!cols.some((c) => c.name === "session_id")) continue;
          if (t === "session") continue;
          try {
            const r = db.prepare(`DELETE FROM ${JSON.stringify(t).replace(/"/g, "")} WHERE session_id = ?`).run(sessionId);
            if (r.changes > 0) deletedCounts[t] = r.changes;
          } catch { /* 个别表可能只读/无该列语义，跳过 */ }
        }
        const r = db.prepare("DELETE FROM session WHERE id = ?").run(sessionId);
        deletedCounts.session = r.changes;
        db.exec("COMMIT");
      } catch (e) {
        try { db.exec("ROLLBACK"); } catch { /* noop */ }
        throw e;
      }
    } finally { db.close(); }
  } catch (e) {
    return errResult("HARD_DELETE_FAILED", `db 硬删失败（tombstone 已打，数据仍在）: ${errText(e)}`, { taskIndex });
  }

  // 硬删后 tasks-index 行也删（软删才需要 tombstone；数据已不存在，无复活风险）
  let taskRowRemoved = false;
  try {
    const dbt = openDbRw(DB_TASKS);
    try {
      taskRowRemoved = dbt.prepare("DELETE FROM tasks WHERE task_id = ?").run(sessionId).changes > 0;
    } finally { dbt.close(); }
  } catch (e) { log("tasks 行清理失败:", errText(e)); }

  // 可选文件清理：artifacts / agents 目录（model-io rollout 由客户端自动轮转，不动）
  const filesRemoved = [];
  if (cleanFiles) {
    for (const dir of [join(ARTIFACTS_DIR, sessionId), join(AGENTS_DIR, sessionId)]) {
      try {
        if (existsSync(dir)) { rmSync(dir, { recursive: true, force: true }); filesRemoved.push(dir); }
      } catch (e) { log("清理目录失败:", dir, errText(e)); }
    }
  }

  return okResult({
    ok: true, sessionId, hard: true, confirm: true,
    deletedRows: deletedCounts, taskRowRemoved, filesRemoved,
    note: "已永久删除（db 行 + tasks-index 行，可选文件）。subagent 子会话是独立 session 行，如需一并删除请逐个调用。",
  });
}

// ---------------------------------------------------------------- session_status（判活/后台代理）

function sessionStatus(args) {
  const sessionId = args.sessionId;
  if (!sessionId) return errResult("BAD_ARGS", "缺少必填参数 sessionId（sess_*，可从 session_list 获取）");
  const now = Date.now();
  const out = { ok: true, sessionId, checkedAt: new Date(now).toISOString(), activeWindowMinutes: ACTIVE_WINDOW_MS / 60000 };

  try {
    const adir = join(AGENTS_DIR, sessionId);
    if (existsSync(adir)) {
      const agents = [];
      for (const name of readdirSync(adir)) {
        if (!name.startsWith("agent_")) continue;
        const a = join(adir, name);
        let meta = {};
        try { meta = JSON.parse(readFileSync(join(a, "metadata.json"), "utf8")); } catch { /* 缺 metadata */ }
        const files = {};
        for (const f of ["transcript.jsonl", "task.output", "output.txt"]) {
          const fp = join(a, f);
          if (existsSync(fp)) { const st = statSync(fp); files[f] = { bytes: st.size, mtime: st.mtime.toISOString() }; }
        }
        const mtimes = Object.values(files).map((f) => Date.parse(f.mtime)).filter(Boolean);
        const lastActivityAt = mtimes.length ? new Date(Math.max(...mtimes)).toISOString() : null;
        agents.push({ agentId: meta.agentId || name, childSessionId: meta.childSessionId || null, description: meta.description || null, lastActivityAt, running: Boolean(lastActivityAt && now - Date.parse(lastActivityAt) < ACTIVE_WINDOW_MS), files });
      }
      out.agents = agents;
      out.agentsRunning = agents.filter((a) => a.running).length;
    } else { out.agents = []; out.agentsRunning = 0; }
  } catch (e) { out.agents = []; out.agentsError = errText(e); }

  try {
    const db = openDbRo(DB_MAIN);
    try {
      out.childSessions = db.prepare(`
        SELECT s.id, MAX(m.time_created) AS last_msg_at, COUNT(m.id) AS msg_count
        FROM session s LEFT JOIN message m ON m.session_id = s.id
        WHERE s.parent_id = ? GROUP BY s.id`).all(sessionId)
        .map((r) => ({
          session_id: r.id, msg_count: r.msg_count,
          last_message_at: r.last_msg_at ? new Date(r.last_msg_at).toISOString() : null,
          running: Boolean(r.last_msg_at && now - r.last_msg_at < ACTIVE_WINDOW_MS),
        }));
      out.childSessionsRunning = out.childSessions.filter((c) => c.running).length;
      const own = db.prepare("SELECT MAX(time_created) AS t, COUNT(*) AS c FROM message WHERE session_id = ?").get(sessionId);
      out.ownLastMessageAt = own && own.t ? new Date(own.t).toISOString() : null;
      out.ownMessageCount = own ? own.c : 0;
      out.ownActive = Boolean(own && own.t && now - own.t < ACTIVE_WINDOW_MS);
    } finally { db.close(); }
  } catch (e) { out.dbError = errText(e); }

  try {
    const dbt = openDbRo(DB_TASKS);
    try {
      const t = dbt.prepare("SELECT task_status, model, title, updated_at FROM tasks WHERE task_id = ? AND deleted=0").get(sessionId);
      if (t) out.taskIndex = { task_status: t.task_status, model: t.model, title: t.title, updated_at: new Date(t.updated_at).toISOString() };
    } finally { dbt.close(); }
  } catch (e) { out.taskIndexError = errText(e); }

  out.summary = `后台代理 ${out.agentsRunning}/${out.agents.length} 在飞(agents目录), 子代理会话 ${out.childSessionsRunning}/${out.childSessions.length} 在飞(db), 会话本体${out.ownActive ? "活跃中" : "空闲"}${out.taskIndex ? `, tasks-index.status=${out.taskIndex.task_status}` : ""}`;
  return okResult(out);
}

// ---------------------------------------------------------------- 原生派发通道（经桌面 scheduler 的 automations 表）

function openTasksIndexRW() { return openDbRw(DB_TASKS, DB_TASKS_TIMEOUT_MS); }

function nativeAutomationInsert(args) {
  // args: { prompt, cwd, targetTaskId?, mode?, title? }
  const now = Date.now();
  const automationId = "automation-" + randomUUID();
  const prefix = args.targetTaskId ? "session send" : "session run";
  const title = (args.title || (prefix + ": " + (args.prompt || "").replace(/\s+/g, " ").slice(0, 60))).slice(0, 200);
  let db;
  try {
    db = openTasksIndexRW();
    const cnt = db.prepare("SELECT COUNT(*) AS c FROM automations").get();
    if (Number(cnt.c) >= 20) {
      return { error: { code: "AUTOMATION_LIMIT", message: "automations 已达 20 条上限（桌面端同款限制），请先在 自动化 页清理后再试" } };
    }
    db.prepare(`INSERT INTO automations (
        automation_id, title, cron_expr, prompt, model, provider, model_selection,
        workspace_key, workspace_path, workspace_identity, target_task_id, bot_delivery_target, location_kind,
        recurring, max_runs, end_at, schedule_rule, schedule_edited_by_user,
        run_count, enabled, lifecycle_status,
        next_run_at, last_run_at, running, claimed_at,
        dispatch_status, dispatch_attempts, retry_at, last_error,
        mode, thought_level, created_at, updated_at
      ) VALUES (
        ?, ?, '*', ?, NULL, NULL, 'null',
        ?, ?, NULL, ?, NULL, 'local',
        0, 1, NULL, NULL, 0,
        0, 1, 'active',
        ?, NULL, 0, NULL,
        'idle', 0, NULL, NULL,
        ?, NULL, ?, ?
      )`).run(
      automationId, title, args.prompt,
      args.cwd, args.cwd, args.targetTaskId || null,
      now - 200, // ≈now：必须落在 misfire 宽限（5 分钟）内
      args.mode || "yolo", now, now,
    );
  } catch (e) {
    return { error: { code: "AUTOMATION_INSERT_FAILED", message: errText(e) } };
  } finally { try { db?.close(); } catch { /* noop */ } }
  return { automationId, title, queuedAt: new Date(now).toISOString() };
}

async function nativeDispatchWait(automationId, args) {
  const claimTimeoutMs = Math.min(Math.max(Number(args.claimTimeoutMs) || 90000, 15000), 300000);
  const sessionTimeoutMs = Math.min(Math.max(Number(args.sessionTimeoutMs) || 120000, 15000), 600000);
  const startedAt = Date.now();
  const s3 = () => new Promise((r) => setTimeout(r, 3000));
  const readRow = () => {
    let db;
    try {
      db = openDbRo(DB_TASKS);
      const row = db.prepare("SELECT * FROM automations WHERE automation_id = ?").get(automationId);
      const run = db.prepare("SELECT * FROM automation_runs WHERE automation_id = ? ORDER BY created_at DESC LIMIT 1").get(automationId);
      return { row, run };
    } catch (e) {
      return { error: errText(e) };
    } finally { try { db?.close(); } catch { /* noop */ } }
  };

  let claimed = false, last = null;
  while (Date.now() - startedAt < claimTimeoutMs) {
    last = readRow();
    const r = last.row;
    if (last.error) return { ok: false, error: { code: "AUTOMATION_POLL_FAILED", message: last.error }, automationId };
    if (!r) return { ok: false, error: { code: "AUTOMATION_VANISHED", message: "插入的 automation 行消失了（可能被桌面端清理）" }, automationId };
    if (r.running === 1 || r.dispatch_status !== "idle" || r.last_error || r.retry_at) { claimed = true; break; }
    await s3();
  }
  if (!claimed) {
    return { ok: false, pending: true, automationId, dispatchStatus: last.row.dispatch_status,
      error: { code: "CLAIM_TIMEOUT", message: `等待 ${claimTimeoutMs}ms 调度器未认领（desktop scheduler 每 ${SCHEDULER_POLL_MS / 1000}s 轮询一次；若桌面端已退出则该一次性任务会在下次启动时按 misfire 跳过，不留尾巴）` } };
  }
  const row1 = last.row;
  if (row1.last_error && !row1.running) {
    return { ok: false, automationId, dispatchStatus: row1.dispatch_status, lastError: row1.last_error,
      error: { code: "DISPATCH_FAILED", message: "调度器认领后派发失败：" + row1.last_error } };
  }

  let runRow = null;
  while (Date.now() - startedAt < claimTimeoutMs + sessionTimeoutMs) {
    last = readRow();
    if (last.error) return { ok: false, error: { code: "AUTOMATION_POLL_FAILED", message: last.error }, automationId };
    if (last.run && last.run.session_id) { runRow = last.run; break; }
    if (last.row.last_error && last.row.dispatch_status !== "claimed" && !last.row.running) {
      return { ok: false, automationId, dispatchStatus: last.row.dispatch_status, lastError: last.row.last_error,
        error: { code: "DISPATCH_FAILED", message: "派发失败：" + last.row.last_error } };
    }
    await s3();
  }
  if (!runRow) {
    return { ok: false, pending: true, automationId, dispatchStatus: last.row.dispatch_status,
      error: { code: "SESSION_TIMEOUT", message: "已认领但未在时限内拿到会话 id（长派发？稍后用 session_list 复核，勿重发）" } };
  }

  let sessionTitle = null;
  try {
    const db = openDbRo(DB_MAIN);
    try {
      const s = db.prepare("SELECT title FROM session WHERE id = ?").get(runRow.session_id);
      sessionTitle = s ? s.title : null;
    } finally { db.close(); }
  } catch { /* best-effort */ }
  return {
    ok: true, native: true, automationId,
    runId: runRow.run_id, trigger: runRow.trigger,
    sessionId: runRow.session_id, title: sessionTitle,
    dispatchStatus: last.row.dispatch_status,
    durationMs: Date.now() - startedAt,
  };
}

async function nativeDispatch(args) {
  if (!args.prompt) return errResult("BAD_ARGS", "缺少必填参数 prompt");
  const cwd = args.cwd || DEFAULT_CWD;
  if (!existsSync(cwd)) return errResult("CWD_NOT_FOUND", `工作目录不存在: ${cwd}`);
  if (args.targetTaskId) {
    const need = requireSession(args.targetTaskId);
    if (need.error) return need.error;
  }
  const ins = nativeAutomationInsert({ ...args, cwd });
  if (ins.error) return errResult(ins.error.code, ins.error.message);
  const result = await nativeDispatchWait(ins.automationId, args);
  return okResult({
    ...result,
    jobId: ins.automationId,
    note: result.ok
      ? "原生派发成功：桌面调度器认领并经 workspace host 原生执行，侧栏已实时显示（task_created 原生事件）。"
      : "原生派发未完成；automation 行保留在 tasks-index（一次性任务不会被重复执行），可排查后重试。",
  });
}

// ---------------------------------------------------------------- 进程执行 / 后台作业

function runProcess(cmd, args, timeoutMs) {
  return new Promise((resolve) => {
    const child = spawn(cmd, args, { cwd: HOME, windowsHide: true, env: process.env });
    let stdout = "", stderr = "";
    let settled = false;
    const timer = setTimeout(() => {
      if (!settled) { try { child.kill(); } catch { /* noop */ } settled = true; resolve({ timedOut: true, code: null, stdout, stderr }); }
    }, timeoutMs);
    child.stdout.on("data", (d) => { stdout += d; if (stdout.length > 200000) stdout = stdout.slice(-100000); });
    child.stderr.on("data", (d) => { stderr += d; if (stderr.length > 100000) stderr = stderr.slice(-50000); });
    child.on("error", (e) => { if (!settled) { settled = true; clearTimeout(timer); resolve({ spawnError: errText(e), code: null, stdout, stderr }); } });
    child.on("close", (code) => { if (!settled) { settled = true; clearTimeout(timer); resolve({ timedOut: false, code, stdout, stderr }); } });
  });
}

function parseLastJson(text) {
  // zcode.cjs --json 的最终 result 是 pretty-print 多行 JSON：从后往前找"{"行，拼接到末尾再 parse
  const lines = text.trim().split(/\r?\n/);
  for (let i = lines.length - 1; i >= 0; i--) {
    const line = lines[i].trim();
    if (line === "{") {
      try { return JSON.parse(lines.slice(i).join("\n")); } catch { /* 继续向前找 */ }
    } else if (line.startsWith("{")) {
      try { return JSON.parse(line); } catch { /* 单行 JSON 继续 */ }
    }
  }
  return null;
}

const LOG_DIR = join(SCRIPT_DIR, "logs");

function newJobId() {
  return new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14) + Math.random().toString(36).slice(2, 6);
}

function launchBackground(kind, args) {
  try { mkdirSync(LOG_DIR, { recursive: true }); } catch { /* noop */ }
  const id = newJobId();
  const jobPath = join(LOG_DIR, id + ".job.json");
  const resultPath = join(LOG_DIR, id + ".result.json");
  writeFileSync(jobPath, JSON.stringify({ kind, args, createdAt: new Date().toISOString() }, null, 2), "utf8");
  const child = spawn(NODE_EXE, [fileURLToPath(import.meta.url), "--background-run", jobPath],
    { cwd: SCRIPT_DIR, detached: true, stdio: "ignore", windowsHide: true, env: process.env });
  child.unref();
  return { jobId: id, jobPath, resultPath, pid: child.pid || null };
}

async function bgRun(jobPath) {
  let job = null, result;
  try {
    job = JSON.parse(readFileSync(jobPath, "utf8"));
    result = job.kind === "send"
      ? await sessionSend({ ...job.args, wait: true })
      : await sessionRun({ ...job.args, wait: true });
  } catch (e) { result = errResult("BG_RUN_FAILED", errText(e)); }
  const resultPath = jobPath.replace(/\.job\.json$/, ".result.json");
  try { writeFileSync(resultPath, JSON.stringify(result, null, 2), "utf8"); } catch (e) { log("后台写结果失败:", errText(e)); }
  process.exit(0);
}

// ---------------------------------------------------------------- 客户端可见性回写

function touchTaskIndex(sessionId) {
  // headless 只写 db.sqlite 不写 tasks-index，且前端会从 message 表 error 重建任务状态，两处都要同步
  const nowMs = Date.now();
  const out = { messageErrorCleared: 0, taskIndexUpdated: false, taskIndexEnsured: false };
  let sessInfo = null;
  try {
    const db = openDbRw(DB_MAIN);
    try {
      sessInfo = db.prepare("SELECT directory, title FROM session WHERE id = ?").get(sessionId) || null;
      const rows = db.prepare(
        "SELECT id, data FROM message WHERE session_id = ? AND json_extract(data,'$.error') IS NOT NULL"
      ).all(sessionId);
      const upd = db.prepare("UPDATE message SET data = ? WHERE id = ?");
      for (const r of rows) {
        let d;
        try { d = JSON.parse(r.data); } catch { continue; }
        delete d.error;
        upd.run(JSON.stringify(d), r.id);
        out.messageErrorCleared++;
      }
    } finally { db.close(); }
  } catch (e) { out.messageErrorClearError = errText(e); }
  try {
    const dbt = openDbRw(DB_TASKS);
    try {
      ensureTaskRow(dbt, sessionId, { title: sessInfo && sessInfo.title, directory: sessInfo && sessInfo.directory });
      out.taskIndexEnsured = true;
      const row = dbt.prepare("SELECT meta_json FROM tasks WHERE task_id = ? AND deleted = 0").get(sessionId);
      if (row && row.meta_json) {
        try {
          const meta = JSON.parse(row.meta_json);
          meta.status = "idle";
          meta.updatedAt = nowMs;
          delete meta.lastError;
          dbt.prepare("UPDATE tasks SET updated_at=?, unread_at=?, task_status='idle', meta_json=? WHERE task_id=? AND deleted=0")
            .run(nowMs, nowMs, JSON.stringify(meta), sessionId);
        } catch { /* meta 坏行 */ }
      } else {
        dbt.prepare("UPDATE tasks SET updated_at=?, unread_at=?, task_status='idle' WHERE task_id=? AND deleted=0")
          .run(nowMs, nowMs, sessionId);
      }
      out.taskIndexUpdated = true;
    } finally { dbt.close(); }
  } catch (e) { out.taskIndexError = errText(e); }
  return out;
}

// ---------------------------------------------------------------- session_send（注入提示词）

async function sessionSend(args) {
  const sessionId = args.sessionId, prompt = args.prompt, mode = args.mode || "append";
  if (!sessionId || !prompt) return errResult("BAD_ARGS", "缺少必填参数 sessionId / prompt");
  if (!["append", "interrupt"].includes(mode)) return errResult("BAD_ARGS", "mode 只支持 append | interrupt");

  // 原生通道（native=true）：target_task_id=sessionId 的一次性自动化行 → 桌面调度器 → host resumeTask+sendPrompt
  if (Boolean(args.native)) {
    if (!existsSync(DB_TASKS)) return errResult("DB_NOT_FOUND", `未找到 ${DB_TASKS}`);
    let directory = null;
    try {
      const db = openDbRo(DB_MAIN);
      try {
        const s = db.prepare("SELECT id, directory FROM session WHERE id = ?").get(sessionId);
        if (!s) return errResult("SESSION_NOT_FOUND", `db.sqlite 中不存在会话 ${sessionId}`);
        if (!s.directory) return errResult("NO_CWD", `会话 ${sessionId} 缺少 directory，无法确定派发 workspace`);
        directory = s.directory;
      } finally { db.close(); }
    } catch (e) { return errResult("DB_OPEN_FAILED", errText(e)); }
    if (!existsSync(directory)) return errResult("CWD_NOT_FOUND", `会话工作目录不存在: ${directory}`);
    if (!Boolean(args.wait)) {
      const jb = launchBackground("send", args);
      return okResult({
        ok: true, accepted: true, async: true, native: true, mode, sessionId,
        jobId: jb.jobId, resultPath: jb.resultPath,
        note: "原生注入已受理（后台插 automations 行并等调度器认领派发）。复核：Read resultPath；侧栏该会话会实时显示运行状态。",
      });
    }
    const result = await nativeDispatch({ prompt, targetTaskId: sessionId, cwd: directory, claimTimeoutMs: args.claimTimeoutMs, sessionTimeoutMs: args.sessionTimeoutMs });
    if (result.content) {
      try {
        const body = JSON.parse(result.content[0].text);
        if (body.ok) body.directory = directory;
      } catch { /* noop */ }
    }
    return result;
  }

  if (!existsSync(ZCODE_CJS)) return errResult("ZCODE_CJS_NOT_FOUND", `未找到 zcode CLI (${ZCODE_CJS})。可用环境变量 ZCODE_SESSION_CLI 指定 zcode.cjs 路径，或改用 native=true 走桌面调度器（需 ZCode 桌面端在运行）。`);
  if (!existsSync(NODE_EXE)) return errResult("NODE_EXE_NOT_FOUND", `未找到 node >= 22.5 (${NODE_EXE})。可用 ZCODE_SESSION_NODE 指定。`);

  const need = requireSession(sessionId);
  if (need.error) return need.error;
  const session = need.row;
  if (!session.directory) return errResult("NO_CWD", `会话 ${sessionId} 缺少 directory，无法确定 --cwd`);
  if (!existsSync(session.directory)) return errResult("CWD_NOT_FOUND", `会话工作目录不存在: ${session.directory}`);

  const startedAt = Date.now();

  // mode=interrupt 语义：会话在飞 → 拒绝注入（避免插队/冲突）；空闲 → 等价 append
  if (mode === "interrupt") {
    const st = sessionStatus({ sessionId });
    let body = {};
    try { body = JSON.parse(st.content[0].text); } catch { /* ignore */ }
    if (body.ownActive || body.childSessionsRunning > 0 || body.agentsRunning > 0) {
      return errResult("REFUSED_ACTIVE",
        `会话仍在飞（ownActive=${body.ownActive}, childSessionsRunning=${body.childSessionsRunning}, agentsRunning=${body.agentsRunning}），interrupt 注入被拒绝。等其空闲后再试，或改用 mode=append 排队补指令。`);
    }
  }

  if (!Boolean(args.wait)) {
    const jb = launchBackground("send", args);
    return okResult({
      ok: true, accepted: true, async: true, mode, sessionId,
      jobId: jb.jobId, resultPath: jb.resultPath,
      note: "注入已受理并在后台执行，一次性返回不阻塞。复核：① Read resultPath（含 verified/stdoutTail）；② session_status 看消息数增长。结果文件生成前（可能数分钟）绝不重发注入，否则双跑。若目标会话在飞，后台会以 REFUSED_ACTIVE_PROMPT_EXISTS 拒绝，见结果文件。",
    });
  }

  // 可选：按指定模型注入（纯 JS：写 session_entry runtime/model_selection，resume 侧按条目选模型）
  const timeoutMs = Math.min(Math.max(Number(args.timeoutMs) || 300000, 30000), 3600000);
  let modelInfo = null;
  if (args.modelRef || args.matchSessionModel) {
    let modelFull = args.modelRef || null;
    if (!modelFull) {
      let sel = null;
      try {
        const db = openDbRo(DB_MAIN);
        try { sel = getSessionModelEntry(db, sessionId); } finally { db.close(); }
      } catch { /* ignore */ }
      const pid = sel && sel.providerId, mid = sel && sel.modelId;
      if (!pid || !mid) return errResult("SESSION_MODEL_UNKNOWN", "会话未保存模型选择，matchSessionModel 无法执行");
      modelFull = `${pid}/${mid}`;
    }
    const r = resolveModelRef(String(modelFull));
    if (!r) return errResult("MODEL_REF_UNRESOLVED", `无法解析模型引用 "${modelFull}"`);
    try {
      const db = openDbRw(DB_MAIN);
      try {
        setSessionModelEntry(db, sessionId, r.providerId, r.modelId, "max");
      } finally { db.close(); }
    } catch (e) { return errResult("SESSION_MODEL_SET_FAILED", `会话模型条目写入失败（目标 ${r.providerId}/${r.modelId}）: ${errText(e)}`); }
    modelInfo = { requested: modelFull, providerId: r.providerId, modelId: r.modelId, thoughtLevel: "max" };
  }

  // WAL checkpoint（缩小 headless 与桌面端的 WAL 并发冲突面；best-effort：桌面持锁时跳过）
  let walCheckpoint = "skipped";
  try {
    const dbw = openDbRw(DB_MAIN);
    try { dbw.exec("PRAGMA wal_checkpoint(TRUNCATE)"); walCheckpoint = "ok"; } finally { dbw.close(); }
  } catch (e) { walCheckpoint = "failed: " + errText(e); }

  const res = await runProcess(NODE_EXE, [ZCODE_CJS, "-p", prompt, "--resume", sessionId, "--cwd", session.directory, "--mode", "yolo", "--json"], timeoutMs);
  const tail = (s) => (s || "").slice(-2000);

  if (res.timedOut) {
    return errResult("SEND_TIMEOUT", `注入超时(${timeoutMs}ms)被终止。会话可能已收到指令，请用 session_status 复核。`, { stdoutTail: tail(res.stdout), model: modelInfo });
  }
  if (res.spawnError) return errResult("SPAWN_FAILED", res.spawnError);

  if (/active prompt exists/i.test(res.stdout + res.stderr)) {
    return errResult("REFUSED_ACTIVE_PROMPT_EXISTS",
      "桌面端正在使用该会话（active prompt exists），放弃注入以免打断用户。",
      { stdoutTail: tail(res.stdout) });
  }
  if (/unable to open database/i.test(res.stderr)) {
    return errResult("DB_BUSY", "headless 启动撞上 WAL 锁（unable to open database）。等10秒后重试一次。", { stderrTail: tail(res.stderr) });
  }
  if (res.code !== 0) {
    return errResult("ZCODE_EXIT_NONZERO", `zcode CLI 退出码 ${res.code}`, { stdoutTail: tail(res.stdout), stderrTail: tail(res.stderr) });
  }

  // 校验 resume 生效：末行 JSON sessionId 必须 == 目标
  const lastJson = parseLastJson(res.stdout);
  const resumedSid = lastJson && lastJson.sessionId;
  const verified = resumedSid === sessionId;
  const visibilitySync = verified ? touchTaskIndex(sessionId) : null;
  return okResult({
    ok: verified,
    mode,
    sessionId,
    resumedSessionId: resumedSid || null,
    verified,
    walCheckpoint,
    model: modelInfo,
    visibilitySync,
    durationMs: Date.now() - startedAt,
    stdoutTail: tail(res.stdout),
    stderrTail: tail(res.stderr),
    note: verified
      ? "resume 校验通过，客户端可见性已回写（未读/状态复位 + 清历史 error 标记）。"
      : (resumedSid ? `判定失败：实际新建了会话 ${resumedSid} 而非续跑，不要当成功。` : "未能从 stdout 解析 sessionId JSON。"),
  });
}

// ---------------------------------------------------------------- session_run（新起会话）

async function sessionRun(args) {
  const prompt = args.prompt;
  if (!prompt) return errResult("BAD_ARGS", "缺少必填参数 prompt");

  const native = args.native === undefined ? true : Boolean(args.native);
  if (native) {
    if (!Boolean(args.wait)) {
      const jb = launchBackground("run", args);
      return okResult({
        ok: true, accepted: true, async: true, native: true,
        jobId: jb.jobId, resultPath: jb.resultPath,
        note: "原生 spawn 已受理（后台插 automations 行并等调度器认领）。复核：Read resultPath 拿 sessionId（一般 ≤20s 认领 + 数秒建会话）；桌面侧栏会实时出现该会话并显示运行状态。结果文件生成前绝不重发。",
      });
    }
    return nativeDispatch({ prompt, cwd: args.cwd, mode: "yolo", claimTimeoutMs: args.claimTimeoutMs, sessionTimeoutMs: args.sessionTimeoutMs });
  }

  if (!existsSync(ZCODE_CJS)) return errResult("ZCODE_CJS_NOT_FOUND", `未找到 zcode CLI (${ZCODE_CJS})。可用环境变量 ZCODE_SESSION_CLI 指定，或改用 native=true（需 ZCode 桌面端在运行）。`);
  if (!existsSync(NODE_EXE)) return errResult("NODE_EXE_NOT_FOUND", `未找到 node >= 22.5 (${NODE_EXE})。可用 ZCODE_SESSION_NODE 指定。`);

  const cwd = args.cwd || DEFAULT_CWD;
  if (!existsSync(cwd)) return errResult("CWD_NOT_FOUND", `工作目录不存在: ${cwd}`);

  if (!Boolean(args.wait)) {
    const jb = launchBackground("run", args);
    return okResult({
      ok: true, accepted: true, async: true, cwd,
      jobId: jb.jobId, resultPath: jb.resultPath,
      note: "新会话已在后台创建执行，一次性返回不阻塞。拿 sessionId：① Read resultPath（content[0].text 的 sessionId 字段）；② 稍后 session_list 按 title/directory 查。结果文件生成前绝不重发，否则重复建会话。",
    });
  }

  const timeoutMs = Math.min(Math.max(Number(args.timeoutMs) || 600000, 30000), 1800000);
  const startedAt = Date.now();
  const res = await runProcess(NODE_EXE, [ZCODE_CJS, "-p", prompt, "--cwd", cwd, "--mode", "yolo", "--json", "--surface", "terminal"], timeoutMs);
  const tail = (s) => (s || "").slice(-2000);

  if (res.timedOut) return errResult("RUN_TIMEOUT", `新会话运行超时(${timeoutMs}ms)被终止（会话可能已持久化，可用 session_list 查找）。`, { stdoutTail: tail(res.stdout) });
  if (res.spawnError) return errResult("SPAWN_FAILED", res.spawnError);
  if (res.code !== 0) return errResult("ZCODE_EXIT_NONZERO", `zcode CLI 退出码 ${res.code}`, { stdoutTail: tail(res.stdout), stderrTail: tail(res.stderr) });

  const lastJson = parseLastJson(res.stdout);
  const sid = (lastJson && lastJson.sessionId) || null;
  const visibilitySync = sid ? touchTaskIndex(sid) : null;
  return okResult({
    ok: true,
    sessionId: sid,
    cwd,
    durationMs: Date.now() - startedAt,
    result: lastJson || null,
    stdoutTail: tail(res.stdout),
    visibilitySync,
    note: "新会话已回写客户端任务列表（前端侧栏可见）；若仍未显示，重启前端或把该会话在桌面端打开一次。",
  });
}

// ---------------------------------------------------------------- session_model（改模型选择，纯 JS）

function sessionModelSet(args) {
  const sessionId = args.sessionId, modelRef = args.modelRef;
  const thoughtLevel = String(args.thoughtLevel || "high");
  if (!sessionId || !modelRef) return errResult("BAD_ARGS", "缺少必填参数 sessionId / modelRef");
  const need = requireSession(sessionId);
  if (need.error) return need.error;

  const r = resolveModelRef(String(modelRef));
  if (!r) return errResult("MODEL_REF_UNRESOLVED", `无法解析模型引用 "${modelRef}"（支持 providerId/modelId 全称或模型 id 片段；注册表来自 ~/.zcode/v2/config.json）`);

  try {
    const db = openDbRw(DB_MAIN);
    try {
      setSessionModelEntry(db, sessionId, r.providerId, r.modelId, thoughtLevel);
    } finally { db.close(); }
  } catch (e) { return errResult("SESSION_MODEL_SET_FAILED", errText(e)); }

  // 侧栏展示列（best-effort）
  let taskIndex = null;
  try {
    const dbt = openDbRw(DB_TASKS);
    try {
      const full = `${r.providerId}/${r.modelId}`;
      dbt.prepare("UPDATE tasks SET model = ?, updated_at = ? WHERE task_id = ?").run(full, Date.now(), sessionId);
      patchTaskMeta(dbt, sessionId, { model: full });
      taskIndex = { updated: true };
    } finally { dbt.close(); }
  } catch (e) { taskIndex = { updated: false, error: errText(e) }; }

  return okResult({
    ok: true, sessionId, providerId: r.providerId, modelId: r.modelId, thoughtLevel, taskIndex,
    note: "已写 runtime/model_selection（真源）+ tasks-index.model（侧栏展示）：桌面端 UI 与后续人工续跑生效；headless resume 不读它——按模型注入请用 session_send 的 modelRef。",
  });
}

// ---------------------------------------------------------------- plan_quota（可选：智谱 coding plan 额度）

function parseQuota(json) {
  const inner = (json && json.data) || {};
  const out = { level: inner.level || "?", fiveHour: null, weekly: null, toolsLimit: null };
  for (const lim of inner.limits || []) {
    if (lim.type === "TOKENS_LIMIT" && lim.unit === 3) out.fiveHour = { percentage: lim.percentage, resetAt: lim.nextResetTime ? new Date(lim.nextResetTime).toISOString() : null };
    else if (lim.type === "TOKENS_LIMIT" && lim.unit === 6) out.weekly = { percentage: lim.percentage, resetAt: lim.nextResetTime ? new Date(lim.nextResetTime).toISOString() : null };
    else if (lim.type === "TIME_LIMIT") out.toolsLimit = { percentage: lim.percentage, remaining: lim.remaining, resetAt: lim.nextResetTime ? new Date(lim.nextResetTime).toISOString() : null };
  }
  return out;
}

async function planQuota(args) {
  const { config, path, created } = ensureConfig();
  if (created) {
    return errResult("CONFIG_TEMPLATE_CREATED",
      `首次运行：已在 ${path} 生成模板。若需要智谱 coding plan 额度查询，把 apiKey 填入 keys 后重试；不需要可直接删除该文件。`);
  }
  const alias = args.keyAlias;
  const keys = (config && config.keys) || {};
  if (!alias) {
    const current = Object.keys(keys).find((k) => keys[k] && keys[k].current) || Object.keys(keys)[0];
    if (!current) return errResult("KEY_NOT_CONFIGURED", `配置文件 ${path} 中没有任何 keys（该工具仅用于智谱 coding plan 额度查询，可忽略此工具）`);
    return planQuota({ keyAlias: current });
  }
  const entry = keys[alias];
  if (!entry) return errResult("KEY_NOT_CONFIGURED", `配置中不存在 keyAlias "${alias}"。可用别名: ${Object.keys(keys).join(", ") || "(无)"}`);
  if (!entry.apiKey) return errResult("KEY_NOT_CONFIGURED", `keyAlias "${alias}" 的 apiKey 未填写。编辑 ${path} 后重试。`);

  try {
    const res = await fetchWithTimeout(QUOTA_BASE + "/quota/limit", {
      headers: { "Authorization": entry.apiKey, "Content-Type": "application/json", "Accept-Language": "en-US,en" },
    }, 15000);
    const body = await res.json();
    if (!res.ok || body.success === false) {
      return errResult("QUOTA_API_ERROR", `额度接口返回异常: HTTP ${res.status} ${body.msg || ""}`);
    }
    const parsed = parseQuota(body);
    return okResult({
      ok: true, keyAlias: alias, plan: parsed.level, planMatchesConfig: parsed.level === entry.plan || undefined,
      fiveHour: parsed.fiveHour, weekly: parsed.weekly, toolsLimit: parsed.toolsLimit,
      fetchedAt: new Date().toISOString(), source: QUOTA_BASE + "/quota/limit",
    });
  } catch (e) {
    return errResult("QUOTA_FETCH_FAILED", `额度查询失败: ${errText(e)}`);
  }
}

// ---------------------------------------------------------------- MCP stdio 层

const TOOLS = [
  {
    name: "session_list",
    description: "列出本机 ZCode 会话（读 cli/db/db.sqlite + v2/tasks-index.sqlite）：标题/目录/消息数/时间/在飞判定/置顶归档删除组织态/模型/定时任务绑定。支持 keyword 标题过滤、directory 前缀过滤、includeSubagents/includeArchived/includeDeleted。List local ZCode sessions with org state.",
    inputSchema: {
      type: "object",
      properties: {
        limit: { type: "number", description: "返回条数，默认20，上限100" },
        keyword: { type: "string", description: "标题关键词过滤" },
        directory: { type: "string", description: "工作目录（前缀匹配）" },
        includeSubagents: { type: "boolean", description: "包含子代理会话(sess_subagent_*)，默认 false" },
        includeArchived: { type: "boolean", description: "包含已归档，默认 true" },
        includeDeleted: { type: "boolean", description: "包含已软删 tombstone，默认 false" },
      },
    },
  },
  {
    name: "session_get",
    description: "会话详情：核心字段、消息统计、模型选择真源(session_entry)、token 用量(turn/model_usage)、todo、运行目标心跳、子会话、后台代理、model-io/artifacts 文件、侧栏组织态、在飞启发式。Full detail of one session.",
    inputSchema: {
      type: "object",
      properties: { sessionId: { type: "string", description: "sess_* 会话 id" } },
      required: ["sessionId"],
    },
  },
  {
    name: "session_messages",
    description: "读会话消息（message+part 表，排序对齐客户端 sequence/time/id）：compact 格式=角色+正文+工具概要，raw=原始 JSON。默认取最新 tail 段；可按 role 过滤、includeReasoning 带思考。Read messages of a session.",
    inputSchema: {
      type: "object",
      properties: {
        sessionId: { type: "string", description: "sess_* 会话 id" },
        limit: { type: "number", description: "条数，默认20，上限200" },
        offset: { type: "number", description: "从末尾往前跳过条数（tail 模式分页用）" },
        tail: { type: "boolean", description: "默认 true=取最新段；false=从头取" },
        role: { type: "string", enum: ["user", "assistant"], description: "按角色过滤" },
        format: { type: "string", enum: ["compact", "raw"], description: "默认 compact" },
        includeReasoning: { type: "boolean", description: "附带 reasoning 文本(截断4k)" },
      },
      required: ["sessionId"],
    },
  },
  {
    name: "session_search",
    description: "跨会话搜索：标题和/或消息正文 LIKE 匹配，返回命中会话+消息上下文摘要。Search across sessions by keyword.",
    inputSchema: {
      type: "object",
      properties: {
        keyword: { type: "string", description: "关键词" },
        scope: { type: "string", enum: ["title", "messages", "all"], description: "默认 all" },
        limit: { type: "number", description: "返回会话数，默认20" },
      },
      required: ["keyword"],
    },
  },
  {
    name: "session_create",
    description: "新建会话记录（写 db.sqlite session 行 + tasks-index 侧栏收编）：复刻客户端 ensureSessionPersisted 语义（project_id 派生/标题截断/title_source/permission.mode），可选 modelRef 预设模型。创建的是空会话（无消息），要跑起来用 session_send。Create a session record.",
    inputSchema: {
      type: "object",
      properties: {
        directory: { type: "string", description: "工作目录（必须已存在）" },
        title: { type: "string", description: "标题；缺省 'Untitled session'，给标题则 title_source=custom" },
        sessionId: { type: "string", description: "可选自定义 sess_* id，缺省自动生成" },
        parentId: { type: "string", description: "可选父会话（fork 语义）" },
        modelRef: { type: "string", description: "可选；预设模型 providerId/modelId 或模型 id 片段" },
        thoughtLevel: { type: "string", description: "推理档位，默认 high" },
        mode: { type: "string", enum: ["build", "plan", "yolo"], description: "permission.mode，默认 build" },
        taskType: { type: "string", description: "默认 interactive" },
      },
      required: ["directory"],
    },
  },
  {
    name: "session_rename",
    description: "重命名会话标题：title_source 置 custom（粘性，客户端 LLM 自动命名被短路），双写 db.sqlite 与 tasks-index（title_overridden）。Rename a session (custom sticky title, dual write).",
    inputSchema: {
      type: "object",
      properties: {
        sessionId: { type: "string", description: "sess_* 会话 id" },
        title: { type: "string", description: "新标题（超60字符自动截断对齐客户端）" },
      },
      required: ["sessionId", "title"],
    },
  },
  {
    name: "session_pin",
    description: "侧栏置顶/取消置顶（tasks-index pinned 组织态，不改会话内容）。Pin/unpin a session in the client sidebar.",
    inputSchema: {
      type: "object",
      properties: {
        sessionId: { type: "string", description: "sess_* 会话 id" },
        pinned: { type: "boolean", description: "默认 true" },
      },
      required: ["sessionId"],
    },
  },
  {
    name: "session_archive",
    description: "归档/取消归档：双写 tasks-index.archived 与 session.time_archived（对齐客户端：CLI resume 会拒绝已归档会话）。Archive/unarchive a session.",
    inputSchema: {
      type: "object",
      properties: {
        sessionId: { type: "string", description: "sess_* 会话 id" },
        archived: { type: "boolean", description: "默认 true" },
      },
      required: ["sessionId"],
    },
  },
  {
    name: "session_delete",
    description: "删除会话。默认软删=tasks-index tombstone（产品级删除语义，数据全保留、可逆）。hard=true+confirm=true 永久删除：显式级联清 message/part/session_entry/todo/usage 等，可选 files=true 连 artifacts/agents 目录。Delete (soft tombstone by default; hard is irreversible).",
    inputSchema: {
      type: "object",
      properties: {
        sessionId: { type: "string", description: "sess_* 会话 id" },
        hard: { type: "boolean", description: "true=永久删除（默认 false 软删）" },
        confirm: { type: "boolean", description: "hard=true 时必填 true；会话疑似在飞时软删也需要" },
        files: { type: "boolean", description: "hard 时连 artifacts/agents 目录一起清" },
      },
      required: ["sessionId"],
    },
  },
  {
    name: "session_status",
    description: "会话在飞状态：cli/agents 后台代理 + 子会话(parent_id) + 近10分钟消息判活 + tasks-index 对照。Liveness & background agents of a session.",
    inputSchema: {
      type: "object",
      properties: { sessionId: { type: "string", description: "sess_* 会话 id" } },
      required: ["sessionId"],
    },
  },
  {
    name: "session_run",
    description: "新起会话跑一个提示词。默认 native=true：写 tasks-index 一次性 automation 行，由 ZCode 桌面调度器（20s 轮询）认领后原生 createTask+sendPrompt——侧栏实时出现、状态正确。native=false 走 zcode CLI headless（需 ZCODE_SESSION_CLI 指向 zcode.cjs）。默认一次性受理后台执行立即返回 resultPath。Start a new session with a prompt.",
    inputSchema: {
      type: "object",
      properties: {
        prompt: { type: "string", description: "提示词" },
        cwd: { type: "string", description: "工作目录，默认 ~/.zcode/workspace；原生通道要求是桌面端已打开的 workspace" },
        native: { type: "boolean", description: "默认 true=原生通道；false=headless" },
        wait: { type: "boolean", description: "默认 false=受理即返回；true=阻塞等跑完" },
        timeoutMs: { type: "number", description: "headless 运行超时，默认600000" },
        claimTimeoutMs: { type: "number", description: "native 等认领超时，默认90000" },
        sessionTimeoutMs: { type: "number", description: "native 等会话创建超时，默认120000" },
      },
      required: ["prompt"],
    },
  },
  {
    name: "session_send",
    description: "向既有会话注入提示词：headless resume（WAL checkpoint → resume → sessionId 校验 → 客户端可见性回写）或 native=true 原生通道（桌面端 resumeTask+sendPrompt，在飞时原生排队）。mode=append 排队补指令；interrupt 在飞时拒绝。modelRef/matchSessionModel 按指定模型跑（写 session_entry 真源，纯 JS 无需 python）。Send a prompt into an existing session.",
    inputSchema: {
      type: "object",
      properties: {
        sessionId: { type: "string", description: "目标会话 id (sess_*)" },
        prompt: { type: "string", description: "要注入的提示词" },
        mode: { type: "string", enum: ["append", "interrupt"], description: "默认 append" },
        native: { type: "boolean", description: "true=原生通道（侧栏实时可见、在飞时原生排队）" },
        modelRef: { type: "string", description: "可选；按此模型注入（providerId/modelId 或模型 id 片段），仅 headless 通道生效" },
        matchSessionModel: { type: "boolean", description: "可选；按会话已存模型注入（无则报错）" },
        wait: { type: "boolean", description: "默认 false=受理即返回 resultPath；true=阻塞等跑完" },
        timeoutMs: { type: "number", description: "headless 运行超时，默认300000，上限3600000" },
        claimTimeoutMs: { type: "number", description: "native 等认领超时" },
        sessionTimeoutMs: { type: "number", description: "native 等派发确认超时" },
      },
      required: ["sessionId", "prompt"],
    },
  },
  {
    name: "session_model",
    description: "设置会话保存的模型选择：写 db.sqlite session_entry(runtime/model_selection) 真源 + tasks-index.model 侧栏展示。桌面端 UI 与后续人工续跑生效；headless resume 不读它（按模型注入用 session_send 的 modelRef）。Set the session's saved model selection.",
    inputSchema: {
      type: "object",
      properties: {
        sessionId: { type: "string", description: "sess_* 会话 id" },
        modelRef: { type: "string", description: "providerId/modelId 全称或模型 id 片段（如 GLM-5.2）" },
        thoughtLevel: { type: "string", description: "推理档位，默认 high" },
      },
      required: ["sessionId", "modelRef"],
    },
  },
  {
    name: "plan_quota",
    description: "查询智谱 (Z.ai) coding plan 用量额度（5小时窗/周窗）。可选能力：需在 session.json 配置 keys（alias -> apiKey），未配置时返回指引。Optional Zhipu coding-plan quota check.",
    inputSchema: {
      type: "object",
      properties: { keyAlias: { type: "string", description: "session.json keys 里的别名；缺省取 current" } },
    },
  },
];

function dispatch(name, args) {
  switch (name) {
    case "session_list": return Promise.resolve(listSessions(args || {}));
    case "session_get": return Promise.resolve(sessionGet(args || {}));
    case "session_messages": return Promise.resolve(sessionMessages(args || {}));
    case "session_search": return Promise.resolve(sessionSearch(args || {}));
    case "session_create": return Promise.resolve(sessionCreate(args || {}));
    case "session_rename": return Promise.resolve(sessionRename(args || {}));
    case "session_pin": return Promise.resolve(sessionPin(args || {}));
    case "session_archive": return Promise.resolve(sessionArchive(args || {}));
    case "session_delete": return Promise.resolve(sessionDelete(args || {}));
    case "session_status": return Promise.resolve(sessionStatus(args || {}));
    case "session_run": return sessionRun(args || {});
    case "session_send": return sessionSend(args || {});
    case "session_model": return Promise.resolve(sessionModelSet(args || {}));
    case "plan_quota": return planQuota(args || {});
    // 旧 zcode-fleet-mcp 工具名兼容映射（不在 tools/list 里广告）
    case "fleet_list_sessions": return Promise.resolve(listSessions(args || {}));
    case "fleet_agent_status": return Promise.resolve(sessionStatus(args || {}));
    case "fleet_inject_prompt": return sessionSend(args || {});
    case "fleet_spawn_session": return sessionRun(args || {});
    case "fleet_set_session_model": return Promise.resolve(sessionModelSet(args || {}));
    case "fleet_quota": return planQuota(args || {});
    default: return Promise.resolve(errResult("UNKNOWN_TOOL", `未知工具: ${name}（本 server 是 zcode-session-mcp，工具列表见 tools/list）`));
  }
}

// 入口分流：--background-run = 受理时的后台自跑模式（不启动 stdio 服务）
if (process.argv[2] === "--background-run" && process.argv[3]) {
  bgRun(process.argv[3]).catch((e) => { log("bgRun 异常:", errText(e)); process.exit(1); });
} else {

  let buf = "";
  process.stdin.setEncoding("utf8");
  process.stdin.on("data", (chunk) => {
    buf += chunk;
    let idx;
    while ((idx = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, idx).trim();
      buf = buf.slice(idx + 1);
      if (line) handleMessage(line).catch((e) => log("handleMessage 异常:", errText(e)));
    }
  });
  process.stdin.on("end", () => process.exit(0));

  function send(obj) { process.stdout.write(JSON.stringify(obj) + "\n"); }

  async function handleMessage(line) {
    let msg;
    try { msg = JSON.parse(line); } catch {
      return send({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } });
    }
    const { id, method, params } = msg;
    const reply = (result) => { if (id !== undefined && id !== null) send({ jsonrpc: "2.0", id, result }); };

    try {
      switch (method) {
        case "initialize":
          return reply({
            protocolVersion: (params && params.protocolVersion) || "2024-11-05",
            capabilities: { tools: {} },
            serverInfo: { name: "zcode-session-mcp", version: VERSION },
          });
        case "notifications/initialized":
        case "notifications/cancelled":
          return; // 通知不回包
        case "ping":
          return reply({});
        case "tools/list":
          return reply({ tools: TOOLS });
        case "tools/call": {
          const name = params && params.name;
          const toolArgs = (params && params.arguments) || {};
          log("tools/call", name);
          const result = await dispatch(name, toolArgs);
          return reply(result);
        }
        default:
          if (id !== undefined && id !== null) {
            return send({ jsonrpc: "2.0", id, error: { code: -32601, message: `Method not found: ${method}` } });
          }
      }
    } catch (e) {
      log("处理异常:", errText(e));
      if (id !== undefined && id !== null) {
        send({ jsonrpc: "2.0", id, error: { code: -32603, message: "Internal error: " + errText(e) } });
      }
    }
  }

  log(`zcode-session-mcp v${VERSION} 启动 (pid ${process.pid})`);

} // end stdio mode
