#!/usr/bin/env node
// zcode-session-mcp —— ZCode (zai-org/ZCode) 本机会话管理 MCP server (stdio, JSON-RPC 2.0)
//
// 纯 Node.js 单文件、零依赖；要求 node >= 22.5（node:sqlite）。运行/注入只走客户端原生通道（桌面调度器 automations 表）。
// 能力对照 zcode 客户端源码（apps/zcode-cli）的会话管理面：
//   查: session_list / session_get / session_messages / session_export / session_search / session_status
//   增: session_create（落 session 行，复刻 ensureSessionPersisted 语义）/ session_run（跑一个新会话）
//   改: session_rename / session_pin / session_archive / session_model / session_send
//   删: session_delete（默认软删=tasks-index tombstone；hard=true 真删，FK/显式级联）
// 额度面: plan_quota（apiKey 额度）/ plan_start（start plan 套餐+余额）/ plan_reset_card（重置卡）
//         / plan_claim（领取面）/ plan_switch（切 current key）——zcode.z.ai 账号端点+凭据
//         解密对齐桌面端 app.asar（credentialService enc:v1 AES-256-GCM）
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
// 可选配置 session.json：智谱额度 keyAlias 与 providerMap。

import { DatabaseSync } from "node:sqlite";
import { spawn } from "node:child_process";
import { existsSync, readFileSync, writeFileSync, readdirSync, statSync, mkdirSync, rmSync, copyFileSync, renameSync } from "node:fs";
import { homedir, platform, userInfo } from "node:os";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID, createHash, createDecipheriv } from "node:crypto";
import process from "node:process";

const VERSION = "0.5.0";
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
const QUOTA_BASE = process.env.ZCODE_SESSION_QUOTA_BASE || "https://open.bigmodel.cn/api/monitor/usage";
// ZCode 账号面（start plan / 重置卡 / 领取）——端点与请求形状对齐桌面端 app.asar（usage-stats + codingPlanSubscription 模块）
const ZCODE_PLAN_BASE = process.env.ZCODE_SESSION_PLAN_BASE || "https://zcode.z.ai";
const ZCODE_APP_VERSION = process.env.ZCODE_SESSION_APP_VERSION || "3.14.4"; // 服务端校验该参数，须用客户端在发版本
const CREDENTIALS_FILE = join(ZCODE_HOME, "v2", "credentials.json");
const TELEMETRY_STATE = join(ZCODE_HOME, "v2", "telemetry-state.json");
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

// ---------------------------------------------------------------- session.json 配置（可选）

function loadConfig() {
  if (existsSync(CONFIG_PATH)) {
    try {
      return { config: JSON.parse(readFileSync(CONFIG_PATH, "utf8")), path: CONFIG_PATH };
    } catch (e) { log("配置解析失败:", CONFIG_PATH, errText(e)); }
  }
  return { config: null, path: CONFIG_PATH };
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
  return { config: cfg, path: CONFIG_PATH, created: true };
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
    return { providerId: pid, modelId: mid.trim() }; // 注册表缺失时放行（新模型可能尚未入注册表）
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
  // 无行先收编（CLI/手工建的会话默认不在侧栏）——字段形状照抄桌面端现有行，前端渲染依赖 meta_json
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

// ---------------------------------------------------------------- session_export（原样导出 + 分片）

// 启发式估 token：ASCII≈4字符/token，CJK/全角≈1字符/token；仅用于切片粒度，非精确计数
function estimateTokens(text) {
  let ascii = 0, wide = 0;
  for (let i = 0; i < text.length; i++) {
    const cp = text.codePointAt(i);
    if (cp > 0xffff) i++;
    if (cp >= 0x2e80) wide++; else ascii++;
  }
  return Math.ceil(ascii / 4 + wide);
}

const EXPORT_TOOL_PREVIEW = 2000; // txt 格式工具 input/output 概览截断长度（全量走 jsonl）
function exportPreview(v, cap = EXPORT_TOOL_PREVIEW) {
  const s = typeof v === "string" ? v : JSON.stringify(v, null, 1);
  if (s === undefined || s === null) return "";
  return s.length > cap ? s.slice(0, cap) + "…[截断，全量见 jsonl 导出]" : s;
}

function sessionExport(args) {
  const need = requireSession(args.sessionId);
  if (need.error) return need.error;
  const sessionId = args.sessionId;
  const format = String(args.format || "jsonl").toLowerCase();
  if (!["jsonl", "txt"].includes(format)) return errResult("BAD_ARGS", `format 只支持 jsonl | txt（收到 ${args.format}）`);
  const chunk = args.chunk === undefined ? true : Boolean(args.chunk);
  const includeReasoning = Boolean(args.includeReasoning);
  const chunkTokens = Math.min(Math.max(Number(args.chunkTokens) || 100000, 1000), 1000000);
  const overwrite = Boolean(args.overwrite);
  const outputDir = String(args.outputDir || "").trim() || join(SCRIPT_DIR, "exports", sessionId, format);

  let db;
  try { db = openDbRo(DB_MAIN); } catch (e) { return errResult("DB_OPEN_FAILED", errText(e)); }
  try {
    const s = getSessionRow(db, sessionId);
    // 排序对齐客户端 messages()：(sequence, time_created, id)
    const rows = db.prepare(`
      SELECT id, data, sequence, time_created FROM message
      WHERE session_id = ? ORDER BY sequence, time_created, id`).all(sessionId);
    const partStmt = db.prepare("SELECT data FROM part WHERE message_id = ? ORDER BY sequence, time_created, id");
    const messages = rows.map((r) => {
      let data = {};
      try { data = JSON.parse(r.data); } catch { /* 坏行保底 */ }
      const parts = partStmt.all(r.id).map((p) => { try { return JSON.parse(p.data); } catch { return null; } }).filter(Boolean);
      return { id: r.id, role: data.role || null, time_created: r.time_created, sequence: r.sequence, data, parts };
    });

    // 渲染为字符串序列：jsonl=每消息一行库内原样 JSON；txt=普通会话式块
    const sep = "─".repeat(48);
    const render = (m, no) => {
      if (format === "jsonl") {
        return JSON.stringify({
          message_id: m.id, role: m.role,
          time_created: m.time_created ? new Date(m.time_created).toISOString() : null,
          sequence: m.sequence, data: m.data, parts: m.parts,
        });
      }
      const out = [`[${String(no).padStart(3, "0")}] ${m.role || "?"} · ${m.time_created ? new Date(m.time_created).toISOString() : "?"}`];
      for (const p of m.parts) {
        if (!p || typeof p !== "object") continue;
        if ((p.type === "text" || p.type === "reasoning") && p.text) {
          if (p.type === "reasoning" && !includeReasoning) continue;
          out.push("", p.text);
        } else if (p.type === "tool") {
          const st = p.state || {};
          out.push("", `  [tool] ${p.tool || p.toolName || "?"} · ${st.status || "?"}`);
          if (st.input !== undefined && st.input !== null) out.push(`  input : ${exportPreview(st.input)}`);
          if (st.output !== undefined && st.output !== null) out.push(`  output: ${exportPreview(st.output)}`);
        }
      }
      return out.join("\n");
    };
    const renders = messages.map((m, i) => render(m, i + 1));

    if (existsSync(outputDir) && readdirSync(outputDir).length && !overwrite) {
      return errResult("OUTPUT_DIR_NOT_EMPTY", `输出目录已存在且非空: ${outputDir}（overwrite=true 覆盖本工具产物）`);
    }
    mkdirSync(outputDir, { recursive: true });
    if (overwrite) {
      // 只清理本工具命名模式的产物，不碰目录里其他内容
      const ours = /^(part-\d+\.(jsonl|txt)|manifest\.json|sess_[A-Za-z0-9-]+\.(jsonl|txt))$/;
      for (const f of readdirSync(outputDir)) if (ours.test(f)) { try { rmSync(join(outputDir, f)); } catch { /* noop */ } }
    }

    const joiner = format === "jsonl" ? "\n" : `\n\n${sep}\n\n`;
    const txtHeader = format === "txt" ? [
      "# ZCode 会话导出（txt 普通对话式）",
      `标题: ${s.title}`,
      `会话: ${sessionId}`,
      `目录: ${s.directory || "?"}`,
      `消息数: ${rows.length}`,
      `时间: ${s.time_created ? new Date(s.time_created).toISOString() : "?"} ~ ${s.time_updated ? new Date(s.time_updated).toISOString() : "?"}`,
      `生成: ${new Date().toISOString()}`,
      `说明: 正文全量；reasoning 按 includeReasoning=${includeReasoning}；工具 input/output 各截 ${EXPORT_TOOL_PREVIEW} 字符（全量原样用 jsonl 格式）`,
    ].join("\n") : null;

    // 切片：消息原子，单条超限独占一片；chunk=false 时单文件
    let groups;
    if (!chunk) {
      groups = [{ items: renders, tokens: renders.reduce((a, x) => a + estimateTokens(x), 0) }];
    } else {
      groups = [];
      let cur = { items: [], tokens: 0 };
      const flush = () => { if (cur.items.length) { groups.push(cur); cur = { items: [], tokens: 0 }; } };
      for (const text of renders) {
        const t = estimateTokens(text);
        if (cur.items.length && cur.tokens + t > chunkTokens) flush();
        cur.items.push(text);
        cur.tokens += t;
      }
      flush();
    }

    const files = groups.map((g, i) => {
      const name = chunk ? `part-${String(i + 1).padStart(3, "0")}.${format}` : `${sessionId}.${format}`;
      let content = g.items.join(joiner) + "\n";
      if (format === "txt") content = (i === 0 ? txtHeader + "\n\n" : `（会话 ${sessionId} · 第 ${i + 1}/${groups.length} 片）\n\n`) + content;
      const p = join(outputDir, name);
      writeFileSync(p, content, "utf8");
      return { file: name, path: p, messages: g.items.length, estTokens: g.tokens, bytes: statSync(p).size };
    });

    const manifest = {
      tool: "session_export", version: VERSION,
      sessionId,
      title: s.title, directory: s.directory,
      time_created: s.time_created ? new Date(s.time_created).toISOString() : null,
      time_updated: s.time_updated ? new Date(s.time_updated).toISOString() : null,
      generatedAt: new Date().toISOString(),
      sourceDb: DB_MAIN,
      format, chunk, chunkTokens, includeReasoning,
      formatDesc: format === "jsonl"
        ? "jsonl（每行一条消息：message_id/role/time_created/sequence/data/parts，data 与 parts 为库内原样 JSON）"
        : `txt（普通会话式：消息头+正文全量+工具 input/output 概览各截 ${EXPORT_TOOL_PREVIEW} 字符）`,
      msgCount: rows.length,
      estTokensTotal: files.reduce((a, f) => a + f.estTokens, 0),
      tokenEstimator: "ASCII≈4字符/token，CJK≈1字符/token（切片启发式，非精确）",
      parts: files,
    };
    writeFileSync(join(outputDir, "manifest.json"), JSON.stringify(manifest, null, 2), "utf8");

    return okResult({
      ok: true, sessionId, format, chunk, outputDir, msgCount: rows.length,
      partCount: files.length, chunkTokens, estTokensTotal: manifest.estTokensTotal,
      parts: files.map((f) => ({ file: f.file, messages: f.messages, estTokens: f.estTokens })),
      note: format === "jsonl"
        ? "恢复=按序拼接 part-*.jsonl 后逐行 JSON.parse。"
        : "txt 为普通对话式阅读版（工具概览有截断）；全量结构化请再导一份 jsonl。",
    });
  } catch (e) {
    return errResult("EXPORT_FAILED", errText(e));
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
        "这是空会话记录（无消息）：可用于组织/归档/改名/预选模型；要让它干活请用 session_send 注入首条提示词。",
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
    // 自清洁（2026-10-12 修复 AUTOMATION_LIMIT 死锁）：一次性注入行派发完即变 completed 且无人清理，
    // 攒满 20 条上限后所有后续注入死锁。只删确定死亡行：一次性+已禁用+已完成（pending/recurring 一律不动）。
    db.prepare("DELETE FROM automations WHERE recurring = 0 AND enabled = 0 AND lifecycle_status = 'completed'").run();
    const cnt = db.prepare("SELECT COUNT(*) AS c FROM automations").get();
    if (Number(cnt.c) >= 20) {
      return { error: { code: "AUTOMATION_LIMIT", message: "automations 已达 20 条上限（自清洁后仍满：存在大量 pending/active 行），请先在 自动化 页清理后再试" } };
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
        ?, ?, '*', ?, NULL, NULL, ?,
        ?, ?, NULL, ?, NULL, 'local',
        0, 1, NULL, NULL, 0,
        0, 1, 'active',
        ?, NULL, 0, NULL,
        'idle', 0, NULL, NULL,
        ?, NULL, ?, ?
      )`).run(
      automationId, title, args.prompt,
      args.modelSelectionJson || 'null', // 'null' 字面量=跟随桌面首选；JSON=本次派发的固定 ModelSelection（调度器传 resumeTask，Host 首次有效选择即固定）
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

// ---------------------------------------------------------------- 后台作业（受理即返回的异步执行）

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

// ---------------------------------------------------------------- session_send（注入提示词）

// —— interrupt 真中断辅助：定位并终止 workspace 宿主（zcode.cjs app-server 子进程）——
const INFLIGHT_WINDOW_MS = 10 * 60 * 1000; // 与判活同窗：近 10 分钟有新消息视为在飞
const normDir = (p) => String(p || "").replace(/[\\/]+$/, "").toLowerCase();

function psOut(script, timeoutMs = 20000) {
  return new Promise((resolve, reject) => {
    const child = spawn("powershell", ["-NoProfile", "-Command", script], { windowsHide: true });
    let out = "", err = "";
    const timer = setTimeout(() => { try { child.kill(); } catch { /* noop */ } reject(new Error("powershell 超时")); }, timeoutMs);
    child.stdout.on("data", (d) => { out += d; });
    child.stderr.on("data", (d) => { err += d; });
    child.on("error", (e) => { clearTimeout(timer); reject(e); });
    child.on("exit", (code) => { clearTimeout(timer); code === 0 ? resolve(out) : reject(new Error(`powershell exit ${code}: ${err.slice(0, 200)}`)); });
  });
}

// workspace 宿主 = 命令行含 zcode.cjs app-server 的 ZCode.exe（每 workspace 一个，桌面 NodeService 之子）
async function listAppServerPids() {
  try {
    const out = await psOut(`Get-CimInstance Win32_Process -Filter "Name='ZCode.exe'" | Where-Object { $_.CommandLine -match 'zcode\\.cjs\\s+app-server' } | ForEach-Object { $_.ProcessId }`);
    return [...new Set(out.split(/\s+/).filter(Boolean).map(Number).filter(Number.isFinite))];
  } catch { return []; }
}

async function cpuMs(pid) {
  const out = await psOut(`(Get-Process -Id ${pid} -ErrorAction Stop).TotalProcessorTime.TotalMilliseconds`);
  return Number(String(out).trim().replace(/,/g, ".")) || 0;
}

// CPU 探针：在飞会话的宿主在流式干活（CPU 增量显著），空闲宿主≈0；差距不够=定位失败
async function findHostPidByCpu(pids, sampleMs = 4000) {
  if (pids.length === 1) return pids[0];
  const base = new Map();
  for (const pid of pids) { try { base.set(pid, await cpuMs(pid)); } catch { base.set(pid, null); } }
  await new Promise((r) => setTimeout(r, sampleMs));
  const deltas = [];
  for (const pid of pids) {
    const b = base.get(pid);
    if (b == null) continue;
    try { deltas.push({ pid, d: (await cpuMs(pid)) - b }); } catch { /* 进程没了 */ }
  }
  deltas.sort((a, b) => b.d - a.d);
  const top = deltas[0], second = deltas[1]?.d ?? 0;
  return top && top.d > 100 && top.d > second * 2 + 100 ? top.pid : null;
}

async function killPidTree(pid) {
  return new Promise((resolve) => {
    const child = spawn("taskkill", ["/F", "/T", "/PID", String(pid)], { windowsHide: true });
    let out = "";
    child.stdout.on("data", (d) => { out += d; });
    child.on("error", () => resolve({ ok: false, out }));
    child.on("exit", (code) => resolve({ ok: code === 0, out: out.trim() }));
  });
}

// 近窗口内有新消息的会话（跨 workspace 全局），用于评估杀宿主的连带面
function recentActiveSessions(excludeId) {
  let db;
  try { db = openDbRo(DB_MAIN); } catch { return []; }
  try {
    const since = Date.now() - INFLIGHT_WINDOW_MS;
    const rows = db.prepare("SELECT session_id, MAX(time_created) AS last_at FROM message WHERE time_created > ? GROUP BY session_id").all(since);
    const out = [];
    for (const r of rows) {
      if (r.session_id === excludeId) continue;
      const s = getSessionRow(db, r.session_id);
      if (s) out.push({ sessionId: r.session_id, title: s.title, directory: s.directory, lastAt: r.last_at ? new Date(r.last_at).toISOString() : null });
    }
    return out;
  } catch { return []; }
  finally { try { db.close(); } catch { /* noop */ } }
}

async function sessionSend(args) {
  const sessionId = args.sessionId, prompt = args.prompt, mode = args.mode || "append";
  if (!sessionId || !prompt) return errResult("BAD_ARGS", "缺少必填参数 sessionId / prompt");
  if (!["append", "interrupt"].includes(mode)) return errResult("BAD_ARGS", "mode 只支持 append | interrupt");

  // 按次模型覆盖：写进注入行 model_selection，派发时由 Host 固定——不受桌面端回写会话选择的竞态影响
  let modelSelectionJson = null, resolvedModel = null;
  if (args.modelRef !== undefined) {
    const r = resolveModelRef(String(args.modelRef));
    if (!r) return errResult("MODEL_REF_UNRESOLVED", `无法解析模型引用 "${args.modelRef}"`);
    const tl = String(args.thoughtLevel || "high");
    modelSelectionJson = JSON.stringify({ providerId: r.providerId, modelId: r.modelId, options: { reasoningLevel: tl } });
    resolvedModel = { providerId: r.providerId, modelId: r.modelId, thoughtLevel: tl };
  }

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

  // mode=interrupt：在飞 → 先入队注入行，再断宿主；调度器（主进程，宿主外）≤20s 认领 resumeTask 重开并送入新指令
  if (mode === "interrupt") {
    const st = sessionStatus({ sessionId });
    let body = {};
    try { body = JSON.parse(st.content[0].text); } catch { /* ignore */ }
    const active = Boolean(body.ownActive || body.childSessionsRunning > 0 || body.agentsRunning > 0);

    if (active) {
      // 连带面评估：同 workspace 内其他在飞会话会随宿主一起被断
      const others = recentActiveSessions(sessionId).filter((s) => normDir(s.directory) === normDir(directory));
      if (others.length && !args.force) {
        return errResult("REFUSED_ACTIVE",
          `会话在飞，且同 workspace 还有 ${others.length} 个在飞会话会随宿主中断：`
          + others.map((s) => `${s.sessionId}（${(s.title || "").slice(0, 40)}）`).join("、")
          + `。确认连带中断请带 force=true。`,
          { others });
      }

      const ins = nativeAutomationInsert({ prompt, cwd: directory, targetTaskId: sessionId, modelSelectionJson });
      if (ins.error) return errResult(ins.error.code, ins.error.message);

      const hostPids = await listAppServerPids();
      let killed = null, hostNote;
      if (!hostPids.length) {
        hostNote = "未发现 app-server 宿主（可能刚退出）：注入行已入队，派发时会自动重开宿主，无需中断。";
      } else {
        const hostPid = await findHostPidByCpu(hostPids);
        if (!hostPid) {
          hostNote = `有 ${hostPids.length} 个宿主但 CPU 探针无法定位目标（目标可能在等模型响应、CPU 增量不明显）：注入行保持排队，等当前回合自然结束后照常送入（退化为排队注入）。可稍后重试 interrupt。`;
        } else {
          const kr = await killPidTree(hostPid);
          killed = kr.ok ? hostPid : null;
          hostNote = kr.ok
            ? `已断宿主 pid=${hostPid}（连带其 MCP 子进程）。调度器 ≤20s 认领注入行并重开宿主送入新指令。`
            : `taskkill 失败（${(kr.out || "exit!=0").slice(0, 120)}）：注入行保持排队，退化为排队注入。`;
        }
      }

      // 等待派发确认（调用方若与目标同 workspace，会在断宿主时一并掉线——后台复核）
      let dispatch = null;
      if (args.wait !== false && killed) {
        dispatch = await nativeDispatchWait(ins.automationId, args);
      }
      return okResult({
        ok: true, mode: "interrupt", sessionId, directory,
        queued: true, automationId: ins.automationId,
        model: resolvedModel,
        killedHostPid: killed, hostNote,
        dispatch: dispatch ? { ok: dispatch.ok, sessionId: dispatch.sessionId, pending: dispatch.pending || false, error: dispatch.error?.code || null } : null,
        note: "interrupt 语义：注入行先入队（调度器在主进程不受断宿主影响），随后断宿主中止当前回合；若调用方与目标同 workspace，本次调用连接可能随宿主一起断开，用 resultPath / session_list 复核。",
      });
    }
    // 空闲 → 等价 append，走下方常规派发
  }

  if (!Boolean(args.wait)) {
    const jb = launchBackground("send", args);
    return okResult({
      ok: true, accepted: true, async: true, mode, sessionId,
      jobId: jb.jobId, resultPath: jb.resultPath,
      note: "原生注入已受理（后台插 automations 行并等调度器认领派发）。复核：Read resultPath；侧栏该会话会实时显示运行状态。",
    });
  }
  const result = await nativeDispatch({ prompt, targetTaskId: sessionId, cwd: directory, modelSelectionJson, claimTimeoutMs: args.claimTimeoutMs, sessionTimeoutMs: args.sessionTimeoutMs });
  if (result.content) {
    try {
      const body = JSON.parse(result.content[0].text);
      if (body.ok) { body.directory = directory; if (resolvedModel) body.model = resolvedModel; }
    } catch { /* noop */ }
  }
  return result;
}

// ---------------------------------------------------------------- session_run（新起会话）

async function sessionRun(args) {
  const prompt = args.prompt;
  if (!prompt) return errResult("BAD_ARGS", "缺少必填参数 prompt");

  // 原生通道（唯一通道）：写 tasks-index 一次性 automation 行 → 桌面调度器 → host 原生 createTask+sendPrompt
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
    note: "已写 runtime/model_selection（真源）+ tasks-index.model（侧栏展示）：桌面端 UI、人工续跑与原生注入均按此模型跑。",
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

// ---------------------------------------------------------------- ZCode 账号面凭据（start plan / 重置卡）

// 凭据解密与客户端同源：app.asar credentialService（enc:v1:{iv}.{tag}.{ct}，AES-256-GCM，
// key = sha256(ZCODE_CREDENTIAL_SECRET || "zcode-credential-fallback:{platform}:{homedir}:{username}")）
function zcodeCredentialDecrypt(value) {
  const PREFIX = "enc:v1:";
  if (typeof value !== "string" || !value.startsWith(PREFIX)) return value;
  let username = "unknown";
  try { username = userInfo().username; } catch { /* 客户端同款兜底 */ }
  const secret = process.env.ZCODE_CREDENTIAL_SECRET
    || `zcode-credential-fallback:${platform()}:${homedir()}:${username}`;
  const key = createHash("sha256").update(secret).digest();
  const [ivB, tagB, ctB] = value.slice(PREFIX.length).split(".");
  if (!ivB || !tagB || !ctB) throw new Error("凭据密文格式非法");
  const d = createDecipheriv("aes-256-gcm", key, Buffer.from(ivB, "base64url"));
  d.setAuthTag(Buffer.from(tagB, "base64url"));
  return Buffer.concat([d.update(Buffer.from(ctB, "base64url")), d.final()]).toString("utf8");
}

function loadZcodeAccountCredentials() {
  if (!existsSync(CREDENTIALS_FILE)) {
    return { error: errResult("CREDENTIALS_NOT_FOUND",
      `未找到 ${CREDENTIALS_FILE}。start plan/重置卡/领取凭据由 ZCode 桌面端登录后写入；请先在桌面端登录 bigmodel 账号。`) };
  }
  let store;
  try { store = JSON.parse(readFileSync(CREDENTIALS_FILE, "utf8")); }
  catch (e) { return { error: errResult("CREDENTIALS_PARSE_FAILED", errText(e)) }; }
  let out;
  try {
    out = {
      zcodeJwt: store["zcodejwttoken"] ? zcodeCredentialDecrypt(store["zcodejwttoken"]) : "",
      maasJwt: store["oauth:bigmodel:access_token"] ? zcodeCredentialDecrypt(store["oauth:bigmodel:access_token"]) : "",
      activeProvider: store["oauth:active_provider"] ? zcodeCredentialDecrypt(store["oauth:active_provider"]) : "",
    };
  } catch (e) {
    return { error: errResult("CREDENTIALS_DECRYPT_FAILED",
      `凭据解密失败（桌面端登录过期或 ZCODE_CREDENTIAL_SECRET 非默认值？）: ${errText(e)}`) };
  }
  if (!out.zcodeJwt) return { error: errResult("CREDENTIALS_INCOMPLETE", "credentials.json 缺少 zcodejwttoken（桌面端未登录 bigmodel 账号）。") };
  return { creds: out };
}

// 客户端全局源码头（buildZCodeSourceHeadersFromContext 同款）；balance/preview 对此校验，缺了返回 3001
function zcodeSourceHeaders() {
  const osName = platform() === "win32" ? "windows" : platform();
  const arch = process.arch === "x64" ? "x86_64" : process.arch;
  let tz = "Asia/Shanghai";
  try { tz = Intl.DateTimeFormat().resolvedOptions().timeZone || tz; } catch { /* noop */ }
  const headers = {
    "User-Agent": `ZCode/${ZCODE_APP_VERSION}`,
    "X-ZCode-App-Version": ZCODE_APP_VERSION,
    "X-Title": "Z Code@electron",
    "X-Platform": `${osName}-${arch}`,
    "HTTP-Referer": ZCODE_PLAN_BASE,
    "X-Client-Language": "zh-CN",
    "X-Client-Timezone": tz,
    "X-Os-Category": osName,
  };
  try {
    const t = JSON.parse(readFileSync(TELEMETRY_STATE, "utf8"));
    if (t && typeof t.deviceMid === "string" && t.deviceMid) headers["X-Device-Mid"] = t.deviceMid;
  } catch { /* 无 telemetry-state 则不发设备号 */ }
  return headers;
}

// zcode.z.ai 信封调用：{code, msg, data}，code=0 为成功；acceptedCodes 用于业务码放行（如机会未中 3301）
async function zcodePlanApi(pathname, { method = "GET", query = {}, headers = {}, body = null, acceptedCodes = [], timeoutMs = 15000 } = {}) {
  const url = new URL(pathname, ZCODE_PLAN_BASE + "/");
  for (const [k, v] of Object.entries(query)) url.searchParams.set(k, String(v));
  let res, json;
  try {
    res = await fetchWithTimeout(url.toString(), {
      method,
      headers,
      ...(body !== null ? { body: JSON.stringify(body) } : {}),
    }, timeoutMs);
    json = await res.json();
  } catch (e) {
    throw new Error(`请求 ${pathname} 失败: ${errText(e)}`);
  }
  if (json && typeof json.code === "number" && json.code !== 0 && !acceptedCodes.includes(json.code)) {
    const err = new Error(`业务错误 code=${json.code}${json.msg ? " " + json.msg : ""} (HTTP ${res.status})`);
    err.businessCode = json.code;
    err.businessMsg = json.msg || "";
    throw err;
  }
  return { httpStatus: res.status, json };
}

function unitsFormat(n) {
  if (!Number.isFinite(n)) return "?";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + "K";
  return String(n);
}

function isoFromEpochSec(sec) {
  return Number.isFinite(sec) && sec > 0 ? new Date(sec * 1000).toISOString() : null;
}

async function planStartView() {
  const auth = loadZcodeAccountCredentials();
  if (auth.error) return auth.error;
  const { zcodeJwt, activeProvider } = auth.creds;
  const headers = { ...zcodeSourceHeaders(), Authorization: `Bearer ${zcodeJwt}` };
  const q = { app_version: ZCODE_APP_VERSION };

  let current = null, balance = null, preview = null;
  const errs = {};
  const grab = (key, path, extraQuery) => zcodePlanApi(path, { headers, query: { ...q, ...extraQuery } })
    .then((r) => { if (key === "balance") balance = r.json; else if (key === "current") current = r.json; else preview = r.json; })
    .catch((e) => { errs[key] = errText(e); });
  await Promise.all([
    grab("current", "/api/v1/zcode-plan/billing/current"),
    grab("balance", "/api/v1/zcode-plan/billing/balance"),
    grab("preview", "/api/v1/zcode-plan/billing/preview", { platform: zcodeSourceHeaders()["X-Platform"] }),
  ]);
  if (!balance && !current) {
    return errResult("PLAN_API_FAILED", `start plan 接口全部失败（401/过期请先在桌面端重新登录 bigmodel 账号）`, { errors: errs });
  }

  const plans = (balance || current)?.data?.plans || [];
  const balances = (balance?.data?.balances || []).map((b) => ({
    model: b.show_name || null, capabilities: b.capabilities || [],
    total: b.total_units ?? null, used: b.used_units ?? null, remaining: b.remaining_units ?? null,
    percentage: (Number.isFinite(b.total_units) && b.total_units > 0 && Number.isFinite(b.remaining_units))
      ? Math.round((b.remaining_units / b.total_units) * 1000) / 10 : null,
    expiresAt: isoFromEpochSec(b.expires_at ?? b.period_end),
  }));
  const activePlans = plans.filter((p) => String(p.status || "").toLowerCase() === "active");
  const nextExpire = activePlans.map((p) => Number(p.ends_at)).filter((n) => n > 0).sort((a, b) => a - b)[0];
  const claimable = (preview?.data?.plans || []).map((p) => ({ plan_id: p.plan_id, name: p.name, description: p.description ?? null, subtitle: p.subtitle ?? null }));
  const remainingSum = balances.reduce((s, b) => s + (b.remaining || 0), 0);

  return okResult({
    ok: true, activeProvider: activeProvider || null,
    plans: plans.map((p) => ({
      plan_id: p.plan_id, name: p.name, status: p.status, priority: p.priority,
      startsAt: isoFromEpochSec(p.starts_at), endsAt: isoFromEpochSec(p.ends_at),
      entitlements: (p.entitlements || []).map((e) => ({ show_name: e.show_name, grant_units: e.grant_units, period: e.period, capabilities: e.capabilities || [] })),
    })),
    balances, claimablePlans: claimable,
    errors: Object.keys(errs).length ? errs : undefined,
    fetchedAt: new Date().toISOString(),
    summary: `start plan（${activeProvider || "bigmodel"}）：${activePlans.map((p) => p.name).join("、") || "无 active 套餐"}`
      + (balances.length ? `；余额 ${balances.map((b) => `${b.model} 剩${unitsFormat(b.remaining)}/${unitsFormat(b.total)}(${b.percentage ?? "?"}%)`).join("、")}`
        + (nextExpire ? `，最早 ${new Date(nextExpire * 1000).toISOString()} 到期` : "")
        : remainingSum === 0 ? "；余额桶为空/已耗尽" : "")
      + (claimable.length ? `；⚠ 有 ${claimable.length} 个可领取套餐（plan_claim 查看详情；领取需过人机验证）` : "；当前无可领取套餐"),
  });
}

// ---------------------------------------------------------------- 重置卡（coding-plan reset）

const RESET_BASE = "/api/v1/coding-plan/reset";
const RESET_TYPES = ["FIVE_HOUR", "WEEK"];

async function planResetCard(args) {
  const action = String(args.action || "status").toLowerCase();
  const auth = loadZcodeAccountCredentials();
  if (auth.error) return auth.error;
  const { zcodeJwt, maasJwt, activeProvider } = auth.creds;
  if (!maasJwt) return errResult("CREDENTIALS_INCOMPLETE", "credentials.json 缺少 oauth:bigmodel:access_token（重置卡需要双令牌）。");
  if (activeProvider && activeProvider !== "bigmodel") {
    return errResult("UNSUPPORTED_FAMILY", `当前 active provider 为 ${activeProvider}，本工具仅实现了 bigmodel 家族（zai 家族需 oauth:zai:access_token）。`);
  }
  // 双令牌头 + 目标类型（ow/createCodingPlanResetHeaders 同款；个人版不带 org/project）
  const headers = {
    ...zcodeSourceHeaders(),
    "Authorization": `Bearer ${zcodeJwt}`,
    "X-Bigmodel-Authorization": maasJwt,
    "Bigmodel-Target-Type": "PERSONAL",
  };

  const readStatus = async () => {
    const r = await zcodePlanApi(RESET_BASE + "/status", { headers });
    const d = r.json.data || {};
    const five = (d.available_five_hour_resets || []).map((x) => x.expire_at).filter(Number.isFinite);
    const week = (d.available_week_resets || []).map((x) => x.expire_at).filter(Number.isFinite);
    const isoAsc = (a) => [...a].sort((x, y) => x - y).map((t) => new Date(t).toISOString());
    return {
      availableFiveHourResets: { count: five.length, earliestExpireAt: five.length ? new Date(Math.min(...five)).toISOString() : null, cardExpireAts: isoAsc(five) },
      availableWeekResets: { count: week.length, earliestExpireAt: week.length ? new Date(Math.min(...week)).toISOString() : null, cardExpireAts: isoAsc(week) },
      latestFiveHourResetUsedAt: d.latest_five_hour_reset_history ? new Date(d.latest_five_hour_reset_history.used_at).toISOString() : null,
      latestWeekResetUsedAt: d.latest_week_reset_history ? new Date(d.latest_week_reset_history.used_at).toISOString() : null,
      hasUnreadHistory: Boolean(d.has_unread_history),
    };
  };

  if (action === "status") {
    try {
      const st = await readStatus();
      return okResult({ ok: true, action, ...st, fetchedAt: new Date().toISOString(),
        summary: `重置卡：5小时窗 ${st.availableFiveHourResets.count} 张${st.availableFiveHourResets.earliestExpireAt ? `（最早 ${st.availableFiveHourResets.earliestExpireAt} 到期）` : ""}，周窗 ${st.availableWeekResets.count} 张。` });
    } catch (e) { return errResult("RESET_STATUS_FAILED", errText(e)); }
  }

  if (action === "use") {
    if (!args.confirm) {
      let inventory = null;
      try { inventory = await readStatus(); } catch { /* 预览失败不阻塞确认门 */ }
      return errResult("RESET_USE_REQUIRES_CONFIRM",
        `use 会真实消费一张重置卡并立即重置对应额度窗口（不可撤销）。确认后带 confirm=true 重试。`
        + (inventory ? ` 当前库存：5小时窗 ${inventory.availableFiveHourResets.count} 张，周窗 ${inventory.availableWeekResets.count} 张。` : ""),
        { inventory: inventory || undefined });
    }
    const resetType = String(args.resetType || "FIVE_HOUR").toUpperCase();
    if (!RESET_TYPES.includes(resetType)) return errResult("BAD_ARGS", `resetType 只支持 ${RESET_TYPES.join(" | ")}（收到 ${args.resetType}）`);
    const idempotencyKey = String(args.idempotencyKey || randomUUID()).slice(0, 64);
    let before = null;
    try { before = await readStatus(); } catch { /* noop */ }
    try {
      await zcodePlanApi(RESET_BASE + "/use", {
        method: "POST", headers: { ...headers, "Content-Type": "application/json" },
        body: { idempotency_key: idempotencyKey, reset_type: resetType },
      });
    } catch (e) {
      const hint = e.businessCode ? `（业务码 ${e.businessCode}：常见于窗口未耗尽时不允许重置/卡已过期）` : "";
      return errResult("RESET_USE_FAILED", errText(e) + hint, { resetType, idempotencyKey });
    }
    let after = null;
    try { after = await readStatus(); } catch { /* noop */ }
    return okResult({
      ok: true, action: "use", resetType, idempotencyKey, usedAt: new Date().toISOString(),
      before, after,
      note: "重置卡已消费：对应窗口额度即刻恢复（以 after 为准复核）。幂等键可防重复提交。",
    });
  }

  if (action === "opportunity") {
    // 客户端 coding-plan-reset coordinator 会自动轮询该端点攒卡；此处仅手动触发一次，勿做循环
    const idempotencyKey = String(args.idempotencyKey || randomUUID()).slice(0, 64);
    try {
      const r = await zcodePlanApi(RESET_BASE + "/opportunity", {
        method: "POST", headers: { ...headers, "Content-Type": "application/json" },
        body: { idempotency_key: idempotencyKey },
        acceptedCodes: [3301],
      });
      const d = r.json.data || {};
      const granted = d.granted === true && r.json.code === 0;
      return okResult({
        ok: true, action: "opportunity", granted, nextTryAt: d.next_try_at ? new Date(d.next_try_at).toISOString() : null,
        idempotencyKey,
        note: granted ? "获得一次重置机会（卡入库，plan_reset_card action=status 查看）。"
          : "本次未获得（3301=机会未中/冷却中）。客户端会自动轮询，请勿高频重试（429=限流）。",
      });
    } catch (e) {
      const throttled = /429/.test(errText(e));
      return errResult("RESET_OPPORTUNITY_FAILED", errText(e) + (throttled ? "（被限流：稍后再试，客户端本身也在自动轮询）" : ""));
    }
  }

  if (action === "history_read") {
    try {
      await zcodePlanApi(RESET_BASE + "/history/read", { method: "POST", headers });
      return okResult({ ok: true, action: "history_read", note: "已标记重置历史为已读（清除客户端未读提示）。" });
    } catch (e) { return errResult("RESET_HISTORY_READ_FAILED", errText(e)); }
  }

  return errResult("BAD_ARGS", `action 只支持 status | use | opportunity | history_read（收到 ${args.action}）`);
}

// ---------------------------------------------------------------- 领取面（billing preview / claim）

async function planClaim(args) {
  const action = String(args.action || "preview").toLowerCase();
  const auth = loadZcodeAccountCredentials();
  if (auth.error) return auth.error;
  const { zcodeJwt } = auth.creds;
  const headers = { ...zcodeSourceHeaders(), Authorization: `Bearer ${zcodeJwt}` };

  if (action === "preview") {
    try {
      const r = await zcodePlanApi("/api/v1/zcode-plan/billing/preview", {
        headers,
        query: { app_version: ZCODE_APP_VERSION, platform: zcodeSourceHeaders()["X-Platform"] },
      });
      const plans = (r.json.data && r.json.data.plans) || [];
      return okResult({
        ok: true, action, claimableCount: plans.length,
        plans: plans.map((p) => ({
          plan_id: p.plan_id, name: p.name, subtitle: p.subtitle ?? p.description ?? null,
          manualClaim: p.manual_claim ?? null, priority: p.priority ?? null,
        })),
        fetchedAt: new Date().toISOString(),
        summary: plans.length
          ? `有 ${plans.length} 个可领取套餐：${plans.map((p) => p.name || p.plan_id).join("、")}。领取需过阿里云人机验证（见 plan_claim action=claim）。`
          : "当前没有可领取的活动套餐（preview 为空）。",
      });
    } catch (e) { return errResult("CLAIM_PREVIEW_FAILED", errText(e)); }
  }

  if (action === "claim") {
    const planId = String(args.planId || "").trim();
    if (!planId) return errResult("BAD_ARGS", "claim 需要 planId（先 action=preview 拿 plan_id）");
    const captcha = String(args.captchaVerifyParam || "").trim();
    if (!captcha) {
      return errResult("CLAIM_CAPTCHA_REQUIRED",
        "领取强制阿里云人机验证（X-Aliyun-Captcha-Verify-Param），无法纯 API 全自动。全自动路径见 README「自动领取」：受控页面加载 AliyunCaptcha.js（须先设 window.AliyunCaptchaConfig={region,prefix}，用 plan_start 场景配置），startTracelessVerification() 无感通过后抓 captchaVerifyParam 传入本工具。"
        + "也可在 ZCode 客户端领取弹窗完成验证。claim body={plan_id}，验证码参数有时效且一次性。",
        { planId });
    }
    // region 头是命门：实测缺了它服务端 VerifyIntelligentCaptcha 必失败（3007），带上才进入业务校验
    const captchaRegion = String(args.captchaRegion || "cn").trim();
    try {
      const r = await zcodePlanApi("/api/v1/zcode-plan/billing/claim", {
        method: "POST",
        headers: { ...headers, "Content-Type": "application/json", "X-Aliyun-Captcha-Verify-Param": captcha, "X-Aliyun-Captcha-Verify-Region": captchaRegion },
        body: { plan_id: planId },
      });
      const d = r.json.data || {};
      return okResult({
        ok: true, action: "claim", planId,
        claimedPlan: d.plan ? { plan_id: d.plan.plan_id, name: d.plan.name, endsAt: isoFromEpochSec(d.plan.ends_at) } : null,
        serverMessage: typeof d.message === "string" ? d.message : undefined,
        note: d.plan ? "领取成功：plan_start 复核新套餐与余额。" : `服务端未返回 plan（code=${r.json.code} ${r.json.msg || ""}），以 plan_start 复核为准。`,
      });
    } catch (e) {
      return errResult("CLAIM_FAILED", errText(e) + (e.businessCode === 3007 ? "（captcha verify failed：参数过期/已用/缺 Region 头——参数一次性，重新抓）" : e.businessCode ? "（验证码参数可能已过期，重新获取后重试）" : ""), { planId });
    }
  }

  return errResult("BAD_ARGS", `action 只支持 preview | claim（收到 ${args.action}）`);
}

// ---------------------------------------------------------------- 切换额度 key（session.json current）

function planSwitch(args) {
  const { config, path } = ensureConfig();
  const keys = (config && config.keys) || {};
  const overview = () => Object.entries(keys).map(([alias, k]) => ({
    keyAlias: alias, plan: k.plan || null, current: Boolean(k.current),
    apiKeyMasked: k.apiKey ? String(k.apiKey).slice(0, 6) + "…" : null,
    note: k.note || null,
  }));
  if (!args.keyAlias) {
    const current = Object.keys(keys).find((k) => keys[k] && keys[k].current) || null;
    return okResult({
      ok: true, currentKeyAlias: current, keys: overview(),
      note: "current=plan_quota 默认查询的 key。要切换：plan_switch { keyAlias }。仅影响本 MCP 额度面的默认 key，不改 ZCode 客户端 provider 配置。",
    });
  }
  const alias = String(args.keyAlias);
  if (!keys[alias]) {
    return errResult("KEY_NOT_CONFIGURED", `配置中不存在 keyAlias "${alias}"。可用: ${Object.keys(keys).join(", ") || "(无)"}`, { keys: overview() });
  }
  if (!keys[alias].apiKey) return errResult("KEY_NOT_CONFIGURED", `keyAlias "${alias}" 的 apiKey 未填写，先编辑 ${path}。`);
  for (const k of Object.keys(keys)) keys[k].current = k === alias;
  try {
    writeFileSync(path, JSON.stringify(config, null, 2) + "\n", "utf8");
  } catch (e) { return errResult("SWITCH_WRITE_FAILED", errText(e)); }
  const mapped = Object.entries((config && config.providerMap) || {}).filter(([, v]) => v === alias).map(([p]) => p);
  return okResult({
    ok: true, currentKeyAlias: alias, keys: overview(), providerMapHits: mapped,
    note: `已切换 current → "${alias}"（plan_quota 缺省即查它）。providerMap 里指向它的 provider：${mapped.join(", ") || "(无)"}。`,
  });
}

// ---------------------------------------------------------------- MCP stdio 层

// ---------------------------------------------------------------- Provider 管理（v2/provider_config.json，桌面端「管理模型」面板真源）
// 源码对齐 zai-org/ZCode：packages/provider/src/config/{provider-data-schema,rule-data-schema}.ts（zod .strict()，多一字段即拒）
// packages/provider-node/src/provider-config-file-codec.ts（schemaVersion=1）；每次派发经 provisioning envelope 喂给运行时 → 写入后下个会话即生效。

const PROVIDER_CONFIG_FILE = join(ZCODE_HOME, "v2", "provider_config.json");
const PROVIDER_API_TYPES = ["anthropic-messages", "openai-chat-completions", "openai-responses"];
const PROVIDER_ACCESS_TYPES = ["api-key", "zhipu-coding-plan-api-key"];
const maskKey = (k) => (typeof k === "string" && k ? `…${k.slice(-4)}(len=${k.length})` : null);

function loadProviderConfigFile() {
  if (!existsSync(PROVIDER_CONFIG_FILE)) return { schemaVersion: 1, config: { providerConfigRules: { providerRules: [] }, modelConfigRules: { providerModelRules: [], manualProviderModelRules: [] } } };
  const doc = JSON.parse(readFileSync(PROVIDER_CONFIG_FILE, "utf8"));
  if (doc.schemaVersion !== 1) throw new Error(`provider_config.json schemaVersion=${doc.schemaVersion}，本工具仅支持 1`);
  doc.config ??= {};
  doc.config.providerConfigRules ??= {};
  doc.config.providerConfigRules.providerRules ??= [];
  doc.config.modelConfigRules ??= {};
  doc.config.modelConfigRules.providerModelRules ??= [];
  doc.config.modelConfigRules.manualProviderModelRules ??= [];
  return doc;
}

function saveProviderConfigFile(doc) {
  // 只回写 schema 允许的四个 config 键；先备份再 tmp+rename 原子落盘
  const out = { schemaVersion: 1, config: {} };
  for (const k of ["providerOrder", "providerConfigRules", "modelConfigRules", "defaultModelSelection"]) {
    if (doc.config[k] !== undefined) out.config[k] = doc.config[k];
  }
  const backup = join(dirname(PROVIDER_CONFIG_FILE), `provider_config.backup-${new Date().toISOString().replace(/[-:.TZ]/g, "").slice(0, 14)}.json`);
  copyFileSync(PROVIDER_CONFIG_FILE, backup);
  const tmp = PROVIDER_CONFIG_FILE + ".tmp";
  writeFileSync(tmp, JSON.stringify(out, null, 2) + "\n", "utf8");
  renameSync(tmp, PROVIDER_CONFIG_FILE);
  return backup;
}

function findPersonalRule(doc, id) {
  return doc.config.providerConfigRules.providerRules.find((r) => r.providerId === id) || null;
}

function providerList() {
  let doc;
  try { doc = loadProviderConfigFile(); } catch (e) { return errResult("PROVIDER_CONFIG_INVALID", errText(e)); }
  const personal = doc.config.providerConfigRules.providerRules.map((r) => ({
    providerId: r.providerId, providerName: r.providerName ?? null, templateId: r.templateId ?? null,
    enabled: r.enabled !== false, group: r.config?.group ?? null,
    accessType: r.config?.access?.type ?? null,
    apiKeyMasked: maskKey(r.config?.access?.apiKey),
    apiType: r.config?.api?.type ?? null, baseUrl: r.config?.api?.baseUrl ?? null,
    personalModelIds: r.config?.personalModelIds ?? null, modelOrder: r.config?.modelOrder ?? null,
    builtinModelIds: r.config?.builtinModelIds ?? null,
  }));
  let registry = [];
  try {
    const cfg = JSON.parse(readFileSync(V2_CONFIG, "utf8"));
    registry = Object.entries(cfg.provider || {}).map(([pid, p]) => ({ providerId: pid, models: Object.keys((p && p.models) || {}) }));
  } catch { /* registry 读取失败不阻塞 personal 展示 */ }
  return okResult({
    ok: true, personal, registry,
    note: "personal=「管理模型」面板真源（可增删改，key 已脱敏）；registry=桌面端解析后的模型注册表（只读展示）。改动对下一个派发的会话生效；桌面端 UI 里保存模型设置可能以其内存态覆盖外部写入。",
  });
}

function providerAdd(args) {
  const apiKey = String(args.apiKey || "").trim();
  if (!apiKey) return errResult("BAD_ARGS", "apiKey 必填");
  const accessType = String(args.accessType || "api-key");
  if (!PROVIDER_ACCESS_TYPES.includes(accessType)) return errResult("BAD_ARGS", `accessType 只支持 ${PROVIDER_ACCESS_TYPES.join(" | ")}`);
  const id = String(args.id || "").trim() || randomUUID();
  if (/^(builtin:|account:)/.test(id)) return errResult("BAD_ARGS", "providerId 不能使用 builtin:/account: 前缀（保留给内建账号面）");

  let doc;
  try { doc = loadProviderConfigFile(); } catch (e) { return errResult("PROVIDER_CONFIG_INVALID", errText(e)); }
  if (findPersonalRule(doc, id)) return errResult("PROVIDER_EXISTS", `providerId ${id} 已存在（改用 provider_set）`);

  let rule;
  const templateId = String(args.templateId || "").trim();
  if (templateId) {
    // 模板型（如 zai-api）：模型与 endpoint 继承模板，只填 access
    rule = {
      providerId: id, templateId,
      ...(String(args.name || "").trim() ? { providerName: String(args.name).trim() } : {}),
      config: { group: "standard-personal", access: { type: accessType, apiKey } },
    };
  } else {
    // 自定义型：需要 baseUrl + models
    const baseUrl = String(args.baseUrl || "").trim();
    const models = Array.isArray(args.models) ? args.models.map((m) => String(m).trim()).filter(Boolean) : [];
    if (!baseUrl) return errResult("BAD_ARGS", "自定义 provider 必填 baseUrl（或给 templateId 走模板）");
    if (!models.length) return errResult("BAD_ARGS", "自定义 provider 必填 models（非空数组）");
    const apiType = String(args.apiType || "anthropic-messages");
    if (!PROVIDER_API_TYPES.includes(apiType)) return errResult("BAD_ARGS", `apiType 只支持 ${PROVIDER_API_TYPES.join(" | ")}`);
    rule = {
      providerId: id,
      ...(String(args.name || "").trim() ? { providerName: String(args.name).trim() } : {}),
      config: {
        group: "standard-personal", access: { type: accessType, apiKey },
        api: { type: apiType, baseUrl },
        personalModelIds: models, modelOrder: Array.isArray(args.modelOrder) && args.modelOrder.length ? args.modelOrder.map((m) => String(m).trim()) : models,
      },
    };
  }
  doc.config.providerConfigRules.providerRules.push(rule);
  let backup;
  try { backup = saveProviderConfigFile(doc); } catch (e) { return errResult("PROVIDER_SAVE_FAILED", errText(e)); }
  return okResult({
    ok: true, providerId: id, apiKeyMasked: maskKey(apiKey), backup,
    note: "已写入 provider_config.json（原文件已备份）。下一个派发的会话经 provisioning 生效；注入前用 session_model 把会话路由到该 provider（modelRef 传 \"<providerId>/<modelId>\"）。",
  });
}

function providerSet(args) {
  const id = String(args.id || "").trim();
  if (!id) return errResult("BAD_ARGS", "缺少必填参数 id");
  let doc;
  try { doc = loadProviderConfigFile(); } catch (e) { return errResult("PROVIDER_CONFIG_INVALID", errText(e)); }
  const rule = findPersonalRule(doc, id);
  if (!rule) return errResult("PROVIDER_NOT_FOUND", `personal 规则中不存在 ${id}（builtin:/account: 账号面不可经此修改）`);
  rule.config ??= {};

  if (args.name !== undefined) { if (String(args.name).trim()) rule.providerName = String(args.name).trim(); else delete rule.providerName; }
  if (args.enabled !== undefined) { if (args.enabled === false) rule.enabled = false; else delete rule.enabled; }
  if (args.apiKey !== undefined) {
    const k = String(args.apiKey).trim();
    if (!k) return errResult("BAD_ARGS", "apiKey 不能为空串（移除 provider 用 provider_remove）");
    rule.config.access = { ...(rule.config.access || { type: "api-key" }), type: rule.config.access?.type || "api-key", apiKey: k };
  }
  if (args.baseUrl !== undefined || args.apiType !== undefined) {
    const api = { ...(rule.config.api || {}) };
    if (args.baseUrl !== undefined) api.baseUrl = String(args.baseUrl).trim();
    if (args.apiType !== undefined) {
      if (!PROVIDER_API_TYPES.includes(args.apiType)) return errResult("BAD_ARGS", `apiType 只支持 ${PROVIDER_API_TYPES.join(" | ")}`);
      api.type = args.apiType;
    } else if (!api.type) api.type = "anthropic-messages";
    rule.config.api = api;
  }
  if (args.models !== undefined) {
    const models = Array.isArray(args.models) ? args.models.map((m) => String(m).trim()).filter(Boolean) : [];
    if (!models.length) return errResult("BAD_ARGS", "models 不能为空数组");
    rule.config.personalModelIds = models;
    if (args.modelOrder === undefined) rule.config.modelOrder = models;
  }
  if (args.modelOrder !== undefined) rule.config.modelOrder = Array.isArray(args.modelOrder) ? args.modelOrder.map((m) => String(m).trim()) : undefined;

  let backup;
  try { backup = saveProviderConfigFile(doc); } catch (e) { return errResult("PROVIDER_SAVE_FAILED", errText(e)); }
  return okResult({
    ok: true, providerId: id, updated: { name: args.name, enabled: args.enabled, apiKey: args.apiKey ? maskKey(String(args.apiKey)) : undefined, baseUrl: args.baseUrl, apiType: args.apiType, models: args.models, modelOrder: args.modelOrder }, backup,
    note: "已更新并备份。改动对下一个派发的会话生效。",
  });
}

function providerRemove(args) {
  const id = String(args.id || "").trim();
  if (!id) return errResult("BAD_ARGS", "缺少必填参数 id");
  if (!args.confirm) return errResult("PROVIDER_REMOVE_REQUIRES_CONFIRM", `remove 会从 provider_config.json 删除该 provider（其 apiKey 随之失效）。确认后带 confirm=true 重试。`);
  let doc;
  try { doc = loadProviderConfigFile(); } catch (e) { return errResult("PROVIDER_CONFIG_INVALID", errText(e)); }
  const before = doc.config.providerConfigRules.providerRules.length;
  doc.config.providerConfigRules.providerRules = doc.config.providerConfigRules.providerRules.filter((r) => r.providerId !== id);
  if (doc.config.providerConfigRules.providerRules.length === before) return errResult("PROVIDER_NOT_FOUND", `personal 规则中不存在 ${id}`);
  doc.config.modelConfigRules.providerModelRules = doc.config.modelConfigRules.providerModelRules.filter((r) => r.providerId !== id);
  doc.config.modelConfigRules.manualProviderModelRules = doc.config.modelConfigRules.manualProviderModelRules.filter((r) => r.providerId !== id);
  let backup;
  try { backup = saveProviderConfigFile(doc); } catch (e) { return errResult("PROVIDER_SAVE_FAILED", errText(e)); }
  return okResult({ ok: true, providerId: id, removed: true, backup, note: "已删除并清理其模型规则。已派发中的会话不受影响（按派发时的快照跑）。" });
}

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
    name: "session_export",
    description: "会话导出：format=jsonl 结构化原样（每消息一行库内 data+parts 原始 JSON）/ txt 普通对话式（消息头+正文全量，工具概览截断）；chunk 控制是否按估算 token 切片（默认开，100k/片，消息原子；chunk=false 单文件）。Export a session: structured raw JSONL or plain conversational txt; optional ~100k-token chunking (default on).",
    inputSchema: {
      type: "object",
      properties: {
        sessionId: { type: "string", description: "sess_* 会话 id" },
        format: { type: "string", enum: ["jsonl", "txt"], description: "jsonl=结构化原样（默认）；txt=普通对话式" },
        chunk: { type: "boolean", description: "是否切片，默认 true；false=单文件" },
        chunkTokens: { type: "number", description: "每片估算 token 上限，默认 100000" },
        includeReasoning: { type: "boolean", description: "txt 是否含 reasoning 思考文本，默认 false" },
        outputDir: { type: "string", description: "输出目录，缺省 <server目录>/exports/<sessionId>/<format>" },
        overwrite: { type: "boolean", description: "输出目录非空时是否清理本工具产物后覆盖，默认拒绝" },
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
    description: "新起会话跑一个提示词：写 tasks-index 一次性 automation 行，由 ZCode 桌面调度器（20s 轮询）认领后原生 createTask+sendPrompt——侧栏实时出现、状态正确。默认一次性受理后台执行立即返回 resultPath。Start a new session with a prompt.",
    inputSchema: {
      type: "object",
      properties: {
        prompt: { type: "string", description: "提示词" },
        cwd: { type: "string", description: "工作目录，默认 ~/.zcode/workspace；要求是桌面端已打开的 workspace" },
        wait: { type: "boolean", description: "默认 false=受理即返回；true=阻塞等跑完" },
        claimTimeoutMs: { type: "number", description: "等认领超时，默认90000" },
        sessionTimeoutMs: { type: "number", description: "等会话创建超时，默认120000" },
      },
      required: ["prompt"],
    },
  },
  {
    name: "session_send",
    description: "向既有会话注入提示词（原生通道）：写 automations 行 → 桌面端 resumeTask+sendPrompt，侧栏实时可见。mode=append 排队补指令（在飞时原生排队）；mode=interrupt 断开式注入——在飞时先入队注入行再断该 workspace 宿主中止当前回合，调度器 ≤20s 重开宿主送入新指令（同 workspace 其他在飞会话会被连带，需 force=true；调用方与目标同 workspace 时自身连接会掉线，用 resultPath 复核）。modelRef=按次模型覆盖（写在注入行 model_selection 上，派发时由 Host 固定，无竞态；格式 providerId/modelId，可配 thoughtLevel）——要在注入前换 key/模型就用它，比 session_model 的会话级写入更可靠。按指定模型注入：先 session_model 写会话模型再注入。Send a prompt into an existing session: append queues natively; interrupt aborts the running turn and injects; modelRef pins the model for this dispatch.",
    inputSchema: {
      type: "object",
      properties: {
        sessionId: { type: "string", description: "目标会话 id (sess_*)" },
        prompt: { type: "string", description: "要注入的提示词" },
        mode: { type: "string", enum: ["append", "interrupt"], description: "默认 append 排队；interrupt=断开式注入（在飞时断宿主中止当前回合再注入）" },
        force: { type: "boolean", description: "interrupt 时确认连带中断同 workspace 其他会话" },
        modelRef: { type: "string", description: "本次派发的模型覆盖（providerId/modelId，写注入行无竞态）；缺省跟随会话/桌面选择" },
        thoughtLevel: { type: "string", description: "modelRef 的推理档位，默认 high" },
        wait: { type: "boolean", description: "默认 false=受理即返回 resultPath；true=阻塞等跑完" },
        claimTimeoutMs: { type: "number", description: "等认领超时" },
        sessionTimeoutMs: { type: "number", description: "等派发确认超时" },
      },
      required: ["sessionId", "prompt"],
    },
  },
  {
    name: "session_model",
    description: "设置会话保存的模型选择：写 db.sqlite session_entry(runtime/model_selection) 真源 + tasks-index.model 侧栏展示。桌面端 UI、人工续跑与原生注入（session_send）均按此模型跑。Set the session's saved model selection.",
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
  {
    name: "plan_start",
    description: "查看 ZCode start plan 账号面（zcode.z.ai，凭据取自桌面端登录 ~/.zcode/v2/credentials.json）：生效套餐列表、各模型余额桶（total/used/remaining/到期）、当前可领取套餐。View ZCode start-plan subscriptions, balances and claimable plans.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "plan_reset_card",
    description: "智谱重置卡。**须桌面端登录**：凭据取自 ~/.zcode/v2/credentials.json 的 zcodejwttoken，账户绑定该登录态——session.json 里的 API key 既查不了本功能也不能切换账户。action=status 查看库存（5小时窗/周窗逐卡 expire_at、最近使用历史）；action=use 消费一张立即重置对应额度窗口（需 confirm=true，resetType=FIVE_HOUR|WEEK，幂等键自动生成）；action=opportunity 手动触发一次领卡机会（客户端本就自动轮询，勿循环调用）；action=history_read 标记历史已读。Coding-plan reset cards (desktop login REQUIRED; API keys cannot query or switch accounts): status / use (confirm-gated) / opportunity / history read.",
    inputSchema: {
      type: "object",
      properties: {
        action: { type: "string", enum: ["status", "use", "opportunity", "history_read"], description: "默认 status" },
        resetType: { type: "string", enum: ["FIVE_HOUR", "WEEK"], description: "use 时必选语义：重置哪个窗口，默认 FIVE_HOUR" },
        confirm: { type: "boolean", description: "use 时必填 true（真实消费一张卡）" },
        idempotencyKey: { type: "string", description: "可选幂等键（≤64字符），缺省 UUID" },
      },
    },
  },
  {
    name: "plan_claim",
    description: "start plan 领取面：action=preview 列出当前可领取的活动套餐（可配合定时任务轮询）；action=claim 按 plan_id 领取——服务端强制阿里云人机验证，需传 captchaVerifyParam（无感验证抓参法见 README「自动领取」；缺 X-Aliyun-Captcha-Verify-Region 头必 3007）。Manual-claim plan preview / claim (captcha-gated).",
    inputSchema: {
      type: "object",
      properties: {
        action: { type: "string", enum: ["preview", "claim"], description: "默认 preview" },
        planId: { type: "string", description: "claim 时的 plan_id（preview 返回）" },
        captchaVerifyParam: { type: "string", description: "claim 必填：阿里云验证码 verify 参数（一次性，有时效）" },
        captchaRegion: { type: "string", description: "验证码 region，默认 cn（对应 X-Aliyun-Captcha-Verify-Region 头，必带）" },
      },
    },
  },
  {
    name: "plan_switch",
    description: "查看/切换 session.json 额度 keys 的 current（plan_quota 缺省查询的 keyAlias）。无参=列出全部 key 与 current；带 keyAlias=切换。只影响本 MCP 额度面，不改 ZCode 客户端 provider。Switch the default quota key.",
    inputSchema: {
      type: "object",
      properties: { keyAlias: { type: "string", description: "要置为 current 的别名；缺省只列出" } },
    },
  },
  {
    name: "provider_list",
    description: "列出模型 Provider：personal=「管理模型」面板真源 v2/provider_config.json（自定义 provider，key 脱敏显示）；registry=桌面端解析后的模型注册表（只读）。List model providers (personal config + resolved registry).",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "provider_add",
    description: "新增自定义模型 Provider（写入 provider_config.json，原文件先备份）：templateId 走模板（如 zai-api，只填 apiKey）；否则自定义型必填 baseUrl+models。之后 session_model 用 \"<providerId>/<modelId>\" 即可把会话路由到这把 key。Add a custom model provider (backed up, atomic write).",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "providerId，缺省自动 UUID；不能用 builtin:/account: 前缀" },
        name: { type: "string", description: "显示名（如「昵称」）" },
        templateId: { type: "string", description: "模板型必填（如 zai-api）：模型/endpoint 继承模板" },
        apiKey: { type: "string", description: "必填：该 provider 的 API key" },
        accessType: { type: "string", enum: ["api-key", "zhipu-coding-plan-api-key"], description: "默认 api-key" },
        baseUrl: { type: "string", description: "自定义型必填（如 https://api.z.ai/api/anthropic）" },
        apiType: { type: "string", enum: ["anthropic-messages", "openai-chat-completions", "openai-responses"], description: "默认 anthropic-messages" },
        models: { type: "array", items: { type: "string" }, description: "自定义型必填：模型 id 列表" },
        modelOrder: { type: "array", items: { type: "string" }, description: "展示顺序，缺省同 models" },
      },
      required: ["apiKey"],
    },
  },
  {
    name: "provider_set",
    description: "更新自定义模型 Provider（换 key、改 baseUrl/apiType、改模型列表、改名、启停）。写入前备份。Update a custom model provider (rotate apiKey, edit models, enable/disable).",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "providerId（personal 规则）" },
        name: { type: "string", description: "显示名" },
        enabled: { type: "boolean", description: "false=停用（不删）" },
        apiKey: { type: "string", description: "新 key（轮换）" },
        baseUrl: { type: "string", description: "endpoint" },
        apiType: { type: "string", enum: ["anthropic-messages", "openai-chat-completions", "openai-responses"] },
        models: { type: "array", items: { type: "string" } },
        modelOrder: { type: "array", items: { type: "string" } },
      },
      required: ["id"],
    },
  },
  {
    name: "provider_remove",
    description: "删除自定义模型 Provider（confirm 门控；连带清理其模型规则，原文件备份）。在飞会话不受影响。Remove a custom model provider (confirm-gated, backed up).",
    inputSchema: {
      type: "object",
      properties: {
        id: { type: "string", description: "providerId" },
        confirm: { type: "boolean", description: "必填 true 才真删" },
      },
      required: ["id"],
    },
  },
];

function dispatch(name, args) {
  switch (name) {
    case "session_list": return Promise.resolve(listSessions(args || {}));
    case "session_get": return Promise.resolve(sessionGet(args || {}));
    case "session_messages": return Promise.resolve(sessionMessages(args || {}));
    case "session_export": return Promise.resolve(sessionExport(args || {}));
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
    case "plan_start": return planStartView(args || {});
    case "plan_reset_card": return planResetCard(args || {});
    case "plan_claim": return planClaim(args || {});
    case "plan_switch": return Promise.resolve(planSwitch(args || {}));
    case "provider_list": return Promise.resolve(providerList());
    case "provider_add": return Promise.resolve(providerAdd(args || {}));
    case "provider_set": return Promise.resolve(providerSet(args || {}));
    case "provider_remove": return Promise.resolve(providerRemove(args || {}));
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
