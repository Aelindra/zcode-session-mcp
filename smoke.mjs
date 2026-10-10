#!/usr/bin/env node
// zcode-session-mcp 冒烟测试：std 方式起 server，走 JSON-RPC 全工具面。
// 读路径用真实本机数据（只读）；写路径在临时目录造测试会话并在结束时硬删清理。
import { spawn } from "node:child_process";
import { mkdirSync, rmSync, existsSync, writeFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import process from "node:process";

const SERVER = join(dirname(fileURLToPath(import.meta.url)), "server.mjs");
const NODE_EXE = process.execPath;
const TEST_DIR = join(process.env.USERPROFILE || process.env.HOME || ".", ".zcode", "tmp", "session-mcp-smoke");

let proc = null;
let buf = "";
let nextId = 1;
const pending = new Map();

function start() {
  proc = spawn(NODE_EXE, [SERVER], { stdio: ["pipe", "pipe", "pipe"] });
  proc.stderr.on("data", (d) => process.stderr.write("[server] " + d));
  proc.stdout.setEncoding("utf8");
  proc.stdout.on("data", (chunk) => {
    buf += chunk;
    let idx;
    while ((idx = buf.indexOf("\n")) >= 0) {
      const line = buf.slice(0, idx).trim();
      buf = buf.slice(idx + 1);
      if (!line) continue;
      let msg;
      try { msg = JSON.parse(line); } catch { continue; }
      const p = pending.get(msg.id);
      if (p) { pending.delete(msg.id); p(msg); }
    }
  });
}

function rpc(method, params, timeoutMs = 60000) {
  return new Promise((resolve, reject) => {
    const id = nextId++;
    const timer = setTimeout(() => { pending.delete(id); reject(new Error(`rpc timeout: ${method}`)); }, timeoutMs);
    pending.set(id, (msg) => { clearTimeout(timer); resolve(msg); });
    proc.stdin.write(JSON.stringify({ jsonrpc: "2.0", id, method, params }) + "\n");
  });
}

async function call(name, args, timeoutMs) {
  const res = await rpc("tools/call", { name, arguments: args || {} }, timeoutMs);
  let body = null;
  try { body = JSON.parse(res.result.content[0].text); } catch { /* fallthrough */ }
  return { raw: res.result, body, isError: Boolean(res.result.isError) };
}

function check(label, cond, detail) {
  const okMark = cond ? "PASS" : "FAIL";
  console.log(`${okMark}  ${label}${cond ? "" : "  <<<< " + JSON.stringify(detail).slice(0, 400)}`);
  if (!cond) process.exitCode = 1;
}

async function main() {
  mkdirSync(TEST_DIR, { recursive: true });
  start();
  await new Promise((r) => setTimeout(r, 800));

  const init = await rpc("initialize", { protocolVersion: "2024-11-05" });
  check("initialize", init.result && init.result.serverInfo.name === "zcode-session-mcp", init);

  const tl = await rpc("tools/list", {});
  const names = tl.result.tools.map((t) => t.name);
  check("tools/list 18个", names.length === 18, names);

  // ---- 只读面（真实数据）
  const list = await call("session_list", { limit: 5 });
  check("session_list ok", list.body && list.body.ok && list.body.count > 0, list.body);
  const target = list.body.sessions.find((s) => !s.is_subagent) || list.body.sessions[0];
  check("session_list 有字段", target && "pinned" in target && "active_now" in target, target);

  const get = await call("session_get", { sessionId: target.session_id });
  check("session_get ok", get.body && get.body.ok && get.body.activity && "running_heuristic" in get.body, get.body && get.body.error);

  const msgs = await call("session_messages", { sessionId: target.session_id, limit: 5 });
  check("session_messages ok", msgs.body && msgs.body.ok && Array.isArray(msgs.body.messages), msgs.body && msgs.body.error);
  if (msgs.body && msgs.body.messages.length) {
    const m = msgs.body.messages[0];
    check("session_messages compact 形状", "role" in m && "message_id" in m, m);
  }

  const search = await call("session_search", { keyword: (target.title || "zcode").slice(0, 4) });
  check("session_search ok", search.body && search.body.ok && search.body.count >= 1, search.body && search.body.error);

  const status = await call("session_status", { sessionId: target.session_id });
  check("session_status ok", status.body && status.body.ok && typeof status.body.summary === "string", status.body && status.body.error);

  // ---- CRUD 生命周期（临时会话）
  const sid = `sess_smoke-${Date.now().toString(36)}`;
  const created = await call("session_create", { directory: TEST_DIR, title: "smoke 测试会话", mode: "build" });
  check("session_create ok", created.body && created.body.ok, created.body && created.body.error);
  const createdSid = created.body && created.body.session_id;

  if (createdSid) {
    const g1 = await call("session_get", { sessionId: createdSid });
    check("create 后 get 可见", g1.body && g1.body.ok && g1.body.activity.msg_count === 0, g1.body && g1.body.error);
    check("create project_id 派生", g1.body && g1.body.session && /^proj_/.test(g1.body.session.project_id || ""), g1.body && g1.body.session);

    const renamed = await call("session_rename", { sessionId: createdSid, title: "smoke 改名后的标题" });
    check("session_rename ok", renamed.body && renamed.body.ok, renamed.body && renamed.body.error);
    const g2 = await call("session_get", { sessionId: createdSid });
    check("rename 生效(custom)", g2.body && g2.body.session.title_source === "custom" && g2.body.session.title === "smoke 改名后的标题", g2.body && g2.body.session);

    const pin = await call("session_pin", { sessionId: createdSid, pinned: true });
    check("session_pin ok", pin.body && pin.body.ok, pin.body && pin.body.error);

    const model = await call("session_model", { sessionId: createdSid, modelRef: "GLM-5.2" });
    check("session_model ok(解析+写入)", model.body && model.body.ok && model.body.modelId, model.body && model.body.error);
    const g3 = await call("session_get", { sessionId: createdSid });
    check("model_selection 真源可读", g3.body && g3.body.model_selection && g3.body.model_selection.modelId === (model.body ? model.body.modelId : "?"), g3.body && g3.body.model_selection);

    const arch = await call("session_archive", { sessionId: createdSid, archived: true });
    check("session_archive ok", arch.body && arch.body.ok, arch.body && arch.body.error);
    const g4 = await call("session_get", { sessionId: createdSid });
    check("archive 双写(time_archived+taskIndex)", g4.body && g4.body.session.time_archived && g4.body.taskIndex && g4.body.taskIndex.archived === true, g4.body && (g4.body.session, g4.body.taskIndex));
    const unarch = await call("session_archive", { sessionId: createdSid, archived: false });
    check("取消归档 ok", unarch.body && unarch.body.ok, unarch.body && unarch.body.error);

    const softDel = await call("session_delete", { sessionId: createdSid });
    check("软删 ok(tombstone)", softDel.body && softDel.body.ok && softDel.body.hard === false, softDel.body && softDel.body.error);
    const g5 = await call("session_get", { sessionId: createdSid });
    check("软删后 db 行仍在", g5.body && g5.body.ok && g5.body.taskIndex && g5.body.taskIndex.deleted === true, g5.body && g5.body.taskIndex);

    const hardRefused = await call("session_delete", { sessionId: createdSid, hard: true });
    check("硬删无 confirm 被拒", hardRefused.isError && hardRefused.body.error.code === "HARD_DELETE_REQUIRES_CONFIRM", hardRefused.body);

    const hardDel = await call("session_delete", { sessionId: createdSid, hard: true, confirm: true, files: true });
    check("硬删 ok", hardDel.body && hardDel.body.ok && hardDel.body.deletedRows && hardDel.body.deletedRows.session === 1, hardDel.body && hardDel.body.error);
    const g6 = await call("session_get", { sessionId: createdSid });
    check("硬删后 db 无行", g6.isError && g6.body.error.code === "SESSION_NOT_FOUND", g6.body);
  }

  // ---- 错误面
  const badGet = await call("session_get", { sessionId: "sess_no-such-session" });
  check("不存在的会话报 SESSION_NOT_FOUND", badGet.isError && badGet.body.error.code === "SESSION_NOT_FOUND", badGet.body);
  const badDel = await call("session_delete", { sessionId: target.session_id });
  check("活跃会话软删需 confirm", badDel.isError && badDel.body.error.code === "REFUSED_ACTIVE", badDel.body);

  // ---- 额度（可选能力：有 legacy 配置则真查，没有只验证结构化报错）
  const quota = await call("plan_quota", {});
  check("plan_quota 结构化返回", quota.body && (quota.body.ok || quota.body.error), quota.body);

  // ---- 额度面（start plan / 重置卡 / 领取 / 切 key；凭据缺失时全部结构化报错）
  const pstart = await call("plan_start", {});
  check("plan_start 结构化返回", pstart.body && (pstart.body.ok || pstart.body.error), pstart.body);
  if (pstart.body && pstart.body.ok) {
    check("plan_start 套餐/余额形状", Array.isArray(pstart.body.plans) && Array.isArray(pstart.body.balances) && typeof pstart.body.summary === "string", pstart.body.summary);
  }

  const rcStatus = await call("plan_reset_card", { action: "status" });
  check("plan_reset_card status 结构化", rcStatus.body && (rcStatus.body.ok || rcStatus.body.error), rcStatus.body);
  if (rcStatus.body && rcStatus.body.ok) {
    check("plan_reset_card 库存形状", rcStatus.body.availableFiveHourResets && typeof rcStatus.body.availableFiveHourResets.count === "number", rcStatus.body);
  }

  const rcRefused = await call("plan_reset_card", { action: "use" });
  check("plan_reset_card use 无 confirm 被拒", rcRefused.isError && rcRefused.body.error.code === "RESET_USE_REQUIRES_CONFIRM", rcRefused.body);

  const rcBad = await call("plan_reset_card", { action: "nonsense" });
  check("plan_reset_card 非法 action 报 BAD_ARGS", rcBad.isError && rcBad.body.error.code === "BAD_ARGS", rcBad.body);

  const pclaim = await call("plan_claim", { action: "preview" });
  check("plan_claim preview 结构化", pclaim.body && (pclaim.body.ok || pclaim.body.error), pclaim.body);

  const pclaimNoCaptcha = await call("plan_claim", { action: "claim", planId: "smoke-fake-plan" });
  check("plan_claim claim 无验证码被拒", pclaimNoCaptcha.isError && pclaimNoCaptcha.body.error.code === "CLAIM_CAPTCHA_REQUIRED", pclaimNoCaptcha.body);

  const pswList = await call("plan_switch", {});
  check("plan_switch 列表结构化", pswList.body && pswList.body.ok && Array.isArray(pswList.body.keys), pswList.body);
  if (pswList.body && pswList.body.ok && pswList.body.currentKeyAlias) {
    const pswBack = await call("plan_switch", { keyAlias: pswList.body.currentKeyAlias });
    check("plan_switch 切回 current 幂等", pswBack.body && pswBack.body.ok && pswBack.body.currentKeyAlias === pswList.body.currentKeyAlias, pswBack.body);
  }
  const pswBad = await call("plan_switch", { keyAlias: "smoke-no-such-key" });
  check("plan_switch 未知别名报 KEY_NOT_CONFIGURED", pswBad.isError && pswBad.body.error.code === "KEY_NOT_CONFIGURED", pswBad.body);

  console.log(process.exitCode ? "\n== 有失败项 ==" : "\n== 全部通过 ==");
  proc.kill();
}

main().catch((e) => { console.error("smoke 异常:", e); try { proc && proc.kill(); } catch { /* noop */ } process.exit(1); });
