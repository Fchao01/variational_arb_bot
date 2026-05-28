// Variational CDP forwarder — orchestration only.
//
// Attaches the CDP debugger to a variational.io tab, captures allowlisted
// REST responses and WebSocket frames, and forwards them to local receiver
// sockets. Also runs a command channel that executes order/quote actions
// inside the page. API logic lives in var_api.js, socket plumbing in
// forward_socket.js, optional traffic recording in recorder.js.

import { buildScript } from "./var_api.js";
import { ForwardSocket, CommandClient } from "./forward_socket.js";
import { recorder } from "./recorder.js";

const DEBUGGER_VERSION = "1.3";
const AUTO_RELOAD_COOLDOWN_MS = 5000;

// Self-heal: if no allowlisted WS frame is forwarded for FEED_STALE_MS while
// forwarding is active, the page's market-data WS is stuck — reload the tab to
// resync. Independent of the python engine, so it also heals a freeze that
// happens mid-round (while python is busy inside trader.run_round and its own
// stale watchdog can't run). Rate-limited by SELF_HEAL_COOLDOWN_MS.
const FEED_WATCHDOG_ALARM = "feedWatchdog";
const FEED_WATCHDOG_PERIOD_MIN = 0.5;   // 30s — the MV3 alarm floor
const FEED_STALE_MS = 20000;
const SELF_HEAL_COOLDOWN_MS = 30000;

const DEFAULT_CONFIG = {
  wsEndpoint: "ws://127.0.0.1:8766",
  restEndpoint: "ws://127.0.0.1:8767",
  commandEndpoint: "ws://127.0.0.1:8768",
  domainFilter: "variational",
  restAllowlist: ["https://omni.variational.io/api/quotes/indicative"],
  wsAllowlist: [
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/events",
    "wss://omni-ws-server.prod.ap-northeast-1.variational.io/portfolio"
  ]
};

const state = {
  active: false,
  attachedTabId: null,
  config: { ...DEFAULT_CONFIG },
  configLoaded: false,
  reqQty: new Map(),          // requestId -> {qty, underlying} for allowlisted REST
  pendingResponses: new Map(), // requestId -> response meta awaiting body
  websocketMeta: new Map(),    // requestId -> {url, matchedPattern}
  lastError: null,
  lastAutoReloadAt: 0,
  lastFrameAt: 0,              // ms of last forwarded allowlisted WS frame
  pendingCommands: 0          // in-flight runInPage calls (don't reload mid-order)
};

const nowIso = () => new Date().toISOString();

// ---- config ----

function asStringOrDefault(value, fallback) {
  const s = typeof value === "string" ? value.trim() : "";
  return s.length > 0 ? s : fallback;
}

function sanitizeAllowlist(value, fallback) {
  if (!Array.isArray(value)) return [...fallback];
  const cleaned = value
    .filter((x) => typeof x === "string")
    .map((x) => x.trim())
    .filter((x) => x.length > 0);
  return cleaned.length ? cleaned : [...fallback];
}

function sanitizeConfig(incoming = {}) {
  // REST allowlist is locked to the indicative endpoint: the receiver only
  // understands that body shape, anything else is noise.
  const rest = sanitizeAllowlist(incoming.restAllowlist, DEFAULT_CONFIG.restAllowlist)
    .filter((x) => x === DEFAULT_CONFIG.restAllowlist[0]);
  return {
    wsEndpoint: asStringOrDefault(incoming.wsEndpoint, DEFAULT_CONFIG.wsEndpoint),
    restEndpoint: asStringOrDefault(incoming.restEndpoint, DEFAULT_CONFIG.restEndpoint),
    commandEndpoint: asStringOrDefault(incoming.commandEndpoint, DEFAULT_CONFIG.commandEndpoint),
    domainFilter: asStringOrDefault(incoming.domainFilter, DEFAULT_CONFIG.domainFilter),
    restAllowlist: rest.length ? rest : [...DEFAULT_CONFIG.restAllowlist],
    wsAllowlist: sanitizeAllowlist(incoming.wsAllowlist, DEFAULT_CONFIG.wsAllowlist)
  };
}

async function ensureConfigLoaded() {
  if (state.configLoaded) return;
  const stored = await chrome.storage.local.get("forwarderConfig");
  state.config = sanitizeConfig(stored.forwarderConfig);
  state.configLoaded = true;
}

// ---- url matching ----

function matchesDomainFilter(url) {
  const filter = state.config.domainFilter.trim().toLowerCase();
  return !filter || (url || "").toLowerCase().includes(filter);
}

function originPath(rawUrl) {
  try {
    const u = new URL(rawUrl);
    return { originPath: `${u.origin}${u.pathname}`, full: u.toString() };
  } catch {
    return { originPath: rawUrl, full: rawUrl };
  }
}

function matchPattern(url, patterns) {
  if (!patterns || !patterns.length) return null;
  const t = originPath(url);
  for (const p of patterns) {
    if (t.originPath === originPath(p).originPath || t.full.startsWith(p)) return p;
  }
  return null;
}

// ---- CDP helpers ----

function sendDebuggerCommand(tabId, method, params = {}) {
  return new Promise((resolve, reject) => {
    chrome.debugger.sendCommand({ tabId }, method, params, (result) => {
      const err = chrome.runtime.lastError;
      if (err) reject(new Error(err.message));
      else resolve(result || {});
    });
  });
}

function debuggerAttach(tabId) {
  return new Promise((resolve, reject) => {
    chrome.debugger.attach({ tabId }, DEBUGGER_VERSION, () => {
      const err = chrome.runtime.lastError;
      if (err) reject(new Error(err.message));
      else resolve();
    });
  });
}

function debuggerDetach(tabId) {
  return new Promise((resolve) => {
    chrome.debugger.detach({ tabId }, () => resolve());
  });
}

recorder.attach({ sendDebuggerCommand, matchesDomainFilter });

// ---- status / sockets ----

function getStatus() {
  return {
    active: state.active,
    attachedTabId: state.attachedTabId,
    config: state.config,
    sockets: {
      websocket: wsForwarder.status,
      rest: restForwarder.status,
      command: commandClient.status
    },
    recording: recorder.recording,
    recordCount: recorder.count,
    lastError: state.lastError
  };
}

function notifyStatus() {
  chrome.runtime.sendMessage({ event: "status", status: getStatus() }).catch(() => {});
}

const socketDeps = { isActive: () => state.active, onStatus: notifyStatus };

const wsForwarder = new ForwardSocket("websocket", {
  ...socketDeps,
  getEndpoint: () => state.config.wsEndpoint,
  onConnected: () => autoReloadAttachedTab("forward receiver connected")
});
const restForwarder = new ForwardSocket("rest", {
  ...socketDeps,
  getEndpoint: () => state.config.restEndpoint
});
const commandClient = new CommandClient({
  ...socketDeps,
  getEndpoint: () => state.config.commandEndpoint,
  runInPage,
  reloadTab: () => forceReloadAttachedTab("python RELOAD command")
});

async function runInPage(action, params) {
  if (!state.active || state.attachedTabId == null) {
    return { ok: false, error: "No attached tab; start forwarding first." };
  }
  state.pendingCommands += 1;
  try {
    const res = await sendDebuggerCommand(state.attachedTabId, "Runtime.evaluate", {
      expression: buildScript(action, params),
      awaitPromise: true,
      returnByValue: true,
      userGesture: true
    });
    if (res.exceptionDetails) {
      const ex = res.exceptionDetails;
      const txt = (ex.exception && (ex.exception.description || ex.exception.value)) || ex.text || "Page evaluation error";
      return { ok: false, error: String(txt) };
    }
    const value = res.result && res.result.value;
    if (value && typeof value === "object") return value;
    return { ok: false, error: "Unexpected page result.", raw: value };
  } catch (error) {
    return { ok: false, error: error.message };
  } finally {
    if (state.pendingCommands > 0) state.pendingCommands -= 1;
  }
}

// ---- tab auto-reload (re-establishes the page WS once the receiver is up) ----

function autoReloadAttachedTab(reason) {
  if (!state.active || state.attachedTabId == null) return;
  const now = Date.now();
  if (now - state.lastAutoReloadAt < AUTO_RELOAD_COOLDOWN_MS) return;
  state.lastAutoReloadAt = now;
  chrome.tabs.reload(state.attachedTabId, {}, () => {
    state.lastError = chrome.runtime.lastError
      ? `Auto reload failed (${reason}): ${chrome.runtime.lastError.message}`
      : null;
    notifyStatus();
  });
}

// Forced reload — used by the python RELOAD command and the feed watchdog.
// Bypasses the cooldown gate (callers self-pace), resolves with an {ok}
// result, and resets the frame/reload clocks so the watchdog grants the
// reloaded page time to re-quote before firing again.
function forceReloadAttachedTab(reason) {
  if (!state.active || state.attachedTabId == null) {
    return Promise.resolve({ ok: false, error: "no attached tab; forwarding not started" });
  }
  const tabId = state.attachedTabId;
  const now = Date.now();
  state.lastAutoReloadAt = now;
  state.lastFrameAt = now;  // reloaded page hasn't framed yet; don't re-trip immediately
  return new Promise((resolve) => {
    chrome.tabs.reload(tabId, {}, () => {
      const err = chrome.runtime.lastError;
      state.lastError = err ? `Reload failed (${reason}): ${err.message}` : null;
      notifyStatus();
      resolve(err ? { ok: false, error: err.message } : { ok: true, reloaded: true, reason });
    });
  });
}

// ---- feed watchdog (self-heal a stuck market-data WS) ----

function checkFeedWatchdog() {
  if (!state.active || state.attachedTabId == null) return;
  if (!state.lastFrameAt) return;            // no baseline yet (no frame seen)
  if (state.pendingCommands > 0) return;     // a command (maybe an order) is in flight
  const now = Date.now();
  if (now - state.lastFrameAt < FEED_STALE_MS) return;
  if (now - state.lastAutoReloadAt < SELF_HEAL_COOLDOWN_MS) return;
  const staleS = Math.round((now - state.lastFrameAt) / 1000);
  forceReloadAttachedTab(`self-heal: no WS frame for ${staleS}s`).then(notifyStatus);
}

// ---- REST forwarding (with paired request qty) ----

// Parse the indicative request body so the forwarded response carries the
// qty the quote was priced for — the quote_id is bound to that qty and the
// engine needs it to size the hedge. (The response body does not echo it.)
function parseReqMeta(postData) {
  try {
    const b = JSON.parse(postData);
    return { qty: b.qty != null ? String(b.qty) : null, underlying: b.instrument?.underlying ?? null };
  } catch {
    return { qty: null, underlying: null };
  }
}

function trackRequest(params) {
  const url = params.request?.url || "";
  if (!matchesDomainFilter(url) || !matchPattern(url, state.config.restAllowlist)) return;
  const postData = params.request?.postData;
  if (postData != null) state.reqQty.set(params.requestId, parseReqMeta(postData));
}

function trackResponse(params) {
  const url = params.response?.url || "";
  if (!url || !matchesDomainFilter(url)) return;
  if (params.type !== "Fetch" && params.type !== "XHR") return;
  const matchedPattern = matchPattern(url, state.config.restAllowlist);
  if (!matchedPattern) return;
  state.pendingResponses.set(params.requestId, {
    requestId: params.requestId,
    url,
    status: params.response.status,
    statusText: params.response.statusText,
    mimeType: params.response.mimeType,
    headers: params.response.headers,
    type: params.type,
    matchedPattern,
    capturedAt: nowIso()
  });
}

async function forwardResponseBody(requestId, encodedDataLength) {
  const meta = state.pendingResponses.get(requestId);
  if (!meta || state.attachedTabId == null) return;
  state.pendingResponses.delete(requestId);
  const req = state.reqQty.get(requestId) || { qty: null, underlying: null };
  state.reqQty.delete(requestId);
  try {
    const result = await sendDebuggerCommand(state.attachedTabId, "Network.getResponseBody", { requestId });
    restForwarder.send({
      kind: "rest_response",
      requestId,
      timestamp: nowIso(),
      encodedDataLength,
      ...meta,
      reqQty: req.qty,
      reqUnderlying: req.underlying,
      body: result.body ?? "",
      base64Encoded: Boolean(result.base64Encoded)
    });
  } catch (error) {
    restForwarder.send({
      kind: "rest_response_error", requestId, timestamp: nowIso(), ...meta, error: error.message
    });
  }
}

function forwardWebSocketFrame(direction, params) {
  const meta = state.websocketMeta.get(params.requestId);
  if (!meta) return;
  // Inbound frames are the live market-data heartbeat the feed watchdog tracks.
  if (direction === "received") state.lastFrameAt = Date.now();
  wsForwarder.send({
    kind: "ws_frame",
    direction,
    requestId: params.requestId,
    url: meta.url,
    matchedPattern: meta.matchedPattern || "",
    timestamp: nowIso(),
    opcode: params.response?.opcode,
    mask: params.response?.mask,
    payloadData: params.response?.payloadData ?? ""
  });
}

// ---- CDP event router ----

async function handleDebuggerEvent(source, method, params) {
  if (!state.active || source.tabId !== state.attachedTabId) return;

  recorder.onEvent(method, params, source.tabId).catch((e) => {
    state.lastError = `Record hook failed: ${e.message}`;
  });

  if (method === "Network.requestWillBeSent") {
    trackRequest(params);
  } else if (method === "Network.responseReceived") {
    trackResponse(params);
  } else if (method === "Network.loadingFinished") {
    await forwardResponseBody(params.requestId, params.encodedDataLength);
  } else if (method === "Network.loadingFailed") {
    state.pendingResponses.delete(params.requestId);
    state.reqQty.delete(params.requestId);
  } else if (method === "Network.webSocketCreated") {
    const matchedPattern = matchPattern(params.url, state.config.wsAllowlist);
    if (matchesDomainFilter(params.url) && matchedPattern) {
      state.websocketMeta.set(params.requestId, { url: params.url, matchedPattern });
    }
  } else if (method === "Network.webSocketClosed") {
    const meta = state.websocketMeta.get(params.requestId);
    if (meta) {
      wsForwarder.send({
        kind: "ws_closed", requestId: params.requestId, url: meta.url,
        matchedPattern: meta.matchedPattern || "", timestamp: nowIso()
      });
      state.websocketMeta.delete(params.requestId);
    }
  } else if (method === "Network.webSocketFrameReceived") {
    forwardWebSocketFrame("received", params);
  } else if (method === "Network.webSocketFrameSent") {
    forwardWebSocketFrame("sent", params);
  } else if (method === "Network.webSocketFrameError") {
    const meta = state.websocketMeta.get(params.requestId);
    if (meta) {
      wsForwarder.send({
        kind: "ws_frame_error", requestId: params.requestId, url: meta.url,
        matchedPattern: meta.matchedPattern || "", timestamp: nowIso(),
        errorMessage: params.errorMessage || "Unknown WebSocket frame error"
      });
    }
  }
}

// ---- lifecycle ----

function cleanupForwardingState() {
  state.active = false;
  state.attachedTabId = null;
  state.lastAutoReloadAt = 0;
  state.lastFrameAt = 0;
  state.pendingCommands = 0;
  state.reqQty.clear();
  state.pendingResponses.clear();
  state.websocketMeta.clear();
  wsForwarder.close();
  restForwarder.close();
  commandClient.close();
}

async function startForwarding(tabId) {
  await ensureConfigLoaded();
  if (state.active) return getStatus();

  let targetTabId = tabId;
  if (targetTabId == null) {
    const tabs = await chrome.tabs.query({ active: true, currentWindow: true });
    if (!tabs.length || tabs[0].id == null) throw new Error("No active tab found.");
    targetTabId = tabs[0].id;
  }

  await debuggerAttach(targetTabId);
  try {
    await sendDebuggerCommand(targetTabId, "Network.enable");
  } catch (error) {
    await debuggerDetach(targetTabId);
    throw error;
  }

  state.active = true;
  state.attachedTabId = targetTabId;
  state.lastError = null;
  state.lastFrameAt = 0;
  state.pendingCommands = 0;
  wsForwarder.connect();
  restForwarder.connect();
  commandClient.connect();
  chrome.alarms.create(FEED_WATCHDOG_ALARM, { periodInMinutes: FEED_WATCHDOG_PERIOD_MIN });
  autoReloadAttachedTab("forwarder started");
  notifyStatus();
  return getStatus();
}

async function stopForwarding() {
  const attachedTabId = state.attachedTabId;
  cleanupForwardingState();
  chrome.alarms.clear(FEED_WATCHDOG_ALARM);
  if (attachedTabId != null) await debuggerDetach(attachedTabId);
  notifyStatus();
  return getStatus();
}

// ---- chrome event wiring ----

chrome.debugger.onEvent.addListener((source, method, params) => {
  handleDebuggerEvent(source, method, params).catch((error) => {
    state.lastError = `CDP event handling failed: ${error.message}`;
    notifyStatus();
  });
});

chrome.debugger.onDetach.addListener((source) => {
  if (source.tabId !== state.attachedTabId) return;
  state.lastError = "Debugger detached";
  cleanupForwardingState();
  chrome.alarms.clear(FEED_WATCHDOG_ALARM);
  notifyStatus();
});

chrome.alarms.onAlarm.addListener((alarm) => {
  if (alarm.name === FEED_WATCHDOG_ALARM) checkFeedWatchdog();
});

const messageHandlers = {
  getStatus: () => ({ ok: true, status: getStatus() }),
  updateConfig: async (m) => {
    state.config = sanitizeConfig(m.config);
    await chrome.storage.local.set({ forwarderConfig: state.config });
    if (state.active) {
      wsForwarder.restart();
      restForwarder.restart();
      commandClient.restart();
    }
    notifyStatus();
    return { ok: true, status: getStatus() };
  },
  start: async (m) => ({ ok: true, status: await startForwarding(m.tabId ?? null) }),
  stop: async () => ({ ok: true, status: await stopForwarding() }),
  startRecord: () => {
    if (!state.active) return { ok: false, error: "Start forwarding first (need attached tab)." };
    recorder.start();
    notifyStatus();
    return { ok: true, status: getStatus() };
  },
  stopRecord: () => {
    recorder.stop();
    notifyStatus();
    return { ok: true, status: getStatus() };
  },
  getRecording: () => ({ ok: true, recording: recorder.buffer }),
  clearRecording: () => {
    recorder.clear();
    notifyStatus();
    return { ok: true, status: getStatus() };
  }
};

chrome.runtime.onMessage.addListener((message, _sender, sendResponse) => {
  (async () => {
    await ensureConfigLoaded();
    const handler = messageHandlers[message.action];
    if (!handler) return { ok: false, error: `Unknown action: ${message.action}` };
    return handler(message);
  })()
    .then(sendResponse)
    .catch((error) => sendResponse({ ok: false, error: error.message }));
  return true;
});

// ---- auto-start: attach to a Variational tab as soon as one finishes loading,
// so a fresh Chrome launch needs no manual "Start" click (forwarding, then the
// engine's command channel, come up on their own once the page is there). ----

function isVariationalUrl(url) {
  return typeof url === "string" && url.toLowerCase().includes("variational.io");
}

async function maybeAutoStart(tabId, url) {
  if (state.active || tabId == null || !isVariationalUrl(url)) return;
  try {
    await startForwarding(tabId);
  } catch (error) {
    state.lastError = `Auto-start failed: ${error.message}`;
    notifyStatus();
  }
}

async function autoStartFromExistingTabs() {
  await ensureConfigLoaded();
  if (state.active) return;
  try {
    const tabs = await chrome.tabs.query({ url: "*://*.variational.io/*" });
    const tab = tabs.find((t) => t.id != null);
    if (tab) await maybeAutoStart(tab.id, tab.url);
  } catch (error) {
    state.lastError = `Auto-start scan failed: ${error.message}`;
  }
}

chrome.tabs.onUpdated.addListener((tabId, changeInfo, tab) => {
  if (changeInfo.status === "complete") maybeAutoStart(tabId, tab && tab.url);
});

chrome.runtime.onStartup.addListener(() => {
  autoStartFromExistingTabs().catch(() => {});
});

chrome.runtime.onInstalled.addListener(() => {
  ensureConfigLoaded().then(autoStartFromExistingTabs).catch(() => {});
});
