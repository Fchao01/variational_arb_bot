// Optional diagnostic recorder. Captures raw CDP request/response/ws
// traffic into an in-memory ring buffer the popup can export as JSONL.
// Purely additive: the forwarder never reads this, and deleting this
// file (plus its import in background.js) removes recording entirely.

const MAX_ENTRIES = 5000;

function redactHeaders(headers) {
  const out = {};
  for (const [k, v] of Object.entries(headers || {})) {
    const lk = k.toLowerCase();
    if (lk === "authorization" || lk === "cookie" || lk === "set-cookie") {
      const sv = String(v);
      out[k] = `<<redacted len=${sv.length} prefix=${sv.slice(0, 8)}...>>`;
    } else {
      out[k] = v;
    }
  }
  return out;
}

const nowIso = () => new Date().toISOString();

class Recorder {
  constructor() {
    this.recording = false;
    this.buffer = [];
    this.reqMeta = new Map();   // requestId -> {url, method, headers, postData, status...}
    this.wsMeta = new Map();    // requestId -> {url}
    // Injected by background.js once: CDP command runner + domain predicate.
    this._sendDebuggerCommand = null;
    this._matchesDomainFilter = () => true;
  }

  attach({ sendDebuggerCommand, matchesDomainFilter }) {
    this._sendDebuggerCommand = sendDebuggerCommand;
    this._matchesDomainFilter = matchesDomainFilter;
  }

  get count() {
    return this.buffer.length;
  }

  start() {
    this.recording = true;
    this.reqMeta.clear();
    this.wsMeta.clear();
  }

  stop() {
    this.recording = false;
  }

  clear() {
    this.buffer = [];
    this.reqMeta.clear();
    this.wsMeta.clear();
  }

  _push(entry) {
    if (!this.recording) return;
    this.buffer.push({ t: nowIso(), ...entry });
    if (this.buffer.length > MAX_ENTRIES) this.buffer.shift();
  }

  // Single entry point called from the background CDP event router.
  async onEvent(method, params, tabId) {
    if (!this.recording) return;
    try {
      if (method === "Network.requestWillBeSent") {
        await this._onRequest(tabId, params);
      } else if (method === "Network.responseReceived") {
        this._onResponseMeta(params);
      } else if (method === "Network.loadingFinished") {
        await this._onResponseBody(tabId, params);
      } else if (method === "Network.webSocketCreated") {
        this._onWsCreated(params);
      } else if (method === "Network.webSocketFrameSent") {
        this._onWsFrame("sent", params);
      } else if (method === "Network.webSocketFrameReceived") {
        this._onWsFrame("received", params);
      }
    } catch (error) {
      this._push({ kind: "record_error", method, error: error.message });
    }
  }

  async _onRequest(tabId, params) {
    const req = params.request || {};
    const url = req.url || "";
    if (!this._matchesDomainFilter(url)) return;

    let postData = req.postData;
    if (postData == null && req.hasPostData && this._sendDebuggerCommand) {
      try {
        const res = await this._sendDebuggerCommand(tabId, "Network.getRequestPostData", {
          requestId: params.requestId
        });
        postData = res.postData;
      } catch (error) {
        postData = `<<unavailable: ${error.message}>>`;
      }
    }
    this.reqMeta.set(params.requestId, {
      url, method: req.method, requestHeaders: req.headers || {}, postData: postData ?? null
    });
    this._push({
      kind: "request", requestId: params.requestId, url, method: req.method,
      requestHeaders: redactHeaders(req.headers || {}), postData: postData ?? null
    });
  }

  _onResponseMeta(params) {
    const url = params.response?.url || "";
    if (!this._matchesDomainFilter(url)) return;
    const meta = this.reqMeta.get(params.requestId);
    if (meta) {
      meta.status = params.response?.status;
      meta.responseHeaders = params.response?.headers || {};
      meta.mimeType = params.response?.mimeType;
    }
  }

  async _onResponseBody(tabId, params) {
    const meta = this.reqMeta.get(params.requestId);
    if (!meta) return;
    this.reqMeta.delete(params.requestId);

    let body = "";
    let base64Encoded = false;
    try {
      const res = await this._sendDebuggerCommand(tabId, "Network.getResponseBody", {
        requestId: params.requestId
      });
      body = res.body ?? "";
      base64Encoded = Boolean(res.base64Encoded);
    } catch (error) {
      body = `<<body unavailable: ${error.message}>>`;
    }
    this._push({
      kind: "response", requestId: params.requestId, url: meta.url, method: meta.method,
      status: meta.status, mimeType: meta.mimeType,
      responseHeaders: redactHeaders(meta.responseHeaders || {}), body, base64Encoded
    });
  }

  _onWsCreated(params) {
    if (!this._matchesDomainFilter(params.url || "")) return;
    this.wsMeta.set(params.requestId, { url: params.url });
    this._push({ kind: "ws_created", requestId: params.requestId, url: params.url });
  }

  _onWsFrame(direction, params) {
    const meta = this.wsMeta.get(params.requestId);
    if (!meta) return;
    this._push({
      kind: "ws_frame", direction, requestId: params.requestId, url: meta.url,
      opcode: params.response?.opcode, payloadData: params.response?.payloadData ?? ""
    });
  }
}

export const recorder = new Recorder();
