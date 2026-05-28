// Reconnecting WebSocket plumbing shared by the two outbound channels:
//   ForwardSocket  — pushes captured REST/WS traffic to a local receiver.
//   CommandClient   — receives commands from python and runs them in-page.
// Both had near-identical connect/reconnect/close/status code; that lives
// once in ReconnectingSocket. Dependencies (endpoint, active-check, status
// callback, ...) are injected so this file imports nothing.

const MAX_QUEUE_SIZE = 1000;

class ReconnectingSocket {
  constructor(label, deps) {
    this.label = label;
    this.status = "disconnected";
    this.ws = null;
    this.retryTimer = null;
    this._getEndpoint = deps.getEndpoint;
    this._isActive = deps.isActive;
    this._onStatus = deps.onStatus || (() => {});
  }

  _setStatus(status) {
    this.status = status;
    this._onStatus();
  }

  connect() {
    if (!this._isActive()) return;
    if (this.ws && (this.ws.readyState === WebSocket.OPEN || this.ws.readyState === WebSocket.CONNECTING)) {
      return;
    }
    const endpoint = this._getEndpoint();
    if (!endpoint) {
      this._setStatus("disconnected");
      return;
    }
    this._setStatus("connecting");
    let socket;
    try {
      socket = new WebSocket(endpoint);
    } catch (error) {
      this._setStatus("error");
      this.scheduleReconnect();
      return;
    }
    this.ws = socket;
    socket.onopen = () => {
      if (this.ws !== socket) return;
      this._setStatus("connected");
      this.onOpen();
    };
    socket.onmessage = (event) => {
      if (this.ws === socket) this.onMessage(event.data);
    };
    socket.onclose = () => {
      if (this.ws !== socket) return;
      this.ws = null;
      this._setStatus("disconnected");
      this.scheduleReconnect();
    };
    socket.onerror = () => {
      if (this.ws === socket) this._setStatus("error");
    };
  }

  scheduleReconnect() {
    if (!this._isActive() || this.retryTimer) return;
    this.retryTimer = setTimeout(() => {
      this.retryTimer = null;
      this.connect();
    }, 1000);
  }

  restart() {
    this.close();
    this.connect();
  }

  close() {
    if (this.retryTimer) {
      clearTimeout(this.retryTimer);
      this.retryTimer = null;
    }
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
    this._setStatus("disconnected");
  }

  // Overridden by subclasses.
  onOpen() {}
  onMessage() {}
}

// Outbound: queue-and-flush captured traffic to the local receiver. Drops
// the oldest entry past MAX_QUEUE_SIZE so a dead receiver can't grow memory
// unbounded.
export class ForwardSocket extends ReconnectingSocket {
  constructor(label, deps) {
    super(label, deps);
    this.queue = [];
    this._onConnected = deps.onConnected || (() => {});
  }

  onOpen() {
    this.flush();
    this._onConnected();
  }

  send(payload) {
    const data = JSON.stringify(payload);
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(data);
      return;
    }
    this.queue.push(data);
    if (this.queue.length > MAX_QUEUE_SIZE) this.queue.shift();
    this.connect();
  }

  flush() {
    if (!this.ws || this.ws.readyState !== WebSocket.OPEN) return;
    while (this.queue.length > 0) this.ws.send(this.queue.shift());
  }
}

// Inbound: register as the extension, answer PING, dispatch commands to
// the injected page runner and reply with a *_RESULT message.
export class CommandClient extends ReconnectingSocket {
  constructor(deps) {
    super("command", deps);
    this._runInPage = deps.runInPage;
    this._reloadTab = deps.reloadTab || (async () => ({ ok: false, error: "reload unsupported" }));
  }

  onOpen() {
    this.send({ type: "REGISTER", role: "extension" });
  }

  send(payload) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(payload));
    }
  }

  async onMessage(raw) {
    let msg;
    try {
      msg = JSON.parse(raw);
    } catch (e) {
      return;
    }
    if (!msg || typeof msg !== "object") return;

    const type = String(msg.type || "").toUpperCase();
    if (type === "PING") {
      this.send({ type: "PONG" });
      return;
    }
    // RELOAD reloads the attached tab to resync the market-data path; it does
    // not run in-page (no fetch, no positions touched), so handle it here.
    if (type === "RELOAD") {
      const result = await this._reloadTab();
      this.send({
        type: "RELOAD_RESULT",
        requestId: msg.requestId,
        ok: !!result.ok,
        error: result.ok ? undefined : (result.error || "reload failed"),
        result,
        timestamp: new Date().toISOString()
      });
      return;
    }
    const actionByType = {
      PLACE_ORDER: "ORDER", QUOTE: "QUOTE",
      POSITIONS: "POSITIONS", ORDERS: "ORDERS", CANCEL: "CANCEL"
    };
    const action = actionByType[type];
    if (!action) return;

    const result = await this._runInPage(action, msg);
    this.send({
      type: `${type === "PLACE_ORDER" ? "ORDER" : type}_RESULT`,
      requestId: msg.requestId,
      ok: !!result.ok,
      error: result.ok ? undefined : (result.error || `${action} failed.`),
      result,
      timestamp: new Date().toISOString()
    });
  }
}
