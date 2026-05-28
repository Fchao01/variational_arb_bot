// Variational page API. `buildScript` returns a self-contained expression
// string that is evaluated INSIDE the attached variational.io tab (via CDP
// Runtime.evaluate) so the page's own session cookies authenticate the
// fetch calls. Pure: no extension state, no imports.
//
// Actions: POSITIONS / ORDERS / CANCEL / QUOTE / ORDER. ORDER has three
// shapes: limit, market-reuse (reuse a prior quote_id, skip a re-quote
// round-trip), and market-close (fresh indicative then market).

export function buildScript(action, params) {
  const fn = async (action, o) => {
    const host = location.host || "";
    if (!host.includes("variational.io")) {
      return { ok: false, step: "precheck", error: "Attached tab is not a variational.io page (host=" + host + ")." };
    }

    // Connected wallet address: explicit param, else first 0x40 hex found
    // in localStorage. Sent as the vr-connected-address header when present.
    let address = (o.account && /^0x[0-9a-fA-F]{40}$/.test(o.account)) ? o.account : null;
    if (!address) {
      try {
        for (let i = 0; i < localStorage.length; i++) {
          const k = localStorage.key(i);
          const m = (k + " " + localStorage.getItem(k)).match(/0x[0-9a-fA-F]{40}/);
          if (m) { address = m[0]; break; }
        }
      } catch (e) { /* ignore */ }
    }
    const headers = { "content-type": "application/json" };
    if (address) headers["vr-connected-address"] = address;

    const request = async (method, path, body) => {
      const opts = { method, credentials: "include", headers };
      if (body !== undefined) opts.body = JSON.stringify(body);
      const res = await fetch("https://omni.variational.io" + path, opts);
      const text = await res.text();
      let json = null;
      try { json = text ? JSON.parse(text) : null; } catch (e) { /* keep text */ }
      const rl = res.headers.get("x-rate-limit-resets-in-ms");
      return { status: res.status, ok: res.ok, json, text, rateLimitResetMs: rl ? Number(rl) : null };
    };
    const post = (path, body) => request("POST", path, body);
    const get = (path) => request("GET", path, undefined);
    // Uniform error envelope; `extra` carries per-endpoint diagnostics
    // (rateLimitResetMs / quoteId / reuseFailed / ...).
    const fail = (step, r, extra) => ({
      ok: false, step, httpStatus: r.status,
      error: r.text || ("HTTP " + r.status),
      addressUsed: !!address, ...(extra || {})
    });

    if (action === "POSITIONS") {
      const r = await get("/api/positions");
      if (!r.ok) return fail("positions", r);
      return { ok: true, positions: r.json, httpStatus: r.status, addressUsed: !!address };
    }

    if (action === "ORDERS") {
      const r = await get("/api/orders/v2?status=pending&order_by=created_at&order=desc");
      if (!r.ok) return fail("orders/v2", r);
      return { ok: true, orders: (r.json && r.json.result) || [], httpStatus: r.status, addressUsed: !!address };
    }

    if (action === "CANCEL") {
      const r = await post("/api/orders/cancel", { rfq_id: o.rfqId });
      if (!r.ok) return fail("orders/cancel", r);
      return { ok: true, rfqId: o.rfqId, httpStatus: r.status, addressUsed: !!address };
    }

    const instrument = {
      underlying: o.market,
      instrument_type: "perpetual_future",
      settlement_asset: o.settlementAsset || "USDC",
      funding_interval_s: o.fundingIntervalS || 3600
    };

    if (action === "QUOTE") {
      const ind = await post("/api/quotes/indicative", { instrument, qty: String(o.amount) });
      if (!ind.ok || !ind.json) {
        return fail("quotes/indicative", ind, { rateLimitResetMs: ind.rateLimitResetMs });
      }
      const q = ind.json;
      return {
        ok: true, quoteId: q.quote_id, bid: q.bid, ask: q.ask,
        markPrice: q.mark_price, indexPrice: q.index_price, quoteTimestamp: q.timestamp,
        raw: q,
        httpStatus: ind.status, rateLimitResetMs: ind.rateLimitResetMs, addressUsed: !!address
      };
    }

    // ===== ORDER =====
    const side = o.side === "BUY" ? "buy" : "sell";

    // (a) LIMIT
    if (o.orderType === "limit") {
      const r = await post("/api/orders/new/limit", {
        order_type: "limit",
        limit_price: String(o.limitPrice),
        side,
        instrument,
        qty: String(o.amount),
        slippage_limit: String(o.maxSlippage),
        is_auto_resize: false,
        use_mark_price: false,
        is_reduce_only: !!o.reduceOnly
      });
      if (!r.ok) return fail("new/limit", r);
      return { ok: true, orderType: "limit", rfqId: r.json && r.json.rfq_id, httpStatus: r.status, addressUsed: !!address };
    }

    // (b) MARKET reuse — bind to a prior quote_id, skip a whole indicative
    // RTT. No fresh fallback: a stale quote means the edge was a stale
    // artifact, so fail and let the caller skip the round.
    if (o.reuseQuoteId) {
      const mr = await post("/api/orders/new/market", {
        quote_id: o.reuseQuoteId,
        side,
        max_slippage: o.maxSlippage,
        is_reduce_only: !!o.reduceOnly
      });
      if (mr.ok) {
        return {
          ok: true, orderType: "market", reused: true,
          rfqId: mr.json && mr.json.rfq_id, quoteId: o.reuseQuoteId,
          httpStatus: mr.status, addressUsed: !!address
        };
      }
      return fail("reuse/new/market", mr, { reuseFailed: true, quoteId: o.reuseQuoteId });
    }

    // (c) MARKET close — fresh indicative then market (no reuseQuoteId).
    const ind = await post("/api/quotes/indicative", { instrument, qty: String(o.amount) });
    if (!ind.ok || !ind.json || !ind.json.quote_id) {
      return fail("quotes/indicative", ind);
    }
    const q = ind.json;
    const m = await post("/api/orders/new/market", {
      quote_id: q.quote_id,
      side,
      max_slippage: o.maxSlippage,
      is_reduce_only: !!o.reduceOnly
    });
    if (!m.ok) {
      return fail("new/market", m, {
        quoteId: q.quote_id, bid: q.bid, ask: q.ask,
        markPrice: q.mark_price, quoteTimestamp: q.timestamp
      });
    }
    return {
      ok: true, orderType: "market",
      rfqId: m.json && m.json.rfq_id,
      quoteId: q.quote_id, bid: q.bid, ask: q.ask, markPrice: q.mark_price, quoteTimestamp: q.timestamp,
      httpStatus: m.status, addressUsed: !!address
    };
  };
  return "(" + fn.toString() + ")(" + JSON.stringify(action) + "," + JSON.stringify(params) + ")";
}
