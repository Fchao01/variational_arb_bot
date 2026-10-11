"""Lean Lighter order executor.

ONE SignerClient (the underlying client already takes market_index per
call) plus a per-coin config cache pulled once from /api/v1/orderBooks.
Read-only book prices come from lighter.book_feed.BookFeed; this class
places orders and (optionally) tracks their REAL fills via an
account_orders WS subscription.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import requests
import websockets
from lighter.signer_client import SignerClient

from arb_bot.config import (
    LIGHTER_CHAIN_ID,
    LIGHTER_WS_PING_INTERVAL_SECONDS,
    LIGHTER_WS_PING_TIMEOUT_SECONDS,
    LIGHTER_WS_URL,
)
from arb_bot.helpers import env_flag, required_int_env, to_decimal

_MAX_TRACKED_ORDERS = 10_000
# How long to wait for the jsonapi/sendtx ack on the tx WebSocket before
# treating the order as sent-but-unconfirmed (account_orders WS is the real
# fill truth either way, so this only bounds the ack wait).
_TX_WS_RECV_TIMEOUT = 2.0


class _WsSendFailed(Exception):
    """The tx WS could not be (re)connected or the frame never left the socket
    -> nothing was submitted, so it is safe to roll back the nonce and fall
    back to the HTTP path."""


class _WsRejected(Exception):
    """The server returned an error envelope for our sendtx -> the order was
    rejected; roll back the nonce."""


class _WsSentNoResponse(Exception):
    """The frame was sent but no correlated ack came back. The order MAY have
    landed, so we must NOT resubmit (would double the position) -- let the
    account_orders WS / cancel-on-timeout path resolve it."""


@dataclass(frozen=True, slots=True)
class MarketConfig:
    market_index: int
    base_mult: int
    price_mult: int
    min_base: Decimal
    min_quote: Decimal


class Executor:
    def __init__(self, base_url: str, logger: logging.Logger, live_config) -> None:
        self._base_url = base_url
        self._log = logger
        # cushion read live from live_config so it's hot-tunable
        self._live = live_config
        self._client: SignerClient | None = None
        self._lock = asyncio.Lock()
        self._cfg: dict[str, MarketConfig] = {}
        # Real-fill readback (account_orders WS): client_order_id -> avg fill
        # price. Empty until start_fill_feed()'s WS delivers status=filled.
        self._fills: dict[int, Decimal] = {}
        # client_order_id -> monotonic() at the instant the account_orders WS
        # first delivered status=filled. This is the PRECISE fill time; the
        # trader's poll loop only *notices* it up to one poll-interval later, so
        # measuring lt_fill off this (not off poll-detection) removes the
        # ~0-100ms polling quantization.
        self._fill_ts: dict[int, float] = {}
        # client_order_id -> Event set the instant the fill lands, so waiters
        # wake immediately (no poll/sleep loop).
        self._fill_events: dict[int, asyncio.Event] = {}
        # client_order_id -> exchange order_index (from the same WS, every
        # status). Needed to cancel a specific resting order on confirm-timeout.
        self._order_index: dict[int, int] = {}
        self._filled_base: dict[int, Decimal] = {}
        self._fill_feed_task: asyncio.Task[None] | None = None
        self._fill_feed_stop = False
        self._tx_ws_keepalive_task: asyncio.Task[None] | None = None
        # Dedicated, always-warm WebSocket for jsonapi/sendtx order submission
        # (opt-in via LIGHTER_WS_SUBMIT). Kept alive by websockets auto-ping so
        # every order skips the cold TLS handshake the HTTP path pays when idle
        # > keepalive. None until first use / after a drop.
        self._tx_ws: Any = None
        # Per-process client_order_id source: seeded from wall clock (no
        # cross-restart collision), incremented per submit so same-ms orders
        # get distinct ids and fill readback never cross-maps.
        self._next_client_order_id = int(time.time() * 1000) % 2_000_000_000

    def ensure_client(self) -> SignerClient:
        if self._client is None:
            key = os.getenv("LIGHTER_PRIVATE_KEY", "").strip()
            legacy_key = os.getenv("API_KEY_PRIVATE_KEY", "").strip()
            if key and legacy_key and key.removeprefix("0x") != legacy_key.removeprefix("0x"):
                raise RuntimeError(
                    "Conflicting LIGHTER_PRIVATE_KEY and API_KEY_PRIVATE_KEY; "
                    "remove the legacy API_KEY_PRIVATE_KEY and use RH credentials."
                )
            key = key or legacy_key
            if not key:
                raise RuntimeError("LIGHTER_PRIVATE_KEY / API_KEY_PRIVATE_KEY not set")
            client = SignerClient(
                url=self._base_url,
                account_index=required_int_env("LIGHTER_ACCOUNT_INDEX"),
                api_private_keys={required_int_env("LIGHTER_API_KEY_INDEX"): key},
                chain_id=LIGHTER_CHAIN_ID,
            )
            err = client.check_client()
            if err is not None:
                raise RuntimeError(f"Lighter CheckClient error: {err}")
            self._client = client
        return self._client

    def load_market_configs(self, symbols: list[str]) -> None:
        resp = requests.get(
            f"{self._base_url}/api/v1/orderBooks",
            headers={"accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        by_sym = {str(m.get("symbol")): m for m in resp.json().get("order_books", [])}
        want = {s.upper() for s in symbols}
        for sym, m in by_sym.items():
            if sym not in want:
                continue
            try:
                self._cfg[sym] = MarketConfig(
                    market_index=int(m["market_id"]),
                    base_mult=pow(10, int(m["supported_size_decimals"])),
                    price_mult=pow(10, int(m["supported_price_decimals"])),
                    min_base=to_decimal(m.get("min_base_amount")) or Decimal("0"),
                    min_quote=to_decimal(m.get("min_quote_amount")) or Decimal("0"),
                )
            except (KeyError, ValueError, TypeError):
                continue

    def config(self, symbol: str) -> MarketConfig | None:
        return self._cfg.get(symbol.upper())

    def hedge_blocked(
        self, symbol: str, qty: Decimal, ref_price: Decimal
    ) -> str | None:
        """Reason the Lighter hedge can't be placed at this size, or None
        if it can. Checked before the Variational leg so we never open a
        position we cannot hedge (naked-leg guard)."""
        cfg = self.config(symbol)
        if cfg is None:
            return f"{symbol} not in Lighter config"
        if cfg.min_base and qty < cfg.min_base:
            return f"qty {qty} < min_base {cfg.min_base}"
        if cfg.min_quote and qty * ref_price < cfg.min_quote:
            return f"notional {qty * ref_price:.4f} < min_quote {cfg.min_quote}"
        if int(qty * cfg.base_mult) <= 0:
            return f"qty {qty} rounds to zero base amount"
        return None

    async def submit(
        self,
        symbol: str,
        side: str,
        qty: Decimal,
        best_bid: Decimal,
        best_ask: Decimal,
        reduce_only: bool,
    ) -> tuple[int, str | None]:
        """Place a marketable limit (crosses the tight zero-fee book).
        side = BUY/SELL. Returns (client_order_id, tx_hash)."""
        cfg = self.config(symbol)
        if cfg is None:
            raise RuntimeError(f"No Lighter config for {symbol}")
        slippage = Decimal(str(self._live.lighter_slippage_bps)) / Decimal("10000")
        if side == "BUY":
            is_ask = False
            limit_price = best_ask * (Decimal("1") + slippage)
        else:
            is_ask = True
            limit_price = best_bid * (Decimal("1") - slippage)
        base_amount = int(qty * cfg.base_mult)
        if base_amount <= 0:
            raise RuntimeError(f"{symbol} base amount rounds to zero ({qty})")
        price_int = int(limit_price * cfg.price_mult)

        async with self._lock:
            client = self.ensure_client()
            self._next_client_order_id = (
                self._next_client_order_id + 1
            ) % 2_000_000_000
            client_order_id = self._next_client_order_id

            # Sign locally (replicating create_order's optimistic-nonce
            # handling) then submit over the always-warm tx WebSocket.
            api_key_index, nonce = client.nonce_manager.next_nonce()
            tx_type, tx_info, tx_hash, sign_err = client.sign_create_order(
                cfg.market_index,
                client_order_id,
                base_amount,
                price_int,
                int(is_ask),
                client.ORDER_TYPE_LIMIT,
                client.ORDER_TIME_IN_FORCE_GOOD_TILL_TIME,
                reduce_only,
                client.NIL_TRIGGER_PRICE,
                client.DEFAULT_28_DAY_ORDER_EXPIRY,
                nonce=nonce,
                api_key_index=api_key_index,
            )
            if sign_err is not None:
                client.nonce_manager.acknowledge_failure(api_key_index)
                raise RuntimeError(f"Lighter sign error ({symbol}): {sign_err}")

            try:
                await self._ws_send_tx(tx_type, tx_info, client_order_id)
                return client_order_id, tx_hash
            except _WsRejected as exc:
                client.nonce_manager.acknowledge_failure(api_key_index)
                raise RuntimeError(f"Lighter ws sendtx rejected ({symbol}): {exc}")
            except _WsSendFailed as exc:
                # Nothing left the socket -> roll back the nonce and fail this
                # submit; the caller aborts the round (no order was placed).
                client.nonce_manager.acknowledge_failure(api_key_index)
                raise RuntimeError(f"Lighter ws sendtx send failed ({symbol}): {exc}")
            except _WsSentNoResponse as exc:
                # Sent but unacked: order may be live -> do NOT resubmit; the
                # account_orders WS / cancel-on-timeout path resolves it.
                self._log.warning(
                    "Lighter ws sendtx unconfirmed coid=%s (%s); "
                    "relying on account_orders WS", client_order_id, exc,
                )
                return client_order_id, tx_hash

    async def _ensure_tx_ws(self) -> Any:
        """Return a live tx WebSocket, connecting if needed. The initial
        {"type":"connected"} frame is drained so the next recv() is our ack."""
        if self._tx_ws is not None:
            return self._tx_ws
        ws = await websockets.connect(
            LIGHTER_WS_URL,
            ping_interval=LIGHTER_WS_PING_INTERVAL_SECONDS,
            ping_timeout=LIGHTER_WS_PING_TIMEOUT_SECONDS,
        )
        try:
            await asyncio.wait_for(ws.recv(), timeout=3.0)  # "connected" hello
        except Exception:
            pass
        self._tx_ws = ws
        return ws

    async def _tx_ws_keepalive_loop(self) -> None:
        """Every 20s: ensure the tx WS is connected and round-trip a ping. On
        any failure, drop it so the next cycle reconnects — this keeps the
        connection genuinely warm so a sparse order never pays a cold connect.
        Runs under self._lock so it never races a concurrent submit; the ping
        round-trip is ~one warm RTT, so it blocks a submit only when the link is
        actually dying (exactly when we'd want to reconnect anyway)."""
        while not self._fill_feed_stop:
            await asyncio.sleep(20.0)
            if self._fill_feed_stop:
                break
            try:
                async with self._lock:
                    ws = await self._ensure_tx_ws()
                    pong_waiter = await ws.ping()
                    await asyncio.wait_for(pong_waiter, timeout=2.0)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._log.warning(
                    "Lighter tx WS keepalive failed; reconnecting next cycle: %s", exc)
                async with self._lock:
                    await self._drop_tx_ws()

    async def _drop_tx_ws(self) -> None:
        ws, self._tx_ws = self._tx_ws, None
        if ws is not None:
            try:
                await ws.close()
            except Exception:
                pass

    async def _ws_send_tx(self, tx_type: int, tx_info: str, corr_id: int) -> None:
        """Submit one signed tx over the tx WebSocket and await its ack.

        Raises _WsSendFailed (nothing sent), _WsRejected (server error), or
        _WsSentNoResponse (sent, no correlated ack). On clean success returns
        None. Caller (submit) holds self._lock, so only one tx is in flight."""
        payload = json.dumps({
            "type": "jsonapi/sendtx",
            "data": {
                "id": str(corr_id),
                "tx_type": int(tx_type),
                "tx_info": json.loads(tx_info),
            },
        })
        # (Re)connect + send. A send that raises did not deliver, so retrying
        # the send once on a fresh connection cannot double-submit.
        ws = None
        last_exc: Exception | None = None
        for _ in range(2):
            try:
                ws = await self._ensure_tx_ws()
                await ws.send(payload)
                break
            except Exception as exc:
                last_exc = exc
                await self._drop_tx_ws()
                ws = None
        if ws is None:
            raise _WsSendFailed(last_exc)

        # Past this point the frame is out -> never resend; only classify the ack.
        deadline = time.monotonic() + _TX_WS_RECV_TIMEOUT
        try:
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise _WsSentNoResponse("recv timeout")
                raw = await asyncio.wait_for(ws.recv(), timeout=remaining)
                if isinstance(raw, bytes):
                    raw = raw.decode("utf-8", errors="replace")
                msg = json.loads(raw)
                if str(msg.get("id")) != str(corr_id):
                    continue  # stale/late ack for an earlier tx -> drain
                if msg.get("error"):
                    raise _WsRejected(msg["error"])
                return  # accepted
        except (_WsRejected, _WsSentNoResponse):
            raise
        except Exception as exc:
            # recv side died after a successful send -> unconfirmed, no resend.
            await self._drop_tx_ws()
            raise _WsSentNoResponse(exc)

    def fill_price(self, client_order_id: int) -> Decimal | None:
        """Real avg fill price for a client_order_id, or None if the
        account_orders WS has not (yet) reported it filled."""
        try:
            return self._fills.get(int(client_order_id))
        except (TypeError, ValueError):
            return None

    def fill_ts(self, client_order_id: int) -> float | None:
        """monotonic() at which the fill first arrived on the account_orders
        WS (precise), or None if not filled yet. Use for lt_fill timing instead
        of the poll-detection instant."""
        try:
            return self._fill_ts.get(int(client_order_id))
        except (TypeError, ValueError):
            return None

    async def wait_fill(self, client_order_id: int, timeout: float) -> bool:
        """Wait up to `timeout`s for a full fill, waking the instant the
        account_orders WS reports it (event-driven, no polling). Returns True
        iff filled. Safe against the fill landing before we register: we
        re-check _fills after creating the Event."""
        coid = int(client_order_id)
        if coid in self._fills:
            return True
        ev = self._fill_events.get(coid)
        if ev is None:
            ev = asyncio.Event()
            self._fill_events[coid] = ev
        if coid in self._fills:  # filled in the gap before we registered ev
            return True
        try:
            await asyncio.wait_for(ev.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        return coid in self._fills

    def filled_base(self, client_order_id: int) -> Decimal | None:
        try:
            return self._filled_base.get(int(client_order_id))
        except (TypeError, ValueError):
            return None

    async def cancel(self, symbol: str, client_order_id: int) -> bool:
        """Best-effort cancel of a possibly-still-resting order.

        Returns True if a cancel was accepted or the order is already
        fully filled (nothing to cancel); False if we could not cancel
        and the order may still be live. Callers re-check fill_price()
        after this to resolve the cancel/fill race.

        Precise path: the account_orders WS maps our client_order_id to
        the exchange order_index; cancel exactly that order. If the WS
        has not reported it yet (no order_index), fall back to an
        account-wide cancel-all — safe because this Lighter account is
        dedicated to this single-coin bot."""
        if self.fill_price(client_order_id) is not None:
            return True
        cfg = self.config(symbol)
        if cfg is None:
            self._log.error("Lighter cancel: no config for %s", symbol)
            return False
        order_index = self._order_index.get(int(client_order_id))
        async with self._lock:
            client = self.ensure_client()
            try:
                if order_index is not None:
                    _, _, error = await client.cancel_order(
                        market_index=cfg.market_index,
                        order_index=order_index,
                    )
                    scope = f"order_index={order_index}"
                else:
                    _, _, error = await client.cancel_all_orders(
                        client.CANCEL_ALL_TIF_IMMEDIATE,
                        0,  # IMMEDIATE -> CancelAllTime must be 0/nil; a real
                        # timestamp makes the signer reject "CancelAllTime
                        # should be nil", silently breaking this cancel-all
                        # fallback and stranding the Lighter leg (naked-leg).
                    )
                    scope = "ALL (order_index unknown)"
            except Exception as exc:
                self._log.error(
                    "Lighter cancel %s coid=%s raised: %s",
                    symbol, client_order_id, exc,
                )
                return False
        if error is not None:
            self._log.error(
                "Lighter cancel %s coid=%s (%s) error: %s",
                symbol, client_order_id, scope, error,
            )
            return False
        self._log.warning(
            "Lighter cancel %s coid=%s (%s) sent",
            symbol, client_order_id, scope,
        )
        return True

    async def start_fill_feed(self, market_indices: list[int]) -> None:
        """Subscribe to account_orders for the given markets so submit()'s
        coids can later be resolved to real fill prices. Best-effort: a WS
        failure only degrades recording to the book-touch estimate, it never
        blocks trading."""
        if self._fill_feed_task is not None:
            return
        idx = sorted({int(i) for i in market_indices})
        if not idx:
            return
        self._fill_feed_task = asyncio.create_task(self._fill_feed_loop(idx))
        # Pre-warm the tx WS so the FIRST order skips the ~36ms cold connect
        # (auto-ping then keeps it warm for the process lifetime). Best-effort:
        # if it fails, the first submit just connects on demand.
        try:
            await self._ensure_tx_ws()
            self._log.info("Lighter tx WS pre-warmed for jsonapi/sendtx")
        except Exception as exc:
            self._log.warning("Lighter tx WS pre-warm failed (first order will connect): %s", exc)
        # Keep it warm: ping + proactively reconnect every 20s. Pre-warm alone
        # isn't enough — an idle connection drops during sparse-trade gaps and
        # auto-ping only *detects* the drop (never reconnects), so the first
        # order after a quiet spell would otherwise pay the cold-connect ~36ms.
        if self._tx_ws_keepalive_task is None:
            self._tx_ws_keepalive_task = asyncio.create_task(self._tx_ws_keepalive_loop())

    async def _fill_feed_loop(self, market_indices: list[int]) -> None:
        account_index = required_int_env("LIGHTER_ACCOUNT_INDEX")
        api_key_index = required_int_env("LIGHTER_API_KEY_INDEX")
        url = (
            f"{LIGHTER_WS_URL}?server_pings=true"
            if env_flag("LIGHTER_WS_SERVER_PINGS")
            else LIGHTER_WS_URL
        )
        while not self._fill_feed_stop:
            try:
                async with websockets.connect(
                    url,
                    ping_interval=LIGHTER_WS_PING_INTERVAL_SECONDS,
                    ping_timeout=LIGHTER_WS_PING_TIMEOUT_SECONDS,
                ) as ws:
                    async with self._lock:
                        client = self.ensure_client()
                        auth_token, err = client.create_auth_token_with_expiry(
                            api_key_index=api_key_index
                        )
                    if err is not None:
                        self._log.warning("Lighter fill-feed auth token error: %s", err)
                        await asyncio.sleep(2.0)
                        continue
                    for mi in market_indices:
                        await ws.send(
                            json.dumps(
                                {
                                    "type": "subscribe",
                                    "channel": f"account_orders/{mi}/{account_index}",
                                    "auth": auth_token,
                                }
                            )
                        )
                    self._log.info(
                        "Lighter fill-feed: subscribed account_orders for %d markets",
                        len(market_indices),
                    )
                    while not self._fill_feed_stop:
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        data = json.loads(raw)
                        if data.get("type") in (
                            "update/account_orders",
                            "subscribed/account_orders",
                        ):
                            self._ingest_account_orders(data.get("orders", {}))
            except asyncio.CancelledError:
                return
            except Exception as exc:
                if not self._fill_feed_stop:
                    self._log.warning("Lighter fill-feed disconnected, retrying: %s", exc)
            if not self._fill_feed_stop:
                await asyncio.sleep(1.0)

    def _ingest_account_orders(self, orders_by_market: Any) -> None:
        if not isinstance(orders_by_market, dict):
            return
        for orders in orders_by_market.values():
            if not isinstance(orders, list):
                continue
            for order in orders:
                if not isinstance(order, dict):
                    continue
                try:
                    client_order_id = int(order.get("client_order_id"))
                except (TypeError, ValueError):
                    continue
                # order_index for every status, so a resting order can be
                # cancelled precisely on confirm-timeout.
                try:
                    self._order_index[client_order_id] = int(
                        order.get("order_index")
                    )
                except (TypeError, ValueError):
                    pass
                fb_seen = to_decimal(order.get("filled_base_amount"))
                if fb_seen is not None and fb_seen > 0:
                    self._filled_base[client_order_id] = fb_seen
                if order.get("status") != "filled":
                    continue
                fq = to_decimal(order.get("filled_quote_amount"))
                fb = to_decimal(order.get("filled_base_amount"))
                if fq is None or fb is None or fb == 0:
                    continue
                self._fills[client_order_id] = fq / fb
                # stamp the precise fill arrival once (earliest report wins)
                self._fill_ts.setdefault(client_order_id, time.monotonic())
                ev = self._fill_events.get(client_order_id)
                if ev is not None:
                    ev.set()  # wake any wait_fill() immediately
        self._trim_tracking()

    def _trim_tracking(self) -> None:
        for d in (self._fills, self._fill_ts, self._fill_events,
                  self._order_index, self._filled_base):
            excess = len(d) - _MAX_TRACKED_ORDERS
            if excess > 0:
                for key in list(d.keys())[:excess]:
                    del d[key]

    async def close(self) -> None:
        self._fill_feed_stop = True
        if self._tx_ws_keepalive_task is not None:
            self._tx_ws_keepalive_task.cancel()
            try:
                await self._tx_ws_keepalive_task
            except (asyncio.CancelledError, Exception):
                pass
            self._tx_ws_keepalive_task = None
        await self._drop_tx_ws()
        if self._fill_feed_task is not None:
            self._fill_feed_task.cancel()
            try:
                await self._fill_feed_task
            except (asyncio.CancelledError, Exception):
                pass
            self._fill_feed_task = None
        if self._client is not None:
            fn = getattr(self._client, "close", None)
            if callable(fn):
                try:
                    res = fn()
                    if asyncio.iscoroutine(res):
                        await res
                except Exception:
                    pass
