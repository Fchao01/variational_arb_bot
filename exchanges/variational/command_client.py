"""In-process requester client for the local CommandBroker.

Connects back to the loopback command server (same protocol the
order_lifecycle_test / limit_order_test scripts use) so the arbitrage
engine can place / query / cancel Variational orders programmatically.

A single long-lived connection with a background read loop dispatches
results to per-requestId futures, so concurrent POSITIONS polling and
PLACE_ORDER calls do not steal each other's replies.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from typing import Any

import websockets

_RESULT_SUFFIXES = (
    "ORDER_RESULT",
    "QUOTE_RESULT",
    "POSITIONS_RESULT",
    "ORDERS_RESULT",
    "CANCEL_RESULT",
    "RELOAD_RESULT",
)


class CommandClient:
    def __init__(self, host: str, port: int, logger: logging.Logger) -> None:
        self._uri = f"ws://{host}:{port}"
        self._logger = logger
        self._ws: Any = None
        self._recv_task: asyncio.Task[None] | None = None
        self._pending: dict[str, asyncio.Future[dict[str, Any]]] = {}
        self._lock = asyncio.Lock()
        self._connected = asyncio.Event()
        self._closed = False

    async def start(self) -> None:
        self._recv_task = asyncio.create_task(self._run())

    async def close(self) -> None:
        self._closed = True
        if self._recv_task is not None:
            self._recv_task.cancel()
            await asyncio.gather(self._recv_task, return_exceptions=True)
        if self._ws is not None:
            with_close = getattr(self._ws, "close", None)
            if callable(with_close):
                try:
                    await self._ws.close()
                except Exception:
                    pass

    async def _run(self) -> None:
        while not self._closed:
            try:
                async with websockets.connect(self._uri, max_size=None) as ws:
                    self._ws = ws
                    await ws.send(json.dumps({"type": "REGISTER", "role": "requester"}))
                    self._connected.set()
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        self._dispatch(raw)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._logger.warning("CommandClient disconnected, retrying: %s", exc)
            finally:
                self._connected.clear()
                self._ws = None
                self._fail_pending("command channel disconnected")
            if not self._closed:
                await asyncio.sleep(1.0)

    def _dispatch(self, raw: str) -> None:
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            return
        if not isinstance(data, dict):
            return
        request_id = str(data.get("requestId", ""))
        if not request_id:
            return
        mtype = str(data.get("type", ""))
        if mtype == "ORDER_DISPATCHED":
            return  # intermediate ack; keep waiting for the final result
        if not any(mtype.endswith(suffix) for suffix in _RESULT_SUFFIXES):
            return
        future = self._pending.pop(request_id, None)
        if future is not None and not future.done():
            future.set_result(data)

    def _fail_pending(self, reason: str) -> None:
        for future in list(self._pending.values()):
            if not future.done():
                future.set_exception(ConnectionError(reason))
        self._pending.clear()

    async def _request(
        self, payload: dict[str, Any], timeout: float
    ) -> dict[str, Any]:
        await asyncio.wait_for(self._connected.wait(), timeout=timeout)
        request_id = str(uuid.uuid4())
        loop = asyncio.get_running_loop()
        future: asyncio.Future[dict[str, Any]] = loop.create_future()
        async with self._lock:
            self._pending[request_id] = future
            ws = self._ws
            if ws is None:
                self._pending.pop(request_id, None)
                raise ConnectionError("command channel not connected")
            await ws.send(json.dumps({**payload, "requestId": request_id}))
        try:
            return await asyncio.wait_for(future, timeout=timeout)
        finally:
            self._pending.pop(request_id, None)

    async def quote(
        self, market: str, amount: str, account: str | None = None, timeout: float = 20.0
    ) -> dict[str, Any]:
        return await self._request(
            {"type": "QUOTE", "market": market, "amount": amount, "account": account},
            timeout,
        )

    async def positions(
        self, account: str | None = None, timeout: float = 20.0
    ) -> dict[str, Any]:
        return await self._request(
            {"type": "POSITIONS", "account": account}, timeout
        )

    async def place_order(
        self,
        *,
        side: str,
        amount: str,
        market: str,
        order_type: str = "market",
        limit_price: str | None = None,
        max_slippage: float = 0.005,
        reduce_only: bool = False,
        reuse_quote_id: str | None = None,
        account: str | None = None,
        timeout: float = 30.0,
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "type": "PLACE_ORDER",
            "orderType": order_type,
            "side": side.upper(),
            "amount": amount,
            "market": market,
            "maxSlippage": max_slippage,
            "reduceOnly": reduce_only,
            "account": account,
        }
        if order_type == "limit":
            payload["limitPrice"] = limit_price
        if reuse_quote_id:
            payload["reuseQuoteId"] = reuse_quote_id
        return await self._request(payload, timeout)

    async def cancel(
        self, rfq_id: str, account: str | None = None, timeout: float = 20.0
    ) -> dict[str, Any]:
        return await self._request(
            {"type": "CANCEL", "rfqId": rfq_id, "account": account}, timeout
        )

    async def reload_page(self, timeout: float = 10.0) -> dict[str, Any]:
        """Ask the extension to reload its attached Variational tab.

        The market-data path (indicative WS + active RFQ fetches) lives in
        the page; when it goes silent the engine calls this to resync —
        reloading re-runs the page bootstrap, re-establishes the WS, and
        re-auths fetches from the session cookies. Positions/orders live on
        the Variational/Lighter servers, so a tab reload never touches them.
        Returns fast: chrome.tabs.reload acks once reload is *initiated*."""
        return await self._request({"type": "RELOAD"}, timeout)
