"""Order/command layer: relays PLACE_ORDER/QUOTE/POSITIONS/ORDERS/CANCEL to the
extension command client and routes results back to the requester."""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any

import websockets

from exchanges.variational.util import as_float, utc_now


class CommandBroker:
    def __init__(self, quiet: bool = False) -> None:
        self.quiet = quiet
        self._lock = asyncio.Lock()
        self._roles: dict[websockets.ServerConnection, str] = {}
        self._extension: websockets.ServerConnection | None = None
        self._pending_requests: dict[str, websockets.ServerConnection] = {}

    async def on_connect(self, websocket: websockets.ServerConnection) -> None:
        async with self._lock:
            self._roles[websocket] = "unknown"

    async def on_disconnect(self, websocket: websockets.ServerConnection) -> None:
        async with self._lock:
            role = self._roles.pop(websocket, "unknown")
            if websocket is self._extension:
                self._extension = None
                failures = list(self._pending_requests.items())
                self._pending_requests.clear()
                for request_id, requester in failures:
                    await self._send(
                        requester,
                        {
                            "type": "ORDER_RESULT",
                            "requestId": request_id,
                            "ok": False,
                            "error": "Extension disconnected before order result.",
                            "timestamp": utc_now(),
                        },
                    )

            stale_request_ids = [req for req, requester in self._pending_requests.items() if requester is websocket]
            for req in stale_request_ids:
                self._pending_requests.pop(req, None)

            if not self.quiet:
                print(f"[COMMAND] disconnected role={role}", flush=True)

    async def handle_raw_message(self, websocket: websockets.ServerConnection, raw: str) -> None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            await self._send(
                websocket,
                {
                    "type": "ERROR",
                    "ok": False,
                    "error": "Invalid JSON payload.",
                    "timestamp": utc_now(),
                },
            )
            return

        if not isinstance(payload, dict):
            await self._send(
                websocket,
                {
                    "type": "ERROR",
                    "ok": False,
                    "error": "Command payload must be an object.",
                    "timestamp": utc_now(),
                },
            )
            return

        msg_type = str(payload.get("type", "")).upper()
        if msg_type == "REGISTER":
            await self._handle_register(websocket, payload)
            return
        if msg_type == "PING":
            await self._send(websocket, {"type": "PONG", "timestamp": utc_now()})
            return
        if msg_type == "PLACE_ORDER":
            await self._handle_place_order(websocket, payload)
            return
        if msg_type == "QUOTE":
            await self._handle_query(websocket, payload, "QUOTE")
            return
        if msg_type == "POSITIONS":
            await self._handle_query(websocket, payload, "POSITIONS")
            return
        if msg_type == "ORDERS":
            await self._handle_query(websocket, payload, "ORDERS")
            return
        if msg_type == "CANCEL":
            await self._handle_cancel(websocket, payload)
            return
        if msg_type in {
            "ORDER_RESULT",
            "QUOTE_RESULT",
            "POSITIONS_RESULT",
            "ORDERS_RESULT",
            "CANCEL_RESULT",
        }:
            await self._relay_result(payload)
            return

        await self._send(
            websocket,
            {
                "type": "ERROR",
                "ok": False,
                "error": f"Unsupported message type: {msg_type or 'UNKNOWN'}",
                "timestamp": utc_now(),
            },
        )

    async def _handle_register(self, websocket: websockets.ServerConnection, payload: dict[str, Any]) -> None:
        role = str(payload.get("role", "")).strip().lower() or "unknown"
        async with self._lock:
            self._roles[websocket] = role
            if role == "extension":
                self._extension = websocket

        await self._send(
            websocket,
            {
                "type": "REGISTER_ACK",
                "ok": True,
                "role": role,
                "timestamp": utc_now(),
            },
        )
        if not self.quiet:
            print(f"[COMMAND] registered role={role}", flush=True)

    async def _handle_place_order(self, websocket: websockets.ServerConnection, payload: dict[str, Any]) -> None:
        request_id = str(payload.get("requestId") or uuid.uuid4())
        side = str(payload.get("side", "")).upper()
        amount = str(payload.get("amount", "")).strip()
        order_type = str(payload.get("orderType", "market")).strip().lower() or "market"
        limit_price = str(payload.get("limitPrice", "")).strip()

        async def reject(error: str) -> None:
            await self._send(
                websocket,
                {
                    "type": "ORDER_RESULT",
                    "requestId": request_id,
                    "ok": False,
                    "error": error,
                    "timestamp": utc_now(),
                },
            )

        if side not in {"BUY", "SELL"}:
            await reject("Invalid side. Use BUY or SELL.")
            return
        if order_type not in {"market", "limit"}:
            await reject("Invalid orderType. Use market or limit.")
            return
        try:
            if float(amount) <= 0:
                raise ValueError
        except ValueError:
            await reject("Invalid amount. Must be positive.")
            return

        market = str(payload.get("market", "")).strip()
        if not market:
            await reject("Missing market (underlying asset symbol).")
            return

        if order_type == "limit":
            try:
                if float(limit_price) <= 0:
                    raise ValueError
            except ValueError:
                await reject("Invalid limitPrice. Required and positive for limit orders.")
                return

        max_slippage = as_float(payload.get("maxSlippage"))
        if max_slippage is None:
            max_slippage = 0.005
        if max_slippage < 0:
            await reject("Invalid maxSlippage. Must be >= 0.")
            return

        reduce_only = bool(payload.get("reduceOnly", False))
        funding_interval_s = payload.get("fundingIntervalS")
        try:
            funding_interval_s = int(funding_interval_s) if funding_interval_s is not None else 3600
        except (TypeError, ValueError):
            await reject("Invalid fundingIntervalS.")
            return
        settlement_asset = str(payload.get("settlementAsset", "USDC")).strip() or "USDC"

        async with self._lock:
            extension = self._extension
            if extension is None:
                await reject("No extension command client connected.")
                return

            self._pending_requests[request_id] = websocket
            forward_payload = {
                "type": "PLACE_ORDER",
                "requestId": request_id,
                "side": side,
                "amount": amount,
                "orderType": order_type,
                "limitPrice": limit_price if order_type == "limit" else None,
                "maxSlippage": max_slippage,
                "reduceOnly": reduce_only,
                "reuseQuoteId": payload.get("reuseQuoteId"),
                "market": market,
                "fundingIntervalS": funding_interval_s,
                "settlementAsset": settlement_asset,
                "account": payload.get("account"),
                "timeoutMs": payload.get("timeoutMs"),
                "timestamp": utc_now(),
            }
            await self._send(extension, forward_payload)

        await self._send(
            websocket,
            {
                "type": "ORDER_DISPATCHED",
                "requestId": request_id,
                "ok": True,
                "timestamp": utc_now(),
            },
        )

    async def _handle_query(
        self,
        websocket: websockets.ServerConnection,
        payload: dict[str, Any],
        forward_type: str,
    ) -> None:
        request_id = str(payload.get("requestId") or uuid.uuid4())

        async def reject(error: str) -> None:
            await self._send(
                websocket,
                {
                    "type": f"{forward_type}_RESULT",
                    "requestId": request_id,
                    "ok": False,
                    "error": error,
                    "timestamp": utc_now(),
                },
            )

        forward_payload: dict[str, Any] = {
            "type": forward_type,
            "requestId": request_id,
            "account": payload.get("account"),
            "timestamp": utc_now(),
        }
        if forward_type == "QUOTE":
            market = str(payload.get("market", "")).strip()
            amount = str(payload.get("amount", "")).strip()
            if not market:
                await reject("Missing market (underlying asset symbol).")
                return
            try:
                if float(amount) <= 0:
                    raise ValueError
            except ValueError:
                await reject("Invalid amount. Must be positive.")
                return
            funding_interval_s = payload.get("fundingIntervalS")
            try:
                funding_interval_s = int(funding_interval_s) if funding_interval_s is not None else 3600
            except (TypeError, ValueError):
                await reject("Invalid fundingIntervalS.")
                return
            forward_payload.update(
                {
                    "market": market,
                    "amount": amount,
                    "fundingIntervalS": funding_interval_s,
                    "settlementAsset": str(payload.get("settlementAsset", "USDC")).strip() or "USDC",
                }
            )

        async with self._lock:
            extension = self._extension
            if extension is None:
                await reject("No extension command client connected.")
                return
            self._pending_requests[request_id] = websocket
            await self._send(extension, forward_payload)

    async def _handle_cancel(
        self,
        websocket: websockets.ServerConnection,
        payload: dict[str, Any],
    ) -> None:
        request_id = str(payload.get("requestId") or uuid.uuid4())
        rfq_id = str(payload.get("rfqId", "")).strip()

        async def reject(error: str) -> None:
            await self._send(
                websocket,
                {
                    "type": "CANCEL_RESULT",
                    "requestId": request_id,
                    "ok": False,
                    "error": error,
                    "timestamp": utc_now(),
                },
            )

        if not rfq_id:
            await reject("Missing rfqId.")
            return

        async with self._lock:
            extension = self._extension
            if extension is None:
                await reject("No extension command client connected.")
                return
            self._pending_requests[request_id] = websocket
            await self._send(
                extension,
                {
                    "type": "CANCEL",
                    "requestId": request_id,
                    "rfqId": rfq_id,
                    "account": payload.get("account"),
                    "timestamp": utc_now(),
                },
            )

    async def _relay_result(self, payload: dict[str, Any]) -> None:
        request_id = str(payload.get("requestId", "")).strip()
        if not request_id:
            return
        async with self._lock:
            requester = self._pending_requests.pop(request_id, None)

        if requester is not None:
            await self._send(requester, payload)
            if not self.quiet:
                print(
                    f"[COMMAND] {payload.get('type')} requestId={request_id} ok={payload.get('ok')}",
                    flush=True,
                )

    async def _send(self, websocket: websockets.ServerConnection, payload: dict[str, Any]) -> None:
        try:
            await websocket.send(json.dumps(payload, ensure_ascii=True))
        except Exception:
            return


async def run_command_server(
    host: str,
    port: int,
    broker: CommandBroker,
) -> websockets.asyncio.server.Server:
    async def handler(websocket: websockets.ServerConnection) -> None:
        await broker.on_connect(websocket)
        try:
            async for message in websocket:
                if isinstance(message, bytes):
                    message = message.decode("utf-8", errors="replace")
                await broker.handle_raw_message(websocket, message)
        finally:
            await broker.on_disconnect(websocket)

    return await websockets.serve(handler, host, port, max_size=None, ping_interval=20, ping_timeout=20)
