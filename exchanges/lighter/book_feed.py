"""Read-only Lighter order-book feed.

Public, unauthenticated top-of-book over one WebSocket. Kept separate
from the order path (executor.py) so pricing the Lighter leg never
touches the signer.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import requests
import websockets

from arb_bot.config import (
    LIGHTER_WS_PING_INTERVAL_SECONDS,
    LIGHTER_WS_PING_TIMEOUT_SECONDS,
    LIGHTER_WS_URL,
)


class _Book:
    __slots__ = ("bids", "asks", "offset", "best_bid", "best_ask", "ready",
                 "last_update_at", "last_server_at")

    def __init__(self) -> None:
        self.bids: dict[Decimal, Decimal] = {}
        self.asks: dict[Decimal, Decimal] = {}
        self.offset = 0
        self.best_bid: Decimal | None = None
        self.best_ask: Decimal | None = None
        self.ready = False
        # monotonic seconds of the most recent WS update merged here —
        # callers measure cache staleness at decision time (dt_book_age).
        self.last_update_at: float = 0.0
        # Lighter match-engine apply time for this frame (epoch seconds, from
        # order_book.last_updated_at μs). Captures the L→us network leg, not
        # just local cache age.
        self.last_server_at: float = 0.0

    def apply(self, side: str, levels: list[Any]) -> None:
        book = self.bids if side == "bids" else self.asks
        for level in levels:
            if isinstance(level, list) and len(level) >= 2:
                price = Decimal(str(level[0]))
                size = Decimal(str(level[1]))
            elif isinstance(level, dict):
                price = Decimal(str(level.get("price", 0)))
                size = Decimal(str(level.get("size", 0)))
            else:
                continue
            if size > 0:
                book[price] = size
            else:
                book.pop(price, None)

    def recompute(self) -> None:
        self.best_bid = max(self.bids) if self.bids else None
        self.best_ask = min(self.asks) if self.asks else None


class BookFeed:
    """Maintains top-of-book for a fixed set of Lighter symbols over one WS."""

    def __init__(
        self,
        base_url: str,
        symbols: list[str],
        logger: logging.Logger,
        stop_fn: Callable[[], bool],
    ) -> None:
        self._base_url = base_url
        self._symbols = list(dict.fromkeys(s.upper() for s in symbols))
        self._logger = logger
        self._stop = stop_fn

        self._symbol_to_id: dict[str, int] = {}
        self._id_to_symbol: dict[int, str] = {}
        self._books: dict[int, _Book] = {}
        self._lock = asyncio.Lock()
        self._task: asyncio.Task[None] | None = None

    def load_market_meta(self) -> None:
        resp = requests.get(
            f"{self._base_url}/api/v1/orderBooks",
            headers={"accept": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        catalog = {
            str(m.get("symbol")): int(m["market_id"])
            for m in resp.json().get("order_books", [])
            if m.get("symbol") is not None and m.get("market_id") is not None
        }
        for sym in self._symbols:
            mid = catalog.get(sym)
            if mid is None:
                self._logger.warning("Lighter book: %s not listed, skipping", sym)
                continue
            self._symbol_to_id[sym] = mid
            self._id_to_symbol[mid] = sym
            self._books[mid] = _Book()

    async def start(self) -> None:
        if not self._symbol_to_id:
            self.load_market_meta()
        self._task = asyncio.create_task(self._run())

    async def close(self) -> None:
        if self._task is not None and not self._task.done():
            self._task.cancel()
            await asyncio.gather(self._task, return_exceptions=True)

    async def get_book(
        self, symbol: str
    ) -> tuple[Decimal | None, Decimal | None, float, float]:
        """(best_bid, best_ask, last_update_monotonic_seconds,
        last_server_unix_seconds). Both timestamps are 0.0 if the book
        is not ready yet. The monotonic one drives local-staleness
        bookkeeping; the server one is the Lighter match-engine apply
        time (`order_book.last_updated_at` μs → s), used to attribute
        the L→us network leg in latency logs."""
        mid = self._symbol_to_id.get(symbol.upper())
        if mid is None:
            return None, None, 0.0, 0.0
        async with self._lock:
            book = self._books.get(mid)
            if book is None or not book.ready:
                return None, None, 0.0, 0.0
            return (book.best_bid, book.best_ask,
                    book.last_update_at, book.last_server_at)

    async def _subscribe_all(self, ws: Any) -> None:
        for mid in self._symbol_to_id.values():
            await ws.send(
                json.dumps({"type": "subscribe", "channel": f"order_book/{mid}"})
            )

    async def _run(self) -> None:
        while not self._stop():
            try:
                async with websockets.connect(
                    LIGHTER_WS_URL,
                    ping_interval=LIGHTER_WS_PING_INTERVAL_SECONDS,
                    ping_timeout=LIGHTER_WS_PING_TIMEOUT_SECONDS,
                ) as ws:
                    await self._subscribe_all(ws)
                    while not self._stop():
                        raw = await ws.recv()
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8", errors="replace")
                        data = json.loads(raw)
                        await self._handle(ws, data)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self._logger.warning("Lighter book WS reconnect: %s", exc)
                await asyncio.sleep(1)

    @staticmethod
    def _channel_market_id(data: dict[str, Any]) -> int | None:
        channel = str(data.get("channel", ""))
        if "order_book" in channel:
            tail = channel.rsplit("/", 1)[-1] if "/" in channel else channel
            try:
                return int(tail.split(":")[-1])
            except ValueError:
                return None
        return None

    async def _handle(self, ws: Any, data: dict[str, Any]) -> None:
        msg_type = data.get("type")
        if msg_type == "ping":
            await ws.send(json.dumps({"type": "pong"}))
            return

        if msg_type not in ("subscribed/order_book", "update/order_book"):
            return

        mid = self._channel_market_id(data)
        if mid is None or mid not in self._books:
            return
        order_book = data.get("order_book", {})
        new_offset = int(order_book.get("offset", 0) or 0)

        # Server-side apply time of this frame (μs since epoch); see last_server_at.
        server_us = order_book.get("last_updated_at")
        try:
            server_at = float(server_us) / 1e6 if server_us is not None else 0.0
        except (TypeError, ValueError):
            server_at = 0.0

        async with self._lock:
            book = self._books[mid]
            if msg_type == "subscribed/order_book":
                book.bids.clear()
                book.asks.clear()
                book.offset = new_offset
                book.apply("bids", order_book.get("bids", []))
                book.apply("asks", order_book.get("asks", []))
                book.recompute()
                book.ready = True
                book.last_update_at = time.monotonic()
                book.last_server_at = server_at
                return

            if not book.ready:
                return
            if new_offset <= book.offset:
                # Sequence gap — re-snapshot this market and stop serving the
                # now-suspect top-of-book until the fresh snapshot lands.
                book.ready = False
                await ws.send(
                    json.dumps({"type": "subscribe", "channel": f"order_book/{mid}"})
                )
                return
            book.apply("bids", order_book.get("bids", []))
            book.apply("asks", order_book.get("asks", []))
            book.offset = new_offset
            book.recompute()
            book.last_update_at = time.monotonic()
            book.last_server_at = server_at
