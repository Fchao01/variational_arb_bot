"""Monitor/data layer: parses forwarded CDP events into quotes/trades/positions."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import websockets

from exchanges.variational.util import (
    WS_EVENTS_PATH,
    WS_PORTFOLIO_PATH,
    QUOTES_INDICATIVE_PATH,
    _append_line,
    as_float,
    classify_rest_endpoint,
    classify_ws_stream,
    compute_spread,
    decode_response_body,
    decode_ws_frame_payload,
    try_parse_json,
    utc_now,
    write_json_file,
)


QUOTE_LOG_INTERVAL_SECONDS = 30
PORTFOLIO_LOG_INTERVAL_SECONDS = 300
HEARTBEAT_STALE_SECONDS = 11
HEARTBEAT_RECHECK_SECONDS = 10
HEARTBEAT_HOURLY_SECONDS = 3600


@dataclass(slots=True)
class VariationalMonitor:
    trade_limit: int = 20
    snapshot_file: Path | None = None
    trade_event_limit: int = 2000
    quotes: dict[str, dict[str, Any]] = field(default_factory=dict)
    current_quote_asset: str | None = None
    positions: dict[str, dict[str, Any]] = field(default_factory=dict)
    recent_trades: list[dict[str, Any]] = field(default_factory=list)
    trade_events: list[dict[str, Any]] = field(default_factory=list)
    portfolio_summary: dict[str, Any] = field(default_factory=dict)
    last_update_at: str | None = None
    last_heartbeat_iso: str | None = None
    _last_quote_log_ts: float | None = None
    _last_portfolio_log_ts: float | None = None
    _last_heartbeat_monotonic: float | None = None
    _next_heartbeat_check_ts: float = 0.0
    _stale_alert_sent: bool = False
    _last_hourly_alert_hour: int = 0
    _next_trade_event_seq: int = 1
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, repr=False)

    async def process_rest_event(self, payload: dict[str, Any]) -> list[str]:
        if payload.get("kind") != "rest_response":
            return []

        url = str(payload.get("url", ""))
        endpoint = classify_rest_endpoint(url)
        if endpoint != QUOTES_INDICATIVE_PATH:
            return []

        body = decode_response_body(payload)
        if body is None:
            return [f"[MONITOR] Failed to decode REST body for {url}"]

        parsed = try_parse_json(body)
        if parsed is None:
            return [f"[MONITOR] REST body is not JSON for {url}"]

        async with self._lock:
            self._update_quote(parsed, payload.get("reqQty"))
            self.last_update_at = utc_now()
            if self.snapshot_file is not None:
                await asyncio.to_thread(write_json_file, self.snapshot_file, self.snapshot())

        return []

    async def process_ws_event(self, payload: dict[str, Any]) -> list[str]:
        kind = str(payload.get("kind", ""))
        if kind != "ws_frame":
            return []
        if payload.get("direction") != "received":
            return []

        url = str(payload.get("url", ""))
        stream = classify_ws_stream(url)
        if stream is None:
            return []

        message_text = decode_ws_frame_payload(payload)
        if message_text is None:
            return [f"[MONITOR] Failed to decode WS frame for {url}"]

        parsed = try_parse_json(message_text)
        if parsed is None:
            return []

        async with self._lock:
            lines: list[str] = []
            now_ts = asyncio.get_running_loop().time()
            if stream == WS_EVENTS_PATH:
                for event in self._iter_event_messages(parsed):
                    self._update_heartbeat(event, now_ts)
                    trade_line = self._update_trade_event(event)
                    if trade_line:
                        lines.append(trade_line)
                        portfolio_line = self._format_portfolio_line()
                        if portfolio_line:
                            lines.append(f"{portfolio_line} trigger=trade")
                            self._last_portfolio_log_ts = now_ts
            elif stream == WS_PORTFOLIO_PATH:
                self._update_portfolio(parsed)

            if not lines and stream != WS_PORTFOLIO_PATH:
                return []

            self.last_update_at = utc_now()
            if self.snapshot_file is not None:
                await asyncio.to_thread(write_json_file, self.snapshot_file, self.snapshot())
            return lines

    def _iter_event_messages(self, payload: Any) -> list[dict[str, Any]]:
        if isinstance(payload, dict):
            out = [payload]
            events = payload.get("events")
            if isinstance(events, list):
                out.extend([item for item in events if isinstance(item, dict)])
            data = payload.get("data")
            if isinstance(data, list):
                out.extend([item for item in data if isinstance(item, dict) and "type" in item])
            return out

        if isinstance(payload, list):
            return [item for item in payload if isinstance(item, dict)]

        return []

    async def emit_periodic_logs(self) -> tuple[list[str], list[str]]:
        lines: list[str] = []
        alerts: list[str] = []
        async with self._lock:
            now_ts = asyncio.get_running_loop().time()
            if self.quotes and self._should_log_quote(now_ts):
                quote_line = self._format_quote_line()
                if quote_line:
                    lines.append(quote_line)
                    self._last_quote_log_ts = now_ts

            if self.positions and self._should_log_portfolio(now_ts):
                portfolio_line = self._format_portfolio_line()
                if portfolio_line:
                    lines.append(f"{portfolio_line} trigger=interval")
                    self._last_portfolio_log_ts = now_ts

            alerts.extend(self._collect_heartbeat_alerts(now_ts))

        return lines, alerts

    def _update_quote(self, payload: Any, req_qty: Any = None) -> None:
        if not isinstance(payload, dict):
            return

        instrument = payload.get("instrument")
        if not isinstance(instrument, dict):
            return

        asset = str(instrument.get("underlying", "UNKNOWN"))
        bid = payload.get("bid")
        ask = payload.get("ask")
        mark = payload.get("mark_price")
        ts = payload.get("timestamp")

        # `qty` is the size the indicative was priced for (from the forwarder's
        # paired request body, not echoed in the response). The quote_id is
        # bound to this qty, so the engine reuses it to size both legs.
        self.quotes[asset] = {
            "asset": asset,
            "bid": bid,
            "ask": ask,
            "mark_price": mark,
            "timestamp": ts,
            "qty": req_qty,
            "raw": payload,
        }
        self.current_quote_asset = asset

    def _update_trade_event(self, payload: Any) -> str | None:
        if not isinstance(payload, dict):
            return None
        event_type = str(payload.get("type", "")).strip().lower()
        if "trade" not in event_type:
            return None

        data = payload.get("data") if isinstance(payload.get("data"), dict) else payload

        instrument = data.get("instrument")
        asset = "UNKNOWN"
        if isinstance(instrument, dict):
            asset = str(instrument.get("underlying", "UNKNOWN"))

        trade_id = str(data.get("id", ""))
        summary = {
            "timestamp": data.get("created_at") or payload.get("timestamp") or "-",
            "trade_id": trade_id,
            "side": data.get("side", "-"),
            "asset": asset,
            "price": data.get("price", "-"),
            "qty": data.get("qty", "-"),
            "status": data.get("status", "-"),
            "role": data.get("role", "-"),
            "received_at": utc_now(),
            "raw": payload,
        }

        summary["event_seq"] = self._next_trade_event_seq
        self._next_trade_event_seq += 1

        if trade_id:
            self.recent_trades = [t for t in self.recent_trades if t.get("trade_id") != trade_id]
        self.recent_trades.insert(0, summary)
        self.recent_trades = self.recent_trades[: self.trade_limit]
        self.trade_events.append(summary)
        if len(self.trade_events) > self.trade_event_limit:
            self.trade_events = self.trade_events[-self.trade_event_limit:]

        trade_id_short = trade_id[:8] if trade_id else "-"
        return (
            f"[MONITOR] TRADE {summary['side']} {summary['qty']} {summary['asset']} "
            f"@{summary['price']} status={summary['status']} role={summary['role']} id={trade_id_short}"
        )

    def _update_portfolio(self, payload: Any) -> None:
        if not isinstance(payload, dict):
            return

        positions_data = payload.get("positions")
        if not isinstance(positions_data, list):
            return

        next_positions: dict[str, dict[str, Any]] = {}
        for item in positions_data:
            if not isinstance(item, dict):
                continue
            position_info = item.get("position_info")
            if not isinstance(position_info, dict):
                continue
            instrument = position_info.get("instrument")
            if not isinstance(instrument, dict):
                continue

            asset = str(instrument.get("underlying", "UNKNOWN"))
            next_positions[asset] = {
                "asset": asset,
                "qty": position_info.get("qty"),
                "avg_entry_price": position_info.get("avg_entry_price"),
                "updated_at": position_info.get("updated_at"),
                "value": item.get("value"),
                "upnl": item.get("upnl"),
                "rpnl": item.get("rpnl"),
                "raw": item,
            }

        pool = payload.get("pool_portfolio_result")
        margin = {}
        if isinstance(pool, dict):
            margin_raw = pool.get("margin_usage")
            if isinstance(margin_raw, dict):
                margin = {
                    "initial_margin": margin_raw.get("initial_margin"),
                    "maintenance_margin": margin_raw.get("maintenance_margin"),
                }

        self.positions = next_positions
        self.portfolio_summary = {
            "balance": pool.get("balance") if isinstance(pool, dict) else None,
            "upnl": pool.get("upnl") if isinstance(pool, dict) else None,
            "margin_usage": margin,
            "published_at": payload.get("published_at"),
            "raw": pool if isinstance(pool, dict) else {},
        }

    def _should_log_quote(self, now_ts: float) -> bool:
        if self._last_quote_log_ts is None:
            return True
        return now_ts - self._last_quote_log_ts >= QUOTE_LOG_INTERVAL_SECONDS

    def _should_log_portfolio(self, now_ts: float) -> bool:
        if self._last_portfolio_log_ts is None:
            return True
        return now_ts - self._last_portfolio_log_ts >= PORTFOLIO_LOG_INTERVAL_SECONDS

    def _update_heartbeat(self, payload: Any, now_ts: float) -> None:
        if not isinstance(payload, dict):
            return
        if payload.get("type") != "heartbeat":
            return

        self._last_heartbeat_monotonic = now_ts
        timestamp = payload.get("timestamp")
        if isinstance(timestamp, str):
            self.last_heartbeat_iso = timestamp
        else:
            self.last_heartbeat_iso = utc_now()

        self._stale_alert_sent = False
        self._last_hourly_alert_hour = 0
        self._next_heartbeat_check_ts = now_ts + 1

    def _collect_heartbeat_alerts(self, now_ts: float) -> list[str]:
        if self._last_heartbeat_monotonic is None:
            return []
        if now_ts < self._next_heartbeat_check_ts:
            return []

        age_seconds = now_ts - self._last_heartbeat_monotonic
        if age_seconds <= HEARTBEAT_STALE_SECONDS:
            self._next_heartbeat_check_ts = now_ts + 1
            return []

        self._next_heartbeat_check_ts = now_ts + HEARTBEAT_RECHECK_SECONDS
        alerts: list[str] = []
        last_seen = self.last_heartbeat_iso or "unknown"
        if not self._stale_alert_sent:
            alerts.append(
                f"Heartbeat stale: last heartbeat {age_seconds:.1f}s ago (last_seen={last_seen})."
            )
            self._stale_alert_sent = True

        stale_hours = int(age_seconds // HEARTBEAT_HOURLY_SECONDS)
        if stale_hours >= 1 and stale_hours > self._last_hourly_alert_hour:
            alerts.append(
                f"Heartbeat still stale for {stale_hours}h (last_seen={last_seen})."
            )
            self._last_hourly_alert_hour = stale_hours

        return alerts

    def _format_quote_line(self) -> str | None:
        if not self.current_quote_asset:
            return None
        quote = self.quotes.get(self.current_quote_asset)
        if not quote:
            return None
        spread = compute_spread(quote.get("bid"), quote.get("ask"))
        spread_part = f" spread={spread}" if spread is not None else ""
        return (
            f"[MONITOR] QUOTE {self.current_quote_asset} bid={quote.get('bid')} "
            f"ask={quote.get('ask')}{spread_part} mark={quote.get('mark_price')}"
        )

    def _format_portfolio_line(self) -> str | None:
        if not self.current_quote_asset:
            return None
        row = self.positions.get(self.current_quote_asset)
        if row is None:
            position_part = f"{self.current_quote_asset} qty=0 upnl=0"
        else:
            position_part = (
                f"{self.current_quote_asset} qty={row.get('qty')} upnl={row.get('upnl')}"
            )
        return (
            f"[MONITOR] PORTFOLIO balance={self.portfolio_summary.get('balance')} "
            f"upnl={self.portfolio_summary.get('upnl')} asset={position_part}"
        )

    def snapshot(self) -> dict[str, Any]:
        return {
            "generated_at": utc_now(),
            "last_update_at": self.last_update_at,
            "current_quote_asset": self.current_quote_asset,
            "last_heartbeat_iso": self.last_heartbeat_iso,
            "quotes": self.quotes,
            "positions": self.positions,
            "recent_trades": self.recent_trades,
            "trade_events": self.trade_events,
            "portfolio_summary": self.portfolio_summary,
        }

    async def get_trading_state(self) -> dict[str, Any]:
        async with self._lock:
            now_ts = asyncio.get_running_loop().time()
            heartbeat_age: float | None = None
            if self._last_heartbeat_monotonic is not None:
                heartbeat_age = max(0.0, now_ts - self._last_heartbeat_monotonic)

            asset = self.current_quote_asset
            quote = self.quotes.get(asset) if asset else None
            row = self.positions.get(asset) if asset else None
            qty = 0.0
            if isinstance(row, dict):
                qty_val = as_float(row.get("qty"))
                if qty_val is not None:
                    qty = qty_val

            return {
                "asset": asset,
                "position": qty,
                "position_row": row,
                "quote": quote,
                "has_quote": quote is not None,
                "has_portfolio": bool(self.portfolio_summary),
                "last_update_at": self.last_update_at,
                "last_heartbeat_iso": self.last_heartbeat_iso,
                "heartbeat_age": heartbeat_age,
            }

    async def get_trade_events_since(
        self,
        min_event_seq: int,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        async with self._lock:
            events = [event for event in self.trade_events if int(event.get("event_seq", 0)) > min_event_seq]
            if limit > 0:
                events = events[:limit]
            return events

    async def get_latest_trade_event_seq(self) -> int:
        async with self._lock:
            return self._next_trade_event_seq - 1


class EventSink:
    def __init__(
        self,
        output_dir: Path | None,
        quiet: bool = False,
        monitor: VariationalMonitor | None = None,
    ) -> None:
        self.output_dir = output_dir
        self.quiet = quiet
        self.monitor = monitor
        self._write_lock = asyncio.Lock()
        if self.output_dir is not None:
            self.output_dir.mkdir(parents=True, exist_ok=True)

    async def handle(self, channel: str, raw_message: str) -> None:
        parsed: dict[str, Any] | str
        try:
            parsed = json.loads(raw_message)
        except json.JSONDecodeError:
            parsed = raw_message

        envelope = {
            "ingested_at": utc_now(),
            "channel": channel,
            "payload": parsed,
        }

        if self.monitor and isinstance(parsed, dict):
            lines: list[str] = []
            if channel == "rest":
                lines = await self.monitor.process_rest_event(parsed)
            elif channel == "ws":
                lines = await self.monitor.process_ws_event(parsed)
            if not self.quiet:
                for line in lines:
                    print(line, flush=True)

        if self.output_dir is not None:
            file_name = "ws_events.jsonl" if channel == "ws" else "rest_events.jsonl"
            await self._append_jsonl(self.output_dir / file_name, envelope)

    async def _append_jsonl(self, path: Path, obj: dict[str, Any]) -> None:
        line = json.dumps(obj, ensure_ascii=True) + "\n"
        async with self._write_lock:
            await asyncio.to_thread(_append_line, path, line)


async def run_receiver_server(
    channel: str,
    host: str,
    port: int,
    sink: EventSink,
) -> websockets.asyncio.server.Server:
    async def handler(websocket: websockets.ServerConnection) -> None:
        async for message in websocket:
            if isinstance(message, bytes):
                message = message.decode("utf-8", errors="replace")
            await sink.handle(channel, message)

    return await websockets.serve(handler, host, port, max_size=None, ping_interval=20, ping_timeout=20)
