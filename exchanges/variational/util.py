"""Shared helpers for the Variational local tooling (no monitor/command deps)."""

from __future__ import annotations

import base64
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


QUOTES_INDICATIVE_PATH = "/api/quotes/indicative"
WS_EVENTS_PATH = "/events"
WS_PORTFOLIO_PATH = "/portfolio"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _append_line(path: Path, line: str) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(line)


def write_json_file(path: Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=True, indent=2)


def classify_rest_endpoint(url: str) -> str | None:
    try:
        path = urlparse(url).path
    except ValueError:
        return None
    if path == QUOTES_INDICATIVE_PATH:
        return QUOTES_INDICATIVE_PATH
    return None


def classify_ws_stream(url: str) -> str | None:
    try:
        path = urlparse(url).path
    except ValueError:
        return None
    if path == WS_EVENTS_PATH:
        return WS_EVENTS_PATH
    if path == WS_PORTFOLIO_PATH:
        return WS_PORTFOLIO_PATH
    return None


def decode_response_body(payload: dict[str, Any]) -> str | None:
    body = payload.get("body")
    if not isinstance(body, str):
        return None
    if payload.get("base64Encoded"):
        try:
            return base64.b64decode(body).decode("utf-8", errors="replace")
        except Exception:
            return None
    return body


def decode_ws_frame_payload(payload: dict[str, Any]) -> str | None:
    data = payload.get("payloadData")
    if not isinstance(data, str):
        return None

    opcode = payload.get("opcode")
    if opcode == 2:
        stripped = data.lstrip()
        if stripped.startswith("{") or stripped.startswith("["):
            return data
        try:
            decoded = base64.b64decode(data)
            return decoded.decode("utf-8", errors="replace")
        except Exception:
            return data

    return data


def try_parse_json(text: str) -> Any | None:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def as_float(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def compute_spread(bid: Any, ask: Any) -> str | None:
    bid_val = as_float(bid)
    ask_val = as_float(ask)
    if bid_val is None or ask_val is None:
        return None
    return f"{ask_val - bid_val:.8f}"
