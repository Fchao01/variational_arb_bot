import os
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from arb_bot.config import VARIATIONAL_ASSET_TO_LIGHTER_TICKER


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_server_ts(value: Any) -> float | None:
    """Server-side timestamp → unix epoch seconds (float). Tolerates the
    three shapes both venues emit: ISO-8601 strings (Variational `utc_now`
    style), integer ms (Lighter top-level `timestamp`), and integer μs
    (Lighter `order_book.last_updated_at`). Returns None when unparseable
    so callers can fall back to a local-receive timestamp without
    branching on type."""
    if value is None:
        return None
    if isinstance(value, bool):
        # bool is a subclass of int — exclude before the numeric branch.
        return None
    if isinstance(value, (int, float)):
        v = float(value)
        if v <= 0:
            return None
        # Heuristic on magnitude: >1e15 μs (~2001), >1e12 ms (~2001),
        # otherwise seconds. Works for any timestamp this side of 2001.
        if v > 1e15:
            return v / 1e6
        if v > 1e12:
            return v / 1e3
        return v
    if isinstance(value, str):
        s = value.strip()
        if not s:
            return None
        try:
            # `Z` suffix is ISO-valid but datetime.fromisoformat rejects it
            # pre-3.11 quirk-style; normalize for forward-compat.
            return datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return None
    return None


def to_decimal(value: Any) -> Decimal | None:
    if value is None:
        return None
    try:
        return Decimal(str(value))
    except Exception:
        return None


def resolve_lighter_ticker(variational_asset: str) -> str:
    asset = variational_asset.upper()
    return VARIATIONAL_ASSET_TO_LIGHTER_TICKER.get(asset, asset)


def required_env(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def required_int_env(name: str) -> int:
    value = required_env(name)
    try:
        return int(value)
    except ValueError as exc:
        raise RuntimeError(f"{name} must be an integer, got: {value}") from exc


def env_flag(name: str) -> bool:
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def normalize_variational_status(status: str) -> str:
    lowered = status.strip().lower()
    if lowered == "confirmed":
        return "filled"
    return lowered
