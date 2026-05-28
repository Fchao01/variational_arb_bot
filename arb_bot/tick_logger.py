"""Per-tick market-data + basis-signal recorder.

Writes one CSV row per decision-loop tick (fresh Var RFQ + Lighter book)
with both the raw prices and the live basis-MR signal state (s_bps,
mu_bps, sigma_bps, z) so a CSV alone is enough to replay any parameter
sweep without re-running the bot. Append-only and flushed every row so a
`kill` never loses the tail. One file per coin (ticks_<ASSET>.csv).

Trader.tick() calls this on EVERY tick — including hold-loop ticks —
so the time series has no gap during in-flight rounds.

With fees == 0 and slippage taken as 0, realized round-trip PnL is
reconstructable from the raw bid/ask columns alone; the signal columns
exist for online verification / offline replay of Z_ENTRY/Z_EXIT/Z_STOP.
"""

from __future__ import annotations

import csv
from decimal import Decimal
from pathlib import Path

from arb_bot.helpers import utc_now

_BPS = Decimal("10000")

_COLS = [
    "ts", "var_bid", "var_ask", "quote_id", "qty",
    "lighter_bid", "lighter_ask",
    "var_spread_bps", "lighter_spread_bps", "long_bps", "short_bps",
    "s_bps", "mu_bps", "sigma_bps", "z",
]


def _spread_bps(bid: Decimal, ask: Decimal) -> Decimal:
    mid = (bid + ask) / 2
    if mid <= 0:
        return Decimal("0")
    return (ask - bid) / mid * _BPS


def _fmt(value: float | None, fmt: str) -> str:
    return "" if value is None else format(value, fmt)


class TickLogger:
    def __init__(self, path: Path) -> None:
        self._path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        new = not path.exists() or path.stat().st_size == 0
        self._fh = path.open("a", newline="", encoding="utf-8")
        self._w = csv.writer(self._fh)
        if new:
            self._w.writerow(_COLS)
            self._fh.flush()

    def log(
        self,
        var_bid: Decimal,
        var_ask: Decimal,
        quote_id: str,
        qty: Decimal,
        lighter_bid: Decimal,
        lighter_ask: Decimal,
        long_bps: Decimal,
        short_bps: Decimal,
        s_bps: float | None,
        mu_bps: float | None,
        sigma_bps: float | None,
        z: float | None,
    ) -> None:
        self._w.writerow([
            utc_now(),
            f"{var_bid}", f"{var_ask}", quote_id, f"{qty}",
            f"{lighter_bid}", f"{lighter_ask}",
            f"{float(_spread_bps(var_bid, var_ask)):.4f}",
            f"{float(_spread_bps(lighter_bid, lighter_ask)):.4f}",
            f"{float(long_bps):.4f}", f"{float(short_bps):.4f}",
            _fmt(s_bps, ".4f"),
            _fmt(mu_bps, ".4f"),
            _fmt(sigma_bps, ".4f"),
            _fmt(z, ".3f"),
        ])
        self._fh.flush()

    def close(self) -> None:
        try:
            self._fh.flush()
            self._fh.close()
        except Exception:
            pass
