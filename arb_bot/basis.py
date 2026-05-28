"""Basis mean-reversion signal + entry/exit decision (no network IO).

Trades the basis deviation:

    s_t = (var_mid - lighter_mid) / lighter_mid * 1e4   [bps]

against a strictly-causal time-aware online EWMA mean/variance:

    z_t = (s_t - mu_{<t}) / sigma_{<t}

z at tick t uses only mu/sigma from ticks before t, so the live z-series is
reproducible tick-for-tick from a recorded tape (`main.py --record` replays the same path).

What this module DOES:
- maintain per-coin EWMA state (BasisState)
- compute z on each tick and persist it (+ raw prices) to ticks_<COIN>.csv
- decide entry direction when |z| >= Z_ENTRY
- decide exit reason when z reverts to Z_EXIT or blows through Z_STOP

What this module does NOT do:
- place orders, talk to Variational/Lighter, settle PnL, or know anything
  about execution. The execution layer (arb_bot.trader) calls tick() /
  entry() / exit_decision() and acts on their returns.

The Z_* thresholds live ONLY here — trader.py never imports them.
"""

from __future__ import annotations

import math
from decimal import Decimal
from pathlib import Path

from arb_bot.config import (
    BASIS_GAP_RESET_S,
    BASIS_HL_SECONDS,
    BASIS_SIGMA_FLOOR_BPS,
    BASIS_WARMUP_SAMPLES,
    DOCS_DIR,
)
from arb_bot.live_config import LiveConfig
from arb_bot.quotes import Quote, edges
from arb_bot.tick_logger import TickLogger


def basis_bps(
    var_bid: Decimal, var_ask: Decimal,
    lt_bid: Decimal, lt_ask: Decimal,
) -> float | None:
    """s = (var_mid - lt_mid) / lt_mid * 1e4. None on a degenerate side."""
    vmid = (float(var_bid) + float(var_ask)) / 2.0
    lmid = (float(lt_bid) + float(lt_ask)) / 2.0
    if vmid <= 0 or lmid <= 0:
        return None
    return (vmid - lmid) / lmid * 1e4


class BasisState:
    """Strictly-causal, time-aware online EWMA of one coin's basis.

    `update(ts, s)` computes z against mu/sigma_{<t} (BEFORE folding the
    current tick in), then folds the tick in.

    A gap of more than `gap_reset_s` between consecutive ticks (Variational
    quote outage, restart, etc.) wipes mu/var/seen so warmup restarts —
    otherwise the EWMA carries a stale mean across a session break.
    """

    __slots__ = (
        "_hl", "_warmup", "_gap_reset", "_sigma_floor",
        "mu", "var", "seen", "last_ts",
        "z_s", "z_mu", "z_sigma",
    )

    def __init__(
        self, hl_s: float = BASIS_HL_SECONDS,
        warmup: int = BASIS_WARMUP_SAMPLES,
        gap_reset_s: float = BASIS_GAP_RESET_S,
        sigma_floor_bps: float = BASIS_SIGMA_FLOOR_BPS,
    ) -> None:
        self._hl = hl_s
        self._warmup = warmup
        self._gap_reset = gap_reset_s
        self._sigma_floor = sigma_floor_bps
        self.mu: float | None = None
        self.var: float = 0.0
        self.seen: int = 0
        self.last_ts: float | None = None
        # (s, mu, sigma) that produced the most recent z, snapshotted BEFORE
        # folding each tick in — the diag recorder reads these for entry state.
        self.z_s: float | None = None
        self.z_mu: float | None = None
        self.z_sigma: float | None = None

    @property
    def sigma(self) -> float | None:
        """Raw EWMA sqrt(var); None until at least one tick seen. The
        floor below is a TRADE GATE in update(), NOT a clamp here — exposing
        the raw value lets the tick CSV record the actual EWMA state."""
        if self.mu is None or self.seen < 1:
            return None
        return math.sqrt(self.var)

    def update(self, ts: float, s: float) -> tuple[float, bool]:
        """Fold tick (ts, s) into the EWMA; return (z, warm).

        warm requires BOTH gates pass: (a) seen >= warmup samples AND
        (b) sqrt(var) > sigma_floor. The floor is the minimum tradable
        sigma — when raw var is still below it (startup, post-gap, flat
        patch), entries are suppressed instead of letting the floor
        amplify quote noise into fake big-z events. Previously the floor
        was applied via max() inside z, which is why our first live run
        produced 5/5 adverse_stop on -1.66 bps/round.
        """
        if self.last_ts is not None and ts - self.last_ts > self._gap_reset:
            self.mu, self.var, self.seen = None, 0.0, 0
        dt = (ts - self.last_ts) if self.last_ts is not None else None
        self.last_ts = ts

        if self.mu is None or self.seen < self._warmup:
            z, warm = 0.0, False
        else:
            raw_sigma = math.sqrt(self.var)
            if raw_sigma <= self._sigma_floor:
                z, warm = 0.0, False
            else:
                z, warm = (s - self.mu) / raw_sigma, True

        # Snapshot the inputs that produced z, BEFORE folding this tick in.
        self.z_s = s
        self.z_mu = self.mu
        self.z_sigma = math.sqrt(self.var) if self.mu is not None else None

        if self.mu is None:
            self.mu, self.var, self.seen = s, 0.0, 1
        else:
            a = 1.0 - 0.5 ** (max(dt or 1.0, 1e-3) / self._hl)
            diff = s - self.mu
            self.mu += a * diff
            self.var = (1.0 - a) * (self.var + a * diff * diff)
            self.seen += 1
        return z, warm


class BasisStrategy:
    """Per-coin BasisState + per-coin TickLogger, plus the entry/exit
    threshold decisions. The decision loop and the in-round hold loop
    both call `tick()` on EVERY tick, so the EWMA and the tick CSV stay
    continuous across an in-flight round.

    Z thresholds live in `live_config` and may change between ticks
    (hot reload), so entry()/exit_decision() are instance methods that
    read live values on each call."""

    def __init__(self, assets: list[str], live_config: LiveConfig) -> None:
        self.states: dict[str, BasisState] = {a: BasisState() for a in assets}
        self.tick_loggers: dict[str, TickLogger] = {}
        self._live = live_config

    def attach_tick_loggers(self, assets: list[str]) -> None:
        """Open one TickLogger per coin under DOCS_DIR/ticks_<COIN>.csv."""
        for coin in assets:
            self.tick_loggers[coin] = TickLogger(
                Path(DOCS_DIR) / f"ticks_{coin}.csv"
            )

    def close_tick_loggers(self) -> None:
        for tl in self.tick_loggers.values():
            try:
                tl.close()
            except Exception:
                pass
        self.tick_loggers.clear()

    def tick(
        self,
        ts: float,
        coin: str,
        var_quote: Quote,
        lighter_bid: Decimal,
        lighter_ask: Decimal,
    ) -> tuple[float, bool]:
        """Update `coin`'s EWMA and persist one CSV row. Returns (z, warm);
        z=0.0/warm=False while warming up or on degenerate input."""
        s = basis_bps(var_quote.bid, var_quote.ask, lighter_bid, lighter_ask)
        if s is None:
            return 0.0, False
        state = self.states[coin]
        z, warm = state.update(ts, s)

        tl = self.tick_loggers.get(coin)
        if tl is not None:
            long_bps, short_bps = edges(var_quote, lighter_bid, lighter_ask)
            tl.log(
                var_quote.bid, var_quote.ask, var_quote.quote_id, var_quote.qty,
                lighter_bid, lighter_ask, long_bps, short_bps,
                s_bps=s, mu_bps=state.mu, sigma_bps=state.sigma,
                z=z if warm else None,
            )
        return z, warm

    def signal_snapshot(
        self, coin: str
    ) -> tuple[float | None, float | None, float | None]:
        """(s, mu, sigma) of the most recent tick for `coin` — the exact
        values that produced the last z. Read right after tick() at the
        decision moment; the in-round hold loop overwrites this state on
        every tick, so the entry snapshot must be taken before run_round."""
        st = self.states[coin]
        return st.z_s, st.z_mu, st.z_sigma

    def entry(self, z: float, warm: bool) -> tuple[str, float] | None:
        """warm & |z| >= z_entry -> (direction, z); else None.
        z > 0 (Var rich)  -> short_var (sell Var, buy Lighter)
        z < 0 (Var cheap) -> long_var  (buy Var, sell Lighter)
        """
        if not warm or abs(z) < self._live.z_entry:
            return None
        return ("short_var" if z > 0 else "long_var"), z

    def exit_decision(self, z: float, warm: bool, direction: str) -> str | None:
        """'reverted' (|z| decayed to z_exit) /
        'adverse_stop' (z blew past z_stop the wrong way) / None (keep)."""
        if not warm:
            return None
        z_exit = self._live.z_exit
        z_stop = self._live.z_stop
        if direction == "short_var":
            if z <= z_exit:
                return "reverted"
            if z >= z_stop:
                return "adverse_stop"
        else:
            if z >= -z_exit:
                return "reverted"
            if z <= -z_stop:
                return "adverse_stop"
        return None
