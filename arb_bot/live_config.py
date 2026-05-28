"""Hot-reloadable runtime knobs.

Reads ./live_config.json (next to main.py). Each `maybe_reload()` call
stats the file; if mtime has changed, atomically re-parses and overwrites
the live values. The bot never crashes on a typo — a bad JSON or missing
key keeps the previous good values and logs an error.

Why not in config.py? Constants there are imported once at startup;
changing them needs a kill+restart (which means ~250s warmup again).
Anything we want to tweak mid-session lives here.

What lives here vs config.py:
- live_config.json : the *decision* knobs — Z_*, MAX_HOLD, notional, cap,
  slippage budgets. Tweaking these doesn't touch EWMA state, so we can
  do it without restarting the bot.
- config.py        : *structural* constants — endpoints, paths, the EWMA
  HL/warmup/floor (changing these mid-flight breaks the state machine),
  RFQ pacing (changes the signal time scale).
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


LIVE_CONFIG_FILE = Path("./live_config.json")

# Defaults, also written to live_config.json on first start if absent (fresh
# checkout boots without setup). Z_STOP=null in JSON means "no stop" (math.inf).
DEFAULTS: dict[str, Any] = {
    "z_entry": 7.0,
    "z_exit": 0.0,
    "z_stop": None,
    "min_hold_seconds": 0.0,
    "max_hold_seconds": 120.0,
    "rfq_notional_usdc": 12.0,
    "max_trades_per_run": 5,
    "max_slippage_bps": 10.0,
    "lighter_slippage_bps": 30.0,
}


def _parse_z_stop(raw: Any) -> float:
    """JSON `null` -> +inf (no stop), else float()."""
    return math.inf if raw is None else float(raw)


def _parse_opt_int(raw: Any) -> int | None:
    return None if raw is None else int(raw)


@dataclass
class LiveConfig:
    """Mutable runtime config. Fields are overwritten in place on reload
    so anything holding a reference picks up new values on next read."""

    z_entry: float = DEFAULTS["z_entry"]
    z_exit: float = DEFAULTS["z_exit"]
    z_stop: float = math.inf
    min_hold_seconds: float = DEFAULTS["min_hold_seconds"]
    max_hold_seconds: float = DEFAULTS["max_hold_seconds"]
    rfq_notional_usdc: float = DEFAULTS["rfq_notional_usdc"]
    max_trades_per_run: int | None = DEFAULTS["max_trades_per_run"]
    max_slippage_bps: float = DEFAULTS["max_slippage_bps"]
    lighter_slippage_bps: float = DEFAULTS["lighter_slippage_bps"]

    _path: Path = field(default=LIVE_CONFIG_FILE, repr=False)
    _last_mtime: float = field(default=0.0, repr=False)
    _logger: logging.Logger | None = field(default=None, repr=False)

    def attach_logger(self, log: logging.Logger) -> None:
        self._logger = log

    def load_or_initialize(self) -> None:
        """Load live_config.json if it exists; otherwise write defaults
        and immediately load them. Called once at startup."""
        if not self._path.exists():
            self._path.write_text(json.dumps(DEFAULTS, indent=2) + "\n")
            if self._logger is not None:
                self._logger.info(
                    "wrote default live_config.json at %s", self._path
                )
        self._reload_now()

    def maybe_reload(self) -> bool:
        """Stat the file; if mtime changed, re-parse. Returns True iff
        we actually reloaded. Safe to call every tick (stat is cheap)."""
        try:
            st = self._path.stat()
        except FileNotFoundError:
            return False
        if st.st_mtime == self._last_mtime:
            return False
        return self._reload_now()

    def _reload_now(self) -> bool:
        try:
            st = self._path.stat()
            data = json.loads(self._path.read_text())
        except Exception as exc:
            if self._logger is not None:
                self._logger.error(
                    "live_config.json read failed: %s — keeping previous values",
                    exc,
                )
            return False
        try:
            z_entry = float(data["z_entry"])
            z_exit = float(data["z_exit"])
            z_stop = _parse_z_stop(data["z_stop"])
            # Optional/newer key: tolerate old configs that lack it (fall back
            # to default) instead of rejecting the whole reload.
            min_hold = float(data.get("min_hold_seconds", DEFAULTS["min_hold_seconds"]))
            max_hold = float(data["max_hold_seconds"])
            notional = float(data["rfq_notional_usdc"])
            cap = _parse_opt_int(data["max_trades_per_run"])
            max_slip = float(data["max_slippage_bps"])
            lt_slip = float(data["lighter_slippage_bps"])
        except (KeyError, TypeError, ValueError) as exc:
            if self._logger is not None:
                self._logger.error(
                    "live_config.json validation failed: %s — keeping previous values",
                    exc,
                )
            return False
        # All parsed OK — commit atomically.
        self.z_entry = z_entry
        self.z_exit = z_exit
        self.z_stop = z_stop
        self.min_hold_seconds = min_hold
        self.max_hold_seconds = max_hold
        self.rfq_notional_usdc = notional
        self.max_trades_per_run = cap
        self.max_slippage_bps = max_slip
        self.lighter_slippage_bps = lt_slip
        self._last_mtime = st.st_mtime
        if self._logger is not None:
            self._logger.info(
                "live_config reloaded: z_entry=%.1f z_exit=%.1f z_stop=%s "
                "min_hold=%.0fs max_hold=%.0fs notional=$%.0f cap=%s max_slip=%.1f lt_slip=%.1f",
                self.z_entry, self.z_exit,
                "inf" if math.isinf(self.z_stop) else f"{self.z_stop:.1f}",
                self.min_hold_seconds, self.max_hold_seconds, self.rfq_notional_usdc,
                "off" if self.max_trades_per_run is None else self.max_trades_per_run,
                self.max_slippage_bps, self.lighter_slippage_bps,
            )
        return True
