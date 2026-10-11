"""Hot-reloadable runtime knobs.

Reads ./live_config.json (next to main.py). Each `maybe_reload()` call
stats the file; if mtime has changed, atomically re-parses and overwrites
the live values. The bot never crashes on a typo — a bad JSON or missing
key keeps the previous good values and logs an error.

Why not in config.py? Constants there are imported once at startup;
changing them needs a kill+restart (which means ~250s warmup again).
Anything we want to tweak mid-session lives here.

What lives here vs config.py:
- live_config.json : executable-premium thresholds, RFQ size, trade cap,
  slippage budgets, and net-position reconciliation.
- config.py        : *structural* constants — endpoints, paths, the EWMA
  HL/warmup/floor (changing these mid-flight breaks the state machine),
  RFQ pacing (changes the signal time scale).
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


LIVE_CONFIG_FILE = Path("./live_config.json")

# Defaults, also written to live_config.json on first start if absent.
DEFAULTS: dict[str, Any] = {
    "midline_bps": 0.0,
    "upper_bps": 4.0,
    "lower_bps": 4.0,
    "premium_persist_seconds": 0.0,
    "cooldown_seconds": 1.0,
    "rfq_notional_usdc": 12.0,
    "max_trades_per_run": 5,
    "max_slippage_bps": 10.0,
    "lighter_slippage_bps": 30.0,
    "net_tolerance_base": 0.001,
    "reconcile_seconds": 15.0,
}


def _parse_opt_int(raw: Any) -> int | None:
    return None if raw is None else int(raw)


@dataclass
class LiveConfig:
    """Mutable runtime config. Fields are overwritten in place on reload
    so anything holding a reference picks up new values on next read."""

    midline_bps: float = DEFAULTS["midline_bps"]
    upper_bps: float = DEFAULTS["upper_bps"]
    lower_bps: float = DEFAULTS["lower_bps"]
    premium_persist_seconds: float = DEFAULTS["premium_persist_seconds"]
    cooldown_seconds: float = DEFAULTS["cooldown_seconds"]
    rfq_notional_usdc: float = DEFAULTS["rfq_notional_usdc"]
    max_trades_per_run: int | None = DEFAULTS["max_trades_per_run"]
    max_slippage_bps: float = DEFAULTS["max_slippage_bps"]
    lighter_slippage_bps: float = DEFAULTS["lighter_slippage_bps"]
    net_tolerance_base: float = DEFAULTS["net_tolerance_base"]
    reconcile_seconds: float = DEFAULTS["reconcile_seconds"]

    # Kept only so the deprecated BasisStrategy/run_round compatibility path
    # remains importable for old tooling. The live app never reads these.
    z_entry: float = 7.0
    z_exit: float = 0.0
    z_stop: float = float("inf")
    min_hold_seconds: float = 0.0
    max_hold_seconds: float = 120.0

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
            midline = float(data.get("midline_bps", DEFAULTS["midline_bps"]))
            upper = float(data.get("upper_bps", DEFAULTS["upper_bps"]))
            lower = float(data.get("lower_bps", DEFAULTS["lower_bps"]))
            persist = float(data.get("premium_persist_seconds", DEFAULTS["premium_persist_seconds"]))
            cooldown = float(data.get("cooldown_seconds", DEFAULTS["cooldown_seconds"]))
            notional = float(data["rfq_notional_usdc"])
            cap = _parse_opt_int(data.get("max_trades_per_run"))
            max_slip = float(data["max_slippage_bps"])
            lt_slip = float(data["lighter_slippage_bps"])
            tolerance = float(data.get("net_tolerance_base", DEFAULTS["net_tolerance_base"]))
            reconcile = float(data.get("reconcile_seconds", DEFAULTS["reconcile_seconds"]))
            if upper < 0 or lower < 0 or notional <= 0 or tolerance < 0 or reconcile <= 0:
                raise ValueError("thresholds, notional, tolerance, and reconcile_seconds must be positive")
        except (KeyError, TypeError, ValueError) as exc:
            if self._logger is not None:
                self._logger.error(
                    "live_config.json validation failed: %s — keeping previous values",
                    exc,
                )
            return False
        # All parsed OK — commit atomically.
        self.midline_bps = midline
        self.upper_bps = upper
        self.lower_bps = lower
        self.premium_persist_seconds = max(0.0, persist)
        self.cooldown_seconds = max(0.0, cooldown)
        self.rfq_notional_usdc = notional
        self.max_trades_per_run = cap
        self.max_slippage_bps = max_slip
        self.lighter_slippage_bps = lt_slip
        self.net_tolerance_base = tolerance
        self.reconcile_seconds = reconcile
        self._last_mtime = st.st_mtime
        if self._logger is not None:
            self._logger.info(
                "live_config reloaded: midline=%+.2fbps upper=%.2fbps lower=%.2fbps "
                "persist=%.1fs cooldown=%.1fs notional=$%.0f cap=%s "
                "max_slip=%.1f lt_slip=%.1f net_tol=%s reconcile=%.0fs",
                self.midline_bps, self.upper_bps, self.lower_bps,
                self.premium_persist_seconds, self.cooldown_seconds,
                self.rfq_notional_usdc,
                "off" if self.max_trades_per_run is None else self.max_trades_per_run,
                self.max_slippage_bps, self.lighter_slippage_bps,
                self.net_tolerance_base, self.reconcile_seconds,
            )
        return True
