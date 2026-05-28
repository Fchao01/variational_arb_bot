"""Off-the-hot-path settlement & PnL recording.

Once both close legs return ok the position is flat — everything after
that (real-fill readback, PnL math, CSV, counters) is bookkeeping and
must not gate the next opportunity scan. Trader hands a `Settlement`
to this recorder and returns immediately; a single background consumer
drains the queue and does the slow real-fill readback out of band.

On shutdown the queue is flushed with an overall deadline: jobs still
get their bounded real-fill wait, but once the deadline passes any
remaining jobs are written with decision prices and flagged UNVERIFIED
so no real position cycle ever loses its PnL row.
"""

from __future__ import annotations

import asyncio
import contextlib
import csv
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from arb_bot.config import FILL_WAIT_SECONDS, PNL_CSV_FILE, ROUNDS_DIAG_CSV_FILE
from arb_bot.helpers import normalize_variational_status, to_decimal, utc_now

_CSV_COLS = [
    "ts", "coin", "direction", "qty", "entry_z", "hold_s",
    "var_entry_px", "var_exit_px", "lighter_entry_px", "lighter_exit_px",
    "var_leg_cash", "lighter_leg_cash", "net_cash", "net_bps", "note",
]

# Full per-round diagnostic table (joins pnl.csv on lt_entry_order_id). Stores
# anchors only; slip decomposition / leg_skew are derived offline. The 组N
# markers align these columns with the row builder in _write_diag.
_DIAG_COLS = [
    # 组1 标识 & 信号
    "ts", "lt_entry_order_id", "coin", "direction", "qty_nominal",
    "var_qty_filled", "lt_qty_filled", "entry_z", "exit_z", "exit_reason",
    "entry_s_bps", "entry_mu_bps", "entry_sigma_bps", "hold_s",
    # 组2 Var 锚点
    "var_dec_entry", "var_fill_entry", "var_dec_exit", "var_fill_exit",
    "max_slip_bps_entry", "max_slip_bps_exit",
    # 组3 Lighter 锚点
    "lt_dec_entry", "lt_fill_entry", "lt_dec_exit", "lt_fill_exit",
    "cushion_bps_entry", "cushion_bps_exit",
    # 组4 延迟
    "var_rtt_ms", "var_quote_age_ms", "lt_book_age_local_ms",
    "lt_book_age_total_ms", "decision_to_submit_ms",
    "var_ack_ms", "lt_ack_ms", "lt_fill_ms",
    # 组5 漂移
    "lt_entry_drift_bps", "lt_exit_drift_bps",
    # 组6 PnL
    "var_leg_cash", "lt_leg_cash", "net_cash", "net_bps", "gross_bps", "note",
]


@dataclass(frozen=True)
class Settlement:
    """Everything the recorder needs to settle one closed round. Decision
    prices are the fallback when real fills don't land; var_exit_px may be
    None (close ok but price field missing)."""

    var_asset: str
    direction: str
    qty: Decimal
    entry_z: float
    hold_seconds: float
    var_side: str
    var_since: int | None
    lighter_entry_order_id: int
    lighter_exit_order_id: int
    var_entry_px: Decimal | None
    var_exit_px: Decimal | None
    lighter_entry_px: Decimal | None
    lighter_exit_px: Decimal | None

    # diag (rounds_diag.csv): decision-price fallbacks above double as the
    # *_dec_* anchors. All default so the shutdown decision-only path and any
    # future caller need not supply them.
    wall_decision: float = 0.0
    exit_z: float = 0.0
    exit_reason: str = ""
    entry_s_bps: float | None = None
    entry_mu_bps: float | None = None
    entry_sigma_bps: float | None = None
    max_slip_bps: float = 0.0
    cushion_bps: float = 0.0
    var_rtt_ms: float = -1.0
    var_quote_age_ms: float = -1.0
    lt_book_age_local_ms: float = -1.0
    lt_book_age_total_ms: float = -1.0
    decision_to_submit_ms: float = -1.0
    var_ack_ms: float = -1.0
    lt_ack_ms: float = -1.0
    lt_fill_ms: float = -1.0
    lt_entry_drift_bps: float = 0.0
    lt_exit_drift_bps: float = 0.0


class _Sentinel:
    pass


_STOP = _Sentinel()


class Recorder:
    def __init__(self, lighter, monitor, logger: logging.Logger) -> None:
        self._lighter = lighter
        self._monitor = monitor
        self._log = logger

        self._queue: asyncio.Queue = asyncio.Queue()
        self._task: asyncio.Task | None = None
        self._draining = False

        self.rounds = 0
        self.losers = 0
        self.cum_pnl = Decimal("0")

    def ensure_csv(self) -> None:
        PNL_CSV_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not PNL_CSV_FILE.exists():
            with PNL_CSV_FILE.open("w", newline="", encoding="utf-8") as fh:
                csv.writer(fh).writerow(_CSV_COLS)
        ROUNDS_DIAG_CSV_FILE.parent.mkdir(parents=True, exist_ok=True)
        if not ROUNDS_DIAG_CSV_FILE.exists():
            with ROUNDS_DIAG_CSV_FILE.open("w", newline="", encoding="utf-8") as fh:
                csv.writer(fh).writerow(_DIAG_COLS)

    def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._consume())

    def submit(self, s: Settlement) -> None:
        """Non-blocking hand-off from the trader's hot path."""
        if self._draining:
            # Don't queue new work while draining — settle inline with decision prices.
            self._record_decision_only(s)
            return
        self._queue.put_nowait(s)

    async def aclose(self, timeout: float = 30.0) -> None:
        """Flush queued settlements within an overall deadline; whatever is
        left when the deadline passes is written with decision prices."""
        self._draining = True
        if self._task is None:
            self._drain_remaining_decision_only()
            return
        self._queue.put_nowait(_STOP)
        try:
            await asyncio.wait_for(self._task, timeout)
        except asyncio.TimeoutError:
            self._log.warning(
                "Recorder flush exceeded %.0fs — writing %d remaining job(s) "
                "with decision prices", timeout, self._queue.qsize(),
            )
            self._task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._task
            self._drain_remaining_decision_only()

    async def _consume(self) -> None:
        while True:
            item = await self._queue.get()
            try:
                if isinstance(item, _Sentinel):
                    return
                await self._settle(item)
            except Exception:
                self._log.exception("Recorder failed to settle a round")
            finally:
                self._queue.task_done()

    def _drain_remaining_decision_only(self) -> None:
        while not self._queue.empty():
            item = self._queue.get_nowait()
            if isinstance(item, _Sentinel):
                continue
            try:
                self._record_decision_only(item)
            except Exception:
                self._log.exception("Recorder failed to flush a round")

    async def _settle(self, s: Settlement) -> None:
        real, var_entry_qty = await self._real_fills(s)
        lt_entry_qty = self._lighter.filled_base(s.lighter_entry_order_id)
        verified = all(
            real.get(k) is not None
            for k in ("var_entry", "var_exit", "lighter_entry", "lighter_exit")
        )
        self._record_round(s, real, var_entry_qty, lt_entry_qty, verified)

    def _record_decision_only(self, s: Settlement) -> None:
        empty: dict[str, Decimal | None] = {
            "var_entry": None, "var_exit": None,
            "lighter_entry": None, "lighter_exit": None,
        }
        self._record_round(s, empty, None, None, verified=False)

    async def _var_filled(
        self, var_asset: str, side: str, since_seq: int
    ) -> tuple[Decimal | None, Decimal]:
        """(size-weighted avg price, total qty) of this account's filled
        Variational trades for var_asset/side after since_seq."""
        try:
            events = await self._monitor.get_trade_events_since(since_seq, limit=500)
        except Exception:
            return None, Decimal("0")
        want_side, want_coin = side.strip().lower(), var_asset.upper()
        weighted_sum = total_qty = Decimal("0")
        for event in events:
            if str(event.get("asset", "")).strip().upper() != want_coin:
                continue
            if str(event.get("side", "")).strip().lower() != want_side:
                continue
            if normalize_variational_status(str(event.get("status", ""))) != "filled":
                continue
            price = to_decimal(event.get("price"))
            qty = to_decimal(event.get("qty"))
            if price is None or qty is None or qty <= 0:
                continue
            weighted_sum += price * qty
            total_qty += qty
        avg = (weighted_sum / total_qty) if total_qty > 0 else None
        return avg, total_qty

    async def _real_fills(
        self, s: Settlement
    ) -> tuple[dict[str, Decimal | None], Decimal | None]:
        """Returns (real-fill prices, Var entry filled qty). The qty is the
        size-weighted total from the same trade-event readback — used by the
        diag table to surface leg-size mismatch (the naked-leg root cause)."""
        close_side = "sell" if s.var_side == "buy" else "buy"
        out: dict[str, Decimal | None] = {
            "var_entry": None, "var_exit": None,
            "lighter_entry": None, "lighter_exit": None,
        }
        var_entry_qty: Decimal | None = None
        # No trustworthy cursor: skip Var readback (since 0 would pull stale fills).
        # Round is then recorded with decision prices and flagged UNVERIFIED.
        skip_var = s.var_since is None
        deadline = time.monotonic() + FILL_WAIT_SECONDS
        while True:
            if not skip_var and out["var_entry"] is None:
                out["var_entry"], qty = await self._var_filled(
                    s.var_asset, s.var_side, s.var_since
                )
                if out["var_entry"] is not None:
                    var_entry_qty = qty
            if not skip_var and out["var_exit"] is None:
                out["var_exit"], _ = await self._var_filled(
                    s.var_asset, close_side, s.var_since
                )
            if out["lighter_entry"] is None:
                out["lighter_entry"] = self._lighter.fill_price(
                    s.lighter_entry_order_id
                )
            if out["lighter_exit"] is None:
                out["lighter_exit"] = self._lighter.fill_price(
                    s.lighter_exit_order_id
                )
            need = ("lighter_entry", "lighter_exit") if skip_var else out.keys()
            if all(out[k] is not None for k in need) or time.monotonic() >= deadline:
                return out, var_entry_qty
            await asyncio.sleep(0.25)

    @staticmethod
    def _pnl(
        direction: str, qty: Decimal,
        var_entry: Decimal | None, var_exit: Decimal | None,
        lighter_entry: Decimal | None, lighter_exit: Decimal | None,
    ) -> tuple[Decimal, Decimal, Decimal, Decimal] | None:
        """(var_leg, lighter_leg, net, net_bps) from the four prices, or None
        if any price is missing. Used for both the resolved (real-or-decision)
        PnL and the decision-price-only gross."""
        if any(v is None for v in (var_entry, var_exit, lighter_entry, lighter_exit)):
            return None
        if direction == "long_var":
            var_leg = (var_exit - var_entry) * qty
            lighter_leg = (lighter_entry - lighter_exit) * qty
        else:
            var_leg = (var_entry - var_exit) * qty
            lighter_leg = (lighter_exit - lighter_entry) * qty
        net = var_leg + lighter_leg
        notional = var_entry * qty
        net_bps = net / notional * Decimal("10000") if notional > 0 else Decimal("0")
        return var_leg, lighter_leg, net, net_bps

    def _record_round(
        self, s: Settlement, real: dict[str, Decimal | None],
        var_entry_qty: Decimal | None, lt_entry_qty: Decimal | None,
        verified: bool,
    ) -> None:
        # Resolved prices: real fill where it landed, decision price as fallback.
        var_entry = real.get("var_entry") or s.var_entry_px
        var_exit = real.get("var_exit") or s.var_exit_px
        lighter_entry = real.get("lighter_entry") or s.lighter_entry_px
        lighter_exit = real.get("lighter_exit") or s.lighter_exit_px
        resolved = self._pnl(
            s.direction, s.qty, var_entry, var_exit, lighter_entry, lighter_exit
        )

        def _f(v: Decimal | None) -> str:
            return f"{v:.8f}" if v is not None else ""

        if resolved is None:
            # Close succeeded at the exchange but a price came back missing and
            # the real fill never landed — flat but uncomputable.
            missing = ",".join(
                k for k, v in (
                    ("var_entry", var_entry), ("var_exit", var_exit),
                    ("lighter_entry", lighter_entry), ("lighter_exit", lighter_exit),
                ) if v is None
            )
            self.rounds += 1
            note = f"UNVERIFIED(missing fill px: {missing})"
            with PNL_CSV_FILE.open("a", newline="", encoding="utf-8") as fh:
                csv.writer(fh).writerow([
                    utc_now(), s.var_asset, s.direction, str(s.qty),
                    f"{s.entry_z:.2f}", f"{s.hold_seconds:.1f}",
                    _f(var_entry), _f(var_exit),
                    _f(lighter_entry), _f(lighter_exit),
                    "", "", "", "", note,
                ])
            self._log.error(
                "DONE %s %s NO-PNL missing=%s (position is flat) "
                "| cum=$%.6f rounds=%d losers=%d",
                s.var_asset, s.direction, missing,
                float(self.cum_pnl), self.rounds, self.losers,
            )
            var_leg = lighter_leg = net = net_bps = None
        else:
            var_leg, lighter_leg, net, net_bps = resolved
            self.rounds += 1
            self.cum_pnl += net
            if net < 0:
                self.losers += 1
            note = "ok" if verified else "UNVERIFIED(est fill px)"
            with PNL_CSV_FILE.open("a", newline="", encoding="utf-8") as fh:
                csv.writer(fh).writerow([
                    utc_now(), s.var_asset, s.direction, str(s.qty),
                    f"{s.entry_z:.2f}", f"{s.hold_seconds:.1f}",
                    f"{var_entry:.8f}", f"{var_exit:.8f}",
                    f"{lighter_entry:.8f}", f"{lighter_exit:.8f}",
                    f"{var_leg:.6f}", f"{lighter_leg:.6f}", f"{net:.6f}",
                    f"{float(net_bps):.2f}", note,
                ])
            self._log.info(
                "DONE %s %s net=$%.6f (var=%.6f lighter=%.6f) %.1fbps %s "
                "| cum=$%.6f rounds=%d losers=%d",
                s.var_asset, s.direction, float(net), float(var_leg),
                float(lighter_leg), float(net_bps), note,
                float(self.cum_pnl), self.rounds, self.losers,
            )

        self._write_diag(
            s, real, var_entry_qty, lt_entry_qty,
            var_leg, lighter_leg, net, net_bps, note,
        )

    def _write_diag(
        self, s: Settlement, real: dict[str, Decimal | None],
        var_entry_qty: Decimal | None, lt_entry_qty: Decimal | None,
        var_leg: Decimal | None, lighter_leg: Decimal | None,
        net: Decimal | None, net_bps: Decimal | None, note: str,
    ) -> None:
        # gross = PnL on decision prices only (signal edge before fill slippage);
        # net − gross = execution cost. Kept as a column because it's the headline KPI.
        gross = self._pnl(
            s.direction, s.qty, s.var_entry_px, s.var_exit_px,
            s.lighter_entry_px, s.lighter_exit_px,
        )
        gross_bps = gross[3] if gross is not None else None
        ts = (
            datetime.fromtimestamp(s.wall_decision, tz=timezone.utc).isoformat()
            if s.wall_decision > 0 else utc_now()
        )

        def _p(v: Decimal | None) -> str:   # price, 8dp
            return f"{v:.8f}" if v is not None else ""

        def _c(v: Decimal | None) -> str:   # cash, 6dp
            return f"{v:.6f}" if v is not None else ""

        def _b(v) -> str:                   # bps, 2dp
            return f"{float(v):.2f}" if v is not None else ""

        def _sg(v: float | None) -> str:    # signal state, 4dp
            return f"{v:.4f}" if v is not None else ""

        row = [
            # 组1
            ts, s.lighter_entry_order_id, s.var_asset, s.direction, str(s.qty),
            str(var_entry_qty) if var_entry_qty is not None else "",
            str(lt_entry_qty) if lt_entry_qty is not None else "",
            f"{s.entry_z:.2f}", f"{s.exit_z:.2f}", s.exit_reason,
            _sg(s.entry_s_bps), _sg(s.entry_mu_bps), _sg(s.entry_sigma_bps),
            f"{s.hold_seconds:.1f}",
            # 组2 Var 锚点 (dec from decision-price fallback, fill from readback)
            _p(s.var_entry_px), _p(real.get("var_entry")),
            _p(s.var_exit_px), _p(real.get("var_exit")),
            _b(s.max_slip_bps), _b(s.max_slip_bps),
            # 组3 Lighter 锚点
            _p(s.lighter_entry_px), _p(real.get("lighter_entry")),
            _p(s.lighter_exit_px), _p(real.get("lighter_exit")),
            _b(s.cushion_bps), _b(s.cushion_bps),
            # 组4 延迟 (-1 = n/a)
            f"{s.var_rtt_ms:.1f}", f"{s.var_quote_age_ms:.1f}",
            f"{s.lt_book_age_local_ms:.1f}", f"{s.lt_book_age_total_ms:.1f}",
            f"{s.decision_to_submit_ms:.1f}",
            f"{s.var_ack_ms:.1f}", f"{s.lt_ack_ms:.1f}", f"{s.lt_fill_ms:.1f}",
            # 组5 漂移
            f"{s.lt_entry_drift_bps:+.2f}", f"{s.lt_exit_drift_bps:+.2f}",
            # 组6 PnL
            _c(var_leg), _c(lighter_leg), _c(net), _b(net_bps), _b(gross_bps),
            note,
        ]
        with ROUNDS_DIAG_CSV_FILE.open("a", newline="", encoding="utf-8") as fh:
            csv.writer(fh).writerow(row)
