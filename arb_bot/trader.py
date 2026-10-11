"""Two-leg execution for one in-flight round.

Receives the entry decision from BasisStrategy, fires both legs
concurrently, waits in a hold loop that asks the strategy for the exit
reason on every fresh tick, then fires both close legs (with retry +
trade-event readback for the Variational ack), and hands the round to
the recorder.

This module knows nothing about the basis signal itself — it doesn't
import Z_ENTRY/Z_EXIT/Z_STOP/BASIS_HL_SECONDS or compute z. The strategy
in arb_bot.basis owns all of that; trader is pure IO + bookkeeping.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from decimal import ROUND_DOWN, Decimal
from typing import Any

from arb_bot.basis import BasisStrategy
from arb_bot.config import FILL_WAIT_SECONDS
from arb_bot.helpers import normalize_variational_status, to_decimal
from arb_bot.live_config import LiveConfig
from arb_bot.quotes import Quote, QuoteReader
from arb_bot.recorder import Recorder, Settlement

_CLOSE_RETRIES = 3
_CLOSE_RETRY_SECONDS = 0.5
_CANCEL_SETTLE_SECONDS = 1.0


class Trader:
    def __init__(
        self,
        *,
        strategy: BasisStrategy,
        command_client,
        lighter,
        lighter_book,
        monitor,
        quote_reader: QuoteReader,
        recorder: Recorder,
        logger: logging.Logger,
        stop_fn,
        live_config: LiveConfig,
    ) -> None:
        self._strategy = strategy
        self._command = command_client
        self._lighter = lighter
        self.lighter_book = lighter_book
        self._monitor = monitor
        self._quotes = quote_reader
        self._recorder = recorder
        self._log = logger
        self._stop = stop_fn
        self._live = live_config

        self.halted = False
        self._last_hedge_block_log = 0.0

    async def run_arb(
        self,
        var_asset: str,
        lighter_symbol: str,
        quote: Quote,
        direction: str,
        edge_bps: float,
        lighter_bid: Decimal,
        lighter_ask: Decimal,
        t_signal: float | None = None,
        lt_book_at: float = 0.0,
        lt_server_at: float = 0.0,
        t_decision: float = 0.0,
        wall_decision: float = 0.0,
    ) -> bool:
        """Execute one entropy-style two-leg slice.

        Unlike ``run_round``, this method never waits for a z-score exit and
        never assumes the two fills are identical. It rolls back a successful
        leg when the other leg fails, then reconciles both venue positions and
        sends a reduce-only hedge for any residual net base exposure.
        """
        qty = quote.qty
        var_side = "buy" if direction == "long_var" else "sell"
        lighter_side = "SELL" if direction == "long_var" else "BUY"
        hedge_ref_price = lighter_bid if lighter_side == "SELL" else lighter_ask
        blocked = self._lighter.hedge_blocked(
            lighter_symbol, qty, hedge_ref_price
        )
        if blocked:
            now = time.monotonic()
            if now - self._last_hedge_block_log >= 30.0:
                self._log.warning(
                    "%s %s qty=%s SKIPPED — Lighter leg blocked: %s",
                    var_asset, direction, qty, blocked,
                )
                self._last_hedge_block_log = now
            return False

        if t_signal is None:
            t_signal = time.monotonic()
        var_status: str
        var_info: Any
        lighter_status: str
        lighter_info: Any

        async def place_var() -> tuple[str, Any]:
            try:
                resp = await self._command.place_order(
                    side=var_side,
                    amount=str(qty),
                    market=var_asset,
                    order_type="market",
                    max_slippage=self._live.max_slippage_bps / 10000.0,
                    reduce_only=False,
                    reuse_quote_id=quote.quote_id,
                )
            except Exception as exc:
                return "err", exc
            if resp.get("ok"):
                return "ok", quote.ask if var_side == "buy" else quote.bid
            result = (resp.get("result") if isinstance(resp, dict) else None) or {}
            return "reject", str(resp.get("error") or result.get("error") or "")

        async def place_lighter() -> tuple[str, Any]:
            try:
                order_id, _ = await self._lighter.submit(
                    lighter_symbol,
                    lighter_side,
                    qty,
                    lighter_bid,
                    lighter_ask,
                    reduce_only=False,
                )
            except Exception as exc:
                return "err", exc
            if await self._lighter_filled(lighter_symbol, order_id):
                return "ok", order_id
            return "unfilled", f"order {order_id} unfilled"

        # Both legs are submitted concurrently. Sequential submission would
        # turn the quoted cross-venue edge into avoidable naked-leg exposure.
        (var_status, var_info), (lighter_status, lighter_info) = await asyncio.gather(
            place_var(), place_lighter()
        )
        var_ok = var_status == "ok"
        lighter_ok = lighter_status == "ok"

        if var_ok and not lighter_ok:
            self._log.error(
                "%s: Lighter leg failed after Variational fill (%s) — rollback",
                var_asset, lighter_info,
            )
            await self._rollback_var(var_asset, var_side, qty)
            return False
        if lighter_ok and not var_ok:
            self._log.error(
                "%s: Variational leg failed after Lighter fill (%s) — rollback",
                var_asset, var_info,
            )
            await self._rollback_lighter(
                lighter_symbol, lighter_side, qty, lighter_bid, lighter_ask
            )
            return False
        if not var_ok and not lighter_ok:
            self._log.warning(
                "%s: both arb legs failed (var=%s lighter=%s)",
                var_asset, var_info, lighter_info,
            )
            return False

        self._log.info(
            "ARB %s %s qty=%s edge=%+.2fbps var=%s lighter_order=%s "
            "submit_lag=%dms",
            var_asset,
            direction,
            qty,
            edge_bps,
            var_side,
            lighter_info,
            int((time.monotonic() - t_signal) * 1000),
        )
        # Give the Var events stream a short chance to publish the fill before
        # the immediate reconciliation. The periodic reconciliation below is
        # still authoritative if this event arrives later.
        await asyncio.sleep(0.1)
        await self.reconcile_and_hedge(var_asset, lighter_symbol)
        return True

    async def reconcile_and_hedge(
        self, var_asset: str, lighter_symbol: str
    ) -> bool:
        """Read both signed positions and reduce any net base exposure.

        Positive net means the combined account is long and needs a SELL on a
        venue that is carrying a long. Negative net needs a BUY against a
        short. Only the venue carrying the imbalance is touched, and every
        hedge order is reduce-only.
        """
        var_task = asyncio.create_task(self._fetch_var_position(var_asset))
        lighter_task = asyncio.create_task(
            self._lighter.fetch_position(lighter_symbol)
        )
        var_result, lighter_result = await asyncio.gather(
            var_task, lighter_task, return_exceptions=True
        )
        if isinstance(var_result, Exception):
            self._log.warning("%s Var position read failed: %s", var_asset, var_result)
            return False
        if isinstance(lighter_result, Exception):
            self._log.warning(
                "%s Lighter position read failed: %s", lighter_symbol, lighter_result
            )
            return False
        var_pos = var_result
        lighter_pos = lighter_result
        net = var_pos + lighter_pos
        tolerance = Decimal(str(self._live.net_tolerance_base))
        if abs(net) <= tolerance:
            return True

        want_sign = Decimal("1") if net > 0 else Decimal("-1")
        candidates: list[tuple[str, Decimal]] = []
        if var_pos * want_sign > 0:
            candidates.append(("var", abs(var_pos)))
        if lighter_pos * want_sign > 0:
            candidates.append(("lighter", abs(lighter_pos)))
        if not candidates:
            self._log.error(
                "%s net exposure=%s but neither venue has a reducible same-sign position "
                "(var=%s lighter=%s)",
                var_asset, net, var_pos, lighter_pos,
            )
            return False

        venue, carrying = max(candidates, key=lambda item: item[1])
        qty = min(abs(net), carrying)
        if venue == "var":
            qty = self._quotes.align_qty(var_asset, qty)
        else:
            cfg = self._lighter.config(lighter_symbol)
            if cfg is not None and cfg.base_mult > 0:
                qty = (
                    (qty * cfg.base_mult).to_integral_value(rounding=ROUND_DOWN)
                    / Decimal(cfg.base_mult)
                )
        if qty <= 0:
            self._log.warning(
                "%s residual net=%s rounds to zero at %s precision "
                "(var=%s lighter=%s)",
                var_asset, net, venue, var_pos, lighter_pos,
            )
            return False

        hedge_side = "SELL" if net > 0 else "BUY"
        self._log.warning(
            "HEDGE %s net=%s var=%s lighter=%s venue=%s side=%s qty=%s",
            var_asset, net, var_pos, lighter_pos, venue, hedge_side, qty,
        )
        if venue == "var":
            resp = await self._command.place_order(
                side=hedge_side.lower(),
                amount=str(qty),
                market=var_asset,
                order_type="market",
                max_slippage=self._live.max_slippage_bps / 10000.0,
                reduce_only=True,
            )
            if not resp.get("ok"):
                self._log.error("%s Variational hedge failed: %s", var_asset, resp)
                self.halted = True
                return False
            return True

        bid, ask, _, _ = await self.lighter_book.get_book(lighter_symbol)
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            self._log.error("%s cannot hedge: Lighter book unavailable", var_asset)
            return False
        blocked = self._lighter.hedge_blocked(
            lighter_symbol, qty, bid if hedge_side == "SELL" else ask
        )
        if blocked:
            self._log.warning(
                "%s residual hedge below Lighter constraints: %s", var_asset, blocked
            )
            return False
        try:
            order_id, _ = await self._lighter.submit(
                lighter_symbol, hedge_side, qty, bid, ask, reduce_only=True
            )
            if not await self._lighter_filled(lighter_symbol, order_id):
                self._log.error(
                    "%s Lighter hedge order %s did not fill; halting", var_asset, order_id
                )
                self.halted = True
                return False
        except Exception as exc:
            self._log.error("%s Lighter hedge failed: %s — halting", var_asset, exc)
            self.halted = True
            return False
        return True

    async def _fetch_var_position(self, asset: str) -> Decimal:
        """Read a signed Variational position, with WS monitor fallback."""
        try:
            resp = await self._command.positions(timeout=10.0)
            if resp.get("ok"):
                parsed = self._position_from_payload(resp.get("positions"), asset)
                if parsed is not None:
                    return parsed
        except Exception as exc:
            self._log.warning("%s Var POSITIONS query failed: %s", asset, exc)
        state = await self._monitor.get_trading_state()
        row = state.get("position_row") if state.get("asset") == asset else None
        if isinstance(row, dict):
            qty = to_decimal(row.get("qty"))
            if qty is not None:
                return qty
        return Decimal("0")

    @staticmethod
    def _position_from_payload(payload: Any, asset: str) -> Decimal | None:
        """Parse the several `/api/positions` envelope shapes seen in Var."""
        want = asset.upper()

        def walk(value: Any) -> Decimal | None:
            if isinstance(value, list):
                for item in value:
                    found = walk(item)
                    if found is not None:
                        return found
                return None
            if not isinstance(value, dict):
                return None
            info = value.get("position_info")
            instrument = value.get("instrument")
            if not isinstance(instrument, dict) and isinstance(info, dict):
                instrument = info.get("instrument")
            underlying = instrument.get("underlying") if isinstance(instrument, dict) else None
            if str(underlying or "").upper() == want:
                raw_qty = value.get("qty")
                if raw_qty is None and isinstance(info, dict):
                    raw_qty = info.get("qty")
                qty = to_decimal(raw_qty)
                if qty is not None:
                    return qty
            for child in value.values():
                found = walk(child)
                if found is not None:
                    return found
            return None

        return walk(payload)

    async def run_round(
        self,
        var_asset: str,
        lighter_symbol: str,
        quote: Quote,
        direction: str,
        entry_z: float,
        lighter_bid: Decimal,
        lighter_ask: Decimal,
        t_signal: float | None = None,
        lt_book_at: float = 0.0,
        lt_server_at: float = 0.0,
        t_decision: float = 0.0,
        wall_decision: float = 0.0,
        entry_s_bps: float | None = None,
        entry_mu_bps: float | None = None,
        entry_sigma_bps: float | None = None,
    ) -> bool:
        """Returns True iff both legs filled at entry (i.e. the round really
        opened, regardless of close outcome). False for SKIP / rollback /
        halt. Callers use this to count actual trades against a per-run cap.

        t_signal:    monotonic() at the moment app.py saw the entry signal.
        lt_book_at:  monotonic() of the Lighter book snapshot used in
                     decision (last WS update timestamp). Together with
                     t_signal these let us log dt_book_age = how stale
                     the Lighter book was when we decided to enter.
        lt_server_at: unix epoch seconds of the Lighter match-engine
                     apply time for that same snapshot (carried from
                     `order_book.last_updated_at`). Combined with
                     time.time() at decision we get total Lighter
                     staleness, including the L→us network leg the local
                     monotonic clock can't see. 0.0 if not provided.
        t_decision:  monotonic() at the moment app.py froze the decision
                     data (after both feeds read, before z-calc). Anchors
                     lt_book_age_local and decision_to_submit for the diag
                     table. wall_decision is its time.time() twin, used for
                     the server-ts ages. entry_{s,mu,sigma}_bps are the
                     signal state that produced entry_z (for the diag row).
        """
        qty = quote.qty
        var_side = "buy" if direction == "long_var" else "sell"
        lighter_side = "SELL" if direction == "long_var" else "BUY"
        hedge_ref_price = lighter_bid if lighter_side == "SELL" else lighter_ask

        blocked = self._lighter.hedge_blocked(lighter_symbol, qty, hedge_ref_price)
        if blocked:
            now = time.monotonic()
            if now - self._last_hedge_block_log >= 30.0:
                self._log.warning(
                    "%s %s qty=%s SKIPPED — Lighter hedge blocked: %s "
                    "(check RFQ_NOTIONAL_USDC vs Lighter min_base/min_quote)",
                    var_asset, direction, qty, blocked,
                )
                self._last_hedge_block_log = now
            return False

        var_entry_since = await self._latest_seq()

        # Each coroutine writes its submit/ack/fill stamps here for per-venue
        # latency attribution.
        if t_signal is None:
            t_signal = time.monotonic()
        timing: dict[str, float] = {}

        async def place_var() -> tuple[str, Any]:
            t_submit = time.monotonic()
            timing["var_submit"] = t_submit
            try:
                resp = await self._command.place_order(
                    side=var_side, amount=str(qty), market=var_asset,
                    order_type="market", max_slippage=self._live.max_slippage_bps / 10000.0,
                    reduce_only=False, reuse_quote_id=quote.quote_id,
                )
            except Exception as exc:
                timing["var_ack"] = time.monotonic()
                return "err", exc
            timing["var_ack"] = time.monotonic()
            result = (resp.get("result") if isinstance(resp, dict) else None) or {}
            if resp.get("ok"):
                return "ok", (quote.ask if var_side == "buy" else quote.bid)
            return "reject", str(resp.get("error") or result.get("error") or "")

        async def place_lighter() -> tuple[str, Any]:
            t_submit = time.monotonic()
            timing["lt_submit"] = t_submit
            try:
                order_id, _ = await self._lighter.submit(
                    lighter_symbol, lighter_side, qty, lighter_bid, lighter_ask,
                    reduce_only=False,
                )
            except Exception as exc:
                timing["lt_ack"] = time.monotonic()
                return "err", exc
            timing["lt_ack"] = time.monotonic()
            if await self._lighter_filled(lighter_symbol, order_id):
                # precise WS-ingest fill time, not the poll-detection instant
                timing["lt_fill"] = self._fill_time(order_id, "entry")
                return "ok", order_id
            timing["lt_fill"] = time.monotonic()
            return "unfilled", (
                f"order {order_id} unfilled after {FILL_WAIT_SECONDS:.0f}s"
            )

        t_enter_submit = time.monotonic()
        # Wall clock captured pre-submit (cheap: one syscall) so the
        # server-staleness deltas below measure age AT DECISION, not after the
        # order round-trip. The ENTER log lines that consume it are emitted after
        # the gather (below): the formatting + file/stream writes are the
        # expensive part on a single core and must never sit between signal and
        # submit (that time would land in decision_to_submit). Every input is
        # frozen here, so deferring the writes yields byte-identical numbers.
        wall_at_decision = time.time()
        # Snapshot the Lighter book just before gather to measure fill-window drift.
        lt_bid_at_submit, lt_ask_at_submit, _, _ = (
            await self.lighter_book.get_book(lighter_symbol)
        )
        (var_status, var_info), (lighter_status, lighter_info) = (
            await asyncio.gather(place_var(), place_lighter())
        )
        entered_at = time.monotonic()
        # Snapshot again after both legs report fill — the delta is fill_lag drift.
        lt_bid_at_fill, lt_ask_at_fill, _, _ = (
            await self.lighter_book.get_book(lighter_symbol)
        )
        # Drift bps, positive = unfavorable (BUY: ask up is bad; SELL: bid down is bad).
        if lt_bid_at_submit and lt_ask_at_submit and lt_bid_at_fill and lt_ask_at_fill:
            if lighter_side == "BUY":
                lt_drift_bps = float((lt_ask_at_fill - lt_ask_at_submit)
                                      / lt_ask_at_submit * 10000)
            else:
                lt_drift_bps = float((lt_bid_at_submit - lt_bid_at_fill)
                                      / lt_bid_at_submit * 10000)
        else:
            lt_drift_bps = 0.0
        # Deferred from pre-submit: local-monotonic ages (dt_rfq/dt_local/
        # dt_book_age/dt_sig_to_submit) and the server-time staleness deltas
        # (dt_var_server/dt_lt_server, NTP-skewed, sanity-bounded against the
        # monotonic dt_rfq lower bound). All inputs were frozen before the gather
        # so these read identically to logging inline — only the write moved off
        # the signal→submit path.
        book_age_ms = (
            int((t_signal - lt_book_at) * 1000) if lt_book_at > 0 else -1
        )
        self._log.info(
            "ENTER %s %s qty=%s z=%+.2f "
            "dt_rfq=%dms dt_local=%dms dt_book_age=%dms dt_sig_to_submit=%dms",
            var_asset, direction, qty, entry_z,
            int((quote.received_at - quote.rfq_sent_at) * 1000),
            int((t_signal - quote.received_at) * 1000),
            book_age_ms,
            int((t_enter_submit - t_signal) * 1000),
        )
        dt_var_server_ms = (
            int((wall_at_decision - quote.var_server_at) * 1000)
            if quote.var_server_at is not None else -1
        )
        dt_lt_server_ms = (
            int((wall_at_decision - lt_server_at) * 1000)
            if lt_server_at > 0 else -1
        )
        self._log.info(
            "ENTER %s server-staleness: dt_var_server=%dms dt_lt_server=%dms "
            "(monotonic refs: dt_rfq=%dms dt_book_age=%dms)",
            var_asset, dt_var_server_ms, dt_lt_server_ms,
            int((quote.received_at - quote.rfq_sent_at) * 1000),
            book_age_ms,
        )
        self._log.info(
            "ENTER %s ack: var=%dms lt=%dms lt_fill=%dms lt_book_drift=%+.2fbps (submit→fill)",
            var_asset,
            int((timing.get("var_ack", entered_at) - timing.get("var_submit", t_enter_submit)) * 1000),
            int((timing.get("lt_ack", entered_at) - timing.get("lt_submit", t_enter_submit)) * 1000),
            int((timing.get("lt_fill", entered_at) - timing.get("lt_submit", t_enter_submit)) * 1000),
            lt_drift_bps,
        )
        # Entry-side diag metrics (carried into Settlement). Age anchors use the
        # frozen t_decision/wall_decision; submit lag is to the real timing["lt_submit"].
        var_rtt_ms = (quote.received_at - quote.rfq_sent_at) * 1000.0
        var_quote_age_ms = (
            (wall_decision - quote.var_server_at) * 1000.0
            if quote.var_server_at is not None and wall_decision > 0 else -1.0
        )
        lt_book_age_local_ms = (
            (t_decision - lt_book_at) * 1000.0
            if t_decision > 0 and lt_book_at > 0 else -1.0
        )
        lt_book_age_total_ms = (
            (wall_decision - lt_server_at) * 1000.0
            if wall_decision > 0 and lt_server_at > 0 else -1.0
        )
        decision_to_submit_ms = (
            (timing["lt_submit"] - t_decision) * 1000.0
            if t_decision > 0 and "lt_submit" in timing else -1.0
        )
        var_ack_ms = (
            timing.get("var_ack", entered_at)
            - timing.get("var_submit", t_enter_submit)
        ) * 1000.0
        lt_ack_ms = (
            timing.get("lt_ack", entered_at)
            - timing.get("lt_submit", t_enter_submit)
        ) * 1000.0
        lt_fill_ms = (
            timing.get("lt_fill", entered_at)
            - timing.get("lt_submit", t_enter_submit)
        ) * 1000.0
        lt_entry_drift_bps = lt_drift_bps
        var_ok = var_status == "ok"
        lighter_ok = lighter_status == "ok"

        if var_ok and not lighter_ok:
            self._log.error(
                "%s: Lighter hedge FAILED (%s) — rolling back Var",
                var_asset, lighter_info,
            )
            await self._rollback_var(var_asset, var_side, qty)
            return False
        if lighter_ok and not var_ok:
            self._log.error(
                "%s: Variational entry %s (%s) — rolling back Lighter",
                var_asset, var_status, var_info,
            )
            await self._rollback_lighter(
                lighter_symbol, lighter_side, qty, lighter_bid, lighter_ask
            )
            return False
        if not var_ok and not lighter_ok:
            self._log.warning(
                "%s: both legs failed (var=%s lighter=%s)",
                var_asset, var_info, lighter_info,
            )
            return False

        var_entry_price: Decimal = var_info
        lighter_entry_price = lighter_bid if lighter_side == "SELL" else lighter_ask
        lighter_entry_order_id: int = lighter_info

        # Hold: ask the strategy for an exit reason each tick. last_good_quote is
        # the latest fresh RFQ so close_var() can reuse it (path b); on reuseFailed
        # the close-retry loop falls back to a fresh quote (path c).
        exit_reason = "maxhold"
        exit_z = 0.0
        last_good_quote: Quote | None = None
        while (
            not self._stop()
            and (time.monotonic() - entered_at) < self._live.max_hold_seconds
        ):
            held_quote = await self._quotes.read(var_asset)
            if held_quote is None:
                continue
            last_good_quote = held_quote
            bid, ask, _, _ = await self.lighter_book.get_book(lighter_symbol)
            if bid is None or ask is None or bid <= 0 or ask <= 0:
                continue

            # Same tick path as the decision loop: keeps EWMA + tick CSV continuous.
            z, warm = self._strategy.tick(
                time.time(), var_asset, held_quote, bid, ask,
            )
            exit_z = z
            reason = self._strategy.exit_decision(z, warm, direction)
            # Honor min_hold: don't act on an exit signal until the position has
            # been held at least min_hold_seconds (max_hold still bounds the loop
            # above). Keeps the fast-revert churn from closing within ~0.1s.
            if reason is not None and (
                time.monotonic() - entered_at
            ) >= self._live.min_hold_seconds:
                exit_reason = reason
                break

        t_exit_signal = time.monotonic()
        hold_seconds = t_exit_signal - entered_at

        lighter_close_side = "BUY" if lighter_side == "SELL" else "SELL"
        var_close_side = "sell" if var_side == "buy" else "buy"
        close_timing: dict[str, float] = {}

        # Reduce-only must never exceed the REAL fill: a leg can fill a min-unit
        # short of nominal, so closing the nominal qty gets rejected on every retry
        # → naked leg (01:18 incident: held 0.00558, tried to close 0.00559). Size
        # each close from its own entry fill; fall back to nominal only if readback
        # is unavailable. Closing short leaves dust, never rejects — the safe way.
        lt_close_qty = self._lighter.filled_base(lighter_entry_order_id) or qty
        var_close_qty = (
            await self._var_entry_filled_qty(var_asset, var_side, var_entry_since)
            or qty
        )
        if var_close_qty != qty or lt_close_qty != qty:
            self._log.info(
                "%s close qty from real fills: var=%s lt=%s (nominal=%s)",
                var_asset, var_close_qty, lt_close_qty, qty,
            )

        async def close_var(reuse_qid: str | None = None) -> tuple[str, Any]:
            """reuse_qid: pass the latest hold-loop quote_id to skip MM's
            re-quote (matches entry-side behavior). Server may return
            reuseFailed if the quote_id is expired / already redeemed; in
            that case the close-retry loop calls us again without reuse_qid
            and the standard fresh-quote path runs.

            Reduce-only naked-leg guard (#26 incident): Variational's fill
            ledger can sit one qty-tick above its position ledger, so a close
            sized from the real fill gets rejected "exceeds position size X".
            The reject states the true position — clamp var_close_qty to it and
            retry once with a fresh quote, so the close never strands a naked
            Var leg on a 1-tick fill/position mismatch."""
            nonlocal var_close_qty
            err = ""
            for _retry in range(2):
                close_timing["var_submit"] = time.monotonic()
                try:
                    resp = await self._command.place_order(
                        side=var_close_side, amount=str(var_close_qty), market=var_asset,
                        order_type="market", max_slippage=self._live.max_slippage_bps / 10000.0,
                        reduce_only=True,
                        reuse_quote_id=reuse_qid,
                    )
                except Exception as exc:
                    close_timing["var_ack"] = time.monotonic()
                    return "err", exc
                close_timing["var_ack"] = time.monotonic()
                result = (resp.get("result") if isinstance(resp, dict) else None) or {}
                if resp.get("ok"):
                    return "ok", to_decimal(
                        result.get("bid") if var_close_side == "sell"
                        else result.get("ask")
                    )
                err = str(resp.get("error") or result.get("error") or "")
                m = re.search(r"exceeds position size ([0-9.]+)", err)
                if _retry == 0 and m:
                    real = to_decimal(m.group(1))
                    if real is not None and Decimal("0") < real < var_close_qty:
                        self._log.warning(
                            "%s var close qty %s exceeds position %s — clamping & retrying",
                            var_asset, var_close_qty, real,
                        )
                        var_close_qty = real
                        reuse_qid = None  # corrected qty needs a fresh MM quote
                        continue
                return "reject", err
            return "reject", err

        # Snapshot lt book at close decision — reused by close_lighter() below.
        lt_bid_at_close_submit, lt_ask_at_close_submit, _, _ = (
            await self.lighter_book.get_book(lighter_symbol)
        )

        async def close_lighter() -> tuple[str, Any]:
            bid, ask = lt_bid_at_close_submit, lt_ask_at_close_submit
            close_timing["lt_submit"] = time.monotonic()
            try:
                order_id, _ = await self._lighter.submit(
                    lighter_symbol, lighter_close_side, lt_close_qty,
                    bid or lighter_bid, ask or lighter_ask, reduce_only=True,
                )
            except Exception as exc:
                close_timing["lt_ack"] = time.monotonic()
                return "err", exc
            close_timing["lt_ack"] = time.monotonic()
            if not await self._lighter_filled(lighter_symbol, order_id):
                close_timing["lt_fill"] = time.monotonic()
                return "unfilled", (
                    f"order {order_id} unfilled after {FILL_WAIT_SECONDS:.0f}s"
                )
            # precise WS-ingest fill time, not the poll-detection instant
            close_timing["lt_fill"] = self._fill_time(order_id, "exit")
            exit_px = (
                (ask or lighter_ask) if lighter_close_side == "BUY"
                else (bid or lighter_bid)
            )
            return "ok", (order_id, exit_px)

        t_close_submit = time.monotonic()
        self._log.info(
            "EXIT %s %s reason=%s hold=%.1fs z=%+.2f dt_sig_to_submit=%dms",
            var_asset, direction, exit_reason, hold_seconds, exit_z,
            int((t_close_submit - t_exit_signal) * 1000),
        )
        # A market quote_id is bound to the quantity used to create it. Only
        # reuse the hold-loop quote when that bound quantity exactly matches
        # the real filled entry; otherwise the quote can close one quantity
        # tick short (or long) and leave a residual position. The fresh path
        # below sends var_close_qty in its indicative request.
        close_reuse_qid: str | None = None
        if last_good_quote is not None:
            if last_good_quote.qty == var_close_qty:
                close_reuse_qid = last_good_quote.quote_id
            else:
                self._log.info(
                    "%s close quote qty %s != real Var fill %s; "
                    "requesting a fresh close quote",
                    var_asset, last_good_quote.qty, var_close_qty,
                )
        (var_status, var_info), (lighter_status, lighter_info) = (
            await asyncio.gather(
                close_var(reuse_qid=close_reuse_qid), close_lighter()
            )
        )
        t_close_done = time.monotonic()
        # Snapshot after close gather — same drift measure as ENTER.
        lt_bid_after_close, lt_ask_after_close, _, _ = (
            await self.lighter_book.get_book(lighter_symbol)
        )
        if (lt_bid_at_close_submit and lt_ask_at_close_submit
                and lt_bid_after_close and lt_ask_after_close):
            if lighter_close_side == "BUY":
                exit_drift_bps = float((lt_ask_after_close - lt_ask_at_close_submit)
                                        / lt_ask_at_close_submit * 10000)
            else:
                exit_drift_bps = float((lt_bid_at_close_submit - lt_bid_after_close)
                                        / lt_bid_at_close_submit * 10000)
        else:
            exit_drift_bps = 0.0
        self._log.info(
            "EXIT %s ack: var=%dms lt=%dms lt_fill=%dms lt_book_drift=%+.2fbps (submit→fill)",
            var_asset,
            int((close_timing.get("var_ack", t_close_done) - close_timing.get("var_submit", t_close_submit)) * 1000),
            int((close_timing.get("lt_ack", t_close_done) - close_timing.get("lt_submit", t_close_submit)) * 1000),
            int((close_timing.get("lt_fill", t_close_done) - close_timing.get("lt_submit", t_close_submit)) * 1000),
            exit_drift_bps,
        )
        var_ok = var_status == "ok"
        lighter_ok = lighter_status == "ok"

        for attempt in range(1, _CLOSE_RETRIES + 1):
            if var_ok and lighter_ok:
                break
            self._log.warning(
                "%s: close incomplete (var=%s lighter=%s) — retry %d/%d",
                var_asset, var_status, lighter_status, attempt, _CLOSE_RETRIES,
            )
            await asyncio.sleep(_CLOSE_RETRY_SECONDS)
            if not var_ok:
                if await self._var_close_confirmed(
                    var_asset, var_close_side, var_entry_since, var_close_qty
                ):
                    var_ok, var_status, var_info = True, "confirmed", None
                else:
                    var_status, var_info = await close_var()
                    var_ok = var_status == "ok"
            if not lighter_ok:
                lighter_status, lighter_info = await close_lighter()
                lighter_ok = lighter_status == "ok"

        if not var_ok and await self._var_close_confirmed(
            var_asset, var_close_side, var_entry_since, var_close_qty
        ):
            self._log.warning(
                "%s: Var close ack lost but trade-event readback confirms "
                "filled — settling flat without halt", var_asset,
            )
            var_ok, var_info = True, None

        if not (var_ok and lighter_ok):
            self._log.error(
                "%s: close FAILED after %d retries (var=%s lighter=%s) — "
                "NAKED leg, manual unwind, halting",
                var_asset, _CLOSE_RETRIES, var_info, lighter_info,
            )
            self.halted = True
            return True  # both legs DID fill at entry; halting on close failure

        # Exit decision anchor = the hold-loop quote the exit signal was computed
        # on (mirrors entry's var_entry_px = quote price). The reduce-only close
        # response carries no quote price, so close_var()'s var_info is empty on
        # the reuse path (path b) — leaving var_dec_exit/gross_bps blank. Prefer
        # last_good_quote; fall back to the response price only when no hold quote
        # exists (e.g. immediate exit before any hold tick).
        if last_good_quote is not None:
            var_exit_price: Decimal | None = (
                last_good_quote.bid if var_close_side == "sell"
                else last_good_quote.ask
            )
        else:
            var_exit_price = var_info if isinstance(var_info, Decimal) else None
        lighter_exit_order_id, lighter_exit_price = lighter_info

        self._recorder.submit(Settlement(
            var_asset=var_asset, direction=direction, qty=qty,
            entry_z=entry_z, hold_seconds=hold_seconds,
            var_side=var_side, var_since=var_entry_since,
            lighter_entry_order_id=lighter_entry_order_id,
            lighter_exit_order_id=lighter_exit_order_id,
            var_entry_px=var_entry_price, var_exit_px=var_exit_price,
            lighter_entry_px=lighter_entry_price,
            lighter_exit_px=lighter_exit_price,
            wall_decision=wall_decision,
            exit_z=exit_z, exit_reason=exit_reason,
            entry_s_bps=entry_s_bps, entry_mu_bps=entry_mu_bps,
            entry_sigma_bps=entry_sigma_bps,
            max_slip_bps=self._live.max_slippage_bps,
            cushion_bps=self._live.lighter_slippage_bps,
            var_rtt_ms=var_rtt_ms, var_quote_age_ms=var_quote_age_ms,
            lt_book_age_local_ms=lt_book_age_local_ms,
            lt_book_age_total_ms=lt_book_age_total_ms,
            decision_to_submit_ms=decision_to_submit_ms,
            var_ack_ms=var_ack_ms, lt_ack_ms=lt_ack_ms, lt_fill_ms=lt_fill_ms,
            lt_entry_drift_bps=lt_entry_drift_bps,
            lt_exit_drift_bps=exit_drift_bps,
        ))
        return True

    def _fill_time(self, order_id: int, leg: str) -> float:
        """Precise WS-ingest fill time for a confirmed fill. fill_ts is written
        together with the fill price in the account_orders ingest, so it is
        always present once _lighter_filled() returned True. If it is somehow
        missing we log loudly and fall back to now() — never silently record a
        wrong time (which would be invisible in the data)."""
        ft = self._lighter.fill_ts(order_id)
        if ft is None:
            self._log.error(
                "lt_fill timing BUG: %s order %s confirmed filled but fill_ts "
                "missing — measurement fell back to now(), treat as unreliable",
                leg, order_id,
            )
            return time.monotonic()
        return ft

    async def _lighter_filled(self, lighter_symbol: str, order_id: int) -> bool:
        # Event-driven: wake the instant the account_orders WS reports the fill,
        # instead of polling fill_price() on a 0.1s sleep loop (which both
        # delayed progress and quantized the observed fill time).
        if await self._lighter.wait_fill(order_id, FILL_WAIT_SECONDS):
            return True
        # not filled in time -> cancel, then give the cancel/fill race a moment
        await self._lighter.cancel(lighter_symbol, order_id)
        if await self._lighter.wait_fill(order_id, _CANCEL_SETTLE_SECONDS):
            return True
        residual = self._lighter.filled_base(order_id)
        if residual is not None and residual > 0:
            self._log.critical(
                "%s: Lighter order %s PARTIALLY filled (base=%s) then "
                "canceled — residual UNHEDGED position, manual unwind, "
                "halting", lighter_symbol, order_id, residual,
            )
            self.halted = True
        return False

    async def _var_entry_filled_qty(
        self, var_asset: str, side: str, since: int | None,
    ) -> Decimal | None:
        """Total filled base qty of our Var entry, from trade-event readback
        after `since`. None when there's no cursor or the readback fails /
        finds nothing — caller falls back to the nominal qty. Mirrors
        recorder._var_filled's filter but returns only the size."""
        if since is None:
            return None
        try:
            events = await self._monitor.get_trade_events_since(since, limit=500)
        except Exception:
            return None
        want_side, want_coin = side.strip().lower(), var_asset.upper()
        total = Decimal("0")
        for ev in events:
            if str(ev.get("asset", "")).strip().upper() != want_coin:
                continue
            if str(ev.get("side", "")).strip().lower() != want_side:
                continue
            if normalize_variational_status(str(ev.get("status", ""))) != "filled":
                continue
            q = to_decimal(ev.get("qty"))
            if q is not None and q > 0:
                total += q
        return total if total > 0 else None

    async def _var_close_confirmed(
        self, var_asset: str, close_side: str, since: int | None,
        want_qty: Decimal,
    ) -> bool:
        if since is None:
            return False
        try:
            events = await self._monitor.get_trade_events_since(since, limit=500)
        except Exception:
            return False
        want_side, want_coin = close_side.strip().lower(), var_asset.upper()
        total = Decimal("0")
        for ev in events:
            if str(ev.get("asset", "")).strip().upper() != want_coin:
                continue
            if str(ev.get("side", "")).strip().lower() != want_side:
                continue
            if normalize_variational_status(str(ev.get("status", ""))) != "filled":
                continue
            q = to_decimal(ev.get("qty"))
            if q is not None and q > 0:
                total += q
        return total >= want_qty

    async def _rollback_lighter(
        self, lighter_symbol: str, lighter_side: str, qty: Decimal,
        entry_bid: Decimal, entry_ask: Decimal,
    ) -> None:
        reverse = "BUY" if lighter_side == "SELL" else "SELL"
        bid, ask, _, _ = await self.lighter_book.get_book(lighter_symbol)
        bid, ask = bid or entry_bid, ask or entry_ask
        try:
            await self._lighter.submit(lighter_symbol, reverse, qty, bid, ask, reduce_only=True)
            self._log.warning("%s: naked Lighter rolled back OK", lighter_symbol)
        except Exception as exc:
            self._log.error(
                "%s: Lighter rollback FAILED — NAKED Lighter leg! %s "
                "(manual unwind), halting", lighter_symbol, exc,
            )
            self.halted = True

    async def _rollback_var(
        self, var_asset: str, var_side: str, qty: Decimal
    ) -> None:
        close_side = "sell" if var_side == "buy" else "buy"
        try:
            resp = await self._command.place_order(
                side=close_side, amount=str(qty), market=var_asset,
                order_type="market", max_slippage=self._live.max_slippage_bps / 10000.0,
                reduce_only=True,
            )
            if resp.get("ok"):
                self._log.warning("%s: naked Var rolled back OK", var_asset)
                return
            err = resp.get("error")
        except Exception as exc:
            err = exc
        self._log.error(
            "%s: Var rollback FAILED — NAKED Variational leg! %s "
            "(manual unwind), halting", var_asset, err,
        )
        self.halted = True

    async def _latest_seq(self) -> int | None:
        try:
            seq = await self._monitor.get_latest_trade_event_seq()
        except Exception:
            return None
        return seq
