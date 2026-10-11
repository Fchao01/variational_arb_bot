"""Process wiring + decision loop for the executable-premium arb bot.

One class, two modes:
- live (record_only=False): trades ASSETS[0]; brings up the full stack
  (Variational receiver + command, Lighter book + executor + fill feed,
  Recorder for legacy settlement files, Trader for execution). Calls
  trader.run_arb() when
  the strategy reports an entry.
- record-only (record_only=True): round-robins ASSETS; brings up only
  the read path (Variational receiver + command, Lighter book) and the
  strategy (which writes premium_ticks_<COIN>.csv per coin). NO Trader, NO
  Executor, NO Recorder is constructed — running this cannot place an
  order regardless of the premium.

Both modes share the SAME per-tick path through PremiumStrategy.tick(),
so the CSV a record run produces is byte-identical to the one a live
run produces on the same tape.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import logging
import signal
import time

from exchanges.variational.command_client import CommandClient

from arb_bot.basis import PremiumStrategy
from arb_bot.config import (
    APP_LOG_FILE,
    ASSETS,
    DOCS_DIR,
    FORWARDER_COMMAND_PORT,
    FORWARDER_HOST,
    FORWARDER_REST_PORT,
    FORWARDER_WS_PORT,
    LIGHTER_BASE_URL,
    LIGHTER_CHAIN_ID,
    LIGHTER_WS_URL,
    LOG_DIR,
    POLL_INTERVAL_SECONDS,
    READY_TIMEOUT_SECONDS,
    RFQ_MIN_INTERVAL_MS,
)
from arb_bot.feed import VariationalFeed
from arb_bot.helpers import resolve_lighter_ticker
from arb_bot.live_config import LiveConfig
from arb_bot.quotes import QuoteReader
from arb_bot.recorder import Recorder
from arb_bot.trader import Trader
from exchanges.lighter.book_feed import BookFeed
from exchanges.lighter.executor import Executor

# Exit code for "quote feed went stale and page reloads failed to resync"
# — a recoverable condition an external supervisor should restart on (vs a
# clean 0 stop or a non-zero halt that needs a human, e.g. a naked leg).
EXIT_STALE_UNRECOVERABLE = 75


class App:
    def __init__(self, record_only: bool) -> None:
        self.record_only = record_only
        self.stop_flag = False
        self.exit_code = 0  # set to EXIT_STALE_UNRECOVERABLE if recovery fails
        self.opened_rounds = 0  # incremented at trader.run_round() entry

        LOG_DIR.mkdir(parents=True, exist_ok=True)
        DOCS_DIR.mkdir(parents=True, exist_ok=True)
        self.log = logging.getLogger("arb_bot")
        self.log.setLevel(logging.INFO)
        self.log.handlers.clear()
        self.log.propagate = False
        for handler in (
            logging.FileHandler(APP_LOG_FILE, encoding="utf-8"),
            logging.StreamHandler(),
        ):
            handler.setFormatter(
                logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
            )
            self.log.addHandler(handler)

        # Hot-reloadable runtime knobs (premium thresholds, sizing, slippage,
        # circuit breaker, and feed safety),
        # re-read fresh each tick via maybe_reload() in the main loop.
        self.live_config = LiveConfig()
        self.live_config.attach_logger(self.log)
        self.live_config.load_or_initialize()

        if not ASSETS:
            raise RuntimeError("ASSETS is empty")
        # record: all coins round-robin; live: only the first.
        configured = list(dict.fromkeys(a.upper() for a in ASSETS))
        self.assets: list[str] = configured if record_only else configured[:1]
        self.lighter_symbols = {
            a: resolve_lighter_ticker(a) for a in self.assets
        }

        # Read path — both modes.
        self.variational = VariationalFeed(
            host=FORWARDER_HOST,
            ws_port=FORWARDER_WS_PORT,
            rest_port=FORWARDER_REST_PORT,
            command_port=FORWARDER_COMMAND_PORT,
            stop_fn=lambda: self.stop_flag,
            poll_interval=POLL_INTERVAL_SECONDS,
            ready_timeout=READY_TIMEOUT_SECONDS,
        )
        self.monitor = self.variational.runtime.monitor
        self.command_client = CommandClient(
            FORWARDER_HOST, FORWARDER_COMMAND_PORT, self.log
        )
        self.quotes = QuoteReader(
            self.command_client, self.live_config, RFQ_MIN_INTERVAL_MS, self.log,
        )

        # Strategy — executable premium entry; there is no z-score warmup or
        # hold-then-close round. Trader handles independent slices and net
        # exposure reconciliation.
        self.strategy = PremiumStrategy(self.assets, self.live_config)

        # Execution stack — live only; None in record mode so there is no
        # order-placement path at all.
        self.lighter = (
            None if record_only
            else Executor(LIGHTER_BASE_URL, self.log, self.live_config)
        )
        self.recorder = (
            None if record_only
            else Recorder(self.lighter, self.monitor, self.log)
        )
        self.trader: Trader | None = (
            None if record_only
            else Trader(
                strategy=self.strategy,
                command_client=self.command_client,
                lighter=self.lighter,
                lighter_book=None,  # set in _activate
                monitor=self.monitor,
                quote_reader=self.quotes,
                recorder=self.recorder,
                logger=self.log,
                stop_fn=lambda: self.stop_flag,
                live_config=self.live_config,
            )
        )
        self.lighter_book: BookFeed | None = None

    def _shutdown(self, *_a) -> None:
        self.stop_flag = True

    async def _activate(self) -> None:
        """Bring up the Lighter book (both modes) + executor (live only),
        then open per-coin tick CSVs via the strategy."""
        symbols = list(self.lighter_symbols.values())
        self.lighter_book = BookFeed(
            base_url=LIGHTER_BASE_URL, symbols=symbols,
            logger=self.log, stop_fn=lambda: self.stop_flag,
        )
        await self.lighter_book.start()
        self.quotes.lighter_book = self.lighter_book
        if self.trader is not None:
            self.trader.lighter_book = self.lighter_book

        if not self.record_only:
            self.lighter.load_market_configs(symbols)
            # QuoteReader must size the Variational RFQ to the coarser of the
            # two venues' quantity steps, otherwise Lighter truncates its leg
            # and leaves a cross-venue size mismatch.
            self.quotes.lighter_executor = self.lighter
            for sym in symbols:
                market = self.lighter.config(sym)
                if market is None:
                    raise RuntimeError(f"{sym} not tradable on Lighter")
                await self.lighter.start_fill_feed([market.market_index])

        self.strategy.attach_tick_loggers(self.assets)

        self.log.info(
            "Activated [%s] assets=%s lighter=%s",
            "RECORD-ONLY" if self.record_only else "LIVE",
            ",".join(self.assets), ",".join(symbols),
        )

    async def run(self) -> None:
        signal.signal(signal.SIGINT, self._shutdown)
        signal.signal(signal.SIGTERM, self._shutdown)

        self.log.info(
            "Lighter deployment=RH REST=%s WS=%s signing_chain_id=%s",
            LIGHTER_BASE_URL, LIGHTER_WS_URL, LIGHTER_CHAIN_ID,
        )

        if self.recorder is not None:
            self.recorder.ensure_csv()
            self.recorder.start()
        await self.variational.runtime.start()
        self.log.info(
            "Receiver up (ws:%s rest:%s cmd:%s). Load & Start the Chrome "
            "extension on omni.variational.io. %s coins=%s",
            FORWARDER_WS_PORT, FORWARDER_REST_PORT, FORWARDER_COMMAND_PORT,
            "RECORD-ONLY (no orders)" if self.record_only else "LIVE",
            ",".join(self.assets),
        )
        await self.command_client.start()
        await self.variational.wait_for_variational_ready()
        await self._activate()
        if not self.record_only:
            lc = self.live_config
            self.log.info(
                "Armed: premium long>=%.2fbps short>=%.2fbps persist=%.1fs "
                "cooldown=%.1fs notional=$%.0f range=$%.0f..$%.0f cap=%s "
                "net_tol=%s reconcile=%.0fs errors=%d rate_pause=%.1fs stale=%.0fs "
                "(live_config.json hot-reload every tick)",
                lc.lower_bps - lc.midline_bps,
                lc.midline_bps + lc.upper_bps,
                lc.premium_persist_seconds, lc.cooldown_seconds,
                lc.rfq_notional_usdc,
                lc.min_order_notional_usdc, lc.max_order_notional_usdc,
                "off" if lc.max_trades_per_run is None else str(lc.max_trades_per_run),
                lc.net_tolerance_base, lc.reconcile_seconds,
                lc.max_consecutive_errors, lc.rate_limit_pause_seconds,
                lc.staleness_seconds,
            )

        rows = {a: 0 for a in self.assets}
        last_status = 0.0
        # Stale-data watchdog. If no fresh quote arrives for STALE_THRESHOLD_S,
        # the page's market-data path is stuck (extension WS / RFQ fetch / Lighter
        # book WS) while quotes.read() returns None forever. Instead of dying, ask
        # the extension to reload the Variational tab to resync, then keep running;
        # a fresh quote clears the counter. Only after MAX_RECOVER_ATTEMPTS reloads
        # fail to bring quotes back do we exit with EXIT_STALE_UNRECOVERABLE so a
        # supervisor can restart. (This watchdog only runs between rounds; a freeze
        # *during* an open round is healed by the extension's own frame watchdog.)
        # Keep the recovery grace below the configured stale threshold so a
        # short stale setting cannot immediately trigger a second reload.
        stale_threshold_s = self.live_config.staleness_seconds
        recover_grace_s = min(25.0, max(1.0, stale_threshold_s * 0.55))
        MAX_RECOVER_ATTEMPTS = 3
        last_fresh_at = time.monotonic()
        recover_attempts = 0
        last_reconcile_at = 0.0
        while not self.stop_flag and not self._halted():
            # Hot-reload runtime knobs (only re-reads file on mtime change);
            # new values take effect next tick, no restart.
            self.live_config.maybe_reload()
            stale_threshold_s = self.live_config.staleness_seconds
            recover_grace_s = min(25.0, max(1.0, stale_threshold_s * 0.55))

            # Stale-data watchdog: reload-to-resync, escalate to exit only if it
            # keeps failing.
            stale_for = time.monotonic() - last_fresh_at
            if stale_for > stale_threshold_s:
                if recover_attempts >= MAX_RECOVER_ATTEMPTS:
                    self.log.error(
                        "STALE: no fresh quote for %.0fs after %d page reloads — "
                        "exiting for supervisor restart (upstream stuck; check "
                        "Variational extension + Lighter book WS)",
                        stale_for, recover_attempts,
                    )
                    self.exit_code = EXIT_STALE_UNRECOVERABLE
                    self.stop_flag = True
                    break
                recover_attempts += 1
                self.log.warning(
                    "STALE: no fresh quote for %.0fs — reloading Variational page "
                    "to resync (recovery attempt %d/%d)",
                    stale_for, recover_attempts, MAX_RECOVER_ATTEMPTS,
                )
                with contextlib.suppress(Exception):
                    await self.command_client.reload_page()
                # Grant the reloaded page RECOVER_GRACE_S to come back before the
                # next stale check fires (backdate the clock so stale_for resets to
                # STALE_THRESHOLD_S − RECOVER_GRACE_S).
                last_fresh_at = (
                    time.monotonic() - (stale_threshold_s - recover_grace_s)
                )

            for coin in self.assets:
                if self.stop_flag or self._halted():
                    break
                # QuoteReader paces itself to RFQ_MIN_INTERVAL_MS + backoff.
                var_quote = await self.quotes.read(coin)
                if var_quote is None:
                    continue
                last_fresh_at = time.monotonic()
                recover_attempts = 0  # a fresh quote means the feed recovered
                sym = self.lighter_symbols[coin]
                lighter_bid, lighter_ask, lt_book_at, lt_server_at = (
                    await self.lighter_book.get_book(sym)
                )
                if (
                    lighter_bid is None or lighter_ask is None
                    or lighter_bid <= 0 or lighter_ask <= 0
                ):
                    continue

                # Freeze the decision data here after both feeds read.
                t_decision = time.monotonic()
                wall_decision = time.time()
                _long_bps, _short_bps, sig = self.strategy.tick(
                    wall_decision, coin, var_quote, lighter_bid, lighter_ask,
                )
                rows[coin] += 1

                if self.record_only:
                    continue

                if sig is not None:
                    t_signal = time.monotonic()
                    cap = self.live_config.max_trades_per_run
                    if cap is not None and self.opened_rounds >= cap:
                        self.log.info(
                            "max_trades_per_run=%d reached — stopping", cap,
                        )
                        self.stop_flag = True
                        break
                    direction, edge_bps = sig
                    traded = await self.trader.run_arb(
                        coin, sym, var_quote, direction, edge_bps,
                        lighter_bid, lighter_ask,
                        lt_book_at=lt_book_at,
                        lt_server_at=lt_server_at,
                        t_signal=t_signal,
                        t_decision=t_decision,
                        wall_decision=wall_decision,
                    )
                    if traded:
                        self.opened_rounds += 1
                    # run_arb performs an immediate reconciliation; start the
                    # periodic interval from that read so this tick cannot
                    # issue a duplicate hedge request.
                    last_reconcile_at = time.monotonic()

            # Reconcile even when the RFQ or book is temporarily unavailable.
            # This catches partial fills and stale WS position updates after a
            # restart instead of tying the safety path to a fresh quote.
            if not self.record_only and self.trader is not None:
                now = time.monotonic()
                if now - last_reconcile_at >= self.live_config.reconcile_seconds:
                    await self.trader.reconcile_and_hedge(
                        self.assets[0], self.lighter_symbols[self.assets[0]]
                    )
                    last_reconcile_at = now

            if time.monotonic() - last_status >= 30.0:
                self._log_status(rows)
                last_status = time.monotonic()

        self.log.info(
            "Stopped. final rows: %s",
            " ".join(f"{a}={rows[a]}" for a in self.assets),
        )
        if self.recorder is not None:
            self.log.info(
                "rounds=%d losers=%d cum=$%.6f",
                self.recorder.rounds, self.recorder.losers,
                float(self.recorder.cum_pnl),
            )

    def _halted(self) -> bool:
        return self.trader is not None and self.trader.halted

    def _log_status(self, rows: dict[str, int]) -> None:
        if self.record_only:
            self.log.info(
                "REC rows: %s",
                " ".join(f"{a}={rows[a]}" for a in self.assets),
            )
            return
        coin = self.assets[0]
        st = self.strategy.status(coin)
        refresh = self.quotes.refresh_ms
        refresh_str = f"{refresh:.0f}ms" if refresh else "?"
        self.log.info(
            "%s long=%+.2fbps short=%+.2fbps selected=%s edge=%+.2fbps "
            "rfq_refresh~%s rows=%d",
            coin, st.long_bps, st.short_bps, st.selected_direction,
            st.selected_bps, refresh_str, rows[coin],
        )

    async def close(self) -> None:
        self.stop_flag = True
        if self.recorder is not None:
            with contextlib.suppress(Exception):
                await self.recorder.aclose()
        self.strategy.close_tick_loggers()
        if self.lighter_book is not None:
            with contextlib.suppress(Exception):
                await self.lighter_book.close()
        with contextlib.suppress(Exception):
            await self.command_client.close()
        if self.lighter is not None:
            with contextlib.suppress(Exception):
                await self.lighter.close()
        with contextlib.suppress(Exception):
            await self.variational.runtime.stop()


async def _amain(record_only: bool) -> int:
    app = App(record_only=record_only)
    try:
        await app.run()
    finally:
        await app.close()
    return app.exit_code


def main() -> None:
    """Entrypoint. Default: live, trades ASSETS[0]. With --record:
    round-robins ASSETS, writes premium_ticks_<COIN>.csv per coin, places NO orders."""
    parser = argparse.ArgumentParser(
        description="Variational↔Lighter RH executable-premium arb bot"
    )
    parser.add_argument(
        "--record", action="store_true",
        help="record-only mode: round-robin ASSETS for data collection, never trade",
    )
    args = parser.parse_args()
    raise SystemExit(asyncio.run(_amain(record_only=args.record)))
