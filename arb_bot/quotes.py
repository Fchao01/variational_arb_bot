"""Active-RFQ quote pulling and cross-venue edge math.

The signal side of the bot, kept free of order placement. Instead of
passively reading the browser's own indicative quotes (which refresh only
as fast as the website re-quotes — a ~0.5s staleness window), QuoteReader
actively pulls a fresh Variational RFQ on demand via the command channel,
sized to a configured USDC notional and aligned to the per-coin qty tick
the indicative reports. `edges()` prices it against the Lighter book —
used by the tick logger for offline analysis; the live strategy decides
direction off the basis-MR z-score in trader.py, not these edges.
"""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_HALF_UP, Decimal
from typing import Any

from arb_bot.helpers import parse_server_ts, resolve_lighter_ticker, to_decimal

_BPS = Decimal("10000")
_DEFAULT_TICK = Decimal("0.000001")


@dataclass(frozen=True, slots=True)
class Quote:
    """A Variational RFQ snapshot: prices, the reusable quote_id, and the
    tick-aligned qty that quote_id is bound to. Every Quote handed out by
    QuoteReader is fresh and firable; anything less is returned as None.

    Timing — three clocks, each with its purpose:
      rfq_sent_at / received_at  monotonic seconds, framing the RFQ
        round-trip we control end-to-end (no clock-skew worries).
      var_server_at              unix epoch seconds parsed from the
        Variational response's own `timestamp` field — when their pricer
        actually produced this quote. Lets us measure
        decision_wall_clock − var_server_at as the total Var staleness
        (includes their internal compute + WS leg). None when the
        response omitted/malformed the timestamp; callers should fall
        back to received_at."""

    bid: Decimal
    ask: Decimal
    quote_id: str
    qty: Decimal
    rfq_sent_at: float
    received_at: float
    var_server_at: float | None


def _extract_tick(raw: Any) -> Decimal | None:
    """qty tick from an indicative response:
    qty_limits.{ask,bid}.min_qty_tick. Take the max of the two sides so
    the quantised qty satisfies whichever side the order ends up hitting.
    None => keep using the previously learned (or default) tick."""
    if not isinstance(raw, dict):
        return None
    ql = raw.get("qty_limits")
    if not isinstance(ql, dict):
        return None
    ticks: list[Decimal] = []
    for side in ("ask", "bid"):
        sd = ql.get(side)
        if isinstance(sd, dict):
            v = to_decimal(sd.get("min_qty_tick"))
            if v is not None and v > 0:
                ticks.append(v)
    return max(ticks) if ticks else None


def _round_tick(qty: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return qty
    n = (qty / tick).to_integral_value(rounding=ROUND_HALF_UP)
    return (n * tick).quantize(tick)


class QuoteReader:
    """Actively pulls the latest Variational RFQ for one coin.

    Pacing is entirely here, not in the caller's loop: a real RFQ network
    call fires at most once per `min_interval_ms`, and a server 429 (or
    `rateLimitResetMs`) extends that with a backoff. read() returns either
    a fresh, firable quote or None — it never serves a cached or
    non-firable one, so a stale RFQ can never drive a trade.

    `lighter_book` is injected after construction (mirrors Trader's) since
    the Lighter book feed only exists once the coin is locked."""

    def __init__(
        self,
        command_client,
        live_config,
        min_interval_ms: float,
        logger: logging.Logger,
        lighter_book=None,
        lighter_executor=None,
    ) -> None:
        self._command = command_client
        # notional is read per-RFQ so live_config can change it mid-session.
        self._live = live_config
        self._min_interval = min_interval_ms / 1000.0
        self._log = logger
        self.lighter_book = lighter_book
        # Set by App after Lighter has loaded live market metadata. The
        # executor supplies the size step so both legs use the same quantity.
        self.lighter_executor = lighter_executor

        # Per-coin qty tick, learned from each indicative's qty_limits;
        # absent until the coin's first RFQ returns.
        self._tick: dict[str, Decimal] = {}
        # Last successful quote time — only feeds the observed cadence (_refresh_ms).
        self._last_quote_at = 0.0
        self._last_pull_at = 0.0
        self._backoff_until = 0.0
        self._refresh_ms: float | None = None

    @property
    def refresh_ms(self) -> float | None:
        """Observed interval between successive successful RFQ pulls."""
        return self._refresh_ms

    async def read(self, coin: str) -> Quote | None:
        """A fresh, firable RFQ for `coin`, or None — never a cached or
        non-firable quote. None means "no price you can trust this tick"
        (rate-limited, RFQ error, or tick not yet learned); the caller
        just skips the tick. Paces itself: when throttled it sleeps out
        the remainder rather than busy-returning, so the caller's loop is
        naturally bounded to the RFQ cadence (RFQ_MIN_INTERVAL_MS plus any
        429 backoff)."""
        now = time.monotonic()

        # 429 backoff: nap at most one interval (not the full reset window) so
        # the engine doesn't freeze; None meanwhile keeps it from trading.
        if now < self._backoff_until:
            await asyncio.sleep(min(self._backoff_until - now, self._min_interval))
            return None

        # Sleep out the remaining inter-RFQ spacing here, so the caller's loop
        # is paced by this and not by a loop-side sleep.
        next_at = self._last_pull_at + self._min_interval
        if now < next_at:
            await asyncio.sleep(next_at - now)

        return await self._pull(coin)

    async def _pull(self, coin: str) -> Quote | None:
        # Stamp _last_pull_at up front so EVERY exit path (no book / empty book /
        # qty<=0) stays paced by min_interval; otherwise read() never sleeps and
        # the caller hot-spins on a transiently empty Lighter book. Re-stamped
        # before the network call so the RFQ cadence itself is unchanged.
        self._last_pull_at = time.monotonic()
        if self.lighter_book is None:
            return None

        # Size the RFQ off the Lighter mid (notional / mid), rounded to the
        # per-coin qty tick (default tick until this coin's real tick is learned).
        sym = resolve_lighter_ticker(coin)
        bid, ask, _, _ = await self.lighter_book.get_book(sym)
        if bid is None or ask is None or bid <= 0 or ask <= 0:
            return None
        mid = (bid + ask) / Decimal("2")
        raw_qty = Decimal(str(self._live.rfq_notional_usdc)) / mid

        cached_tick = self._tick.get(coin.upper())
        had_tick = cached_tick is not None
        qty = _round_tick(raw_qty, cached_tick if had_tick else _DEFAULT_TICK)

        # Lighter may support fewer size decimals than Variational. Align the
        # RFQ quantity down to Lighter's base-size step before requesting the
        # quote; otherwise the Var leg can be 0.0001 larger than the actual
        # Lighter hedge (e.g. SOL 0.1097 vs 0.109).
        market_cfg = (
            self.lighter_executor.config(sym)
            if self.lighter_executor is not None else None
        )
        if market_cfg is not None and market_cfg.base_mult > 0:
            lighter_qty = (
                (qty * market_cfg.base_mult)
                .to_integral_value(rounding=ROUND_DOWN)
                / Decimal(market_cfg.base_mult)
            )
            qty = lighter_qty
        if qty <= 0:
            return None

        # Anchor next read()'s spacing on the attempt (not success) so errors
        # can't busy-loop.
        self._last_pull_at = time.monotonic()
        rfq_sent_at = self._last_pull_at
        try:
            data = await self._command.quote(market=coin, amount=str(qty))
        except Exception as exc:
            self._log.warning("RFQ %s failed: %s", coin, exc)
            return None
        rfq_received_at = time.monotonic()

        result = data.get("result") if isinstance(data, dict) else None
        if not isinstance(result, dict):
            result = {}

        # Rate limited: arm the backoff read() honours, skip this tick.
        if result.get("httpStatus") == 429:
            reset_ms = to_decimal(result.get("rateLimitResetMs")) or Decimal("0")
            backoff = max(float(reset_ms) / 1000.0, self._min_interval)
            self._backoff_until = time.monotonic() + backoff
            self._log.warning(
                "RFQ %s rate limited (429), backing off %.1fs", coin, backoff
            )
            return None

        if not data.get("ok"):
            self._log.warning(
                "RFQ %s not ok: %s", coin,
                result.get("error") or data.get("error") or "unknown",
            )
            return None

        # Learn the real tick from qty_limits so the NEXT pull sends aligned qty.
        learned = _extract_tick(result.get("raw"))
        if learned is not None and self._tick.get(coin.upper()) != learned:
            self._tick[coin.upper()] = learned
            self._log.info("RFQ tick %s = %s (from qty_limits)", coin, learned)

        # First RFQ of this coin used the default tick, so this quote_id is bound
        # to an unaligned qty and must not be reused. Real tick is now learned —
        # return None and let the next pull produce an aligned, firable quote.
        if not had_tick:
            return None

        bid_q = to_decimal(result.get("bid"))
        ask_q = to_decimal(result.get("ask"))
        quote_id = result.get("quoteId")
        if bid_q is None or ask_q is None or not quote_id or bid_q <= 0 or ask_q <= 0:
            return None

        # Variational pricer's own quote-creation time, from the indicative body
        # under result["raw"]. None-tolerant; parser handles ISO strings + ms/μs ints.
        raw = result.get("raw") if isinstance(result.get("raw"), dict) else {}
        var_server_at = parse_server_ts(raw.get("timestamp"))

        now = time.monotonic()
        if self._last_quote_at:
            self._refresh_ms = (now - self._last_quote_at) * 1000.0
        self._last_quote_at = now
        return Quote(bid_q, ask_q, str(quote_id), qty,
                     rfq_sent_at=rfq_sent_at, received_at=rfq_received_at,
                     var_server_at=var_server_at)


def edges(
    quote: Quote, lighter_bid: Decimal, lighter_ask: Decimal
) -> tuple[Decimal, Decimal]:
    """(long_var_bps, short_var_bps), the directional cross-venue edges:

    long_var : buy Variational @ask, sell Lighter @bid
               edge = (lighter_bid - quote.ask) / quote.ask
    short_var: sell Variational @bid, buy Lighter @ask
               edge = (quote.bid - lighter_ask) / lighter_ask

    Used for tick logging only — the live strategy is basis-MR and picks
    its direction off the z-score, not these.
    """
    long_bps = (lighter_bid - quote.ask) / quote.ask * _BPS
    short_bps = (quote.bid - lighter_ask) / lighter_ask * _BPS
    return long_bps, short_bps
