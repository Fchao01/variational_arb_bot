"""Variational startup-readiness wait (the bot's policy layer).

The server runtime itself lives in
:mod:`exchanges.variational.runtime`; this module only encodes the
bot-side startup policy on top of it: how long to wait for the
events-stream heartbeat before the bot starts trading. Quote/trade
data is read straight off `runtime.monitor` by the rest of the bot.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable

from exchanges.variational.monitor import HEARTBEAT_STALE_SECONDS
from exchanges.variational.runtime import VariationalRuntime


_log = logging.getLogger(__name__)


class VariationalFeed:
    """The Variational runtime plus startup waits. Stop is queried via an
    injected predicate, and the timeouts are injected, so this layer stays
    free of bot config/state."""

    def __init__(
        self,
        host: str,
        ws_port: int,
        rest_port: int,
        command_port: int,
        stop_fn: Callable[[], bool],
        poll_interval: float,
        ready_timeout: float,
    ):
        self.runtime = VariationalRuntime(
            host=host,
            ws_port=ws_port,
            rest_port=rest_port,
            command_port=command_port,
        )
        self._stop = stop_fn
        self._poll_interval = poll_interval
        self._ready_timeout = ready_timeout

    async def wait_for_variational_ready(self) -> None:
        """Block until the Variational events-stream heartbeat is live.

        No hard timeout — tearing the listener down on a slow extension
        also closes the sockets the (already-loaded) Chrome extension
        has connected to, making recovery harder than the failure. So
        we wait indefinitely and re-emit a WARNING every ready_timeout
        seconds so the log tail surfaces the stall without the process
        self-destructing. `ready_timeout` is now the nag interval, not
        a deadline; the operator (or the outer stop_flag) decides when
        to give up."""
        started_at = time.time()
        last_nag_at = started_at
        while not self._stop():
            state = await self.runtime.monitor.get_trading_state()
            hb_age = state.get("heartbeat_age")
            if hb_age is not None and hb_age <= HEARTBEAT_STALE_SECONDS:
                _log.info(
                    "Variational events heartbeat live (waited %.1fs)",
                    time.time() - started_at,
                )
                return
            now = time.time()
            if now - last_nag_at >= self._ready_timeout:
                _log.warning(
                    "no Variational events heartbeat for %.0fs — check "
                    "Chrome extension on omni.variational.io (ws:8766 "
                    "must be ESTAB and pushing events). Bot is waiting, "
                    "not exiting.",
                    now - started_at,
                )
                last_nag_at = now
            await asyncio.sleep(self._poll_interval)
