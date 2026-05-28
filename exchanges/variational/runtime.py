"""Variational receiver/command server runtime.

Owns the three loopback servers the Chrome forwarder connects out to
(ws frames, REST responses, command broker) plus the parsed monitor and
event sink. Pure server orchestration with no bot policy — both the
standalone listener CLI and the arbitrage bot bring this up.
"""

from __future__ import annotations

from pathlib import Path

from exchanges.variational.command import CommandBroker, run_command_server
from exchanges.variational.monitor import (
    EventSink,
    VariationalMonitor,
    run_receiver_server,
)


class VariationalRuntime:
    def __init__(
        self,
        host: str,
        ws_port: int,
        rest_port: int,
        command_port: int,
        output_dir: Path | None = None,
        quiet: bool = True,
        trade_limit: int = 500,
        snapshot_file: Path | None = None,
        enable_monitor: bool = True,
    ) -> None:
        self.monitor = (
            VariationalMonitor(trade_limit=trade_limit, snapshot_file=snapshot_file)
            if enable_monitor
            else None
        )
        self.sink = EventSink(output_dir=output_dir, quiet=quiet, monitor=self.monitor)
        self.broker = CommandBroker(quiet=quiet)
        self.host = host
        self.ws_port = ws_port
        self.rest_port = rest_port
        self.command_port = command_port
        self.ws_server = None
        self.rest_server = None
        self.command_server = None

    async def start(self) -> None:
        self.ws_server = await run_receiver_server("ws", self.host, self.ws_port, self.sink)
        self.rest_server = await run_receiver_server("rest", self.host, self.rest_port, self.sink)
        self.command_server = await run_command_server(self.host, self.command_port, self.broker)

    async def stop(self) -> None:
        for server in (self.command_server, self.ws_server, self.rest_server):
            if server is not None:
                server.close()
                await server.wait_closed()
