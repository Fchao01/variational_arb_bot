"""Server orchestration + CLI for the Variational CDP forwarder receivers.

Implementation lives in focused modules; this file wires them together:
  - :mod:`exchanges.variational.util`    shared helpers
  - :mod:`exchanges.variational.monitor` quote/trade/position monitor + receiver server
  - :mod:`exchanges.variational.command` order command broker + command server
"""

from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from pathlib import Path

from exchanges.variational.command import CommandBroker, run_command_server
from exchanges.variational.monitor import (
    HEARTBEAT_HOURLY_SECONDS,
    HEARTBEAT_RECHECK_SECONDS,
    HEARTBEAT_STALE_SECONDS,
    PORTFOLIO_LOG_INTERVAL_SECONDS,
    QUOTE_LOG_INTERVAL_SECONDS,
    EventSink,
    VariationalMonitor,
    run_receiver_server,
)
from exchanges.variational.runtime import VariationalRuntime
from exchanges.variational.util import (
    QUOTES_INDICATIVE_PATH,
    WS_EVENTS_PATH,
    WS_PORTFOLIO_PATH,
    utc_now,
)

__all__ = [
    "ListenerConfig",
    "CommandBroker",
    "EventSink",
    "VariationalMonitor",
    "run_command_server",
    "run_receiver_server",
    "HEARTBEAT_STALE_SECONDS",
    "HEARTBEAT_RECHECK_SECONDS",
    "HEARTBEAT_HOURLY_SECONDS",
    "QUOTE_LOG_INTERVAL_SECONDS",
    "PORTFOLIO_LOG_INTERVAL_SECONDS",
    "QUOTES_INDICATIVE_PATH",
    "WS_EVENTS_PATH",
    "WS_PORTFOLIO_PATH",
    "utc_now",
    "run",
    "parse_args",
    "main",
]


@dataclass(slots=True)
class ListenerConfig:
    host: str = "127.0.0.1"
    ws_port: int = 8766
    rest_port: int = 8767
    command_port: int = 8768
    output_dir: Path | None = None
    quiet: bool = False
    monitor: bool = True
    trade_limit: int = 20
    snapshot_file: Path | None = None


async def run(config: ListenerConfig) -> None:
    runtime = VariationalRuntime(
        host=config.host,
        ws_port=config.ws_port,
        rest_port=config.rest_port,
        command_port=config.command_port,
        output_dir=config.output_dir,
        quiet=config.quiet,
        trade_limit=config.trade_limit,
        snapshot_file=config.snapshot_file,
        enable_monitor=config.monitor,
    )
    await runtime.start()
    monitor = runtime.monitor
    periodic_task: asyncio.Task[None] | None = None

    if monitor is not None:
        async def periodic_logger() -> None:
            while True:
                await asyncio.sleep(1)
                lines, alerts = await monitor.emit_periodic_logs()
                if not config.quiet:
                    for line in lines:
                        print(line, flush=True)
                for alert in alerts:
                    heartbeat_text = f"[HEARTBEAT_ALERT] {alert}"
                    if not config.quiet:
                        print(heartbeat_text, flush=True)

        periodic_task = asyncio.create_task(periodic_logger())

    print(
        f"Listening for Variational forwarder events on "
        f"ws://{config.host}:{config.ws_port} (WS) and "
        f"ws://{config.host}:{config.rest_port} (REST); "
        f"command broker ws://{config.host}:{config.command_port}",
        flush=True,
    )

    try:
        await asyncio.Future()
    except asyncio.CancelledError:
        pass
    finally:
        if periodic_task is not None:
            periodic_task.cancel()
            await asyncio.gather(periodic_task, return_exceptions=True)
        await runtime.stop()


def parse_args() -> ListenerConfig:
    parser = argparse.ArgumentParser(description="Run local receivers for Variational CDP forwarder events.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind receivers.")
    parser.add_argument("--ws-port", type=int, default=8766, help="Port for WebSocket frame events.")
    parser.add_argument("--rest-port", type=int, default=8767, help="Port for REST response events.")
    parser.add_argument("--command-port", type=int, default=8768, help="Port for PLACE_ORDER command broker.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Optional directory for JSONL event files.",
    )
    parser.add_argument(
        "--quiet",
        action="store_true",
        help="Suppress all monitor logs in terminal (still writes to files when --output-dir is used).",
    )
    parser.add_argument(
        "--no-monitor",
        action="store_true",
        help="Disable live monitor parsing for quotes/trades/positions.",
    )
    parser.add_argument(
        "--trade-limit",
        type=int,
        default=20,
        help="How many recent trade updates to keep in monitor state.",
    )
    parser.add_argument(
        "--snapshot-file",
        type=Path,
        default=None,
        help="Optional path for live monitor snapshot JSON.",
    )
    args = parser.parse_args()
    snapshot_file = args.snapshot_file
    if snapshot_file is None and args.output_dir is not None:
        snapshot_file = args.output_dir / "monitor_state.json"

    return ListenerConfig(
        host=args.host,
        ws_port=args.ws_port,
        rest_port=args.rest_port,
        command_port=args.command_port,
        output_dir=args.output_dir,
        quiet=args.quiet,
        monitor=not args.no_monitor,
        trade_limit=max(1, args.trade_limit),
        snapshot_file=snapshot_file,
    )


def main() -> None:
    config = parse_args()
    try:
        asyncio.run(run(config))
    except KeyboardInterrupt:
        print("\nReceiver stopped.", flush=True)


if __name__ == "__main__":
    main()
