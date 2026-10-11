"""Entrypoint for the basis-MR bot.

  python main.py            live: trades ASSETS[0]
  python main.py --record   record-only: round-robins ASSETS, writes
                            docs/ticks_<COIN>.csv per coin, places NO orders

Run, then load & Start the Chrome extension on omni.variational.io.

Tunables split in two:
  - live_config.json (hot-reloaded every tick): Z_ENTRY/Z_EXIT/Z_STOP,
    MAX_HOLD_SECONDS, RFQ_NOTIONAL_USDC, MAX_TRADES_PER_RUN,
    MAX_SLIPPAGE_BPS, LIGHTER_SLIPPAGE_BPS. Edit and save while the bot
    runs — new values take effect on the next tick, no restart/warmup.
  - arb_bot/config.py (structural, restart required): ASSETS, EWMA
    HL/warmup/floor, RFQ pacing.
"""

from pathlib import Path

from dotenv import load_dotenv

if __name__ == "__main__":
    env_path = Path(__file__).resolve().parent / ".env"
    load_dotenv(env_path)
    print(
        f"Environment file: {env_path} "
        f"({'found' if env_path.is_file() else 'missing; using process environment'})",
        flush=True,
    )
    from arb_bot.app import main

    main()
