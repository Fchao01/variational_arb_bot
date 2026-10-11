"""Entrypoint for the executable-premium arb bot.

  python main.py            live: trades ASSETS[0]
  python main.py --record   record-only: round-robins ASSETS, writes
                            docs/ticks_<COIN>.csv per coin, places NO orders

Run, then load & Start the Chrome extension on omni.variational.io.

Tunables split in two:
  - live_config.json (hot-reloaded every tick): executable premium thresholds,
    RFQ size, trade cap, slippage, and net exposure reconciliation.
  - arb_bot/config.py (structural, restart required): ASSETS and RFQ pacing.
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
