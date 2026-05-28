#!/usr/bin/env bash
# Active-RFQ basis-MR bot. No arguments: trades ASSETS[0] from
# arb_bot/config.py; runtime tunables live in live_config.json (hot-reload).
set -euo pipefail
cd "$(dirname "$0")"
exec .venv/bin/python main.py "$@"
