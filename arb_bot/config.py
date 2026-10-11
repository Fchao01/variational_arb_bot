import os
from pathlib import Path

VARIATIONAL_TICKER_OVERRIDES = {
    "LIT": "LIGHTER",
}
VARIATIONAL_ASSET_TO_LIGHTER_TICKER = {v: k for k, v in VARIATIONAL_TICKER_OVERRIDES.items()}

FORWARDER_HOST = "127.0.0.1"
FORWARDER_WS_PORT = 8766
FORWARDER_REST_PORT = 8767
FORWARDER_COMMAND_PORT = 8768

LOG_DIR = Path("./log")
DOCS_DIR = Path("./docs")
APP_LOG_FILE = LOG_DIR / "runtime.log"
PNL_CSV_FILE = DOCS_DIR / "pnl.csv"
ROUNDS_DIAG_CSV_FILE = DOCS_DIR / "rounds_diag.csv"

READY_TIMEOUT_SECONDS = 60.0
POLL_INTERVAL_SECONDS = 0.05

# Lighter Robinhood Chain deployment (RH), not Lighter Core mainnet.
# RH has a separate account/API-key namespace and uses a different L2 signing
# domain. Do not mix credentials or market metadata with the Core deployment.
LIGHTER_BASE_URL = "https://api.rh.lighter.xyz"
LIGHTER_WS_URL = "wss://api.rh.lighter.xyz/stream"
LIGHTER_CHAIN_ID = 466324
LIGHTER_WS_PING_INTERVAL_SECONDS = 30
LIGHTER_WS_PING_TIMEOUT_SECONDS = 30

# Variational underlying symbols; Lighter ticker derived via the overrides.
# Live mode trades ASSETS[0]; record mode round-robins all.
ASSETS = ["SOL"]

# Basis MR signal: trade s = (var_mid - lighter_mid)/lighter_mid * 1e4 against
# its strictly-causal, time-aware EWMA mean/variance.
BASIS_HL_SECONDS = 300.0
BASIS_WARMUP_SAMPLES = int(os.getenv("BASIS_WARMUP_SAMPLES", "500"))  # ~95% EWMA convergence at 1s/tick, HL=300s; env override for tests
BASIS_GAP_RESET_S = 120.0        # dt above this => session break, reset EWMA
BASIS_SIGMA_FLOOR_BPS = float(os.getenv("BASIS_SIGMA_FLOOR_BPS", "0.3"))  # suppress entries while raw sqrt(var) <= this; env override for tests

# Structural (not hot-reloadable): RFQ pacing sets the signal's time scale.
# 500ms (2 RPS) is CF-safe; 100ms tripped Cloudflare in early live runs.
RFQ_MIN_INTERVAL_MS = 500.0
FILL_WAIT_SECONDS = 5.0          # bounded wait for real fill prices before fallback

# Z_ENTRY/Z_EXIT/Z_STOP, MAX_HOLD_SECONDS, RFQ_NOTIONAL_USDC, MAX_TRADES_PER_RUN,
# MAX_SLIPPAGE_BPS, LIGHTER_SLIPPAGE_BPS are hot-reloaded from live_config.json.
