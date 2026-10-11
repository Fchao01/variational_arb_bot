# variational_arb_bot

### 概述
基于 Chrome 插件转发的 Variational <-> Lighter 跨所 **basis 均值回归**机器人。

行情侧没有 API：登录态浏览器停在 Variational 交易页时持续拉 indicative RFQ，CDP 插件把每条报价
转发给本进程；本进程再按配置的 USDC notional **主动**拉一条新鲜 RFQ（复用其 quote_id）。

信号是 basis 偏离对自身 EWMA 的 z-score：

- `s = (var_mid − lighter_mid) / lighter_mid × 1e4`（bps）
- 对 `s` 维护严格因果、时间感知的在线 EWMA 均值 μ 和方差，`z = (s − μ) / σ`
- `|z| ≥ z_entry` 且已 warm 时**同时**打两条腿：
  - `z > 0`（Var 偏贵）→ `short_var`：Variational 卖出（复用 quote_id）+ Lighter 买入
  - `z < 0`（Var 偏便宜）→ `long_var`：Variational 买入（复用 quote_id）+ Lighter 卖出
- 平仓（任一触发）：z 回归到 `z_exit`（reverted）/ z 反向冲破 `z_stop`（adverse_stop）/
  持仓超 `max_hold_seconds`

复用浏览器 quote_id ⇒ 决策价即成交价，省掉一整个重新询价来回。
币种由 `arb_bot/config.py` 的 `ASSETS` 决定：**实盘只交易 `ASSETS[0]`**（启动锁定），录制模式轮询全部。

### 两套参数
- **`live_config.json`（热加载：每个 tick 重读，改完存盘下一 tick 即生效，不用重启/重新预热）**：
  `z_entry` / `z_exit` / `z_stop`（null = 无止损）/ `max_hold_seconds` / `rfq_notional_usdc` /
  `max_trades_per_run` / `max_slippage_bps`（Variational）/ `lighter_slippage_bps`（Lighter 兜底滑点）
- **`arb_bot/config.py`（结构性，改了要重启）**：`ASSETS`，EWMA 的 `BASIS_HL_SECONDS` /
  `BASIS_WARMUP_SAMPLES` / `BASIS_GAP_RESET_S` / `BASIS_SIGMA_FLOOR_BPS`，RFQ 节奏 `RFQ_MIN_INTERVAL_MS`。
  RFQ 节奏是结构性的：它决定信号的时间尺度（z 分布），所以不放进热加载。

### 项目结构
- `main.py`：唯一入口。`python main.py` 实盘交易 `ASSETS[0]`；
  `python main.py --record` 只录制（轮询 `ASSETS`，**绝不下单**，每币写 `docs/ticks_<COIN>.csv`）
- `arb_bot/app.py`：进程接线 + 两种模式 + 主决策循环
- `arb_bot/basis.py`：basis z-score 信号 + 开/平仓决策（无网络 IO）
- `arb_bot/quotes.py`：主动拉 RFQ + 新鲜度 + 跨所边际（信号侧）
- `arb_bot/trader.py`：一轮的双腿开仓/持有/平仓 + 真实成交结算（执行侧）
- `arb_bot/recorder.py`：脱离热路径的结算与 PnL/诊断落盘
- `arb_bot/live_config.py`：`live_config.json` 热加载
- `arb_bot/`：feed（Variational 适配）、tick_logger、helpers
- `exchanges/variational/`：本地接收与监控解析（command broker / monitor / listener）
- `exchanges/lighter/`：Lighter 盘口 feed + 下单执行器
- `chrome_extension/`：CDP 转发插件（`background.js` 编排 / `var_api.js` 页面 API /
  `forward_socket.js` 连接 / `recorder.js` 可选录制）

### 环境准备
#### macOS / Linux
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

#### Windows（PowerShell）
```powershell
py -3 -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

复制 `.env.example` 为 `.env` 并填入 Lighter RH（Robinhood Chain）信息：
```bash
cp .env.example .env
# 然后编辑 .env 填入：
# LIGHTER_ACCOUNT_INDEX=...
# LIGHTER_API_KEY_INDEX=...
# LIGHTER_PRIVATE_KEY=...
```

本项目的 Lighter 腿固定连接 RH 链：

- REST：`https://api.rh.lighter.xyz`
- WebSocket：`wss://api.rh.lighter.xyz/stream`
- L2 signing chain id：`466324`
- 结算资产：RH 侧使用 USDG

RH 与 Lighter Core 主网是独立部署，不能复用主网的账户、API key、市场 ID 或资金。

程序只加载仓库根目录的 `.env`，不会搜索父目录的配置。启动会显示该文件
路径，以及实际使用的 RH REST/WS 地址和签名 chain id（不会打印私钥）。
`LIGHTER_PRIVATE_KEY` 是 Lighter 的 API 私钥，不是钱包私钥；它必须与 RH 上的
账户索引、API key 索引配套。不要同时留下值不同的旧 `API_KEY_PRIVATE_KEY`，
程序会拒绝这种配置。进程已有的环境变量优先于 `.env`，修改凭据后需重启进程；
若仍然读到旧凭据，检查启动脚本或服务管理器中是否设置了同名环境变量。

如需在 Lighter WebSocket 新旧逻辑切换期间临时强制使用旧的应用层 ping/pong，可设置：
```bash
LIGHTER_WS_SERVER_PINGS=true
```
不设置时默认使用新的兼容模式。

### 加载 Chrome 插件
1. 打开 `chrome://extensions`
2. 右上角开启 `Developer mode`
3. 左上角点 `Load unpacked`，选择本仓库的 `chrome_extension/` 目录

Python 进程启动后，打开 Variational 交易页面，在插件列表点击
"Variational CDP Forwarder" -> 点击 `Start`（已 Start 则刷新网页）。

### 运行

直接跑（或 `./run.sh`，参数透传）：
```bash
python main.py            # 实盘，交易 ASSETS[0]
python main.py --record   # 只录制，轮询 ASSETS，绝不下单
```
然后加载并 Start Chrome 插件（保持在 omni.variational.io 登录态页面即可）。

调参分两层：运行参数全在 `live_config.json`（改完存盘下一 tick 即生效），结构性参数
（币种 `ASSETS`、EWMA、RFQ 节奏）在 `arb_bot/config.py`（改了要重启）。详见上方「两套参数」。

### 输出
- `./log/runtime.log`：唯一运行日志（同时回显到终端）
- `./docs/pnl.csv`：每轮真实成交价结算的 PnL（money ledger）
- `./docs/rounds_diag.csv`：每轮完整诊断（信号状态 / 各腿锚点价 / 延迟 / 漂移）
- `./docs/ticks_<COIN>.csv`：每币逐 tick 信号序列（实盘与录制走同一 tick 路径）

终端不落盘原始 REST/WS 消息。

### 远端部署提示

行情侧**没有 API**：必须有一个**登录态 Chrome + `chrome_extension` CDP 转发**，且和 Python
进程同机（插件 CSP 写死只能连 `127.0.0.1`）。在 headless 服务器上跑通常需要：

- 一个轻量 GUI（如 openbox + tigervnc）+ Google Chrome
- 通过 SSH 隧道把 VNC 端口转发到本机，登录钱包并启动扩展
- Python 进程放进 `tmux`/`screen`，脱离 TTY 仍能持续运行
- 进程退出码：`0` = 正常停 / `75` = 行情卡死且 3 次 reload 仍无救（守护进程可自动重启）/
  其他非 0 = 需人工（如出现 `NAKED ... halting` 单腿残留，需到交易所网页手动平裸腿）

具体步骤因系统/区域/网络环境而异，仓库不提供固定的部署脚本。
