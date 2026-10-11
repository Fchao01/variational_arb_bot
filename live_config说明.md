# live_config.json 参数说明

`live_config.json` 必须保持为合法 JSON。JSON 不支持 `//` 或 `#` 尾部注释，直接把注释写进文件会导致程序读取失败；本文件是对应的说明文档。

## 当前版本实际生效的参数

| 参数 | 当前含义 | 与 entropy-arb 的对应关系 |
|---|---|---|
| `midline_bps` | Variational 相对 Lighter 的长期溢价中枢。默认 `0`。需要根据采集数据手动设置。 | `thresholds.midline_bps`，已对应 |
| `upper_bps` | 正向价差门槛。卖 Variational、买 Lighter 时，`Var bid / Lighter ask - 1` 必须达到 `midline_bps + upper_bps`。 | `thresholds.upper_bps`，已对应 |
| `lower_bps` | 反向价差带宽。买 Variational、卖 Lighter 时，`Lighter bid / Var ask - 1` 必须达到 `lower_bps - midline_bps`。 | `thresholds.lower_bps`，已对应 |
| `premium_persist_seconds` | 价差至少持续多久才触发，单位秒。 | `execution.premium_persist_sec`，名称不同但含义对应 |
| `cooldown_seconds` | 两次信号之间的最短间隔，单位秒。当前同一段价差还必须先回落才会重新触发。 | `execution.cooldown_sec`，基本对应 |
| `rfq_notional_usdc` | 每次向 Variational 请求的 RFQ 名义金额。当前数量由该金额和 Lighter 中间价计算。 | 不是 entropy-arb 的完整 sizing 参数 |
| `max_trades_per_run` | 本次进程最多执行多少个套利切片；达到后停止主循环。设为 `null` 表示不限制。 | entropy-arb 没有完全等价项 |
| `max_slippage_bps` | Variational 下单滑点保护，当前也用于 Variational 对冲。 | 对应 `leg_slippage_bps`，但当前没有独立的 hedge 滑点 |
| `lighter_slippage_bps` | Lighter 可成交限价单的滑点保护，当前也用于 Lighter reduce-only 对冲。 | 接近 `leg_slippage_bps` / `hedge_slippage_bps`，但没有拆分 |
| `net_tolerance_base` | 两边签名仓位相加后的净敞口容忍值，超过才对冲。单位是 SOL 等基础币数量。 | `execution.net_tolerance_base`，已对应 |
| `reconcile_seconds` | 定期读取两边仓位并尝试净敞口对冲的周期，单位秒。 | `execution.reconcile_sec`，名称不同但含义对应 |

## 尚未完全移植的 entropy-arb 参数

当前版本**没有**把下面参数当作可配置且完整生效的 entropy-arb 功能：

- `primary.taker_fee_bps`、`hedge.taker_fee_bps`：当前开仓价差没有从门槛中扣除实际手续费。`upper_bps=4` / `lower_bps=4` 表示毛价差门槛，不能直接当成扣费后的净收益。
- `sizing.take_fraction`：当前不是按盘口深度比例吃单，而是按 `rfq_notional_usdc` 请求固定名义的 Variational RFQ。
- `sizing.max_order_notional_usd`、`min_order_notional_usd`：当前没有独立的 entropy-arb 单笔名义上限和最小名义过滤；Lighter 自身的 `min_base` / `min_quote` 仍会拦截不合规订单。
- `inventory.scale_bps`、`inventory.floor_frac`、`max_position_usd`：当前没有库存阶梯和每个交易所的持仓上限。
- `execution.hedge_slippage_bps`：当前对冲复用 `max_slippage_bps` 或 `lighter_slippage_bps`，没有单独值。
- `execution.settle_timeout_sec`、`max_consecutive_errors`、`rate_limit_pause_sec`、`staleness_sec`、`venue_probe_sec`：当前使用项目自身的成交等待、限频退避和 stale watchdog 参数，不能视为同名参数的一一移植。

因此，当前代码是“entropy-arb 的可执行价差触发 + 净敞口 reduce-only 对冲”的移植版，不是 entropy-arb 全部配置和全部执行风控的逐项复制。

## 当前配置的实际含义

```json
{
  "midline_bps": 0.0,
  "upper_bps": 4.0,
  "lower_bps": 4.0,
  "premium_persist_seconds": 0.0,
  "cooldown_seconds": 1.0,
  "rfq_notional_usdc": 12.0,
  "max_trades_per_run": 5,
  "max_slippage_bps": 10.0,
  "lighter_slippage_bps": 30.0,
  "net_tolerance_base": 0.001,
  "reconcile_seconds": 15.0
}
```

这表示：中枢为零、两方向约 4 bps 毛价差才触发；每次 RFQ 约 12 USDC；最多执行 5 个切片；每 15 秒核对一次两边仓位，净敞口超过 0.001 个基础币才尝试对冲。

