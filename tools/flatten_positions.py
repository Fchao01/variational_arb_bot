#!/usr/bin/env python3
"""应急双边平仓:把某个标的在 Variational 与 Lighter 上的仓位都打平。

用途:套利进程异常中断后(尤其单腿/裸腿残留)快速恢复中性。

依赖:
  1. Variational 腿走本地 command broker,需先起:
       .venv/bin/python -m exchanges.variational.listener --command-port 8768
     且 Chrome 扩展已 Start 并 attach 到已登录的 Variational 页面。
     (broker 没起也没关系:Variational 这段会优雅跳过,只处理 Lighter。)
  2. Lighter 腿直连 Lighter,需 .env 里 LIGHTER_ACCOUNT_INDEX /
     LIGHTER_API_KEY_INDEX / LIGHTER_PRIVATE_KEY(或 API_KEY_PRIVATE_KEY)
     与 Lighter 上注册的 api key 匹配。

默认先只读核对两边持仓;加 --flatten 才会真实下 reduce-only 平仓单。
Variational 用市价 reduce-only;Lighter 用激进可成交限价 reduce-only。

示例:
  .venv/bin/python tools/flatten_positions.py --market ETH            # 只核对
  .venv/bin/python tools/flatten_positions.py --market ETH --flatten --yes

(rework 移植版:Lighter 腿改用 exchanges.lighter 的 Executor+BookFeed,
 不再依赖旧的 var_lighter.lighter_gateway.LighterGateway。)
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import sys
import uuid
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import requests
import websockets
from dotenv import load_dotenv

# 允许从仓库任意位置运行(脚本在 tools/ 下,需把仓库根放上 sys.path)。
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from arb_bot.config import LIGHTER_BASE_URL
from arb_bot.helpers import required_int_env
from exchanges.lighter.book_feed import BookFeed
from exchanges.lighter.executor import Executor

LIGHTER_BASE = LIGHTER_BASE_URL
# 平仓时给 Lighter 限价单的让价(bps):激进点确保可成交,reduce-only 不会反向开仓。
FLATTEN_SLIPPAGE_BPS = 30.0


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--market", default="ETH", help="标的(Variational underlying / Lighter symbol)。")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--port", type=int, default=8768, help="command broker 端口。")
    p.add_argument("--account", default=None, help="Variational vr-connected-address(0x...)。")
    p.add_argument("--slippage", type=float, default=0.01, help="Variational 市价 maxSlippage。")
    p.add_argument("--flatten", action="store_true", help="真实下平仓单(默认只核对)。")
    p.add_argument("--yes", action="store_true", help="跳过平仓确认。")
    p.add_argument("--poll", type=float, default=2.5, help="平仓后持仓轮询间隔秒。")
    p.add_argument("--timeout", type=float, default=40.0, help="单边平仓归零超时秒。")
    return p.parse_args()


# ---------------- Lighter ----------------

def lighter_position(market: str) -> tuple[Decimal, int, dict | None]:
    """返回 (abs_qty, sign, raw)。sign: -1 空 / 1 多 / 0 平。"""
    acc = required_int_env("LIGHTER_ACCOUNT_INDEX")
    r = requests.get(
        f"{LIGHTER_BASE}/api/v1/account",
        params={"by": "index", "value": acc},
        headers={"accept": "application/json"},
        timeout=10,
    )
    r.raise_for_status()
    for a in r.json().get("accounts") or []:
        for pos in a.get("positions", []) or []:
            if str(pos.get("symbol", "")).upper() == market.upper():
                qty = Decimal(str(pos.get("position", "0")))
                if qty == 0:
                    return Decimal("0"), 0, pos
                return qty, int(pos.get("sign", 0)), pos
    return Decimal("0"), 0, None


async def flatten_lighter(market: str, qty: Decimal, sign: int, poll: float, timeout: float) -> bool:
    log = logging.getLogger("flatten.lighter")
    close_side = "BUY" if sign == -1 else "SELL"  # 空头买回 / 多头卖出
    live = SimpleNamespace(lighter_slippage_bps=FLATTEN_SLIPPAGE_BPS)
    feed = BookFeed(LIGHTER_BASE, [market.upper()], log, lambda: False)
    executor = Executor(LIGHTER_BASE, log, live)
    executor.load_market_configs([market.upper()])
    await feed.start()
    loop = asyncio.get_running_loop()
    try:
        # 等盘口就绪
        best_bid = best_ask = None
        ready_deadline = loop.time() + 15.0
        while loop.time() < ready_deadline:
            best_bid, best_ask, _, _ = await feed.get_book(market.upper())
            if best_bid is not None and best_ask is not None:
                break
            await asyncio.sleep(0.3)
        if best_bid is None or best_ask is None:
            print("  ! Lighter 订单簿未就绪,放弃")
            return False
        try:
            coid, _tx = await executor.submit(
                market.upper(), close_side, qty, best_bid, best_ask, reduce_only=True
            )
        except Exception as exc:
            print(f"  ! Lighter 平仓下单失败: {exc}")
            return False
        print(f"  Lighter reduce-only {close_side} {qty} 已发 (coid={coid})")
    finally:
        await feed.close()
        await executor.close()

    deadline = loop.time() + timeout
    while loop.time() < deadline:
        await asyncio.sleep(poll)
        cur, _, _ = lighter_position(market)
        print(f"  Lighter {market} 持仓: {cur}")
        if cur == 0:
            print("  Lighter 已平 ✓")
            return True
    print("  ! Lighter 超时仍未平")
    return False


# ---------------- Variational ----------------

async def _broker(ws, msg: dict[str, Any], suffix: str, timeout: float = 25.0) -> dict[str, Any]:
    rid = str(uuid.uuid4())
    await ws.send(json.dumps({**msg, "requestId": rid}))
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        raw = await asyncio.wait_for(ws.recv(), timeout=deadline - loop.time())
        try:
            d = json.loads(raw)
        except Exception:
            continue
        if isinstance(d, dict) and d.get("requestId") == rid and str(d.get("type", "")).endswith(suffix):
            return d


def _var_qty(positions: Any, market: str) -> float:
    if not isinstance(positions, list):
        return 0.0
    total = 0.0
    for it in positions:
        info = (it or {}).get("position_info") if isinstance(it, dict) else None
        if not isinstance(info, dict):
            continue
        inst = info.get("instrument")
        if isinstance(inst, dict) and str(inst.get("underlying", "")).upper() == market.upper():
            try:
                total += float(info.get("qty"))
            except (TypeError, ValueError):
                pass
    return total


async def variational_flow(args: argparse.Namespace) -> None:
    uri = f"ws://{args.host}:{args.port}"
    try:
        ws_cm = websockets.connect(uri)
        ws = await asyncio.wait_for(ws_cm.__aenter__(), timeout=8.0)
    except Exception as exc:
        print(f"[Variational] 无法连接 broker {uri}: {exc}")
        print("  (是否已起 `.venv/bin/python -m exchanges.variational.listener --command-port %s` 且扩展已连?)" % args.port)
        return
    try:
        p = await _broker(ws, {"type": "POSITIONS", "account": args.account}, "POSITIONS_RESULT")
        q = _var_qty((p.get("result") or {}).get("positions"), args.market)
        print(f"[Variational] {args.market} 持仓 = {q:g}")
        if abs(q) < 1e-9:
            print("  Variational 已平 ✓")
            return
        if not args.flatten:
            print("  (--flatten 未加,跳过平仓)")
            return
        if not args.yes:
            ans = input(f"  确认 reduce-only 市价平掉 Variational {q:g} {args.market}? [y/N] ").strip().lower()
            if ans != "y":
                print("  跳过 Variational 平仓")
                return
        side = "SELL" if q > 0 else "BUY"
        r = await _broker(
            ws,
            {
                "type": "PLACE_ORDER", "orderType": "market", "side": side,
                "amount": str(abs(q)), "market": args.market, "maxSlippage": args.slippage,
                "reduceOnly": True, "fundingIntervalS": 3600,
                "settlementAsset": "USDC", "account": args.account,
            },
            "ORDER_RESULT", timeout=30.0,
        )
        print(f"  Variational reduce-only {side} 已发 ok={r.get('ok')} {r.get('error') or ''}")
        loop = asyncio.get_running_loop()
        deadline = loop.time() + args.timeout
        while loop.time() < deadline:
            await asyncio.sleep(args.poll)
            p = await _broker(ws, {"type": "POSITIONS", "account": args.account}, "POSITIONS_RESULT")
            q = _var_qty((p.get("result") or {}).get("positions"), args.market)
            print(f"  Variational {args.market} 持仓: {q:g}")
            if abs(q) < 1e-9:
                print("  Variational 已平 ✓")
                return
        print("  ! Variational 超时仍未平")
    finally:
        await ws.close()


async def main() -> None:
    load_dotenv()
    args = parse_args()
    logging.basicConfig(level=logging.WARNING)

    print(f"=== Lighter {args.market} ===")
    try:
        qty, sign, _raw = lighter_position(args.market)
    except Exception as exc:
        print(f"  Lighter 查询失败: {exc}")
        qty, sign = Decimal("0"), 0
    if qty == 0:
        print("  Lighter 已平 ✓")
    else:
        word = "空头" if sign == -1 else "多头"
        print(f"  Lighter {args.market} {word} {qty}")
        if args.flatten:
            if args.yes or input(f"  确认 reduce-only 平掉 Lighter {word} {qty}? [y/N] ").strip().lower() == "y":
                await flatten_lighter(args.market, qty, sign, args.poll, args.timeout)
            else:
                print("  跳过 Lighter 平仓")
        else:
            print("  (--flatten 未加,跳过平仓)")

    print(f"=== Variational {args.market} ===")
    await variational_flow(args)
    print("=== 完成 ===")


if __name__ == "__main__":
    asyncio.run(main())
