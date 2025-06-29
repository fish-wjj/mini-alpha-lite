#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""生成周度再平衡委托单（ETF + α 组合）并同步更新 state_portfolio.json"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from loguru import logger

from src.config import load_cfg
from src.utils import build_today_universe, latest_trade_date, q, pro
from src.factor_model import score

# ───────────────────── 配置 & 常量 ─────────────────────
CFG = load_cfg()
TD = latest_trade_date()

LOT_STK = 100  # A股一手
CSV_DIR = Path(__file__).resolve().parent.parent / "orders"
CSV_DIR.mkdir(exist_ok=True)
STATE_FP = Path(__file__).resolve().parent.parent / "state_portfolio.json"

TOTAL_CASH = float(CFG["cash"])
ETF_CASH = TOTAL_CASH * (CFG["core_ratio"] + CFG["bond_ratio"])
ALPHA_CASH = TOTAL_CASH - ETF_CASH

# ───────────────────── 行情截面 ────────────────────────
df = build_today_universe(TD)  # 不含 ETF 行情
ranked = score(df).head(CFG["num_alpha"])

orders: list[list] = []  # ["代码6","B/S", 价格(0市价), 数量]

# ───── ETF 指令 ─────
def _add_etf(code_ts: str, ratio: float) -> None:
    code6 = code_ts.split(".")[0]
    px = df.loc[df.ts_code == code_ts, "close"]
    if px.empty:  # ETF 不在 dataframe，用 fund_daily
        try:
            px = q(pro.fund_daily, ts_code=code_ts, trade_date=TD, fields="close")["close"].iat[0]
            logger.debug(f"{code6} 通过 fund_daily 获取价格 {px}")
        except Exception as e:
            logger.error(f"找不到 {code6} 当日行情（{e}），跳过该 ETF")
            return
    else:
        px = px.iat[0]

    qty = int((TOTAL_CASH * ratio) // px // LOT_STK) * LOT_STK
    if qty >= LOT_STK:
        orders.append([code6, "B", 0, qty])  # ETF 市价
        logger.info(f"ETF {code6} → {qty}")
    else:
        logger.warning(f"资金不足，跳过 {code6}")


_add_etf(CFG["core_etf"], CFG["core_ratio"])
_add_etf(CFG["bond_etf"], CFG["bond_ratio"])

# ───── α 股票指令 ─────
cash_each = ALPHA_CASH / CFG["num_alpha"] if CFG["num_alpha"] else 0
for _, r in ranked.iterrows():
    buy_px = round(r["close"] * 1.01, 2)
    qty = int(cash_each // buy_px // LOT_STK) * LOT_STK
    if qty >= LOT_STK:
        orders.append([r["ts_code"].split(".")[0], "B", buy_px, qty])

# ───── 写 CSV ─────
csv = pd.DataFrame(orders, columns=["证券代码", "买卖标志", "委托价格", "委托数量"])
fn = CSV_DIR / f"orders_{TD}.csv"
csv.to_csv(fn, index=False, encoding="utf-8-sig")
logger.success(f"CSV 生成 → {fn}")

# ───── 更新持仓 ─────
state = {"equity": TOTAL_CASH, "max_equity": TOTAL_CASH, "position": {}}
if STATE_FP.exists():
    state.update(json.loads(STATE_FP.read_text()))

for code6, bs, price, qty in orders:
    ts_code = f"{code6}.SH" if code6.startswith("6") else f"{code6}.SZ"
    if bs == "B" and qty > 0:
        pos = state["position"].get(ts_code, {"qty": 0, "cost": 0})
        new_qty = pos["qty"] + qty
        new_cost = (pos["cost"] * pos["qty"] + (price or 0) * qty) / new_qty
        state["position"][ts_code] = {"qty": new_qty, "cost": new_cost}

STATE_FP.write_text(json.dumps(state, indent=2, ensure_ascii=False))
logger.success(f"仓位快照已更新 → {STATE_FP}")
