#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
生成周度再平衡委托单（含 ETF+α 组合）并同步更新 state_portfolio.json
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
from loguru import logger

# ─────────────────── 项目内部工具 ────────────────────
try:
    # 新版 utils 中暴露 build_today_universe
    from src.utils import build_today_universe
except ImportError:
    # 老版 utils 用 _build_universe 命名
    from src.utils import _build_universe as build_today_universe

from src.utils import latest_trade_date, q, pro
from src.factor_model import score
from src.config import load_cfg

# ─────────────────── 常量 & 配置 ────────────────────
CFG = load_cfg()                         # 读取 config.yaml
TD: str = latest_trade_date()            # 最新交易日
LOT_STK = 100                            # A 股一手 = 100 股
CSV_DIR = Path(__file__).resolve().parent.parent / "orders"
CSV_DIR.mkdir(exist_ok=True)
STATE_FP = Path(__file__).resolve().parent.parent / "state_portfolio.json"

TOTAL_CASH = float(CFG["cash"])          # 当期总可用资金
ETF_CASH   = TOTAL_CASH * (CFG["core_ratio"] + CFG["bond_ratio"])
ALPHA_CASH = TOTAL_CASH - ETF_CASH       # 分给 α 股的现金

# ─────────────────── 获取今日截面 ────────────────────
df = build_today_universe()              # 股票截面（不含 ETF）
logger.success(f"行情截面 {TD} → {len(df):,} 条")

# ─────────────────── 拿到因子 TopN 选股 ───────────────
ranked = score(df).head(CFG["num_alpha"])

# ─────────────────── 生成买卖指令列表 ────────────────
orders: list[list] = []                  # ["代码","B/S",价格,数量]
logger.debug("开始构造买单 …")

def _add_etf(code_ts: str, ratio: float, lot: int = LOT_STK) -> None:
    """根据资金比例买 ETF；若不在股票截面里就去 fund_daily 取价"""
    code6 = code_ts.split(".")[0]                # 510300
    px_series = df.loc[df["ts_code"] == code_ts, "close"]

    if px_series.empty:
        # —— ETF 不在 df，单独调用 fund_daily ——
        try:
            etf_df = q(
                pro.fund_daily,
                ts_code=code_ts,
                trade_date=TD,
                fields="close"
            )
            price = etf_df["close"].iat[0]
            logger.debug(f"{code6} 行情通过 fund_daily 获取：{price}")
        except Exception as e:
            logger.error(f"仍然拿不到 {code6} 行情 ({e})，跳过该 ETF")
            return
    else:
        price = px_series.iat[0]

    cash = TOTAL_CASH * ratio
    qty  = int(cash // price // lot) * lot       # 向下取整到整手
    if qty >= lot:
        orders.append([code6, "B", 0, qty])      # ETF 市价单写 0
        logger.info(f"ETF {code6} 计划买入 {qty} 股")
    else:
        logger.warning(f"资金不足，放弃买入 {code6}")

def _add_stock(row: pd.Series) -> None:
    """把 α 股票写入订单（价格上浮 1% 抗滑点）"""
    price = round(row["close"] * 1.01, 2)
    cash_each = ALPHA_CASH / CFG["num_alpha"]
    qty  = int(cash_each // price // LOT_STK) * LOT_STK
    if qty >= LOT_STK:
        orders.append([row["ts_code"].split(".")[0], "B", price, qty])

# ─────────── ETF 指令 ───────────
_add_etf(CFG["core_etf"],  CFG["core_ratio"])
_add_etf(CFG["bond_etf"],  CFG["bond_ratio"])

# ─────────── α 股票指令 ───────────
ranked.apply(_add_stock, axis=1)

# ─────────────────── 写 CSV ───────────────────────────
csv = pd.DataFrame(orders, columns=["证券代码", "买卖标志", "委托价格", "委托数量"])
fn = CSV_DIR / f"orders_{TD}.csv"
csv.to_csv(fn, index=False, encoding="utf-8-sig")
logger.success(f"≡ 周度再平衡 CSV 已生成 → {fn}")

# ─────────────────── 更新持仓快照 ────────────────────
if STATE_FP.exists():
    state = json.loads(STATE_FP.read_text())
else:
    state = {"equity": TOTAL_CASH, "max_equity": TOTAL_CASH, "position": {}}

for code6, bs, price, qty in orders:
    ts_code = f"{code6}.SH" if code6.startswith("6") else f"{code6}.SZ"
    if bs == "B" and qty > 0:
        pos = state["position"].get(ts_code, {"qty": 0, "cost": 0})
        new_qty   = pos["qty"] + qty
        new_cost  = (pos["cost"] * pos["qty"] + (price or 0) * qty) / new_qty  # ETF 市价单 price 为 0
        state["position"][ts_code] = {"qty": new_qty, "cost": new_cost}

STATE_FP.write_text(json.dumps(state, indent=2, ensure_ascii=False))
logger.success(f"≡ 持仓状态已更新 → {STATE_FP}")
