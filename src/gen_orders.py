# -*- coding: utf-8 -*-
"""
生成今日（或最近一个交易日）的再平衡下单 CSV，同时维护 state_portfolio.json
"""
from __future__ import annotations

import json
from pathlib import Path
from datetime import datetime as dt
from loguru import logger
import pandas as pd

from src.utils import (
    build_today_universe,
    latest_trade_date,
    safe_query,  # ⬅ 用于临时拉取行情
    pro,         # ⬅ tushare.pro client
)

# -----------------------------------------------------------------------------
# ★ 策略 / 资金参数 —— 如需改动，只动这里 --------------------------------------
CFG = {
    # 账户现金（万  = 1e4 CNY）
    "cash": 100 * 1e4,
    # 组合权重
    "alpha_ratio": 0.3,
    "core_ratio": 0.6,
    "bond_ratio": 0.1,
    # α 算法参数
    "num_alpha": 10,      # 每次买入 α 股数量
    "stop_loss": 0.12,    # 亏损超过 12% 清仓
    "take_profit": 0.25,  # 盈利超过 25% 卖一半
    # ETF 代码（不用写交易所后缀）
    "core_etf":  "510300",
    "bond_etf":  "511010",
}
# -----------------------------------------------------------------------------

# 常量
CSV_DIR   = Path(__file__).resolve().parent.parent / "orders"
STATE_FP  = Path(__file__).resolve().parent.parent / "state_portfolio.json"
LOT_STK   = 100   # 股票 100 股/手
LOT_FUND  = 10    # ETF   10 份/手

CSV_DIR.mkdir(exist_ok=True)
TD = latest_trade_date()           # 例如 20250627
DF = build_today_universe(TD)      # 今日截面，已做完整因子 & 基础字段拼接


# -----------------------------------------------------------------------------#
# 辅助：安全取某只标的当日 close 价格
def _px_of(code: str, trade_date: str) -> float | None:
    """
    先从 `DF` 里找；找不到再去 tushare 拉一次（股票: daily / ETF: fund_daily）
    返回 None 表示依旧取不到
    """
    # 1) DF 里（含 .SH / .SZ 后缀）
    row = DF.loc[DF["ts_code"].str.startswith(code), "close"]
    if not row.empty and pd.notna(row.iat[0]) and row.iat[0] > 0:
        return float(row.iat[0])

    # 2) tushare API
    is_fund = code.startswith(("5", "1"))  # 5/1 开头视作 ETF
    api_fn  = pro.fund_daily if is_fund else pro.daily
    df_api  = safe_query(api_fn, ts_code=f"{code}.SH", trade_date=trade_date,
                         fields="close")
    if df_api.empty:
        df_api = safe_query(api_fn, ts_code=f"{code}.SZ", trade_date=trade_date,
                            fields="close")
    if not df_api.empty and pd.notna(df_api["close"].iat[0]):
        return float(df_api["close"].iat[0])

    return None
# -----------------------------------------------------------------------------#


orders: list[list] = []  # [代码, B/S, 价格, 数量]

# === 1. α 股池（已按因子打分排序 ↓）
alpha_df = DF.sort_values("score", ascending=False).head(CFG["num_alpha"])

def _add_stock(row):
    price = float(row["close"])
    if price <= 0:
        logger.error(f"{row.ts_code} 当日价格非法，跳过该股")
        return

    cash_each = CFG["cash"] * CFG["alpha_ratio"] / CFG["num_alpha"]
    qty = int(cash_each // price // LOT_STK) * LOT_STK
    if qty < LOT_STK:
        logger.warning(f"{row.ts_code} 价格 {price:.2f} 太高，买入不足一手，跳过")
        return

    orders.append([row.ts_code.split(".")[0], "B", round(price * 1.01, 2), qty])

alpha_df.apply(_add_stock, axis=1)


# === 2. ETF（核心 & 债券）
def _add_etf(code: str, ratio: float):
    px = _px_of(code, TD)
    if px is None:
        logger.error(f"找不到 {code} 当日行情，跳过该 ETF")
        return

    cash = CFG["cash"] * ratio
    qty  = int(cash // px // LOT_FUND) * LOT_FUND
    if qty < LOT_FUND:
        logger.warning(f"{code} 价格 {px:.2f} 太高，买不足一手，跳过")
        return

    orders.append([code, "B", 0, qty])  # ETF 市价买入

_add_etf(CFG["core_etf"],  CFG["core_ratio"])
_add_etf(CFG["bond_etf"],  CFG["bond_ratio"])


# === 3. 写 CSV  ===============================================================
csv_path = CSV_DIR / f"orders_{TD}.csv"
pd.DataFrame(
    orders, columns=["证券代码", "买卖标志", "委托价格", "委托数量"]
).to_csv(csv_path, index=False, encoding="utf-8-sig")
logger.success(f"CSV 生成 → {csv_path}")

# === 4. 更新仓位快照 ==========================================================
state = {"equity": CFG["cash"], "max_equity": CFG["cash"], "position": {}}
if STATE_FP.exists():
    state = json.loads(STATE_FP.read_text())

for sec_code, bs, price, qty in orders:
    ts_code = (DF.loc[DF["ts_code"].str.startswith(sec_code), "ts_code"]
               .iat[0] if not DF.empty else f"{sec_code}.SH")
    if bs == "B":
        info = state["position"].get(ts_code, {"cost": 0, "qty": 0})
        total_cost = info["cost"] * info["qty"] + (price or _px_of(sec_code, TD)) * qty
        total_qty  = info["qty"] + qty
        state["position"][ts_code] = {
            "cost": total_cost / total_qty,
            "qty":  total_qty
        }
    else:  # S 卖出
        if ts_code in state["position"]:
            state["position"][ts_code]["qty"] -= qty
            if state["position"][ts_code]["qty"] <= 0:
                state["position"].pop(ts_code)

STATE_FP.write_text(json.dumps(state, ensure_ascii=False, indent=2))
logger.success(f"仓位快照已更新 → {STATE_FP}")
