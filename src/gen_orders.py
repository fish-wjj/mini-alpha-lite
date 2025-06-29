# -*- coding: utf-8 -*-
"""
按周再平衡生成交易指令
--------------------------------------------------
* 读取最新截面数据 → 选股
* 同时处理核心 ETF / 债基 ETF 配置
* 根据 stop_loss / take_profit 更新并保存持仓状态
* 输出 资金指令 CSV（银河证券格式） + 更新 state_portfolio.json
"""
from __future__ import annotations
import datetime as dt
import json
from pathlib import Path
from typing import List, Dict

import pandas as pd
from loguru import logger

from src.config import load_cfg
from src.factor_model import score
from src.utils import (
    build_today_universe,        # 避免循环引用：utils 里封装好的函数
    latest_trade_date,
    LOT_STK, LOT_ETF
)

# ----------------- 读取参数 -----------------
CFG = load_cfg()
# alpha_cash 若不存在，用 cash * alpha_ratio 兜底
ALPHA_CASH = CFG.get("alpha_cash",
                     CFG["cash"] * CFG.get("alpha_ratio", 0.4))

CSV_DIR = Path(__file__).resolve().parent.parent / "orders"
CSV_DIR.mkdir(exist_ok=True)
STATE_FP = Path(__file__).resolve().parent.parent / "state_portfolio.json"

td: str = latest_trade_date()
df: pd.DataFrame = build_today_universe()          # 今日截面
df = score(df).head(CFG["num_alpha"])             # Alpha Top-N

orders: List[list] = []   # [sec_code6, B/S, price, qty]
cash_left: float = CFG["cash"]                    # 剩余现金，用于买 ETF

# ---------------------------------------------------------------------
# ETF / 股票下单辅助
# ---------------------------------------------------------------------
def _sec_to_ts(code6: str, frame: pd.DataFrame) -> str | None:
    rows = frame[frame["ts_code"].str.startswith(code6)]
    return rows["ts_code"].iat[0] if not rows.empty else None


def _add_stock(row: pd.Series):
    global cash_left
    sec6 = row["ts_code"][:6]
    price = round(row["close"] * 1.01, 2)                   # 买价加 1% 滑点
    # 每只 alpha 分到的现金
    alloc = ALPHA_CASH / CFG["num_alpha"]
    qty = int(alloc // price // LOT_STK * LOT_STK)
    if qty < LOT_STK:
        return
    cash_left -= price * qty
    orders.append([sec6, "B", 0, qty])


def _add_etf(code_ts: str, ratio: float, lot: int):
    global cash_left
    code6 = code_ts[:6]
    px_series = df[df["ts_code"] == code_ts]["close"]
    if px_series.empty:
        logger.error(f"找不到 {code6}.{code_ts[-2:]} 当日行情，跳过该 ETF")
        return
    price = px_series.iat[0]
    alloc_cash = CFG["cash"] * ratio
    qty = int((alloc_cash // price) // lot * lot)
    if qty <= 0:
        return
    cash_left -= price * qty
    orders.append([code6, "B", 0, qty])


# ------------------------ 1. α 股票买入 -------------------------------
for _, r in df.iterrows():
    _add_stock(r)

# ------------------------ 2. ETF 配置 ---------------------------------
_add_etf(CFG["core_etf"],  CFG["core_ratio"],  LOT_STK)
_add_etf(CFG["bond_etf"],  CFG["bond_ratio"],  LOT_ETF)

# ------------------------ 3. 止盈 / 止损 ------------------------------
state: Dict = {"equity": CFG["cash"],
               "max_equity": CFG["cash"],
               "position": {}}
if STATE_FP.exists():
    state = json.load(STATE_FP.open())

new_pos = state["position"].copy()

for ts_code, info in list(state["position"].items()):
    px_series = df.loc[df["ts_code"] == ts_code, "close"]
    if px_series.empty:
        continue
    cur_px = px_series.iat[0]
    ret = (cur_px - info["cost"]) / info["cost"]

    # 当天已有卖单就不重复
    if any(o[0] == ts_code[:6] and o[1] == "S" for o in orders):
        continue

    # 止损
    if ret <= -CFG["stop_loss"]:
        orders.append([ts_code[:6], "S", 0, info["qty"]])
        del new_pos[ts_code]
        logger.info(f"{ts_code} 跌破止损，清仓")
    # 止盈：卖掉一半（向下取整到手数）
    elif ret >= CFG["take_profit"]:
        half_qty = (info["qty"] // LOT_STK // 2) * LOT_STK
        if half_qty >= LOT_STK:
            orders.append([ts_code[:6], "S", 0, half_qty])
            new_pos[ts_code]["qty"] -= half_qty
            logger.info(f"{ts_code} 浮盈≥{CFG['take_profit']*100:.1f}%，卖出 {half_qty}")

# ------------------------ 4. 合并买卖冲突 -----------------------------
order_df = (pd.DataFrame(orders, columns=["sec", "flag", "price", "qty"])
              .groupby(["sec", "flag"])
              .agg({"qty": "sum"})
              .reset_index())

net_orders = []
for sec in order_df["sec"].unique():
    b_qty = order_df.query("sec == @sec and flag == 'B'")["qty"].sum()
    s_qty = order_df.query("sec == @sec and flag == 'S'")["qty"].sum()
    net = b_qty - s_qty
    if net > 0:
        net_orders.append([sec, "B", 0, int(net)])
    elif net < 0:
        net_orders.append([sec, "S", 0, int(-net)])

orders = net_orders

# ------------------------ 5. 更新持仓快照 ------------------------------
for sec, flag, _, qty in orders:
    ts_code = _sec_to_ts(sec, df)
    if not ts_code:
        continue
    if flag == "B":
        pos = new_pos.get(ts_code, {"cost": 0, "qty": 0})
        cur_px = df.loc[df["ts_code"] == ts_code, "close"].iat[0]
        total_cost = pos["cost"] * pos["qty"] + cur_px * qty
        total_qty = pos["qty"] + qty
        new_pos[ts_code] = {"cost": total_cost / total_qty, "qty": total_qty}
    else:  # S
        if ts_code in new_pos:
            new_pos[ts_code]["qty"] -= qty

new_pos = {k: v for k, v in new_pos.items() if v["qty"] > 0}
state["position"] = new_pos

with open(STATE_FP, "w", encoding="utf-8") as f:
    json.dump(state, f, indent=2, ensure_ascii=False)

# ------------------------ 6. 输出 CSV ---------------------------------
csv = pd.DataFrame(orders, columns=["证券代码", "买卖标志", "委托价格", "委托数量"])
fn = CSV_DIR / f"orders_{td}.csv"
csv.to_csv(fn, index=False, encoding="utf-8-sig")
logger.success(f"≡ 周度再平衡 CSV 已生成 → {fn}")
logger.success(f"≡ 持仓状态已更新 → {STATE_FP}")