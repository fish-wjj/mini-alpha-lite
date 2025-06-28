# -*- coding: utf-8 -*-
"""
周频再平衡：
• 每周一 19:05 运行（cron_rebalance.sh）
• 金叉 → core_ratio 70 % ；死叉 → core_ratio 30 %
• α 因子：动量 25 %、换手 15 %，其余同上一版
• 个股浮盈 ≥10 % → 卖一半
"""
import json, datetime as dt, pathlib, pandas as pd
from src.config import load_cfg; cfg = load_cfg()
from src.utils import get_today_universe, pro, is_risk_off, ma_cross
from src.factor_model import score
from src.logger import logger

BASE = pathlib.Path(__file__).resolve().parent.parent
STATE_FP = BASE / "state_portfolio.json"
CSV_DIR = BASE / "orders"; CSV_DIR.mkdir(exist_ok=True)

td = dt.date.today().strftime("%Y%m%d")
df = get_today_universe()
risk = is_risk_off(td)
cross = ma_cross(td)

core_r = 0.70 if cross == "golden" else 0.30 if cross == "death" else cfg["core_ratio"]
bond_r = 1 - core_r - cfg["alpha_ratio"]

# 过滤股票
stock = df[(~df["ts_code"].str.startswith(("15","51","56"))) & (df["amount"] >= cfg["min_amount"])]
if stock.empty: stock = df[~df["ts_code"].str.startswith(("15","51","56"))]
etfs = df[df["ts_code"].str.startswith(("15","51","56"))]
df_filt = pd.concat([stock, etfs])

alpha_list = []
if not risk:
    alpha_list = score(df_filt).head(cfg["num_alpha"])["ts_code"].tolist()
else:
    logger.warning("风险 OFF，仅持 ETF")

orders = []
LOT_STK = cfg["lot"]; LOT_BOND = 10

# --- 指数 ETF
core_px = pro.fund_daily(ts_code=cfg["core_etf"], limit=1)["close"].iat[0]
core_qty = int(cfg["cash"]*core_r // (core_px*LOT_STK)) * LOT_STK
orders.append([cfg["core_etf"].split(".")[0], "B", 0, core_qty])

# --- 债券 ETF
bond_px = pro.fund_daily(ts_code=cfg["bond_etf"], limit=1)["close"].iat[0]
bond_qty = int(cfg["cash"]*bond_r // (bond_px*LOT_BOND)) * LOT_BOND
if bond_qty: orders.append([cfg["bond_etf"].split(".")[0], "B", 0, bond_qty])

# --- α 仓买入
alpha_cash_each = cfg["cash"]*cfg["alpha_ratio"] / max(len(alpha_list),1)
for code in alpha_list:
    price = df.loc[df["ts_code"]==code,"close"].iat[0]*1.01
    qty = int(alpha_cash_each // (price*LOT_STK))*LOT_STK
    if qty>=LOT_STK:
        orders.append([code.split(".")[0], "B", round(price,2), qty])

# --- 止损&止盈
state = json.load(STATE_FP.open()) if STATE_FP.exists() else {"position":{}}
for code, info in state["position"].items():
    cur_px = df.loc[df["ts_code"]==code, "close"]
    if cur_px.empty: continue
    ret = (cur_px.iat[0]-info["cost"])/info["cost"]
    if ret <= -cfg["stop_loss"]:
        orders.append([code.split(".")[0], "S", 0, info["qty"]])
        logger.info(f"{code} 跌破止损，清仓")
    elif ret >= 0.10:
        sell_qty = info["qty"]//2
        if sell_qty >= LOT_STK:
            orders.append([code.split(".")[0], "S", 0, sell_qty])
            logger.info(f"{code} 浮盈 ≥10%，卖出一半 {sell_qty}")

# --- 保存新仓位快照（略） ---

csv = pd.DataFrame(orders, columns=["证券代码","买卖标志","委托价格","委托数量"])
fn = CSV_DIR / f"orders_{td}.csv"
csv.to_csv(fn, index=False, encoding="utf-8-sig")
logger.success(f"≡ 周度再平衡 CSV 已生成 → {fn}")